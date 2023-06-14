// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::marker::PhantomData;
use std::ops::Bound;

use futures::StreamExt;
use futures_async_stream::{for_await, try_stream};
use itertools::Itertools;
use risingwave_common::array::stream_record::Record;
use risingwave_common::array::StreamChunk;
use risingwave_common::estimate_size::EstimateSize;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::memcmp_encoding::{self, MemcmpEncoded};
use risingwave_common::util::sort_util::OrderType;
use risingwave_expr::function::window::{FrameBounds, WindowFuncCall};
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::StateStore;

use super::diff_btree_map::{Change, DiffBTreeMap};
use super::state::{create_window_state, StateKey};
use crate::cache::{new_unbounded, ManagedLruCache};
use crate::executor::aggregation::ChunkBuilder;
use crate::executor::over_window::diff_btree_map::PositionType;
use crate::executor::over_window::window_states::WindowStates;
use crate::executor::test_utils::prelude::StateTable;
use crate::executor::{
    expect_first_barrier, ActorContextRef, Executor, ExecutorInfo, Message, StreamExecutorError,
    StreamExecutorResult,
};
use crate::task::AtomicU64Ref;

struct Partition {
    /// Fully synced table cache for the partition. `StateKey (order key, input pk)` -> table row.
    cache: BTreeMap<StateKey, OwnedRow>,
}

impl EstimateSize for Partition {
    fn estimated_heap_size(&self) -> usize {
        // TODO()
        0
    }
}

/// Changes happened in one partition in the chunk. `StateKey (order key, input pk)` => `Change`.
type Diff = BTreeMap<StateKey, Change<OwnedRow>>;

/// `partition key` => `Partition`.
type PartitionCache = ManagedLruCache<OwnedRow, Partition>;

/// - State table schema = output schema, state table pk = `partition key | order key | input pk`.
/// - Output schema = input schema + window function results.
struct OverWindowExecutor<S: StateStore> {
    input: Box<dyn Executor>,
    inner: ExecutorInner<S>,
}

struct ExecutorInner<S: StateStore> {
    actor_ctx: ActorContextRef,
    info: ExecutorInfo,

    calls: Vec<WindowFuncCall>,
    partition_key_indices: Vec<usize>,
    order_key_indices: Vec<usize>,
    order_key_order_types: Vec<OrderType>,
    input_pk_indices: Vec<usize>,
    input_pk_order_types: Vec<OrderType>,
    input_schema_len: usize,

    state_table: StateTable<S>,
    watermark_epoch: AtomicU64Ref,

    /// The maximum size of the chunk produced by executor at a time.
    chunk_size: usize,
}

struct ExecutionVars<S: StateStore> {
    partitions: PartitionCache,
    _phantom: PhantomData<S>,
}

impl<S: StateStore> Executor for OverWindowExecutor<S> {
    fn execute(self: Box<Self>) -> crate::executor::BoxedMessageStream {
        self.executor_inner().boxed()
    }

    fn schema(&self) -> &risingwave_common::catalog::Schema {
        &self.inner.info.schema
    }

    fn pk_indices(&self) -> crate::executor::PkIndicesRef<'_> {
        &self.inner.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.inner.info.identity
    }
}

impl<S: StateStore> ExecutorInner<S> {
    fn get_partition_key(&self, full_row: impl Row) -> OwnedRow {
        full_row
            .project(&self.partition_key_indices)
            .into_owned_row()
    }

    fn get_order_key(&self, full_row: impl Row) -> OwnedRow {
        full_row.project(&self.order_key_indices).into_owned_row()
    }

    fn get_input_pk(&self, full_row: impl Row) -> OwnedRow {
        full_row.project(&self.input_pk_indices).into_owned_row()
    }

    /// `full_row` can be an input row or state table row.
    fn encode_order_key(&self, full_row: impl Row) -> StreamExecutorResult<MemcmpEncoded> {
        Ok(memcmp_encoding::encode_row(
            full_row.project(&self.order_key_indices),
            &self.order_key_order_types,
        )?
        .into())
    }

    /// `full_row` can be an input row or state table row.
    fn encode_input_pk(&self, full_row: impl Row) -> StreamExecutorResult<MemcmpEncoded> {
        Ok(memcmp_encoding::encode_row(
            full_row.project(&self.input_pk_indices),
            &self.input_pk_order_types,
        )?
        .into())
    }

    fn row_to_state_key(&self, full_row: impl Row + Copy) -> StreamExecutorResult<StateKey> {
        Ok(StateKey {
            order_key: self.encode_order_key(full_row)?,
            pk: self.get_input_pk(full_row).into(),
        })
    }
}

impl<S: StateStore> OverWindowExecutor<S> {
    pub fn new() -> Self {
        todo!()
    }

    async fn ensure_key_in_cache(
        this: &mut ExecutorInner<S>,
        cache: &mut PartitionCache,
        partition_key: &OwnedRow,
    ) -> StreamExecutorResult<()> {
        if cache.contains(partition_key) {
            return Ok(());
        }

        let mut cache_for_partition = BTreeMap::new();
        let table_iter = this
            .state_table
            .iter_with_pk_prefix(partition_key, PrefetchOptions::new_for_exhaust_iter())
            .await?;

        #[for_await]
        for row in table_iter {
            let row: OwnedRow = row?;
            cache_for_partition.insert(this.row_to_state_key(&row)?, row);
        }

        cache.put(
            partition_key.clone(),
            Partition {
                cache: cache_for_partition,
            },
        );
        Ok(())
    }

    #[try_stream(ok = StreamChunk, error = StreamExecutorError)]
    async fn apply_chunk<'a>(
        this: &'a mut ExecutorInner<S>,
        vars: &'a mut ExecutionVars<S>,
        chunk: StreamChunk,
    ) {
        // `partition key` => `Diff`.
        let mut diffs: HashMap<_, Diff> = HashMap::new();
        // `input pk` of update records of which the `partition key` or `order key` is changed.
        let mut key_change_updated_pks = HashSet::new();

        // Collect changes in the chunk.
        // TODO(): assume all input pks are unique for now.
        for record in chunk.records() {
            match record {
                Record::Insert { new_row } => {
                    let part_key = this.get_partition_key(new_row);
                    let part_diff = diffs.entry(part_key).or_insert(Diff::new());
                    part_diff.insert(
                        this.row_to_state_key(new_row)?,
                        Change::Insert(new_row.into_owned_row()),
                    );
                }
                Record::Delete { old_row } => {
                    let part_key = this.get_partition_key(old_row);
                    let part_diff = diffs.entry(part_key).or_insert(Diff::new());
                    part_diff.insert(this.row_to_state_key(old_row)?, Change::Delete);
                }
                Record::Update { old_row, new_row } => {
                    let old_part_key = this.get_partition_key(old_row);
                    let new_part_key = this.get_partition_key(new_row);
                    let old_sort_key_enc = this.row_to_state_key(old_row)?;
                    let new_sort_key_enc = this.row_to_state_key(new_row)?;
                    if old_part_key == new_part_key && old_sort_key_enc == new_sort_key_enc {
                        // not a key-change update
                        let part_diff = diffs.entry(old_part_key).or_insert(Diff::new());
                        part_diff
                            .insert(old_sort_key_enc, Change::Update(new_row.into_owned_row()));
                    } else if old_part_key == new_part_key {
                        // order-change update
                        key_change_updated_pks.insert(this.get_input_pk(old_row));
                        let part_diff = diffs.entry(old_part_key).or_insert(Diff::new());
                        // split into delete + insert, will be merged after building changes
                        part_diff.insert(old_sort_key_enc, Change::Delete);
                        part_diff
                            .insert(new_sort_key_enc, Change::Insert(new_row.into_owned_row()));
                    } else {
                        // partition-change update
                        key_change_updated_pks.insert(this.get_input_pk(old_row));
                        // split into delete + insert, will be merged after building changes
                        let old_part_diff = diffs.entry(old_part_key).or_insert(Diff::new());
                        old_part_diff.insert(old_sort_key_enc, Change::Delete);
                        let new_part_diff = diffs.entry(new_part_key).or_insert(Diff::new());
                        new_part_diff
                            .insert(new_sort_key_enc, Change::Insert(new_row.into_owned_row()));
                    }
                }
            }
        }

        // `input pk` => `Record`
        let mut final_changes = BTreeMap::new();

        // Build final changes partition by partition.
        for (part_key, diff) in diffs {
            Self::ensure_key_in_cache(this, &mut vars.partitions, &part_key).await?;
            let mut partition = vars.partitions.get_mut(&part_key).unwrap();
            let partition_with_diff = DiffBTreeMap::new(&partition.cache, diff);
            let mut part_final_changes = BTreeMap::new();

            // TODO(): append change to chunk builder and yield chunk if possible.
            let yield_change = |key: StateKey, record: Record<OwnedRow>| {
                // Buffer the change inside current partition for later mutation of partition cache.
                part_final_changes.insert(key.clone(), record.clone());

                // Buffer the change at chunk level (may cross partition) for later mutation of
                // state table and yielding output chunk.
                if !key_change_updated_pks.contains(&key.pk) {
                    // not a key-change update, just keep the change as it is
                    final_changes.insert(key.pk, record);
                } else if let Some(existed) = final_changes.remove(&key.pk) {
                    match (existed, record) {
                        (Record::Insert { new_row }, Record::Delete { old_row })
                        | (Record::Delete { old_row }, Record::Insert { new_row }) => {
                            // merge delete and insert
                            final_changes.insert(key.pk, Record::Update { old_row, new_row });
                        }
                        _ => panic!("other cases should not exist"),
                    }
                } else {
                    final_changes.insert(key.pk, record);
                }
            };

            Self::build_changes_for_partition(this, partition_with_diff, yield_change)?;

            // Update partition cache.
            for (key, record) in part_final_changes {
                match record {
                    Record::Insert { new_row } | Record::Update { new_row, .. } => {
                        // if `Update`, the update is not a key-change update, so it's save
                        partition.cache.insert(key, new_row);
                    }
                    Record::Delete { .. } => {
                        partition.cache.remove(&key);
                    }
                }
            }
        }

        // Materialize changes to state table and yield to downstream.
        let mut chunk_builder = ChunkBuilder::new(this.chunk_size, &this.info.schema.data_types());
        for record in final_changes.into_values() {
            this.state_table.write_record(record.as_ref());
            if let Some(chunk) = chunk_builder.append_record(record) {
                yield chunk;
            }
        }
        if let Some(chunk) = chunk_builder.take() {
            yield chunk;
        }
    }

    fn build_changes_for_partition(
        this: &ExecutorInner<S>,
        part_with_diff: DiffBTreeMap<'_, StateKey, OwnedRow>,
        mut yield_change: impl FnMut(StateKey, Record<OwnedRow>),
    ) -> StreamExecutorResult<()> {
        let snapshot = part_with_diff.snapshot();
        let diff = part_with_diff.diff();
        assert!(!diff.is_empty(), "if there's no diff, we won't be here");

        // Generate delete changes first, because they're hard to handle during window sliding in
        // the next step.
        for (key, change) in diff {
            if change.is_delete() {
                yield_change(
                    key.clone(),
                    Record::Delete {
                        old_row: snapshot.get(key).unwrap().clone(),
                    },
                );
            }
        }

        for (first_frame_start, first_curr_key, last_curr_key, last_frame_end) in
            Self::find_affected_ranges(this, &part_with_diff)
        {
            assert!(first_frame_start <= first_curr_key);
            assert!(first_curr_key <= last_curr_key);
            assert!(last_curr_key <= last_frame_end);

            let mut states =
                WindowStates::new(this.calls.iter().map(create_window_state).try_collect()?);

            // Populate window states with the affected range.
            {
                let mut cursor = part_with_diff
                    .find(&first_frame_start)
                    .expect("first frame start key must exist");
                while {
                    let (key, row) = cursor
                        .key_value()
                        .expect("cursor must be valid until `last_frame_end`");

                    for (call, state) in this.calls.iter().zip_eq_fast(states.iter_mut()) {
                        // TODO(): batch appending
                        state.append(
                            key.clone(),
                            row.project(call.args.val_indices())
                                .into_owned_row()
                                .as_inner()
                                .into(),
                        );
                    }
                    cursor.move_next();

                    key != &last_frame_end
                } {}
            }

            // Slide to the first affected key. We can safely compare to `Some(first_curr_key)` here
            // because it must exist in the states, by the definition of affected range.
            while states.curr_key() != Some(&first_curr_key) {
                states.just_slide_forward();
            }
            let mut curr_key_cursor = part_with_diff.find(&first_curr_key).unwrap();
            assert_eq!(states.curr_key(), curr_key_cursor.key());

            // Slide and yield changes.
            while {
                let (key, row) = curr_key_cursor
                    .key_value()
                    .expect("cursor must be valid until `last_curr_key`");
                let output = states.curr_output()?;
                let new_row = OwnedRow::new(
                    row.as_inner()
                        .iter()
                        .take(this.input_schema_len)
                        .cloned()
                        .chain(output)
                        .collect(),
                );

                match curr_key_cursor.position() {
                    PositionType::Ghost => unreachable!(),
                    PositionType::Snapshot | PositionType::DiffUpdate => {
                        // update
                        let old_row = snapshot.get(key).unwrap().clone();
                        if old_row != new_row {
                            yield_change(key.clone(), Record::Update { old_row, new_row });
                        }
                    }
                    PositionType::DiffInsert => {
                        // insert
                        yield_change(key.clone(), Record::Insert { new_row });
                    }
                }

                states.just_slide_forward();
                curr_key_cursor.move_next();

                key != &last_curr_key
            } {}
        }

        Ok(())
    }

    /// Find all affected ranges in the given partition.
    ///
    /// # Returns
    ///
    /// `Vec<(first_frame_start, first_curr_key, last_curr_key, last_frame_end_incl)>`
    ///
    /// Each affected range is a union of many small window frames affected by some adajcent
    /// keys in the diff.
    ///
    /// Example:
    /// - frame 1: `rows between 2 preceding and current row`
    /// - frame 2: `rows between 1 preceding and 2 following`
    /// - partition: `[1, 2, 4, 5, 7, 8, 9, 10, 11, 12, 14]`
    /// - diff: `[3, 4, 15]`
    /// - affected ranges: `[(1, 1, 7, 9), (10, 12, 15, 15)]`
    ///
    /// TODO(rc):
    /// Note that, since we assume input chunks have data locality on order key columns, we now only
    /// calculate one single affected range. So the affected ranges in the above example will be
    /// `(1, 1, 15, 15)`. Later we may optimize this.
    fn find_affected_ranges(
        this: &ExecutorInner<S>,
        part_with_diff: &DiffBTreeMap<'_, StateKey, OwnedRow>,
    ) -> Vec<(StateKey, StateKey, StateKey, StateKey)> {
        let snapshot = part_with_diff.snapshot();
        let diff = part_with_diff.diff();

        if part_with_diff.first_key().is_none() {
            // all keys are deleted in the diff
            return vec![];
        }

        if part_with_diff.snapshot().is_empty() {
            // all existing keys are inserted in the diff
            return vec![(
                diff.first_key_value().unwrap().0.clone(),
                diff.first_key_value().unwrap().0.clone(),
                diff.last_key_value().unwrap().0.clone(),
                diff.last_key_value().unwrap().0.clone(),
            )];
        }

        let (first_frame_start, first_curr_key) = {
            let first_key = part_with_diff.first_key().unwrap();
            if this
                .calls
                .iter()
                .any(|call| call.frame.bounds.end_is_unbounded())
            {
                // If the frame end is unbounded, the frame corresponding to the first key is always
                // affected.
                (first_key.clone(), first_key.clone())
            } else {
                let (a, b) = this
                    .calls
                    .iter()
                    .map(|call| match &call.frame.bounds {
                        FrameBounds::Rows(start, end) => {
                            let mut ss_cursor = snapshot
                                .lower_bound(Bound::Included(diff.first_key_value().unwrap().0));
                            let n_following_rows = end.to_offset().unwrap().max(0) as usize;
                            for _ in 0..n_following_rows {
                                if ss_cursor.key().is_some() {
                                    ss_cursor.move_prev();
                                }
                            }
                            let first_curr_key = ss_cursor.key().unwrap_or(first_key);
                            let first_frame_start = if let Some(offset) = start.to_offset() {
                                let n_preceding_rows = offset.min(0).unsigned_abs();
                                for _ in 0..n_preceding_rows {
                                    if ss_cursor.key().is_some() {
                                        ss_cursor.move_prev();
                                    }
                                }
                                ss_cursor.key().unwrap_or(first_key)
                            } else {
                                // The frame start is unbounded, so the first affected frame starts
                                // from the beginning.
                                first_key
                            };
                            (first_frame_start, first_curr_key)
                        }
                    })
                    .reduce(|(x1, y1), (x2, y2)| (x1.min(x2), y1.min(y2)))
                    .expect("# of window function calls > 0");
                (a.clone(), b.clone())
            }
        };

        let (last_curr_key, last_frame_end) = {
            let last_key = part_with_diff.last_key().unwrap();
            if this
                .calls
                .iter()
                .any(|call| call.frame.bounds.start_is_unbounded())
            {
                // If the frame start is unbounded, the frame corresponding to the last key is
                // always affected.
                (last_key.clone(), last_key.clone())
            } else {
                let (a, b) = this
                    .calls
                    .iter()
                    .map(|call| match &call.frame.bounds {
                        FrameBounds::Rows(start, end) => {
                            let mut ss_cursor = snapshot
                                .upper_bound(Bound::Included(diff.last_key_value().unwrap().0));
                            let n_preceding_rows = start.to_offset().unwrap().min(0).unsigned_abs();
                            for _ in 0..n_preceding_rows {
                                if ss_cursor.key().is_some() {
                                    ss_cursor.move_next();
                                }
                            }
                            let last_curr_key = ss_cursor.key().unwrap_or(last_key);
                            let last_frame_end = if let Some(offset) = end.to_offset() {
                                let n_following_rows = offset.max(0) as usize;
                                for _ in 0..n_following_rows {
                                    if ss_cursor.key().is_some() {
                                        ss_cursor.move_next();
                                    }
                                }
                                ss_cursor.key().unwrap_or(last_key)
                            } else {
                                // The frame end is unbounded, so the last affected frame ends at
                                // the end.
                                last_key
                            };
                            (last_curr_key, last_frame_end)
                        }
                    })
                    .reduce(|(x1, y1), (x2, y2)| (x1.max(x2), y1.max(y2)))
                    .expect("# of window function calls > 0");
                (a.clone(), b.clone())
            }
        };

        vec![(
            first_frame_start,
            first_curr_key,
            last_curr_key,
            last_frame_end,
        )]
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn executor_inner(self) {
        let OverWindowExecutor {
            input,
            inner: mut this,
        } = self;

        let mut vars = ExecutionVars {
            partitions: new_unbounded(this.watermark_epoch.clone()),
            _phantom: PhantomData::<S>,
        };

        let mut input = input.execute();
        let barrier = expect_first_barrier(&mut input).await?;
        this.state_table.init_epoch(barrier.epoch);
        vars.partitions.update_epoch(barrier.epoch.curr);

        yield Message::Barrier(barrier);

        #[for_await]
        for msg in input {
            let msg = msg?;
            match msg {
                Message::Watermark(_) => {
                    // TODO()
                }
                Message::Chunk(chunk) => {
                    #[for_await]
                    for chunk in Self::apply_chunk(&mut this, &mut vars, chunk) {
                        yield Message::Chunk(chunk?);
                    }
                }
                Message::Barrier(barrier) => {
                    this.state_table.commit(barrier.epoch).await?;
                    vars.partitions.evict();

                    if let Some(vnode_bitmap) = barrier.as_update_vnode_bitmap(this.actor_ctx.id) {
                        let (_, cache_may_stale) =
                            this.state_table.update_vnode_bitmap(vnode_bitmap);
                        if cache_may_stale {
                            vars.partitions.clear();
                        }
                    }

                    vars.partitions.update_epoch(barrier.epoch.curr);

                    yield Message::Barrier(barrier);
                }
            }
        }
    }
}
