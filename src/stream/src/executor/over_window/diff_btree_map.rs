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

use std::collections::{btree_map, BTreeMap};
use std::ops::Bound;

use enum_as_inner::EnumAsInner;

/// [`DiffBTreeMap`] wraps a [`BTreeMap`] reference as a snapshot and an owned diff [`BTreeMap`],
/// providing iterator and cursor that can iterate over the updated version of the snapshot.
pub(super) struct DiffBTreeMap<'part, K: Ord, V> {
    snapshot: &'part BTreeMap<K, V>,
    diff: BTreeMap<K, Change<V>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumAsInner)]
pub(super) enum Change<V> {
    Update(V),
    Insert(V),
    Delete,
}

impl<'part, K: Ord, V> DiffBTreeMap<'part, K, V> {
    pub fn new(snapshot: &'part BTreeMap<K, V>, diff: BTreeMap<K, Change<V>>) -> Self {
        Self { snapshot, diff }
    }

    pub fn snapshot(&self) -> &'part BTreeMap<K, V> {
        self.snapshot
    }

    pub fn diff(&self) -> &BTreeMap<K, Change<V>> {
        &self.diff
    }

    pub fn iter(&self) -> IterWithDiff<'_, K, V> {
        IterWithDiff {
            snapshot_iter: self.snapshot.iter(),
            diff_iter: self.diff.iter(),
        }
    }

    pub fn find(&self, key: &K) -> Option<CursorWithDiff<'_, K, V>> {
        let snapshot_cursor = self.snapshot.lower_bound(Bound::Included(key));
        let diff_cursor = self.diff.lower_bound(Bound::Included(key));
        if snapshot_cursor.key() != Some(key) && diff_cursor.key() != Some(key) {
            return None;
        }
        Some(CursorWithDiff {
            snapshot_cursor,
            diff_cursor,
        })
    }
}

pub(super) struct IterWithDiff<'a, K: Ord, V> {
    snapshot_iter: btree_map::Iter<'a, K, V>,
    diff_iter: btree_map::Iter<'a, K, Change<V>>,
}

impl<'a, K: Ord, V> Iterator for IterWithDiff<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub(super) struct CursorWithDiff<'a, K: Ord, V> {
    snapshot_cursor: btree_map::Cursor<'a, K, V>,
    diff_cursor: btree_map::Cursor<'a, K, Change<V>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumAsInner)]
pub(super) enum PositionType {
    Nowhere,
    Snapshot,
    DiffUpdate,
    DiffInsert,
}

impl<'a, K: Ord, V> CursorWithDiff<'a, K, V> {
    pub fn position(&self) -> PositionType {
        todo!()
    }

    pub fn key(&self) -> Option<&'a K> {
        todo!()
    }

    pub fn value(&self) -> Option<&'a V> {
        todo!()
    }

    pub fn key_value(&self) -> Option<(&'a K, &'a V)> {
        todo!()
    }

    pub fn peek_next(&self) -> Option<(&'a K, &'a V)> {
        todo!()
    }

    pub fn peek_prev(&self) -> Option<(&'a K, &'a V)> {
        todo!()
    }

    pub fn move_next(&mut self) {
        todo!()
    }

    pub fn move_prev(&mut self) {
        todo!()
    }
}
