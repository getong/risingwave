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

use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use fail::fail_point;
use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_hummock_sdk::compact::estimate_state_for_compaction;
use risingwave_hummock_sdk::{HummockCompactionTaskId, HummockContextId};
use risingwave_pb::hummock::subscribe_compact_tasks_response::Task;
use risingwave_pb::hummock::{
    CancelCompactTask, CompactTask, CompactTaskAssignment, CompactTaskProgress, CompactorWorkload,
    SubscribeCompactTasksResponse,
};
use tokio::sync::mpsc::{Receiver, Sender};

use super::compaction_schedule_policy::{CompactionSchedulePolicy, RoundRobinPolicy, ScoredPolicy};
use crate::hummock::error::Result;
use crate::manager::MetaSrvEnv;
use crate::model::MetadataModel;
use crate::storage::MetaStore;
use crate::MetaResult;

pub type CompactorManagerRef = Arc<CompactorManager>;

pub const TASK_RUN_TOO_LONG: &str = "running too long";
pub const TASK_NOT_FOUND: &str = "task not found";
pub const TASK_NORMAL: &str = "task is normal, please wait some time";

/// Wraps the stream between meta node and compactor node.
/// Compactor node will re-establish the stream when the previous one fails.
#[derive(Debug)]
pub struct Compactor {
    context_id: HummockContextId,
    sender: Sender<MetaResult<SubscribeCompactTasksResponse>>,
    max_concurrent_task_number: AtomicU64,
    // state
    pub cpu_ratio: AtomicU32,
    pub total_cpu_core: u32,
}

struct TaskHeartbeat {
    task: CompactTask,
    num_ssts_sealed: u32,
    num_ssts_uploaded: u32,
    num_progress_key: u64,
    create_time: Instant,
    expire_at: u64,
}

impl Compactor {
    pub fn new(
        context_id: HummockContextId,
        sender: Sender<MetaResult<SubscribeCompactTasksResponse>>,
        max_concurrent_task_number: u64,
        cpu_core_num: u32,
    ) -> Self {
        Self {
            context_id,
            sender,
            max_concurrent_task_number: AtomicU64::new(max_concurrent_task_number),
            cpu_ratio: AtomicU32::new(0),
            total_cpu_core: cpu_core_num,
        }
    }

    pub async fn send_task(&self, task: Task) -> MetaResult<()> {
        fail_point!("compaction_send_task_fail", |_| Err(anyhow::anyhow!(
            "compaction_send_task_fail"
        )
        .into()));
        self.sender
            .send(Ok(SubscribeCompactTasksResponse { task: Some(task) }))
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        Ok(())
    }

    pub async fn cancel_task(&self, task_id: u64) -> MetaResult<()> {
        self.sender
            .send(Ok(SubscribeCompactTasksResponse {
                task: Some(Task::CancelCompactTask(CancelCompactTask {
                    context_id: self.context_id,
                    task_id,
                })),
            }))
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        Ok(())
    }

    pub fn context_id(&self) -> HummockContextId {
        self.context_id
    }

    pub fn max_concurrent_task_number(&self) -> u64 {
        self.max_concurrent_task_number.load(Ordering::Relaxed)
    }

    pub fn is_busy(&self, limit: u32) -> bool {
        self.cpu_ratio.load(Ordering::Acquire) > limit
    }
}

/// `CompactorManager` maintains compactors which can process compact task.
/// A compact task is tracked in `HummockManager::Compaction` via both `CompactStatus` and
/// `CompactTaskAssignment`.
///
/// A compact task can be in one of these states:
/// - 1. Pending: a compact task is picked but not assigned to a compactor.
///   Pending-->Success/Failed/Canceled.
/// - 2. Success: an assigned task is reported as success via `CompactStatus::report_compact_task`.
///   It's the final state.
/// - 3. Failed: an Failed task is reported as success via `CompactStatus::report_compact_task`.
///   It's the final state.
/// - 4. Cancelled: a task is reported as cancelled via `CompactStatus::report_compact_task`. It's
///   the final state.
/// We omit Assigned state because there's nothing to be done about this state currently.
///
/// Furthermore, the compactor for a compaction task must be picked with `CompactorManager`,
/// or its internal states might not be correctly maintained.
pub struct CompactorManager {
    policy: RwLock<Box<dyn CompactionSchedulePolicy>>,

    pub task_expiry_seconds: u64,
    // A map: { context_id -> { task_id -> heartbeat } }
    task_heartbeats:
        RwLock<HashMap<HummockContextId, HashMap<HummockCompactionTaskId, TaskHeartbeat>>>,
}

impl CompactorManager {
    pub async fn with_meta<S: MetaStore>(
        env: MetaSrvEnv<S>,
        task_expiry_seconds: u64,
    ) -> MetaResult<Self> {
        // Retrieve the existing task assignments from metastore.
        let task_assignment = CompactTaskAssignment::list(env.meta_store()).await?;
        let manager = Self {
            policy: RwLock::new(Box::new(ScoredPolicy::with_task_assignment(
                &task_assignment,
            ))),
            task_expiry_seconds,
            task_heartbeats: Default::default(),
        };
        // Initialize heartbeat for existing tasks.
        task_assignment.into_iter().for_each(|assignment| {
            manager
                .initiate_task_heartbeat(assignment.context_id, assignment.compact_task.unwrap());
        });
        Ok(manager)
    }

    /// Only used for unit test.
    pub fn for_test() -> Self {
        Self {
            policy: RwLock::new(Box::new(RoundRobinPolicy::new())),
            task_expiry_seconds: 1,
            task_heartbeats: Default::default(),
        }
    }

    /// Only used for unit test.
    pub fn with_policy(policy: Box<dyn CompactionSchedulePolicy>) -> Self {
        Self {
            policy: RwLock::new(policy),
            task_expiry_seconds: 1,
            task_heartbeats: Default::default(),
        }
    }

    /// Gets next idle compactor to assign task.
    pub fn next_idle_compactor(
        &self,
        compactor_assigned_task_num: &HashMap<HummockContextId, u64>,
    ) -> Option<Arc<Compactor>> {
        let policy = self.policy.read();
        policy.next_idle_compactor(compactor_assigned_task_num)
    }

    /// Gets next compactor to assign task.
    pub fn next_compactor(&self) -> Option<Arc<Compactor>> {
        let policy = self.policy.read();
        policy.next_compactor()
    }

    /// Retrieve a receiver of tasks for the compactor identified by `context_id`. The sender should
    /// be obtained by calling one of the compactor getters.
    ///
    ///  If `add_compactor` is called with the same `context_id` more than once, the only cause
    /// would be compactor re-subscription, as `context_id` is a monotonically increasing
    /// sequence.
    pub fn add_compactor(
        &self,
        context_id: HummockContextId,
        max_concurrent_task_number: u64,
        cpu_core_num: u32,
    ) -> Receiver<MetaResult<SubscribeCompactTasksResponse>> {
        let mut policy = self.policy.write();
        let rx = policy.add_compactor(context_id, max_concurrent_task_number, cpu_core_num);
        tracing::info!(
            "Added compactor session {} cpu_core_num {}",
            context_id,
            cpu_core_num
        );
        rx
    }

    /// Used when meta exiting to support graceful shutdown.
    pub fn abort_all_compactors(&self) {
        let mut policy = self.policy.write();
        while let Some(compactor) = policy.next_compactor() {
            policy.remove_compactor(compactor.context_id);
        }
    }

    pub fn pause_compactor(&self, context_id: HummockContextId) {
        let mut policy = self.policy.write();
        policy.pause_compactor(context_id);

        tracing::info!("Paused compactor session {}", context_id);
    }

    pub fn remove_compactor(&self, context_id: HummockContextId) {
        let mut policy = self.policy.write();
        policy.remove_compactor(context_id);

        // To remove the heartbeats, they need to be forcefully purged,
        // which is only safe when the context has been completely removed from meta.
        tracing::info!("Removed compactor session {}", context_id);
    }

    pub fn get_compactor(&self, context_id: HummockContextId) -> Option<Arc<Compactor>> {
        self.policy.read().get_compactor(context_id)
    }

    pub fn assign_compact_task(
        &self,
        context_id: HummockContextId,
        compact_task: &CompactTask,
    ) -> Result<()> {
        self.policy
            .write()
            .assign_compact_task(context_id, compact_task)
    }

    // Report the completion of a compaction task to adjust the compaction schedule policy.
    pub fn report_compact_task(&self, context_id: HummockContextId, compact_task: &CompactTask) {
        self.policy
            .write()
            .report_compact_task(context_id, compact_task)
    }

    /// Forcefully purging the heartbeats for a task is only safe when the
    /// context has been completely removed from meta.
    /// Returns true if there were remaining heartbeats for the task.
    pub fn purge_heartbeats_for_context(&self, context_id: HummockContextId) -> bool {
        self.task_heartbeats.write().remove(&context_id).is_some()
    }

    pub fn check_tasks_status(
        &self,
        tasks: &[HummockCompactionTaskId],
        slow_task_duration: Duration,
    ) -> HashMap<HummockCompactionTaskId, (Duration, &'static str)> {
        let guard = self.task_heartbeats.read();
        let task_heartbeats = guard.deref();
        let tasks_ids: HashSet<u64> = HashSet::from_iter(tasks.to_vec());
        let mut ret = HashMap::default();
        for heartbeats in task_heartbeats.values() {
            for TaskHeartbeat {
                task, create_time, ..
            } in heartbeats.values()
            {
                if !tasks_ids.contains(&task.task_id) {
                    continue;
                }
                let pending_time = create_time.elapsed();
                if pending_time > slow_task_duration {
                    ret.insert(task.task_id, (pending_time, TASK_RUN_TOO_LONG));
                } else {
                    ret.insert(task.task_id, (pending_time, TASK_NORMAL));
                }
            }
        }
        for task_id in tasks {
            if !ret.contains_key(task_id) {
                ret.insert(*task_id, (Duration::from_secs(0), TASK_NOT_FOUND));
            }
        }
        ret
    }

    pub fn get_expired_tasks(&self) -> Vec<(HummockContextId, CompactTask)> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Clock may have gone backwards")
            .as_secs();
        let guard = self.task_heartbeats.read();
        let task_heartbeats = guard.deref();
        Self::get_heartbeat_expired_tasks(task_heartbeats, now)
            .into_iter()
            .map(|(_, info)| info)
            .collect_vec()
    }

    fn get_heartbeat_expired_tasks(
        task_heartbeats: &HashMap<
            HummockContextId,
            HashMap<HummockCompactionTaskId, TaskHeartbeat>,
        >,
        now: u64,
    ) -> Vec<(HummockCompactionTaskId, (HummockContextId, CompactTask))> {
        let mut cancellable_tasks = vec![];
        const MAX_TASK_DURATION_SEC: u64 = 2700;

        for (context_id, heartbeats) in task_heartbeats {
            {
                for TaskHeartbeat {
                    expire_at,
                    task,
                    create_time,
                    num_ssts_sealed,
                    num_ssts_uploaded,
                    num_progress_key,
                } in heartbeats.values()
                {
                    let task_duration_too_long =
                        create_time.elapsed().as_secs() > MAX_TASK_DURATION_SEC;
                    if *expire_at < now || task_duration_too_long {
                        // 1. task heartbeat expire
                        // 2. task duration is too long
                        cancellable_tasks.push((task.get_task_id(), (*context_id, task.clone())));

                        if task_duration_too_long {
                            let (need_quota, total_file_count, total_key_count) =
                                estimate_state_for_compaction(task);
                            tracing::info!(
                                "CompactionGroupId {} Task {} duration too long create_time {:?} num_ssts_sealed {} num_ssts_uploaded {} num_progress_key {} need_quota {} total_file_count {} total_key_count {} target_level {} base_level {} target_sub_level_id {} task_type {}",
                                task.compaction_group_id,
                                task.task_id,
                                create_time,
                                num_ssts_sealed,
                                num_ssts_uploaded,
                                num_progress_key,
                                need_quota,
                                total_file_count,
                                total_key_count,
                                task.target_level,
                                task.base_level,
                                task.target_sub_level_id,
                                task.task_type,
                            );
                        }
                    }
                }
            }
        }
        cancellable_tasks
    }

    pub fn initiate_task_heartbeat(&self, context_id: HummockContextId, task: CompactTask) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Clock may have gone backwards")
            .as_secs();
        let mut guard = self.task_heartbeats.write();
        let entry = guard.entry(context_id).or_insert_with(HashMap::new);
        entry.insert(
            task.task_id,
            TaskHeartbeat {
                task,
                num_ssts_sealed: 0,
                num_ssts_uploaded: 0,
                num_progress_key: 0,
                create_time: Instant::now(),
                expire_at: now + self.task_expiry_seconds,
            },
        );
    }

    pub fn remove_task_heartbeat(&self, context_id: HummockContextId, task_id: u64) {
        let mut guard = self.task_heartbeats.write();
        if let Some(heartbeats) = guard.get_mut(&context_id) {
            heartbeats.remove(&task_id);
            if heartbeats.is_empty() {
                guard.remove(&context_id);
            }
        }
    }

    pub fn update_task_heartbeats(
        &self,
        context_id: HummockContextId,
        progress_list: &Vec<CompactTaskProgress>,
    ) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Clock may have gone backwards")
            .as_secs();
        let mut guard = self.task_heartbeats.write();
        if let Some(heartbeats) = guard.get_mut(&context_id) {
            for progress in progress_list {
                if let Some(task_ref) = heartbeats.get_mut(&progress.task_id) {
                    if task_ref.num_ssts_sealed < progress.num_ssts_sealed
                        || task_ref.num_ssts_uploaded < progress.num_ssts_uploaded
                        || task_ref.num_progress_key < progress.num_progress_key
                    {
                        // Refresh the expiry of the task as it is showing progress.
                        task_ref.expire_at = now + self.task_expiry_seconds;
                        task_ref.num_ssts_sealed = progress.num_ssts_sealed;
                        task_ref.num_ssts_uploaded = progress.num_ssts_uploaded;
                        task_ref.num_progress_key = progress.num_progress_key;
                    }
                }
            }
        }
    }

    pub fn compactor_num(&self) -> usize {
        self.policy.read().compactor_num()
    }

    /// Return the maximum number of tasks that can be assigned to all compactors.
    pub fn max_concurrent_task_number(&self) -> usize {
        self.policy.read().max_concurrent_task_num()
    }

    /// Update compactor state based on its workload.
    pub fn update_compactor_state(
        &self,
        context_id: HummockContextId,
        workload: CompactorWorkload,
    ) {
        if let Some(compactor) = self.policy.read().get_compactor(context_id) {
            compactor.cpu_ratio.store(workload.cpu, Ordering::Release);
        }
    }

    pub fn total_cpu_core_num(&self) -> u32 {
        self.policy.read().total_cpu_core_num()
    }

    pub fn total_running_cpu_core_num(&self) -> u32 {
        self.policy.read().total_running_cpu_core_num()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
    use risingwave_pb::hummock::CompactTaskProgress;

    use crate::hummock::compaction::default_level_selector;
    use crate::hummock::test_utils::{add_ssts, setup_compute_env};
    use crate::hummock::CompactorManager;

    #[tokio::test]
    async fn test_compactor_manager() {
        // Initialize metastore with task assignment.
        let (env, context_id) = {
            let (env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
            let context_id = worker_node.id;
            let compactor_manager = hummock_manager.compactor_manager_ref_for_test();
            let _sst_infos = add_ssts(1, hummock_manager.as_ref(), context_id).await;
            let _receiver = compactor_manager.add_compactor(context_id, 1, 1);
            let _compactor = hummock_manager.get_idle_compactor().await.unwrap();
            let task = hummock_manager
                .get_compact_task(
                    StaticCompactionGroupId::StateDefault.into(),
                    &mut default_level_selector(),
                )
                .await
                .unwrap()
                .unwrap();
            hummock_manager
                .assign_compaction_task(&task, context_id)
                .await
                .unwrap();
            (env, context_id)
        };

        // Restart. Set task_expiry_seconds to 0 only to speed up test.
        let compactor_manager = CompactorManager::with_meta(env, 0).await.unwrap();
        // Because task assignment exists.
        assert_eq!(compactor_manager.task_heartbeats.read().len(), 1);
        // Because compactor gRPC is not established yet.
        assert_eq!(compactor_manager.compactor_num(), 0);
        assert!(compactor_manager.get_compactor(context_id).is_none());

        // Ensure task is expired.
        tokio::time::sleep(Duration::from_secs(2)).await;
        let expired = compactor_manager.get_expired_tasks();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].0, context_id);

        // Mimic no-op compaction heartbeat
        compactor_manager.update_task_heartbeats(
            context_id,
            &vec![CompactTaskProgress {
                task_id: expired[0].1.task_id,
                num_ssts_sealed: 0,
                num_ssts_uploaded: 0,
                num_progress_key: 0,
            }],
        );
        assert_eq!(compactor_manager.get_expired_tasks().len(), 1);

        // Mimic compaction heartbeat with invalid task id
        compactor_manager.update_task_heartbeats(
            context_id,
            &vec![CompactTaskProgress {
                task_id: expired[0].1.task_id + 1,
                num_ssts_sealed: 1,
                num_ssts_uploaded: 1,
                num_progress_key: 100,
            }],
        );
        assert_eq!(compactor_manager.get_expired_tasks().len(), 1);

        // Mimic effective compaction heartbeat
        compactor_manager.update_task_heartbeats(
            context_id,
            &vec![CompactTaskProgress {
                task_id: expired[0].1.task_id,
                num_ssts_sealed: 1,
                num_ssts_uploaded: 1,
                num_progress_key: 100,
            }],
        );
        assert_eq!(compactor_manager.get_expired_tasks().len(), 0);
        assert!(compactor_manager.purge_heartbeats_for_context(context_id));

        // Test add
        assert_eq!(compactor_manager.compactor_num(), 0);
        assert!(compactor_manager.get_compactor(context_id).is_none());
        compactor_manager.add_compactor(context_id, 1, 1);
        assert_eq!(compactor_manager.compactor_num(), 1);
        assert_eq!(
            compactor_manager
                .get_compactor(context_id)
                .unwrap()
                .context_id(),
            context_id
        );
        // Test pause
        compactor_manager.pause_compactor(context_id);
        assert_eq!(compactor_manager.compactor_num(), 0);
        assert!(compactor_manager.get_compactor(context_id).is_none());
        for _ in 0..3 {
            compactor_manager.add_compactor(context_id, 1, 1);
            assert_eq!(compactor_manager.compactor_num(), 1);
            assert_eq!(
                compactor_manager
                    .get_compactor(context_id)
                    .unwrap()
                    .context_id(),
                context_id
            );
        }
        // Test remove
        compactor_manager.remove_compactor(context_id);
        assert_eq!(compactor_manager.compactor_num(), 0);
        assert!(compactor_manager.get_compactor(context_id).is_none());
    }
}
