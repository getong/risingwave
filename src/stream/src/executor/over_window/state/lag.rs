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

use risingwave_common::estimate_size::EstimateSize;
use risingwave_common::must_match;
use risingwave_common::types::Datum;
use risingwave_expr::function::window::{Frame, FrameBound, FrameBounds};
use smallvec::SmallVec;

use super::{EstimatedVecDeque, StateKey, StatePos, WindowState};
use crate::executor::over_window::state::StateEvictHint;
use crate::executor::StreamExecutorResult;

struct BufferEntry(StateKey, Datum);

impl EstimateSize for BufferEntry {
    fn estimated_heap_size(&self) -> usize {
        self.0.estimated_heap_size() + self.1.estimated_heap_size()
    }
}

#[derive(EstimateSize)]
pub(super) struct LagState {
    offset: usize,
    buffer: EstimatedVecDeque<BufferEntry>,
    curr_idx: usize,
}

impl LagState {
    pub fn new(frame: &Frame) -> Self {
        let offset = must_match!(&frame.bounds, FrameBounds::Rows(FrameBound::Preceding(offset), FrameBound::CurrentRow) => *offset);
        Self {
            offset,
            buffer: Default::default(),
            curr_idx: 0,
        }
    }

    fn curr_key(&self) -> Option<&StateKey> {
        self.buffer
            .get(self.curr_idx)
            .map(|BufferEntry(key, _)| key)
    }
}

impl WindowState for LagState {
    fn append(&mut self, key: StateKey, args: SmallVec<[Datum; 2]>) {
        self.buffer
            .push_back(BufferEntry(key, args.into_iter().next().unwrap()));
    }

    fn curr_window(&self) -> StatePos<'_> {
        let curr_key = self.curr_key();
        StatePos {
            key: curr_key,
            is_ready: curr_key.is_some(),
        }
    }

    fn curr_output(&self) -> StreamExecutorResult<Datum> {
        Ok(if self.curr_idx < self.offset {
            // the ready window doesn't have enough preceding rows, just return NULL
            None
        } else {
            // in the other case, the first entry in buffer is always the `lag(offset)` row
            assert_eq!(self.curr_idx, self.offset);
            self.buffer.front().unwrap().1.clone()
        })
    }

    fn slide_forward(&mut self) -> StateEvictHint {
        assert!(
            self.curr_key().is_some(),
            "should not slide when `curr_key` is None"
        );
        if self.curr_idx < self.offset {
            self.curr_idx += 1;
            StateEvictHint::CannotEvict(self.buffer.front().unwrap().0.clone())
        } else {
            // in the other case, the first entry in buffer is always the `lag(offset)` row
            assert_eq!(self.curr_idx, self.offset);
            let BufferEntry(key, _) = self.buffer.pop_front().unwrap();
            StateEvictHint::CanEvict(std::iter::once(key).collect())
        }
    }
}

#[cfg(test)]
mod tests {
    // TODO(rc): need to add some unit tests
}
