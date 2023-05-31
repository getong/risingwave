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

use std::marker::PhantomData;

use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_storage::StateStore;

use crate::executor::test_utils::prelude::StateTable;
use crate::executor::{ActorContextRef, Executor, ExecutorInfo, Message, StreamExecutorError};

struct OverWindowExecutor<S: StateStore> {
    input: Box<dyn Executor>,
    inner: ExecutorInner<S>,
}

struct ExecutorInner<S: StateStore> {
    actor_ctx: ActorContextRef,
    info: ExecutorInfo,

    state_table: StateTable<S>,
}

struct ExecutionVars<S: StateStore> {
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

impl<S: StateStore> OverWindowExecutor<S> {
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn executor_inner(self) {
        let OverWindowExecutor {
            input,
            inner: mut this,
        } = self;

        todo!()
    }
}
