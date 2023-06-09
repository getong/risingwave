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

use risingwave_common::error::Result;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::TopNNode;

use super::generic::TopNLimit;
use super::utils::impl_distill_by_unit;
use super::{
    generic, ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, ToBatchPb, ToDistributedBatch,
};
use crate::optimizer::plan_node::batch::BatchPlanRef;
use crate::optimizer::plan_node::{BatchLimit, ToLocalBatch};
use crate::optimizer::property::{Order, RequiredDist};

/// `BatchTopN` implements [`super::LogicalTopN`] to find the top N elements with a heap
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchTopN {
    pub base: PlanBase,
    logical: generic::TopN<PlanRef>,
}

impl BatchTopN {
    pub fn new(logical: generic::TopN<PlanRef>) -> Self {
        assert!(logical.group_key.is_empty());
        let base = PlanBase::new_batch_from_logical(
            &logical,
            logical.input.distribution().clone(),
            // BatchTopN outputs data in the order of specified order
            logical.order.clone(),
        );
        BatchTopN { base, logical }
    }

    fn two_phase_topn(&self, input: PlanRef) -> Result<PlanRef> {
        let new_limit = TopNLimit::new(
            self.logical.limit_attr.limit() + self.logical.offset,
            self.logical.limit_attr.with_ties(),
        );
        let new_offset = 0;
        let partial_input: PlanRef = if input.order().satisfies(&self.logical.order) {
            let logical_partial_limit = generic::Limit::new(input, new_limit.limit(), new_offset);
            let batch_partial_limit = BatchLimit::new(logical_partial_limit);
            batch_partial_limit.into()
        } else {
            let logical_partial_topn = generic::TopN::without_group(
                input,
                new_limit,
                new_offset,
                self.logical.order.clone(),
            );
            let batch_partial_topn = Self::new(logical_partial_topn);
            batch_partial_topn.into()
        };

        let single_dist = RequiredDist::single();
        let ensure_single_dist = if !partial_input.distribution().satisfies(&single_dist) {
            single_dist.enforce_if_not_satisfies(partial_input, &Order::any())?
        } else {
            // The input's distribution is singleton, so use one phase topn is enough.
            return Ok(partial_input);
        };

        let batch_global_topn = self.clone_with_input(ensure_single_dist);
        Ok(batch_global_topn.into())
    }
}

impl_distill_by_unit!(BatchTopN, logical, "BatchTopN");

impl PlanTreeNodeUnary for BatchTopN {
    fn input(&self) -> PlanRef {
        self.logical.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut logical = self.logical.clone();
        logical.input = input;
        Self::new(logical)
    }
}

impl_plan_tree_node_for_unary! {BatchTopN}

impl ToDistributedBatch for BatchTopN {
    fn to_distributed(&self) -> Result<PlanRef> {
        self.two_phase_topn(self.input().to_distributed()?)
    }
}

impl ToBatchPb for BatchTopN {
    fn to_batch_prost_body(&self) -> NodeBody {
        let column_orders = self.logical.order.to_protobuf();
        NodeBody::TopN(TopNNode {
            limit: self.logical.limit_attr.limit(),
            offset: self.logical.offset,
            column_orders,
            with_ties: self.logical.limit_attr.with_ties(),
        })
    }
}

impl ToLocalBatch for BatchTopN {
    fn to_local(&self) -> Result<PlanRef> {
        self.two_phase_topn(self.input().to_local()?)
    }
}

impl ExprRewritable for BatchTopN {}
