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

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::{DataType, Datum, ScalarImpl};
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_expr::agg::AggKind;
use risingwave_expr::function::window::{Frame, FrameBound, WindowFuncKind};

use super::generic::{GenericPlanRef, OverWindow, PlanWindowFunction, ProjectBuilder};
use super::utils::impl_distill_by_unit;
use super::{
    gen_filter_and_pushdown, ColPrunable, ExprRewritable, LogicalProject, PlanBase, PlanRef,
    PlanTreeNodeUnary, PredicatePushdown, StreamEowcOverWindow, StreamSort, ToBatch, ToStream,
};
use crate::expr::{Expr, ExprImpl, ExprType, FunctionCall, InputRef, WindowFunction};
use crate::optimizer::plan_node::{
    ColumnPruningContext, Literal, PredicatePushdownContext, RewriteStreamContext, ToStreamContext,
};
use crate::optimizer::property::{Order, RequiredDist};
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalOverWindow` performs `OVER` window functions to its input.
///
/// The output schema is the input schema plus the window functions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalOverWindow {
    pub base: PlanBase,
    core: OverWindow<PlanRef>,
}

impl LogicalOverWindow {
    fn new(calls: Vec<PlanWindowFunction>, input: PlanRef) -> Self {
        let core = OverWindow::new(calls, input);
        let base = PlanBase::new_logical_with_core(&core);
        Self { base, core }
    }

    pub fn create(
        input: PlanRef,
        mut select_exprs: Vec<ExprImpl>,
    ) -> Result<(PlanRef, Vec<ExprImpl>)> {
        let mut input_proj_builder = ProjectBuilder::default();
        for (idx, field) in input.schema().fields().iter().enumerate() {
            input_proj_builder
                .add_expr(&InputRef::new(idx, field.data_type()).into())
                .map_err(|err| {
                    ErrorCode::NotImplemented(format!("{err} inside input"), None.into())
                })?;
        }
        let mut input_len = input.schema().len();
        for expr in &select_exprs {
            if let ExprImpl::WindowFunction(window_function) = expr {
                if let WindowFuncKind::Aggregate(agg_kind) = window_function.kind
                    && matches!(
                        agg_kind,
                        AggKind::StddevPop
                            | AggKind::StddevSamp
                            | AggKind::VarPop
                            | AggKind::VarSamp
                    )
                {
                    let input = window_function.args.iter().exactly_one().unwrap();
                    let squared_input_expr = ExprImpl::from(
                        FunctionCall::new(ExprType::Multiply, vec![input.clone(), input.clone()])
                            .unwrap(),
                    );
                    input_len = input_len.max(
                        input_proj_builder
                            .add_expr(&squared_input_expr)
                            .unwrap_or(0)
                            + 1,
                    );
                }
                let input_idx_in_args: Vec<_> = window_function
                    .args
                    .iter()
                    .map(|x| input_proj_builder.add_expr(x))
                    .try_collect()
                    .map_err(|err| {
                        ErrorCode::NotImplemented(format!("{err} inside args"), None.into())
                    })?;
                let input_idx_in_order_by: Vec<_> = window_function
                    .order_by
                    .sort_exprs
                    .iter()
                    .map(|x| input_proj_builder.add_expr(&x.expr))
                    .try_collect()
                    .map_err(|err| {
                        ErrorCode::NotImplemented(format!("{err} inside order_by"), None.into())
                    })?;
                let input_idx_in_partition_by: Vec<_> = window_function
                    .partition_by
                    .iter()
                    .map(|x| input_proj_builder.add_expr(x))
                    .try_collect()
                    .map_err(|err| {
                        ErrorCode::NotImplemented(format!("{err} inside partition_by"), None.into())
                    })?;
                input_len = input_len
                    .max(*input_idx_in_args.iter().max().unwrap_or(&0) + 1)
                    .max(*input_idx_in_order_by.iter().max().unwrap_or(&0) + 1)
                    .max(*input_idx_in_partition_by.iter().max().unwrap_or(&0) + 1);
            }
        }

        let mut window_funcs = vec![];
        for expr in &mut select_exprs {
            if let ExprImpl::WindowFunction(window) = expr {
                let (kind, args, return_type, partition_by, order_by, frame) = (
                    window.kind,
                    &window.args,
                    &window.return_type,
                    &window.partition_by,
                    &window.order_by,
                    &window.frame,
                );
                if let WindowFuncKind::Aggregate(agg_kind) = kind
                    && matches!(
                        agg_kind,
                        AggKind::Avg
                            | AggKind::StddevPop
                            | AggKind::StddevSamp
                            | AggKind::VarPop
                            | AggKind::VarSamp
                    )
                {
                    // Refer to LogicalAggBuilder::try_rewrite_agg_call()
                    match agg_kind {
                        AggKind::Avg => {
                            assert_eq!(args.len(), 1);
                            window_funcs.push(WindowFunction::new(
                                WindowFuncKind::Aggregate(AggKind::Sum),
                                partition_by.clone(),
                                order_by.clone(),
                                args.clone(),
                                frame.clone(),
                            )?);
                            let left_ref = ExprImpl::from(InputRef::new(
                                input_len + window_funcs.len() - 1,
                                window_funcs.last().unwrap().return_type(),
                            ))
                            .cast_explicit(return_type.clone())
                            .unwrap();
                            window_funcs.push(WindowFunction::new(
                                WindowFuncKind::Aggregate(AggKind::Count),
                                partition_by.clone(),
                                order_by.clone(),
                                args.clone(),
                                frame.clone(),
                            )?);
                            let right_ref = ExprImpl::from(InputRef::new(
                                input_len + window_funcs.len() - 1,
                                window_funcs.last().unwrap().return_type(),
                            ));
                            let new_expr = ExprImpl::from(
                                FunctionCall::new(ExprType::Divide, vec![left_ref, right_ref])
                                    .unwrap(),
                            );
                            let _ = std::mem::replace(expr, new_expr);
                        }
                        AggKind::StddevPop
                        | AggKind::StddevSamp
                        | AggKind::VarPop
                        | AggKind::VarSamp => {
                            let input = args.first().unwrap();
                            let squared_input_expr = ExprImpl::from(
                                FunctionCall::new(
                                    ExprType::Multiply,
                                    vec![input.clone(), input.clone()],
                                )
                                .unwrap(),
                            );

                            window_funcs.push(WindowFunction::new(
                                WindowFuncKind::Aggregate(AggKind::Sum),
                                partition_by.clone(),
                                order_by.clone(),
                                vec![squared_input_expr],
                                frame.clone(),
                            )?);

                            let sum_of_squares_expr = ExprImpl::from(InputRef::new(
                                input_len + window_funcs.len() - 1,
                                window_funcs.last().unwrap().return_type(),
                            ))
                            .cast_explicit(return_type.clone())
                            .unwrap();

                            window_funcs.push(WindowFunction::new(
                                WindowFuncKind::Aggregate(AggKind::Sum),
                                partition_by.clone(),
                                order_by.clone(),
                                args.clone(),
                                frame.clone(),
                            )?);
                            let sum_expr = ExprImpl::from(InputRef::new(
                                input_len + window_funcs.len() - 1,
                                window_funcs.last().unwrap().return_type(),
                            ))
                            .cast_explicit(return_type.clone())
                            .unwrap();

                            window_funcs.push(WindowFunction::new(
                                WindowFuncKind::Aggregate(AggKind::Count),
                                partition_by.clone(),
                                order_by.clone(),
                                args.clone(),
                                frame.clone(),
                            )?);
                            let count_expr = ExprImpl::from(InputRef::new(
                                input_len + window_funcs.len() - 1,
                                window_funcs.last().unwrap().return_type(),
                            ));

                            let square_of_sum_expr = ExprImpl::from(
                                FunctionCall::new(
                                    ExprType::Multiply,
                                    vec![sum_expr.clone(), sum_expr],
                                )
                                .unwrap(),
                            );

                            let numerator_expr = ExprImpl::from(
                                FunctionCall::new(
                                    ExprType::Subtract,
                                    vec![
                                        sum_of_squares_expr,
                                        ExprImpl::from(
                                            FunctionCall::new(
                                                ExprType::Divide,
                                                vec![square_of_sum_expr, count_expr.clone()],
                                            )
                                            .unwrap(),
                                        ),
                                    ],
                                )
                                .unwrap(),
                            );

                            let denominator_expr = match agg_kind {
                                AggKind::StddevPop | AggKind::VarPop => count_expr.clone(),
                                AggKind::StddevSamp | AggKind::VarSamp => ExprImpl::from(
                                    FunctionCall::new(
                                        ExprType::Subtract,
                                        vec![
                                            count_expr.clone(),
                                            ExprImpl::from(Literal::new(
                                                Datum::from(ScalarImpl::Int64(1)),
                                                DataType::Int64,
                                            )),
                                        ],
                                    )
                                    .unwrap(),
                                ),
                                _ => unreachable!(),
                            };

                            let mut target_expr = ExprImpl::from(
                                FunctionCall::new(
                                    ExprType::Divide,
                                    vec![numerator_expr, denominator_expr],
                                )
                                .unwrap(),
                            );

                            if matches!(agg_kind, AggKind::StddevPop | AggKind::StddevSamp) {
                                target_expr = ExprImpl::from(
                                    FunctionCall::new(ExprType::Sqrt, vec![target_expr]).unwrap(),
                                );
                            }

                            match agg_kind {
                                AggKind::VarPop | AggKind::StddevPop => {
                                    let _ = std::mem::replace(expr, target_expr);
                                }
                                AggKind::StddevSamp | AggKind::VarSamp => {
                                    let less_than_expr = ExprImpl::from(
                                        FunctionCall::new(
                                            ExprType::LessThanOrEqual,
                                            vec![
                                                count_expr,
                                                ExprImpl::from(Literal::new(
                                                    Datum::from(ScalarImpl::Int64(1)),
                                                    DataType::Int64,
                                                )),
                                            ],
                                        )
                                        .unwrap(),
                                    );
                                    let null_expr =
                                        ExprImpl::from(Literal::new(None, return_type.clone()));

                                    let case_expr = ExprImpl::from(
                                        FunctionCall::new(
                                            ExprType::Case,
                                            vec![less_than_expr, null_expr, target_expr],
                                        )
                                        .unwrap(),
                                    );
                                    let _ = std::mem::replace(expr, case_expr);
                                }
                                _ => unreachable!(),
                            }
                        }
                        _ => unreachable!(),
                    }
                } else {
                    let new_expr =
                        InputRef::new(input_len + window_funcs.len(), expr.return_type().clone())
                            .into();
                    let f = std::mem::replace(expr, new_expr)
                        .into_window_function()
                        .unwrap();
                    window_funcs.push(*f);
                }
            }
            if expr.has_window_function() {
                return Err(ErrorCode::NotImplemented(
                    format!("window function in expression: {:?}", expr),
                    None.into(),
                )
                .into());
            }
        }
        for f in &window_funcs {
            if f.kind.is_rank() {
                if f.order_by.sort_exprs.is_empty() {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "window rank function without order by: {:?}",
                        f
                    ))
                    .into());
                }
                if f.kind == WindowFuncKind::DenseRank {
                    return Err(ErrorCode::NotImplemented(
                        format!("window rank function: {}", f.kind),
                        4847.into(),
                    )
                    .into());
                }
            }
        }

        let plan_window_funcs = window_funcs
            .into_iter()
            .map(|x| Self::convert_window_function(x, &input_proj_builder))
            .try_collect()?;

        Ok((
            Self::new(
                plan_window_funcs,
                LogicalProject::with_core(input_proj_builder.build(input)).into(),
            )
            .into(),
            select_exprs,
        ))
    }

    fn convert_window_function(
        window_function: WindowFunction,
        input_proj_builder: &ProjectBuilder,
    ) -> Result<PlanWindowFunction> {
        let order_by = window_function
            .order_by
            .sort_exprs
            .into_iter()
            .map(|e| {
                ColumnOrder::new(
                    input_proj_builder.expr_index(&e.expr).unwrap(),
                    e.order_type,
                )
            })
            .collect_vec();
        let partition_by = window_function
            .partition_by
            .into_iter()
            .map(|e| InputRef::new(input_proj_builder.expr_index(&e).unwrap(), e.return_type()))
            .collect_vec();

        let mut args = window_function.args;
        let frame = match window_function.kind {
            WindowFuncKind::RowNumber | WindowFuncKind::Rank | WindowFuncKind::DenseRank => {
                // ignore user-defined frame for rank functions
                Frame::rows(
                    FrameBound::UnboundedPreceding,
                    FrameBound::UnboundedFollowing,
                )
            }
            WindowFuncKind::Lag | WindowFuncKind::Lead => {
                let offset = if args.len() > 1 {
                    let offset_expr = args.remove(1);
                    if !offset_expr.return_type().is_int() {
                        return Err(ErrorCode::InvalidInputSyntax(format!(
                            "the `offset` of `{}` function should be integer",
                            window_function.kind
                        ))
                        .into());
                    }
                    offset_expr
                        .cast_implicit(DataType::Int64)?
                        .try_fold_const()
                        .transpose()?
                        .flatten()
                        .map(|v| *v.as_int64() as usize)
                        .unwrap_or(1usize)
                } else {
                    1usize
                };

                // override the frame
                // TODO(rc): We can only do the optimization for constant offset.
                if window_function.kind == WindowFuncKind::Lag {
                    Frame::rows(FrameBound::Preceding(offset), FrameBound::CurrentRow)
                } else {
                    Frame::rows(FrameBound::CurrentRow, FrameBound::Following(offset))
                }
            }
            WindowFuncKind::Aggregate(_) => window_function.frame.unwrap_or({
                // FIXME(rc): The following 2 cases should both be `Frame::Range(Unbounded,
                // CurrentRow)` but we don't support yet.
                if order_by.is_empty() {
                    Frame::rows(
                        FrameBound::UnboundedPreceding,
                        FrameBound::UnboundedFollowing,
                    )
                } else {
                    Frame::rows(FrameBound::UnboundedPreceding, FrameBound::CurrentRow)
                }
            }),
        };

        let args = args
            .into_iter()
            .map(|e| InputRef::new(input_proj_builder.expr_index(&e).unwrap(), e.return_type()))
            .collect_vec();

        Ok(PlanWindowFunction {
            kind: window_function.kind,
            return_type: window_function.return_type,
            args,
            partition_by,
            order_by,
            frame,
        })
    }

    pub fn window_functions(&self) -> &[PlanWindowFunction] {
        &self.core.window_functions
    }

    #[must_use]
    fn rewrite_with_input_and_window(
        &self,
        input: PlanRef,
        window_functions: &[PlanWindowFunction],
        input_col_change: ColIndexMapping,
    ) -> Self {
        let window_functions = window_functions
            .iter()
            .cloned()
            .map(|mut window_function| {
                window_function.args.iter_mut().for_each(|i| {
                    *i = InputRef::new(input_col_change.map(i.index()), i.return_type())
                });
                window_function.order_by.iter_mut().for_each(|o| {
                    o.column_index = input_col_change.map(o.column_index);
                });
                window_function.partition_by.iter_mut().for_each(|i| {
                    *i = InputRef::new(input_col_change.map(i.index()), i.return_type())
                });
                window_function
            })
            .collect();
        Self::new(window_functions, input)
    }

    pub fn split_with_rule(&self, groups: Vec<Vec<usize>>) -> PlanRef {
        assert!(groups.iter().flatten().all_unique());
        assert!(groups
            .iter()
            .flatten()
            .all(|&idx| idx < self.window_functions().len()));

        let input_len = self.input().schema().len();
        let original_out_fields = (0..input_len + self.window_functions().len()).collect_vec();
        let mut out_fields = original_out_fields.clone();
        let mut cur_input = self.input();
        let mut cur_node = self.clone();
        let mut cur_win_func_pos = input_len;
        for func_indices in &groups {
            cur_node = Self::new(
                func_indices
                    .iter()
                    .map(|&idx| {
                        let func = &self.window_functions()[idx];
                        out_fields[input_len + idx] = cur_win_func_pos;
                        cur_win_func_pos += 1;
                        func.clone()
                    })
                    .collect_vec(),
                cur_input.clone(),
            );
            cur_input = cur_node.clone().into();
        }
        if out_fields == original_out_fields {
            cur_node.into()
        } else {
            LogicalProject::with_out_col_idx(cur_node.into(), out_fields.into_iter()).into()
        }
    }
}

impl PlanTreeNodeUnary for LogicalOverWindow {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.core.window_functions.clone(), input)
    }

    #[must_use]
    fn rewrite_with_input(
        &self,
        input: PlanRef,
        input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        let input_len = self.core.input_len();
        let new_input_len = input.schema().len();
        let output_len = self.core.output_len();
        let new_output_len = new_input_len + self.window_functions().len();
        let output_col_change = {
            let mut mapping = ColIndexMapping::empty(output_len, new_output_len);
            for win_func_idx in 0..self.window_functions().len() {
                mapping.put(input_len + win_func_idx, Some(new_input_len + win_func_idx));
            }
            mapping.union(&input_col_change)
        };
        let new_self =
            self.rewrite_with_input_and_window(input, self.window_functions(), input_col_change);
        (new_self, output_col_change)
    }
}

impl_plan_tree_node_for_unary! { LogicalOverWindow }
impl_distill_by_unit!(LogicalOverWindow, core, "LogicalOverWindow");

impl ColPrunable for LogicalOverWindow {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let input_cnt = self.input().schema().len();
        let raw_required_cols = {
            let mut tmp = FixedBitSet::with_capacity(input_cnt);
            required_cols
                .iter()
                .filter(|&&index| index < input_cnt)
                .for_each(|&index| tmp.set(index, true));
            tmp
        };

        let (window_function_required_cols, window_functions) = {
            let mut tmp = FixedBitSet::with_capacity(input_cnt);
            let new_window_functions = required_cols
                .iter()
                .filter(|&&index| index >= input_cnt)
                .map(|&index| {
                    let index = index - input_cnt;
                    let window_function = self.window_functions()[index].clone();
                    tmp.extend(window_function.args.iter().map(|x| x.index()));
                    tmp.extend(window_function.partition_by.iter().map(|x| x.index()));
                    tmp.extend(window_function.order_by.iter().map(|x| x.column_index));
                    window_function
                })
                .collect_vec();
            (tmp, new_window_functions)
        };

        let input_required_cols = {
            let mut tmp = FixedBitSet::with_capacity(input_cnt);
            tmp.union_with(&raw_required_cols);
            tmp.union_with(&window_function_required_cols);
            tmp.ones().collect_vec()
        };
        let input_col_change =
            ColIndexMapping::with_remaining_columns(&input_required_cols, input_cnt);
        let new_self = {
            let input = self.input().prune_col(&input_required_cols, ctx);
            self.rewrite_with_input_and_window(input, &window_functions, input_col_change)
        };
        if new_self.schema().len() == required_cols.len() {
            // current schema perfectly fit the required columns
            new_self.into()
        } else {
            // some columns are not needed so we did a projection to remove the columns.
            let mut new_output_cols = input_required_cols.clone();
            new_output_cols.extend(required_cols.iter().filter(|&&x| x >= input_cnt));
            let mapping =
                &ColIndexMapping::with_remaining_columns(&new_output_cols, self.schema().len());
            let output_required_cols = required_cols
                .iter()
                .map(|&idx| mapping.map(idx))
                .collect_vec();
            let src_size = new_self.schema().len();
            LogicalProject::with_mapping(
                new_self.into(),
                ColIndexMapping::with_remaining_columns(&output_required_cols, src_size),
            )
            .into()
        }
    }
}

impl ExprRewritable for LogicalOverWindow {}

impl PredicatePushdown for LogicalOverWindow {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        let mut window_col = FixedBitSet::with_capacity(self.schema().len());
        window_col.insert_range(self.core.input.schema().len()..self.schema().len());
        let (window_pred, other_pred) = predicate.split_disjoint(&window_col);
        gen_filter_and_pushdown(self, window_pred, other_pred, ctx)
    }
}

impl ToBatch for LogicalOverWindow {
    fn to_batch(&self) -> Result<PlanRef> {
        Err(ErrorCode::NotImplemented(
            "Batch over window is not implemented yet".to_string(),
            9124.into(),
        )
        .into())
    }
}

impl ToStream for LogicalOverWindow {
    fn to_stream(&self, ctx: &mut ToStreamContext) -> Result<PlanRef> {
        let stream_input = self.core.input.to_stream(ctx)?;

        if ctx.emit_on_window_close() {
            if !self.core.funcs_have_same_partition_and_order() {
                return Err(ErrorCode::InvalidInputSyntax(
                    "All window functions must have the same PARTITION BY and ORDER BY".to_string(),
                )
                .into());
            }

            let order_by = &self.window_functions()[0].order_by;
            if order_by.len() != 1 || order_by[0].order_type != OrderType::ascending() {
                return Err(ErrorCode::InvalidInputSyntax(
                    "Only support window functions order by single column and in ascending order"
                        .to_string(),
                )
                .into());
            }
            if !stream_input
                .watermark_columns()
                .contains(order_by[0].column_index)
            {
                return Err(ErrorCode::InvalidInputSyntax(
                    "The column ordered by must be a watermark column".to_string(),
                )
                .into());
            }
            let order_key_index = order_by[0].column_index;

            let partition_key_indices = self.window_functions()[0]
                .partition_by
                .iter()
                .map(|e| e.index())
                .collect_vec();
            if partition_key_indices.is_empty() {
                return Err(ErrorCode::NotImplemented(
                    "Window function with empty PARTITION BY is not supported yet".to_string(),
                    None.into(),
                )
                .into());
            }

            let sort_input =
                RequiredDist::shard_by_key(stream_input.schema().len(), &partition_key_indices)
                    .enforce_if_not_satisfies(stream_input, &Order::any())?;
            let sort = StreamSort::new(sort_input, order_key_index);

            let mut logical = self.core.clone();
            logical.input = sort.into();
            return Ok(StreamEowcOverWindow::new(logical).into());
        }

        Err(ErrorCode::NotImplemented(
            "General version of streaming over window is not implemented yet".to_string(),
            9124.into(),
        )
        .into())
    }

    fn logical_rewrite_for_stream(
        &self,
        ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        let (input, input_col_change) = self.core.input.logical_rewrite_for_stream(ctx)?;
        let (new_self, output_col_change) = self.rewrite_with_input(input, input_col_change);
        Ok((new_self.into(), output_col_change))
    }
}
