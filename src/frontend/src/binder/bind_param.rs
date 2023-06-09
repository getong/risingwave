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

use bytes::Bytes;
use pgwire::types::{Format, FormatIterator};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::{Datum, ScalarImpl};

use super::statement::RewriteExprsRecursive;
use super::BoundStatement;
use crate::expr::{Expr, ExprImpl, ExprRewriter, Literal};

/// Rewrites parameter expressions to literals.
pub(crate) struct ParamRewriter {
    pub(crate) params: Vec<Bytes>,
    pub(crate) parsed_params: Vec<Datum>,
    pub(crate) param_formats: Vec<Format>,
    pub(crate) error: Option<RwError>,
}

impl ParamRewriter {
    pub(crate) fn new(param_formats: Vec<Format>, params: Vec<Bytes>) -> Self {
        Self {
            parsed_params: vec![None; params.len()],
            params,
            param_formats,
            error: None,
        }
    }
}

impl ExprRewriter for ParamRewriter {
    fn rewrite_expr(&mut self, expr: ExprImpl) -> ExprImpl {
        if self.error.is_some() {
            return expr;
        }
        match expr {
            ExprImpl::InputRef(inner) => self.rewrite_input_ref(*inner),
            ExprImpl::Literal(inner) => self.rewrite_literal(*inner),
            ExprImpl::FunctionCall(inner) => self.rewrite_function_call(*inner),
            ExprImpl::AggCall(inner) => self.rewrite_agg_call(*inner),
            ExprImpl::Subquery(inner) => self.rewrite_subquery(*inner),
            ExprImpl::CorrelatedInputRef(inner) => self.rewrite_correlated_input_ref(*inner),
            ExprImpl::TableFunction(inner) => self.rewrite_table_function(*inner),
            ExprImpl::WindowFunction(inner) => self.rewrite_window_function(*inner),
            ExprImpl::UserDefinedFunction(inner) => self.rewrite_user_defined_function(*inner),
            ExprImpl::Parameter(inner) => self.rewrite_parameter(*inner),
            ExprImpl::Now(inner) => self.rewrite_now(*inner),
        }
    }

    fn rewrite_parameter(&mut self, parameter: crate::expr::Parameter) -> ExprImpl {
        let data_type = parameter.return_type();

        // original parameter.index is 1-based.
        let parameter_index = (parameter.index - 1) as usize;

        let format = self.param_formats[parameter_index];
        let scalar = {
            let res = match format {
                Format::Text => {
                    let value = self.params[parameter_index].clone();
                    ScalarImpl::from_text(&value, &data_type)
                }
                Format::Binary => {
                    let value = self.params[parameter_index].clone();
                    ScalarImpl::from_binary(&value, &data_type)
                }
            };

            match res {
                Ok(datum) => datum,
                Err(e) => {
                    self.error = Some(e);
                    return parameter.into();
                }
            }
        };
        self.parsed_params[parameter_index] = Some(scalar.clone());
        Literal::new(Some(scalar), data_type).into()
    }
}

impl BoundStatement {
    pub fn bind_parameter(
        mut self,
        params: Vec<Bytes>,
        param_formats: Vec<Format>,
    ) -> Result<(BoundStatement, Vec<Datum>)> {
        let mut rewriter = ParamRewriter::new(
            FormatIterator::new(&param_formats, params.len())
                .map_err(ErrorCode::BindError)?
                .collect(),
            params,
        );

        self.rewrite_exprs_recursive(&mut rewriter);

        if let Some(err) = rewriter.error {
            return Err(err);
        }

        Ok((self, rewriter.parsed_params))
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;
    use pgwire::types::Format;
    use risingwave_common::types::DataType;
    use risingwave_sqlparser::test_utils::parse_sql_statements;

    use crate::binder::test_utils::{mock_binder, mock_binder_with_param_types};
    use crate::binder::BoundStatement;

    fn create_expect_bound(sql: &str) -> BoundStatement {
        let mut binder = mock_binder();
        let stmt = parse_sql_statements(sql).unwrap().remove(0);
        binder.bind(stmt).unwrap()
    }

    fn create_actual_bound(
        sql: &str,
        param_types: Vec<DataType>,
        params: Vec<Bytes>,
        param_formats: Vec<Format>,
    ) -> BoundStatement {
        let mut binder = mock_binder_with_param_types(param_types);
        let stmt = parse_sql_statements(sql).unwrap().remove(0);
        let bound = binder.bind(stmt).unwrap();
        bound.bind_parameter(params, param_formats).unwrap().0
    }

    fn expect_actual_eq(expect: BoundStatement, actual: BoundStatement) {
        // Use debug format to compare. May modify in future.
        assert_eq!(format!("{:?}", expect), format!("{:?}", actual));
    }

    #[tokio::test]
    async fn basic_select() {
        expect_actual_eq(
            create_expect_bound("select 1::int4"),
            create_actual_bound(
                "select $1::int4",
                vec![],
                vec!["1".into()],
                vec![Format::Text],
            ),
        );
    }

    #[tokio::test]
    async fn basic_value() {
        expect_actual_eq(
            create_expect_bound("values(1::int4)"),
            create_actual_bound(
                "values($1::int4)",
                vec![],
                vec!["1".into()],
                vec![Format::Text],
            ),
        );
    }

    #[tokio::test]
    async fn default_type() {
        expect_actual_eq(
            create_expect_bound("select '1'"),
            create_actual_bound("select $1", vec![], vec!["1".into()], vec![Format::Text]),
        );
    }

    #[tokio::test]
    async fn cast_after_specific() {
        expect_actual_eq(
            create_expect_bound("select 1::varchar"),
            create_actual_bound(
                "select $1::varchar",
                vec![DataType::Int32],
                vec!["1".into()],
                vec![Format::Text],
            ),
        );
    }

    #[tokio::test]
    async fn infer_case() {
        expect_actual_eq(
            create_expect_bound("select 1,1::INT4"),
            create_actual_bound(
                "select $1,$1::INT4",
                vec![],
                vec!["1".into()],
                vec![Format::Text],
            ),
        );
    }
}
