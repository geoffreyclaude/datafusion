// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};

use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{
    not_impl_err, plan_err, DFSchema, Diagnostic, Result, Span, Spans, TableReference,
};
use datafusion_expr::builder::subquery_alias;
use datafusion_expr::planner::{RelationPlannerContext, RelationPlannerDelegate};
use datafusion_expr::{expr::Unnest, Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion_expr::{Subquery, SubqueryAlias};
use sqlparser::ast::TableAlias;
use sqlparser::ast::{FunctionArg, FunctionArgExpr, Spanned, TableFactor};

mod join;

fn relation_alias(relation: &TableFactor) -> Option<TableAlias> {
    match relation {
        TableFactor::Table { alias, .. }
        | TableFactor::Derived { alias, .. }
        | TableFactor::NestedJoin { alias, .. }
        | TableFactor::UNNEST { alias, .. }
        | TableFactor::Function { alias, .. }
        | TableFactor::MatchRecognize { alias, .. } => alias.clone(),
        _ => None,
    }
}

struct SqlToRelRelationDelegate<'a, 'b, S: ContextProvider> {
    planner: &'a SqlToRel<'b, S>,
    planner_context: &'a mut PlannerContext,
    next_index: usize,
}

impl<'a, 'b, S: ContextProvider> RelationPlannerDelegate
    for SqlToRelRelationDelegate<'a, 'b, S>
{
    fn plan_default(&mut self, relation: TableFactor) -> Result<LogicalPlan> {
        self.planner.create_relation_default_with_alias(
            relation,
            self.planner_context,
            None,
        )
    }

    fn plan_relation(&mut self, relation: TableFactor) -> Result<LogicalPlan> {
        self.planner
            .create_relation_internal(relation, self.planner_context, 0)
    }

    fn plan_next(&mut self, relation: TableFactor) -> Result<Option<LogicalPlan>> {
        self.planner.try_plan_relation_with_extensions(
            &relation,
            self.planner_context,
            self.next_index,
            relation_alias(&relation),
        )
    }

    fn sql_to_expr(
        &mut self,
        expr: sqlparser::ast::Expr,
        schema: &DFSchema,
    ) -> Result<Expr> {
        self.planner.sql_to_expr(expr, schema, self.planner_context)
    }

    fn sql_expr_to_logical_expr(
        &mut self,
        expr: sqlparser::ast::Expr,
        schema: &DFSchema,
    ) -> Result<Expr> {
        self.planner
            .sql_expr_to_logical_expr(expr, schema, self.planner_context)
    }

    fn normalize_ident(&self, ident: sqlparser::ast::Ident) -> String {
        self.planner.ident_normalizer.normalize(ident)
    }

    fn object_name_to_table_reference(
        &self,
        name: sqlparser::ast::ObjectName,
    ) -> Result<TableReference> {
        self.planner.object_name_to_table_reference(name)
    }
}

impl<S: ContextProvider> SqlToRel<'_, S> {
    /// Create a `LogicalPlan` that scans the named relation
    fn create_relation(
        &self,
        relation: TableFactor,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        self.create_relation_internal(relation, planner_context, 0)
    }

    fn create_relation_internal(
        &self,
        relation: TableFactor,
        planner_context: &mut PlannerContext,
        start_index: usize,
    ) -> Result<LogicalPlan> {
        let alias = relation_alias(&relation);
        if let Some(plan) = self.try_plan_relation_with_extensions(
            &relation,
            planner_context,
            start_index,
            alias.clone(),
        )? {
            return Ok(plan);
        }

        self.create_relation_default_with_alias(relation, planner_context, alias)
    }

    fn try_plan_relation_with_extensions(
        &self,
        relation: &TableFactor,
        planner_context: &mut PlannerContext,
        start_index: usize,
        alias: Option<TableAlias>,
    ) -> Result<Option<LogicalPlan>> {
        let planners = self.context_provider.get_relation_planners();
        if planners.is_empty() || start_index >= planners.len() {
            return Ok(None);
        }

        let mut index = start_index;
        while let Some(planner) = planners.get(index) {
            let mut delegate = SqlToRelRelationDelegate {
                planner: self,
                planner_context,
                next_index: index + 1,
            };
            let mut context = RelationPlannerContext::new(&mut delegate);
            if let Some(plan) = planner.plan_relation(relation, &mut context)? {
                return self.finalize_relation_plan(plan, alias.clone()).map(Some);
            }
            index += 1;
        }

        Ok(None)
    }

    fn create_relation_default_with_alias(
        &self,
        relation: TableFactor,
        planner_context: &mut PlannerContext,
        alias: Option<TableAlias>,
    ) -> Result<LogicalPlan> {
        let alias = alias.or_else(|| relation_alias(&relation));
        let relation_span = relation.span();
        let plan = match relation {
            TableFactor::Table { name, args, .. } => {
                if let Some(func_args) = args {
                    let tbl_func_name =
                        name.0.first().unwrap().as_ident().unwrap().to_string();
                    let args = func_args
                        .args
                        .into_iter()
                        .flat_map(|arg| {
                            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg
                            {
                                self.sql_expr_to_logical_expr(
                                    expr,
                                    &DFSchema::empty(),
                                    planner_context,
                                )
                            } else {
                                plan_err!("Unsupported function argument type: {}", arg)
                            }
                        })
                        .collect::<Vec<_>>();
                    let provider = self
                        .context_provider
                        .get_table_function_source(&tbl_func_name, args)?;
                    LogicalPlanBuilder::scan(
                        TableReference::Bare {
                            table: format!("{tbl_func_name}()").into(),
                        },
                        provider,
                        None,
                    )?
                    .build()?
                } else {
                    let table_ref = self.object_name_to_table_reference(name)?;
                    let table_name = table_ref.to_string();
                    let cte = planner_context.get_cte(&table_name);
                    match (
                        cte,
                        self.context_provider.get_table_source(table_ref.clone()),
                    ) {
                        (Some(cte_plan), _) => Ok(cte_plan.clone()),
                        (_, Ok(provider)) => {
                            LogicalPlanBuilder::scan(table_ref.clone(), provider, None)?
                                .build()
                        }
                        (None, Err(e)) => {
                            let e = e.with_diagnostic(Diagnostic::new_error(
                                format!("table '{table_ref}' not found"),
                                Span::try_from_sqlparser_span(relation_span),
                            ));
                            Err(e)
                        }
                    }?
                }
            }
            TableFactor::Derived { subquery, .. } => {
                self.query_to_plan(*subquery, planner_context)?
            }
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => self.plan_table_with_joins(*table_with_joins, planner_context)?,
            TableFactor::UNNEST {
                array_exprs,
                with_offset: false,
                with_offset_alias: None,
                with_ordinality,
                ..
            } => {
                if with_ordinality {
                    return not_impl_err!("UNNEST with ordinality is not supported yet");
                }

                let schema = DFSchema::empty();
                let input = LogicalPlanBuilder::empty(true).build()?;
                let unnest_exprs = array_exprs
                    .into_iter()
                    .map(|sql_expr| {
                        let expr = self.sql_expr_to_logical_expr(
                            sql_expr,
                            &schema,
                            planner_context,
                        )?;
                        Self::check_unnest_arg(&expr, &schema)?;
                        Ok(Expr::Unnest(Unnest::new(expr)))
                    })
                    .collect::<Result<Vec<_>>>()?;
                if unnest_exprs.is_empty() {
                    return plan_err!("UNNEST must have at least one argument");
                }
                self.try_process_unnest(input, unnest_exprs)?
            }
            TableFactor::UNNEST { .. } => {
                return not_impl_err!(
                    "UNNEST table factor with offset is not supported yet"
                );
            }
            TableFactor::Function { name, args, .. } => {
                let tbl_func_ref = self.object_name_to_table_reference(name)?;
                let schema = planner_context
                    .outer_query_schema()
                    .cloned()
                    .unwrap_or_else(DFSchema::empty);
                let func_args = args
                    .into_iter()
                    .map(|arg| match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
                        | FunctionArg::Named {
                            arg: FunctionArgExpr::Expr(expr),
                            ..
                        } => {
                            self.sql_expr_to_logical_expr(expr, &schema, planner_context)
                        }
                        _ => plan_err!("Unsupported function argument: {arg:?}"),
                    })
                    .collect::<Result<Vec<Expr>>>()?;
                let provider = self
                    .context_provider
                    .get_table_function_source(tbl_func_ref.table(), func_args)?;
                LogicalPlanBuilder::scan(tbl_func_ref.table(), provider, None)?.build()?
            }
            _ => {
                return not_impl_err!(
                    "Unsupported ast node {relation:?} in create_relation"
                );
            }
        };

        self.finalize_relation_plan(plan, alias)
    }

    fn finalize_relation_plan(
        &self,
        plan: LogicalPlan,
        alias: Option<TableAlias>,
    ) -> Result<LogicalPlan> {
        let optimized_plan = optimize_subquery_sort(plan)?.data;
        if let Some(alias) = alias {
            self.apply_table_alias(optimized_plan, alias)
        } else {
            Ok(optimized_plan)
        }
    }
    pub(crate) fn create_relation_subquery(
        &self,
        subquery: TableFactor,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        // At this point for a syntactically valid query the outer_from_schema is
        // guaranteed to be set, so the `.unwrap()` call will never panic. This
        // is the case because we only call this method for lateral table
        // factors, and those can never be the first factor in a FROM list. This
        // means we arrived here through the `for` loop in `plan_from_tables` or
        // the `for` loop in `plan_table_with_joins`.
        let old_from_schema = planner_context
            .set_outer_from_schema(None)
            .unwrap_or_else(|| Arc::new(DFSchema::empty()));
        let new_query_schema = match planner_context.outer_query_schema() {
            Some(old_query_schema) => {
                let mut new_query_schema = old_from_schema.as_ref().clone();
                new_query_schema.merge(old_query_schema);
                Some(Arc::new(new_query_schema))
            }
            None => Some(Arc::clone(&old_from_schema)),
        };
        let old_query_schema = planner_context.set_outer_query_schema(new_query_schema);

        let plan = self.create_relation(subquery, planner_context)?;
        let outer_ref_columns = plan.all_out_ref_exprs();

        planner_context.set_outer_query_schema(old_query_schema);
        planner_context.set_outer_from_schema(Some(old_from_schema));

        // We can omit the subquery wrapper if there are no columns
        // referencing the outer scope.
        if outer_ref_columns.is_empty() {
            return Ok(plan);
        }

        match plan {
            LogicalPlan::SubqueryAlias(SubqueryAlias { input, alias, .. }) => {
                subquery_alias(
                    LogicalPlan::Subquery(Subquery {
                        subquery: input,
                        outer_ref_columns,
                        spans: Spans::new(),
                    }),
                    alias,
                )
            }
            plan => Ok(LogicalPlan::Subquery(Subquery {
                subquery: Arc::new(plan),
                outer_ref_columns,
                spans: Spans::new(),
            })),
        }
    }
}

fn optimize_subquery_sort(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    // When initializing subqueries, we examine sort options since they might be unnecessary.
    // They are only important if the subquery result is affected by the ORDER BY statement,
    // which can happen when we have:
    // 1. DISTINCT ON / ARRAY_AGG ... => Handled by an `Aggregate` and its requirements.
    // 2. RANK / ROW_NUMBER ... => Handled by a `WindowAggr` and its requirements.
    // 3. LIMIT => Handled by a `Sort`, so we need to search for it.
    let mut has_limit = false;
    let new_plan = plan.transform_down(|c| {
        if let LogicalPlan::Limit(_) = c {
            has_limit = true;
            return Ok(Transformed::no(c));
        }
        match c {
            LogicalPlan::Sort(s) => {
                if !has_limit {
                    has_limit = false;
                    return Ok(Transformed::yes(s.input.as_ref().clone()));
                }
                Ok(Transformed::no(LogicalPlan::Sort(s)))
            }
            _ => Ok(Transformed::no(c)),
        }
    });
    new_plan
}
