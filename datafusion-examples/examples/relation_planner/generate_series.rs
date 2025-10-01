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

//! This example demonstrates using custom relation planners to implement
//! PostgreSQL-style `generate_series` functionality and other utility tables.
//!
//! Usage examples:
//!   - `SELECT * FROM generate_series(1, 10)` generates numbers 1-10
//!   - `SELECT * FROM dual` returns a single row (Oracle-style)
//!   - `SELECT * FROM unnest_values` returns predefined test data
//!
//! This example demonstrates ALL RelationPlannerContext methods:
//! - plan: Recursively plan sub-relations in joins
//! - normalize_ident: Normalize SQL identifiers
//! - sql_to_expr: Convert SQL expressions to DataFusion expressions
//! - sql_expr_to_logical_expr: Convert without rewrites

use std::sync::Arc;

use datafusion::prelude::*;
use datafusion_common::{DFSchema, DataFusionError, Result, ScalarValue};
use datafusion_expr::logical_plan::{builder::LogicalPlanBuilder, LogicalPlan};
use datafusion_expr::planner::{
    PlannedRelation, RelationPlanner, RelationPlannerContext, RelationPlanning,
};
use datafusion_expr::Expr;
use datafusion_sql::sqlparser::ast::{
    self, FunctionArg, FunctionArgExpr, ObjectNamePart, TableFactor,
};
use datafusion_sql::sqlparser::dialect::GenericDialect;
use datafusion_sql::sqlparser::parser::Parser;

/// Implements PostgreSQL-style generate_series as a table-valued function
#[derive(Debug)]
struct GenerateSeriesPlanner;

impl RelationPlanner for GenerateSeriesPlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        _context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        match relation {
            TableFactor::Function {
                lateral,
                name,
                args,
                alias,
            } => {
                if let Some(planned) =
                    try_plan_generate_series(&name, &args, alias.clone())?
                {
                    return Ok(RelationPlanning::Planned(planned));
                }

                Ok(RelationPlanning::Original(TableFactor::Function {
                    lateral,
                    name,
                    args,
                    alias,
                }))
            }
            TableFactor::TableFunction { expr, alias } => {
                if let ast::Expr::Function(func) = &expr {
                    if let ast::FunctionArguments::List(list) = &func.args {
                        if let Some(planned) = try_plan_generate_series(
                            &func.name,
                            &list.args,
                            alias.clone(),
                        )? {
                            return Ok(RelationPlanning::Planned(planned));
                        }
                    }
                }

                Ok(RelationPlanning::Original(TableFactor::TableFunction {
                    expr,
                    alias,
                }))
            }
            other => Ok(RelationPlanning::Original(other)),
        }
    }
}

fn try_plan_generate_series(
    name: &ast::ObjectName,
    args: &[FunctionArg],
    alias: Option<ast::TableAlias>,
) -> Result<Option<PlannedRelation>> {
    if !is_generate_series(name) {
        return Ok(None);
    }

    let (start, end, step) = parse_series_args(args)?;
    if step == 0 {
        return Err(DataFusionError::Plan(
            "generate_series step argument must not be zero".into(),
        ));
    }

    let plan = build_series_plan(start, end, step)?;
    Ok(Some(PlannedRelation::new(plan, alias)))
}

fn is_generate_series(name: &ast::ObjectName) -> bool {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .is_some_and(|ident| ident.value.eq_ignore_ascii_case("generate_series"))
}

fn parse_series_args(args: &[FunctionArg]) -> Result<(i64, i64, i64)> {
    let mut values = Vec::with_capacity(args.len());

    for arg in args {
        let value = match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
            | FunctionArg::Named {
                arg: FunctionArgExpr::Expr(expr),
                ..
            } => parse_i64(expr)?,
            _ => {
                return Err(DataFusionError::Plan(
                    "generate_series arguments must be literal integers".into(),
                ))
            }
        };
        values.push(value);
    }

    match values.len() {
        2 => Ok((values[0], values[1], 1)),
        3 => Ok((values[0], values[1], values[2])),
        other => Err(DataFusionError::Plan(format!(
            "generate_series expects 2 or 3 arguments, got {other}"
        ))),
    }
}

fn parse_i64(expr: &ast::Expr) -> Result<i64> {
    match expr {
        ast::Expr::Value(value) => match &value.value {
            ast::Value::Number(v, _) => v.to_string().parse().map_err(|_| {
                DataFusionError::Plan(format!(
                    "generate_series argument must be an integer literal, got {}",
                    expr
                ))
            }),
            _ => Err(DataFusionError::Plan(format!(
                "generate_series argument must be an integer literal, got {}",
                expr
            ))),
        },
        ast::Expr::UnaryOp { op, expr } => match op {
            ast::UnaryOperator::Minus => parse_i64(expr).map(|v| -v),
            ast::UnaryOperator::Plus => parse_i64(expr),
            _ => Err(DataFusionError::Plan(format!(
                "generate_series argument must be an integer literal, got {}",
                expr
            ))),
        },
        _ => Err(DataFusionError::Plan(format!(
            "generate_series argument must be an integer literal, got {}",
            expr
        ))),
    }
}

fn build_series_plan(start: i64, end: i64, step: i64) -> Result<LogicalPlan> {
    let mut values = Vec::new();
    let mut current = start;

    if step > 0 {
        while current <= end {
            values.push(vec![Expr::Literal(ScalarValue::Int64(Some(current)), None)]);
            current += step;
        }
    } else {
        while current >= end {
            values.push(vec![Expr::Literal(ScalarValue::Int64(Some(current)), None)]);
            current += step;
        }
    }

    LogicalPlanBuilder::values(values)?.build()
}

/// Provides commonly-used utility tables and demonstrates context methods
#[derive(Debug)]
struct UtilityTablesPlanner;

impl RelationPlanner for UtilityTablesPlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        if let TableFactor::Table { name, alias, .. } = &relation {
            let table_name = name.to_string();

            // Demonstrate normalize_ident by normalizing the table name
            let normalized = context.normalize_ident(ast::Ident::new(&table_name));
            println!("[Utility Planner] Table: {table_name} (normalized: {normalized})");

            let values = match table_name.to_lowercase().as_str() {
                // Oracle-style DUAL table (single row, single column)
                "dual" => {
                    vec![vec![Expr::Literal(
                        ScalarValue::Utf8(Some("X".into())),
                        None,
                    )]]
                }

                // Days of the week
                "weekdays" => {
                    vec![
                        vec![Expr::Literal(
                            ScalarValue::Utf8(Some("Monday".into())),
                            None,
                        )],
                        vec![Expr::Literal(
                            ScalarValue::Utf8(Some("Tuesday".into())),
                            None,
                        )],
                        vec![Expr::Literal(
                            ScalarValue::Utf8(Some("Wednesday".into())),
                            None,
                        )],
                        vec![Expr::Literal(
                            ScalarValue::Utf8(Some("Thursday".into())),
                            None,
                        )],
                        vec![Expr::Literal(
                            ScalarValue::Utf8(Some("Friday".into())),
                            None,
                        )],
                    ]
                }

                // All days including weekend
                "days_of_week" => {
                    vec![
                        vec![Expr::Literal(
                            ScalarValue::Utf8(Some("Sunday".into())),
                            None,
                        )],
                        vec![Expr::Literal(
                            ScalarValue::Utf8(Some("Monday".into())),
                            None,
                        )],
                        vec![Expr::Literal(
                            ScalarValue::Utf8(Some("Tuesday".into())),
                            None,
                        )],
                        vec![Expr::Literal(
                            ScalarValue::Utf8(Some("Wednesday".into())),
                            None,
                        )],
                        vec![Expr::Literal(
                            ScalarValue::Utf8(Some("Thursday".into())),
                            None,
                        )],
                        vec![Expr::Literal(
                            ScalarValue::Utf8(Some("Friday".into())),
                            None,
                        )],
                        vec![Expr::Literal(
                            ScalarValue::Utf8(Some("Saturday".into())),
                            None,
                        )],
                    ]
                }

                // Boolean truth table
                "boolean_values" => {
                    vec![
                        vec![Expr::Literal(ScalarValue::Boolean(Some(true)), None)],
                        vec![Expr::Literal(ScalarValue::Boolean(Some(false)), None)],
                    ]
                }

                // Special table that demonstrates sql_to_expr and sql_expr_to_logical_expr
                "expr_demo" => {
                    // Parse a SQL expression "1 + 1"
                    let sql_expr = Parser::parse_sql(&GenericDialect {}, "1 + 1")
                        .ok()
                        .and_then(|mut stmts| stmts.pop())
                        .and_then(|stmt| {
                            if let ast::Statement::Query(q) = stmt {
                                if let ast::SetExpr::Select(s) = *q.body {
                                    s.projection.into_iter().next().and_then(|p| {
                                        if let ast::SelectItem::UnnamedExpr(e) = p {
                                            Some(e)
                                        } else {
                                            None
                                        }
                                    })
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        });

                    if let Some(sql_expr) = sql_expr {
                        let empty_schema = DFSchema::empty();

                        // Demonstrate sql_to_expr (with rewrites)
                        if let Ok(expr_with_rewrites) =
                            context.sql_to_expr(sql_expr.clone(), &empty_schema)
                        {
                            println!(
                                "[Utility Planner] sql_to_expr result: {expr_with_rewrites:?}"
                            );
                        }

                        // Demonstrate sql_expr_to_logical_expr (without rewrites)
                        if let Ok(expr_without_rewrites) =
                            context.sql_expr_to_logical_expr(sql_expr, &empty_schema)
                        {
                            println!(
                                "[Utility Planner] sql_expr_to_logical_expr result: {expr_without_rewrites:?}"
                            );
                        }
                    }

                    vec![vec![Expr::Literal(ScalarValue::Int32(Some(2)), None)]]
                }

                _ => return Ok(RelationPlanning::Original(relation)),
            };

            let plan = LogicalPlanBuilder::values(values)?.build()?;
            return Ok(RelationPlanning::Planned(PlannedRelation::new(
                plan,
                alias.clone(),
            )));
        }

        Ok(RelationPlanning::Original(relation))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Register our planners
    ctx.register_relation_planner(Arc::new(GenerateSeriesPlanner))?;
    ctx.register_relation_planner(Arc::new(UtilityTablesPlanner))?;

    println!("Custom Relation Planner: Generate Series & Utility Tables");
    println!("=========================================================\n");

    // Example 1: Basic series
    println!("Example 1: Basic series (1 to 5)");
    println!("SQL: SELECT * FROM generate_series(1, 5)\n");
    let df = ctx.sql("SELECT * FROM generate_series(1, 5)").await?;
    df.show().await?;

    // Example 2: Series with step
    println!("\nExample 2: Series with step (0 to 100, step 25)");
    println!("SQL: SELECT * FROM generate_series(0, 100, 25)\n");
    let df = ctx.sql("SELECT * FROM generate_series(0, 100, 25)").await?;
    df.show().await?;

    // Example 3: TABLE() invocation (exercises TableFactor::TableFunction)
    println!("\nExample 3: TABLE() invocation with descending step");
    println!("SQL: SELECT * FROM TABLE(generate_series(10, 0, -5)) ORDER BY column1\n");
    let df = ctx
        .sql("SELECT * FROM TABLE(generate_series(10, 0, -5)) ORDER BY column1")
        .await?;
    df.show().await?;

    // Example 4: DUAL table (Oracle-style)
    println!("\nExample 4: DUAL table for testing expressions");
    println!("SQL: SELECT 1 + 1 AS result, 'Hello' AS greeting FROM dual\n");
    let df = ctx
        .sql("SELECT 1 + 1 AS result, 'Hello' AS greeting FROM dual")
        .await?;
    df.show().await?;

    // Example 5: Demonstrate sql_to_expr and sql_expr_to_logical_expr
    println!("\nExample 5: Expression parsing demonstration");
    println!("SQL: SELECT * FROM expr_demo\n");
    let df = ctx.sql("SELECT * FROM expr_demo").await?;
    df.show().await?;

    // Example 6: Weekdays with normalize_ident
    println!("\nExample 6: Weekdays (demonstrating normalize_ident)");
    println!("SQL: SELECT * FROM weekdays WHERE column1 LIKE 'T%'\n");
    let df = ctx
        .sql("SELECT * FROM weekdays WHERE column1 LIKE 'T%'")
        .await?;
    df.show().await?;

    // Example 7: Cartesian product with generate_series (demonstrates plan())
    println!("\nExample 7: Cartesian product using context.plan() for sub-relations");
    println!(
        "SQL: SELECT a.value AS x, b.value AS y FROM generate_series(1, 3) a CROSS JOIN generate_series(1, 3) b\n"
    );
    let df = ctx
        .sql(
            "SELECT a.value AS x, b.value AS y \
             FROM generate_series(1, 3) a CROSS JOIN generate_series(1, 3) b \
             ORDER BY x, y",
        )
        .await?;
    df.show().await?;

    println!("\nKey Concepts:");
    println!("   - Custom planners can implement SQL table-valued functions");
    println!("   - Useful for testing, data generation, and utilities");
    println!("   - Works seamlessly with joins, filters, and aggregations");
    println!();
    println!("Trait methods demonstrated:");
    println!("   - normalize_ident: Normalize SQL identifiers");
    println!("   - sql_to_expr: Parse SQL expressions with DataFusion rewrites");
    println!("   - sql_expr_to_logical_expr: Parse SQL expressions without rewrites");
    println!("   - plan: Implicitly used for CROSS JOIN sub-relations");

    Ok(())
}
