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
//!   - `SELECT * FROM generate_series_1_10` generates numbers 1-10
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
use datafusion_common::{DFSchema, Result, ScalarValue};
use datafusion_expr::logical_plan::builder::LogicalPlanBuilder;
use datafusion_expr::planner::{
    PlannedRelation, RelationPlanner, RelationPlannerContext, RelationPlanning,
};
use datafusion_expr::Expr;
use datafusion_sql::sqlparser::ast::TableFactor;

/// Implements PostgreSQL-style generate_series via table naming convention
#[derive(Debug)]
struct GenerateSeriesPlanner;

impl RelationPlanner for GenerateSeriesPlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        _context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        if let TableFactor::Table { name, alias, .. } = &relation {
            let table_name = name.to_string();

            // Parse generate_series_{start}_{end} or generate_series_{start}_{end}_{step}
            if let Some(params) = table_name.strip_prefix("generate_series_") {
                let parts: Vec<&str> = params.split('_').collect();

                let (start, end, step) = match parts.len() {
                    2 => {
                        // generate_series_1_10
                        let Some(start) = parts[0].parse::<i64>().ok() else {
                            return Ok(RelationPlanning::Original(relation));
                        };
                        let Some(end) = parts[1].parse::<i64>().ok() else {
                            return Ok(RelationPlanning::Original(relation));
                        };
                        (start, end, 1i64)
                    }
                    3 => {
                        // generate_series_1_10_2
                        let Some(start) = parts[0].parse::<i64>().ok() else {
                            return Ok(RelationPlanning::Original(relation));
                        };
                        let Some(end) = parts[1].parse::<i64>().ok() else {
                            return Ok(RelationPlanning::Original(relation));
                        };
                        let Some(step) = parts[2].parse::<i64>().ok() else {
                            return Ok(RelationPlanning::Original(relation));
                        };
                        (start, end, step)
                    }
                    _ => return Ok(RelationPlanning::Original(relation)),
                };

                // Generate the series
                let mut values = Vec::new();
                let mut current = start;

                if step > 0 {
                    while current <= end {
                        values.push(vec![Expr::Literal(
                            ScalarValue::Int64(Some(current)),
                            None,
                        )]);
                        current += step;
                    }
                } else if step < 0 {
                    while current >= end {
                        values.push(vec![Expr::Literal(
                            ScalarValue::Int64(Some(current)),
                            None,
                        )]);
                        current += step;
                    }
                } else {
                    return Ok(RelationPlanning::Original(relation)); // Invalid: step cannot be 0
                }

                let plan = LogicalPlanBuilder::values(values)?.build()?;
                return Ok(RelationPlanning::Planned(PlannedRelation::new(
                    plan,
                    alias.clone(),
                )));
            }
        }

        Ok(RelationPlanning::Original(relation))
    }
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
            let normalized = context
                .normalize_ident(datafusion_sql::sqlparser::ast::Ident::new(&table_name));
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
                    let sql_expr = datafusion_sql::sqlparser::parser::Parser::parse_sql(
                        &datafusion_sql::sqlparser::dialect::GenericDialect {},
                        "1 + 1"
                    ).ok()
                        .and_then(|mut stmts| stmts.pop())
                        .and_then(|stmt| {
                            if let datafusion_sql::sqlparser::ast::Statement::Query(q) = stmt {
                                if let datafusion_sql::sqlparser::ast::SetExpr::Select(s) = *q.body {
                                    s.projection.into_iter().next().and_then(|p| {
                                        if let datafusion_sql::sqlparser::ast::SelectItem::UnnamedExpr(e) = p {
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
    println!("SQL: SELECT * FROM generate_series_1_5\n");
    let df = ctx.sql("SELECT * FROM generate_series_1_5").await?;
    df.show().await?;

    // Example 2: Series with step
    println!("\nExample 2: Series with step (0 to 100, step 25)");
    println!("SQL: SELECT * FROM generate_series_0_100_25\n");
    let df = ctx.sql("SELECT * FROM generate_series_0_100_25").await?;
    df.show().await?;

    // Example 3: DUAL table (Oracle-style)
    println!("\nExample 3: DUAL table for testing expressions");
    println!("SQL: SELECT 1 + 1 AS result, 'Hello' AS greeting FROM dual\n");
    let df = ctx
        .sql("SELECT 1 + 1 AS result, 'Hello' AS greeting FROM dual")
        .await?;
    df.show().await?;

    // Example 4: Demonstrate sql_to_expr and sql_expr_to_logical_expr
    println!("\nExample 4: Expression parsing demonstration");
    println!("SQL: SELECT * FROM expr_demo\n");
    let df = ctx.sql("SELECT * FROM expr_demo").await?;
    df.show().await?;

    // Example 5: Weekdays with normalize_ident
    println!("\nExample 5: Weekdays (demonstrating normalize_ident)");
    println!("SQL: SELECT * FROM weekdays WHERE column1 LIKE 'T%'\n");
    let df = ctx
        .sql("SELECT * FROM weekdays WHERE column1 LIKE 'T%'")
        .await?;
    df.show().await?;

    // Example 6: Cartesian product with generate_series (demonstrates plan())
    println!("\nExample 6: Cartesian product using context.plan() for sub-relations");
    println!("SQL: SELECT a.column1 AS x, b.column1 AS y FROM generate_series_1_3 a CROSS JOIN generate_series_1_3 b\n");
    let df = ctx
        .sql(
            "SELECT a.column1 AS x, b.column1 AS y \
             FROM generate_series_1_3 a CROSS JOIN generate_series_1_3 b \
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
