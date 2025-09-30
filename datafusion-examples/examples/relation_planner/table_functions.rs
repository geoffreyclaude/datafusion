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
//! table-valued functions that work with recursive planning.
//!
//! Shows how to:
//!   - Intercept NestedJoin patterns to override join behavior
//!   - Use `context.plan()` to recursively plan sub-relations
//!   - Create custom join strategies for specific use cases
//!
//! This example demonstrates the `plan()` method of RelationPlannerContext

use datafusion::prelude::*;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::logical_plan::builder::LogicalPlanBuilder;
use datafusion_expr::planner::{
    PlannedRelation, RelationPlanner, RelationPlannerContext, RelationPlanning,
};
use datafusion_expr::Expr;
use datafusion_sql::sqlparser::ast::{JoinOperator, TableFactor};
use std::sync::Arc;

/// Provides sample test tables
#[derive(Debug)]
struct TestDataPlanner;

impl RelationPlanner for TestDataPlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        _context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        match relation {
            TableFactor::Table { name, alias, .. }
                if name.to_string().to_lowercase() == "numbers" =>
            {
                // A simple table of numbers
                let values = vec![
                    vec![Expr::Literal(ScalarValue::Int32(Some(1)), None)],
                    vec![Expr::Literal(ScalarValue::Int32(Some(2)), None)],
                    vec![Expr::Literal(ScalarValue::Int32(Some(3)), None)],
                    vec![Expr::Literal(ScalarValue::Int32(Some(4)), None)],
                    vec![Expr::Literal(ScalarValue::Int32(Some(5)), None)],
                ];

                let plan = LogicalPlanBuilder::values(values)?.build()?;
                Ok(RelationPlanning::Planned(PlannedRelation::new(plan, alias)))
            }
            TableFactor::Table { name, alias, .. }
                if name.to_string().to_lowercase() == "letters" =>
            {
                // A simple table of letters
                let values = vec![
                    vec![Expr::Literal(ScalarValue::Utf8(Some("A".into())), None)],
                    vec![Expr::Literal(ScalarValue::Utf8(Some("B".into())), None)],
                    vec![Expr::Literal(ScalarValue::Utf8(Some("C".into())), None)],
                ];

                let plan = LogicalPlanBuilder::values(values)?.build()?;
                Ok(RelationPlanning::Planned(PlannedRelation::new(plan, alias)))
            }
            TableFactor::Table { name, alias, .. }
                if name.to_string().to_lowercase() == "colors" =>
            {
                let values = vec![
                    vec![Expr::Literal(ScalarValue::Utf8(Some("Red".into())), None)],
                    vec![Expr::Literal(ScalarValue::Utf8(Some("Green".into())), None)],
                    vec![Expr::Literal(ScalarValue::Utf8(Some("Blue".into())), None)],
                ];

                let plan = LogicalPlanBuilder::values(values)?.build()?;
                Ok(RelationPlanning::Planned(PlannedRelation::new(plan, alias)))
            }
            other => Ok(RelationPlanning::Original(other)),
        }
    }
}

/// Intercepts CROSS JOIN operations to always use hash join strategy
/// Demonstrates the context.plan() method for recursive planning
#[derive(Debug)]
struct CrossJoinOptimizer;

impl RelationPlanner for CrossJoinOptimizer {
    fn plan_relation(
        &self,
        relation: TableFactor,
        context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        if let TableFactor::NestedJoin {
            table_with_joins,
            alias,
        } = &relation
        {
            // Only handle simple single joins
            if table_with_joins.joins.len() == 1 {
                let join_info = &table_with_joins.joins[0];
                // Check if it's a CROSS JOIN (no condition)
                if matches!(join_info.join_operator, JoinOperator::CrossJoin) {
                    // DEMONSTRATE context.plan(): Recursively plan both sides using the full planner pipeline
                    // This restarts the planning from the first registered planner
                    println!("[CrossJoinOptimizer] Using context.plan() to recursively plan sub-relations");
                    let left = context.plan(table_with_joins.relation.clone())?;
                    let right = context.plan(join_info.relation.clone())?;

                    println!(
                        "[CrossJoinOptimizer] Planning cross join between {} and {} fields",
                        left.schema().fields().len(),
                        right.schema().fields().len()
                    );

                    // Create the cross join
                    let plan =
                        LogicalPlanBuilder::from(left).cross_join(right)?.build()?;

                    return Ok(RelationPlanning::Planned(PlannedRelation::new(
                        plan,
                        alias.clone(),
                    )));
                }
            }
        }

        Ok(RelationPlanning::Original(relation))
    }
}

/// Intercepts specific JOIN patterns and adds automatic filtering
/// Also demonstrates the context.plan() method
#[derive(Debug)]
struct SmartJoinPlanner;

impl RelationPlanner for SmartJoinPlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        if let TableFactor::NestedJoin {
            table_with_joins,
            alias,
        } = &relation
        {
            if table_with_joins.joins.len() == 1 {
                let join_info = &table_with_joins.joins[0];

                // Only handle inner joins with specific tables
                if matches!(join_info.join_operator, JoinOperator::Inner(_)) {
                    // Check if we're joining specific tables
                    let left_name = match &table_with_joins.relation {
                        TableFactor::Table { name, .. } => {
                            name.to_string().to_lowercase()
                        }
                        _ => return Ok(RelationPlanning::Original(relation)),
                    };

                    let right_name = match &join_info.relation {
                        TableFactor::Table { name, .. } => {
                            name.to_string().to_lowercase()
                        }
                        _ => return Ok(RelationPlanning::Original(relation)),
                    };

                    // Special case: joining numbers with letters
                    if (left_name == "numbers" && right_name == "letters")
                        || (left_name == "letters" && right_name == "numbers")
                    {
                        println!(
                            "[SmartJoinPlanner] Optimizing {left_name} x {right_name} join using context.plan()"
                        );

                        // DEMONSTRATE context.plan() again
                        let left = context.plan(table_with_joins.relation.clone())?;
                        let right = context.plan(join_info.relation.clone())?;

                        // Create a cross join (since they have different schemas)
                        // In a real scenario, you might add smart predicate pushdown
                        let plan =
                            LogicalPlanBuilder::from(left).cross_join(right)?.build()?;

                        return Ok(RelationPlanning::Planned(PlannedRelation::new(
                            plan,
                            alias.clone(),
                        )));
                    }
                }
            }
        }

        Ok(RelationPlanning::Original(relation))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Register planners in order of priority
    ctx.register_relation_planner(Arc::new(CrossJoinOptimizer))?;
    ctx.register_relation_planner(Arc::new(SmartJoinPlanner))?;
    ctx.register_relation_planner(Arc::new(TestDataPlanner))?;

    println!("Custom Relation Planner: Table Functions & Join Optimization");
    println!("=============================================================\n");

    // Example 1: Basic table query
    println!("Example 1: Query test tables");
    println!("SQL: SELECT * FROM numbers\n");
    let df = ctx.sql("SELECT * FROM numbers").await?;
    df.show().await?;

    println!("\nSQL: SELECT * FROM letters\n");
    let df = ctx.sql("SELECT * FROM letters").await?;
    df.show().await?;

    // Example 2: Cross join (triggers CrossJoinOptimizer)
    println!("\nExample 2: Cross join optimization (demonstrates context.plan())");
    println!("SQL: SELECT * FROM numbers CROSS JOIN letters\n");
    let df = ctx
        .sql("SELECT n.column1 AS num, l.column1 AS letter FROM numbers n CROSS JOIN letters l")
        .await?;
    df.show_limit(10).await?;

    // Example 3: Smart join (triggers SmartJoinPlanner)
    println!("\nExample 3: Smart join planning (demonstrates context.plan())");
    println!("SQL: SELECT * FROM numbers INNER JOIN letters ON TRUE\n");
    let df = ctx
        .sql("SELECT n.column1 AS num, l.column1 AS letter FROM numbers n INNER JOIN letters l ON TRUE WHERE n.column1 <= 3")
        .await?;
    df.show().await?;

    // Example 4: Three-way join
    println!("\nExample 4: Three-way join (multiple context.plan() calls)");
    println!("SQL: Complex join with three tables\n");
    let df = ctx
        .sql(
            "SELECT n.column1 AS num, l.column1 AS letter, c.column1 AS color \
             FROM numbers n \
             CROSS JOIN letters l \
             CROSS JOIN colors c \
             WHERE n.column1 <= 2",
        )
        .await?;
    df.show_limit(10).await?;

    // Example 5: Aggregation with joins
    println!("\nExample 5: Aggregation with custom tables");
    println!("SQL: SELECT COUNT(*) as total FROM numbers n CROSS JOIN letters l\n");
    let df = ctx
        .sql("SELECT COUNT(*) as total FROM numbers n CROSS JOIN letters l")
        .await?;
    df.show().await?;

    println!("\nKey Concepts:");
    println!("   - Custom planners can intercept NestedJoin patterns");
    println!("   - Use context.plan() to recursively plan sub-relations");
    println!("   - Multiple planners can handle different join strategies");
    println!("   - Planners can add optimizations or transform join logic");
    println!();
    println!("Trait methods demonstrated:");
    println!("   - plan(): Recursively plans sub-relations through the full pipeline");
    println!("     Used in both CrossJoinOptimizer and SmartJoinPlanner");

    Ok(())
}
