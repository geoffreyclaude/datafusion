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
//! MATCH_RECOGNIZE-style pattern matching on event streams.
//!
//! MATCH_RECOGNIZE is a SQL extension for pattern matching on ordered data,
//! similar to regular expressions but for relational data. This example shows
//! how to use custom planners to implement new SQL syntax.
//!
//! This example demonstrates RelationPlannerContext methods:
//! - plan(): Recursively plan the input table
//! - normalize_ident(): Normalize measure and symbol identifiers
//! - sql_to_expr(): Convert SQL expressions to DataFusion expressions

use std::any::Any;
use std::cmp::Ordering;
use std::hash::Hasher;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::*;
use datafusion_common::{DFSchema, DFSchemaRef, DataFusionError, Result, ScalarValue};
use datafusion_expr::logical_plan::builder::LogicalPlanBuilder;
use datafusion_expr::logical_plan::{
    EmptyRelation, Extension, InvariantLevel, LogicalPlan,
};
use datafusion_expr::planner::{
    PlannedRelation, RelationPlanner, RelationPlannerContext, RelationPlanning,
};
use datafusion_expr::{Expr, UserDefinedLogicalNode};
use datafusion_sql::sqlparser::ast::TableFactor;

/// A custom logical plan node representing MATCH_RECOGNIZE operations
#[derive(Debug)]
struct MiniMatchRecognizeNode {
    input: Arc<LogicalPlan>,
    schema: DFSchemaRef,
    measures: Vec<(String, Expr)>,
    definitions: Vec<(String, Expr)>,
}

impl UserDefinedLogicalNode for MiniMatchRecognizeNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "MiniMatchRecognize"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn check_invariants(&self, _check: InvariantLevel) -> Result<()> {
        Ok(())
    }

    fn expressions(&self) -> Vec<Expr> {
        self.measures
            .iter()
            .chain(&self.definitions)
            .map(|(_, expr)| expr.clone())
            .collect()
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MiniMatchRecognize")?;

        if !self.measures.is_empty() {
            write!(f, " measures=[")?;
            for (idx, (alias, expr)) in self.measures.iter().enumerate() {
                if idx > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{alias} := {expr}")?;
            }
            write!(f, "]")?;
        }

        if !self.definitions.is_empty() {
            write!(f, " define=[")?;
            for (idx, (symbol, expr)) in self.definitions.iter().enumerate() {
                if idx > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{symbol} := {expr}")?;
            }
            write!(f, "]")?;
        }

        Ok(())
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Arc<dyn UserDefinedLogicalNode>> {
        if exprs.len() != self.measures.len() + self.definitions.len() {
            return Err(DataFusionError::Internal(
                "MiniMatchRecognize received an unexpected expression count".into(),
            ));
        }

        let (measure_exprs, definition_exprs) = exprs.split_at(self.measures.len());

        let Some(first_input) = inputs.into_iter().next() else {
            return Err(DataFusionError::Internal(
                "MiniMatchRecognize requires a single input".into(),
            ));
        };

        let measures = self
            .measures
            .iter()
            .zip(measure_exprs.iter())
            .map(|((alias, _), expr)| (alias.clone(), expr.clone()))
            .collect();

        let definitions = self
            .definitions
            .iter()
            .zip(definition_exprs.iter())
            .map(|((symbol, _), expr)| (symbol.clone(), expr.clone()))
            .collect();

        Ok(Arc::new(Self {
            input: Arc::new(first_input),
            schema: Arc::clone(&self.schema),
            measures,
            definitions,
        }))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        state.write_usize(Arc::as_ptr(&self.input) as usize);
        state.write_usize(self.measures.len());
        state.write_usize(self.definitions.len());
    }

    fn dyn_eq(&self, other: &dyn UserDefinedLogicalNode) -> bool {
        other
            .as_any()
            .downcast_ref::<Self>()
            .map(|o| {
                Arc::ptr_eq(&self.input, &o.input)
                    && self.measures == o.measures
                    && self.definitions == o.definitions
            })
            .unwrap_or(false)
    }

    fn dyn_ord(&self, other: &dyn UserDefinedLogicalNode) -> Option<Ordering> {
        if self.dyn_eq(other) {
            Some(Ordering::Equal)
        } else {
            None
        }
    }
}

/// Custom planner that handles MATCH_RECOGNIZE table factor syntax
#[derive(Debug)]
struct MatchRecognizePlanner;

impl RelationPlanner for MatchRecognizePlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        if let TableFactor::MatchRecognize {
            table,
            measures,
            symbols,
            alias,
            ..
        } = relation
        {
            println!("[MatchRecognizePlanner] Processing MATCH_RECOGNIZE clause");

            // DEMONSTRATE context.plan(): Recursively plan the input table
            println!("[MatchRecognizePlanner] Using context.plan() to plan input table");
            let input = context.plan(*table)?;
            let input_schema = input.schema().clone();
            println!(
                "[MatchRecognizePlanner] Input schema has {} fields",
                input_schema.fields().len()
            );

            // DEMONSTRATE normalize_ident() and sql_to_expr(): Process MEASURES
            let planned_measures = measures
                .iter()
                .map(|measure| {
                    // Normalize the measure alias
                    let alias = context.normalize_ident(measure.alias.clone());
                    println!("[MatchRecognizePlanner] Normalized measure alias: {alias}");

                    // Convert SQL expression to DataFusion expression
                    let expr = context
                        .sql_to_expr(measure.expr.clone(), input_schema.as_ref())?;
                    println!(
                        "[MatchRecognizePlanner] Planned measure expression: {expr:?}"
                    );

                    Ok((alias, expr))
                })
                .collect::<Result<Vec<_>>>()?;

            // DEMONSTRATE normalize_ident() and sql_to_expr(): Process DEFINE
            let planned_definitions = symbols
                .iter()
                .map(|symbol| {
                    // Normalize the symbol name
                    let name = context.normalize_ident(symbol.symbol.clone());
                    println!("[MatchRecognizePlanner] Normalized symbol: {name}");

                    // Convert SQL expression to DataFusion expression
                    let expr = context
                        .sql_to_expr(symbol.definition.clone(), input_schema.as_ref())?;
                    println!("[MatchRecognizePlanner] Planned definition: {expr:?}");

                    Ok((name, expr))
                })
                .collect::<Result<Vec<_>>>()?;

            // Create the custom MATCH_RECOGNIZE node
            let node = MiniMatchRecognizeNode {
                schema: Arc::clone(&input_schema),
                input: Arc::new(input),
                measures: planned_measures,
                definitions: planned_definitions,
            };

            let plan = LogicalPlan::Extension(Extension {
                node: Arc::new(node),
            });

            println!("[MatchRecognizePlanner] Successfully created MATCH_RECOGNIZE plan");
            return Ok(RelationPlanning::Planned(PlannedRelation::new(plan, alias)));
        }

        Ok(RelationPlanning::Original(relation))
    }
}

/// Provides sample event data for the MATCH_RECOGNIZE example
#[derive(Debug)]
struct EventDataPlanner;

impl RelationPlanner for EventDataPlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        _context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        match relation {
            TableFactor::Table { name, alias, .. }
                if name.to_string().to_lowercase() == "events" =>
            {
                // Create a simple schema for event data
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "price",
                    DataType::Int32,
                    true,
                )]));
                let df_schema = Arc::new(DFSchema::try_from(schema)?);

                // For demonstration, return an empty relation with the schema
                let plan = LogicalPlan::EmptyRelation(EmptyRelation {
                    produce_one_row: false,
                    schema: df_schema,
                });

                println!("[EventDataPlanner] Created 'events' table with price column");
                Ok(RelationPlanning::Planned(PlannedRelation::new(plan, alias)))
            }
            TableFactor::Table { name, alias, .. }
                if name.to_string().to_lowercase() == "stock_prices" =>
            {
                // Another example table with more realistic stock data schema
                let values = vec![
                    vec![
                        Expr::Literal(ScalarValue::Utf8(Some("DDOG".into())), None),
                        Expr::Literal(ScalarValue::Float64(Some(150.0)), None),
                    ],
                    vec![
                        Expr::Literal(ScalarValue::Utf8(Some("DDOG".into())), None),
                        Expr::Literal(ScalarValue::Float64(Some(155.0)), None),
                    ],
                    vec![
                        Expr::Literal(ScalarValue::Utf8(Some("DDOG".into())), None),
                        Expr::Literal(ScalarValue::Float64(Some(152.0)), None),
                    ],
                    vec![
                        Expr::Literal(ScalarValue::Utf8(Some("DDOG".into())), None),
                        Expr::Literal(ScalarValue::Float64(Some(158.0)), None),
                    ],
                ];

                let plan = LogicalPlanBuilder::values(values)?.build()?;
                println!("[EventDataPlanner] Created 'stock_prices' table");
                Ok(RelationPlanning::Planned(PlannedRelation::new(plan, alias)))
            }
            other => Ok(RelationPlanning::Original(other)),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Register planners in order
    ctx.register_relation_planner(Arc::new(EventDataPlanner))?;
    ctx.register_relation_planner(Arc::new(MatchRecognizePlanner))?;

    println!("Custom Relation Planner: MATCH_RECOGNIZE Pattern Matching");
    println!("==========================================================\n");

    // Example 1: Basic MATCH_RECOGNIZE query
    println!("Example 1: MATCH_RECOGNIZE with measures and definitions");
    println!("SQL: SELECT * FROM events MATCH_RECOGNIZE (");
    println!("       PARTITION BY 1");
    println!("       MEASURES SUM(price) AS total_price, AVG(price) AS avg_price");
    println!("       PATTERN (A)");
    println!("       DEFINE A AS price > 10");
    println!("     ) AS t\n");

    let plan = ctx
        .sql(
            "SELECT * FROM events MATCH_RECOGNIZE (\
             PARTITION BY 1 \
             MEASURES SUM(price) AS total_price, AVG(price) AS avg_price \
             PATTERN (A) \
             DEFINE A AS price > 10\
             ) AS t",
        )
        .await?
        .into_unoptimized_plan();

    println!("\nLogical Plan:");
    println!("{}\n", plan.display_indent());

    // Example 2: Show stock_prices table
    println!("Example 2: Query stock_prices table");
    println!("SQL: SELECT * FROM stock_prices\n");
    let df = ctx.sql("SELECT * FROM stock_prices").await?;
    df.show().await?;

    // Example 3: MATCH_RECOGNIZE on stock_prices to detect high prices
    println!("\nExample 3: Detect stocks above threshold using MATCH_RECOGNIZE");
    println!("SQL: SELECT * FROM stock_prices MATCH_RECOGNIZE (");
    println!("       MEASURES");
    println!("         MIN(column2) AS min_price,");
    println!("         MAX(column2) AS max_price,");
    println!("         AVG(column2) AS avg_price");
    println!("       PATTERN (HIGH+)");
    println!("       DEFINE HIGH AS column2 > 151.0");
    println!("     ) AS trends\n");

    let plan = ctx
        .sql(
            "SELECT * FROM stock_prices MATCH_RECOGNIZE (\
             MEASURES \
               MIN(column2) AS min_price, \
               MAX(column2) AS max_price, \
               AVG(column2) AS avg_price \
             PATTERN (HIGH) \
             DEFINE HIGH AS column2 > 151.0\
             ) AS trends",
        )
        .await?
        .into_unoptimized_plan();

    println!("Logical Plan:");
    println!("{}\n", plan.display_indent());

    println!("\nKey Concepts:");
    println!("   - Custom planners can implement complex SQL extensions");
    println!("   - MATCH_RECOGNIZE enables pattern matching on ordered data");
    println!("   - Useful for detecting trends, sequences, and patterns in time-series");
    println!("   - UserDefinedLogicalNode allows extending DataFusion's logical plan");
    println!();
    println!("Trait methods demonstrated:");
    println!("   - plan(): Used to recursively plan the input table");
    println!("   - normalize_ident(): Normalizes measure aliases and symbol names");
    println!(
        "   - sql_to_expr(): Converts SQL expressions in MEASURES and DEFINE clauses"
    );
    println!();
    println!("Use cases:");
    println!("   - Stock price pattern detection (e.g., W-bottom, head-and-shoulders)");
    println!("   - Fraud detection in transaction sequences");
    println!("   - Anomaly detection in sensor data");
    println!("   - User behavior analysis in clickstream data");

    Ok(())
}
