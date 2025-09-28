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

use std::any::Any;
use std::cmp::Ordering;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::*;
use datafusion_common::{DFSchema, DFSchemaRef, DataFusionError, Result};
use datafusion_expr::logical_plan::{
    EmptyRelation, Extension, InvariantLevel, LogicalPlan,
};
use datafusion_expr::planner::{RelationPlanner, RelationPlannerContext};
use datafusion_expr::{Expr, UserDefinedLogicalNode};
use datafusion_sql::sqlparser::ast::TableFactor;

use std::hash::Hasher;

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

#[derive(Debug)]
struct MatchRecognizePlanner;

impl RelationPlanner for MatchRecognizePlanner {
    fn plan_relation(
        &self,
        relation: &TableFactor,
        context: &mut RelationPlannerContext<'_>,
    ) -> Result<Option<LogicalPlan>> {
        if let TableFactor::MatchRecognize {
            table,
            measures,
            symbols,
            ..
        } = relation
        {
            let input = context.plan_relation(*table.clone())?;
            let input_schema = input.schema().clone();

            let planned_measures = measures
                .iter()
                .map(|measure| {
                    let alias = context.normalize_ident(measure.alias.clone());
                    let expr = context
                        .sql_to_expr(measure.expr.clone(), input_schema.as_ref())?;
                    Ok((alias, expr))
                })
                .collect::<Result<Vec<_>>>()?;

            let planned_definitions = symbols
                .iter()
                .map(|symbol| {
                    let name = context.normalize_ident(symbol.symbol.clone());
                    let expr = context
                        .sql_to_expr(symbol.definition.clone(), input_schema.as_ref())?;
                    Ok((name, expr))
                })
                .collect::<Result<Vec<_>>>()?;

            let node = MiniMatchRecognizeNode {
                schema: Arc::clone(&input_schema),
                input: Arc::new(input),
                measures: planned_measures,
                definitions: planned_definitions,
            };
            let plan = LogicalPlan::Extension(Extension {
                node: Arc::new(node),
            });
            return Ok(Some(plan));
        }

        Ok(None)
    }
}

#[derive(Debug)]
struct ValuesPlanner;

impl RelationPlanner for ValuesPlanner {
    fn plan_relation(
        &self,
        relation: &TableFactor,
        _context: &mut RelationPlannerContext<'_>,
    ) -> Result<Option<LogicalPlan>> {
        if let TableFactor::Table { name, .. } = relation {
            if name.to_string().eq_ignore_ascii_case("events") {
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "price",
                    DataType::Int32,
                    true,
                )]));
                let df_schema = Arc::new(DFSchema::try_from(schema)?);
                let plan = LogicalPlan::EmptyRelation(EmptyRelation {
                    produce_one_row: false,
                    schema: df_schema,
                });
                return Ok(Some(plan));
            }
        }

        Ok(None)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_relation_planner(Arc::new(ValuesPlanner))?;
    ctx.register_relation_planner(Arc::new(MatchRecognizePlanner))?;

    // The query will not execute, but we can still inspect the logical plan
    let plan = ctx
        .sql("SELECT * FROM events MATCH_RECOGNIZE (PARTITION BY 1 MEASURES SUM(price) AS total_price, AVG(price) AS avg_price PATTERN (A) DEFINE A AS price > 10) AS t")
        .await?
        .into_unoptimized_plan();

    println!("Logical Plan:\n{}", plan.display_indent());

    Ok(())
}
