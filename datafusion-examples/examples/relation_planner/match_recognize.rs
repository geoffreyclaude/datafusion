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
use std::sync::Arc;

use datafusion::prelude::*;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::logical_plan::{
    builder::LogicalPlanBuilder, Extension, LogicalPlan,
};
use datafusion_expr::planner::{RelationPlanner, RelationPlannerContext};
use datafusion_expr::{DFSchemaRef, Expr, UserDefinedLogicalNode};
use datafusion_sql::sqlparser::ast::TableFactor;

#[derive(Debug)]
struct MiniMatchRecognizeNode {
    input: Arc<LogicalPlan>,
    schema: DFSchemaRef,
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

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MiniMatchRecognize")
    }

    fn from_template(
        &self,
        inputs: &[LogicalPlan],
        _exprs: &[Expr],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        Arc::new(Self {
            input: Arc::new(inputs[0].clone()),
            schema: Arc::clone(&self.schema),
        })
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
        if let TableFactor::MatchRecognize { table, .. } = relation {
            let input = context.plan_relation(*table.clone())?;
            let node = MiniMatchRecognizeNode {
                schema: input.schema().clone(),
                input: Arc::new(input),
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
                let plan = LogicalPlanBuilder::values(vec![vec![Expr::Literal(
                    ScalarValue::Int32(Some(42)),
                    None,
                )]])?
                .build()?;
                return Ok(Some(plan));
            }
        }

        Ok(None)
    }
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    ctx.register_relation_planner(Arc::new(ValuesPlanner))?;
    ctx.register_relation_planner(Arc::new(MatchRecognizePlanner))?;

    // The query will not execute, but we can still inspect the logical plan
    let plan = ctx
        .sql("SELECT * FROM MATCH_RECOGNIZE (PARTITION BY 1 MEASURES 1 AS x ONE ROW PER MATCH AFTER MATCH SKIP PAST LAST ROW PATTERN (A) DEFINE A AS true) AS t")
        .await?
        .to_logical_plan();

    println!("{}", plan.display_indent());

    Ok(())
}
