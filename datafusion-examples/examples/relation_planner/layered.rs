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

use datafusion::prelude::*;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::col;
use datafusion_expr::logical_plan::builder::LogicalPlanBuilder;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::planner::{RelationPlanner, RelationPlannerContext};
use datafusion_expr::Expr;
use datafusion_sql::sqlparser::ast::TableFactor;

#[derive(Debug)]
struct BaseValues;

impl RelationPlanner for BaseValues {
    fn plan_relation(
        &self,
        relation: &TableFactor,
        _context: &mut dyn RelationPlannerContext,
    ) -> Result<Option<LogicalPlan>> {
        if let TableFactor::Table { name, .. } = relation {
            if name.to_string().eq_ignore_ascii_case("shared") {
                let plan = LogicalPlanBuilder::values(vec![
                    vec![Expr::Literal(ScalarValue::Utf8(Some("alpha".into())), None)],
                    vec![Expr::Literal(ScalarValue::Utf8(Some("beta".into())), None)],
                ])?
                .build()?;
                return Ok(Some(plan));
            }
        }

        Ok(None)
    }
}

#[derive(Debug)]
struct Wrapper;

impl RelationPlanner for Wrapper {
    fn plan_relation(
        &self,
        relation: &TableFactor,
        context: &mut dyn RelationPlannerContext,
    ) -> Result<Option<LogicalPlan>> {
        if let TableFactor::Table { name, .. } = relation {
            if name.to_string().eq_ignore_ascii_case("shared") {
                if let Some(inner) = context.plan_next(relation)? {
                    let plan = LogicalPlanBuilder::from(inner)
                        .project(vec![
                            col("column1"),
                            Expr::Literal(
                                ScalarValue::Utf8(Some("wrapped".into())),
                                None,
                            ),
                        ])?
                        .build()?;
                    return Ok(Some(plan));
                }
            }
        }

        Ok(None)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_relation_planner(Arc::new(BaseValues))?;
    ctx.register_relation_planner(Arc::new(Wrapper))?;

    let df = ctx.sql("SELECT * FROM shared").await?;
    let plan = df.clone().into_unoptimized_plan();
    println!("Logical Plan:\n{}", plan.display_indent());
    df.show().await?;

    Ok(())
}
