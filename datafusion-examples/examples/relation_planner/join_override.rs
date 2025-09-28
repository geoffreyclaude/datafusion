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
use datafusion_expr::logical_plan::builder::LogicalPlanBuilder;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::planner::{RelationPlanner, RelationPlannerContext};
use datafusion_expr::Expr;
use datafusion_sql::sqlparser::ast::TableFactor;

#[derive(Debug)]
struct FixtureTables;

impl RelationPlanner for FixtureTables {
    fn plan_relation(
        &self,
        relation: &TableFactor,
        _context: &mut RelationPlannerContext<'_>,
    ) -> Result<Option<LogicalPlan>> {
        if let TableFactor::Table { name, .. } = relation {
            let values = match name.to_string().to_lowercase().as_str() {
                "left_source" => {
                    vec![vec![Expr::Literal(ScalarValue::UInt32(Some(1)), None)]]
                }
                "right_source" => {
                    vec![vec![Expr::Literal(ScalarValue::UInt32(Some(1)), None)]]
                }
                _ => return Ok(None),
            };

            let plan = LogicalPlanBuilder::values(values)?.build()?;
            return Ok(Some(plan));
        }

        Ok(None)
    }
}

#[derive(Debug)]
struct CrossJoinPlanner;

impl RelationPlanner for CrossJoinPlanner {
    fn plan_relation(
        &self,
        relation: &TableFactor,
        context: &mut RelationPlannerContext<'_>,
    ) -> Result<Option<LogicalPlan>> {
        if let TableFactor::NestedJoin {
            table_with_joins, ..
        } = relation
        {
            if table_with_joins.joins.len() != 1 {
                return Ok(None);
            }
            let left = context.plan_relation(table_with_joins.relation.clone())?;
            let right =
                context.plan_relation(table_with_joins.joins[0].relation.clone())?;
            let plan = LogicalPlanBuilder::from(left).cross_join(right)?.build()?;
            return Ok(Some(plan));
        }

        Ok(None)
    }
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    ctx.register_relation_planner(Arc::new(FixtureTables))?;
    ctx.register_relation_planner(Arc::new(CrossJoinPlanner))?;

    let df = ctx
        .sql("SELECT * FROM left_source AS l JOIN right_source AS r ON l.column1 = r.column1")
        .await?;
    df.show().await?;

    Ok(())
}
