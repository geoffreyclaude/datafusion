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

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::execution::context::{
    SessionContext, SessionRelationPlanner, SessionSqlToRel,
};
use datafusion::logical_expr::{lit, LogicalPlan, LogicalPlanBuilder};
use datafusion::sql::parser::DFParser;
use datafusion::sql::planner::PlannerContext;
use datafusion::sql::sqlparser::ast::TableFactor;

struct SyntheticNumbersPlanner;

impl SessionRelationPlanner for SyntheticNumbersPlanner {
    fn plan_relation<'sql, 'state>(
        &self,
        relation: &TableFactor,
        _sql_to_rel: &SessionSqlToRel<'sql, 'state>,
        _planner_context: &mut PlannerContext,
    ) -> Result<Option<LogicalPlan>> {
        if let TableFactor::Table { name, .. } = relation {
            if name.to_string() == "synthetic_numbers" {
                let rows = vec![vec![lit(1_i64)], vec![lit(2_i64)], vec![lit(3_i64)]];
                let plan = LogicalPlanBuilder::values(rows)?.build()?;
                return Ok(Some(plan));
            }
        }

        Ok(None)
    }
}

fn build_numbers_table() -> Result<Arc<dyn TableProvider>> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![10_i64, 20, 30]))],
    )?;
    MemTable::try_new(schema, vec![vec![batch]])
        .map(|table| Arc::new(table) as Arc<dyn TableProvider>)
}

async fn plan_sql(session: &SessionContext, sql: &str) -> Result<LogicalPlan> {
    let mut statements = DFParser::parse_sql(sql)?;
    let statement = statements.pop_front().ok_or_else(|| {
        DataFusionError::Plan(format!("SQL string contained no statements: {sql}"))
    })?;
    session.state().statement_to_plan(statement).await
}

#[tokio::main]
async fn main() -> Result<()> {
    let table = build_numbers_table()?;

    let session = SessionContext::new();
    session.register_table("numbers", Arc::clone(&table))?;
    session.register_relation_planner(Arc::new(SyntheticNumbersPlanner));

    let synthetic_plan =
        plan_sql(&session, "SELECT * FROM synthetic_numbers AS s").await?;
    println!(
        "Synthetic relation planned by extension:\n{}\n",
        synthetic_plan.display_indent()
    );

    let default_plan = plan_sql(&session, "SELECT * FROM numbers").await?;
    println!(
        "Fallback to default planner for regular table:\n{}\n",
        default_plan.display_indent()
    );

    Ok(())
}
