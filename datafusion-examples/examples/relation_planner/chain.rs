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
use datafusion::sql::sqlparser::ast::{Ident, ObjectName, TableFactor};

struct LimitWrapperPlanner {
    fetch: usize,
}

impl SessionRelationPlanner for LimitWrapperPlanner {
    fn plan_relation<'sql, 'state>(
        &self,
        relation: &TableFactor,
        sql_to_rel: &SessionSqlToRel<'sql, 'state>,
        planner_context: &mut PlannerContext,
    ) -> Result<Option<LogicalPlan>> {
        if let TableFactor::Table { name, .. } = relation {
            let ident = name.to_string();
            if let Some(base) = ident.strip_prefix("limit_") {
                let mut nested = relation.clone();
                if let TableFactor::Table { name, .. } = &mut nested {
                    *name = ObjectName(vec![Ident::new(base)]);
                }
                let plan = sql_to_rel.plan_table_factor(nested, planner_context)?;
                let limited = LogicalPlanBuilder::from(plan)
                    .limit(0, Some(self.fetch))?
                    .build()?;
                return Ok(Some(limited));
            }
        }

        Ok(None)
    }
}

struct SeriesPlanner;

impl SessionRelationPlanner for SeriesPlanner {
    fn plan_relation<'sql, 'state>(
        &self,
        relation: &TableFactor,
        _sql_to_rel: &SessionSqlToRel<'sql, 'state>,
        _planner_context: &mut PlannerContext,
    ) -> Result<Option<LogicalPlan>> {
        if let TableFactor::Table { name, .. } = relation {
            let ident = name.to_string();
            if let Some(size) = ident.strip_prefix("series_") {
                if let Ok(count) = size.parse::<usize>() {
                    if count == 0 {
                        return Ok(None);
                    }
                    let rows = (0..count)
                        .map(|value| vec![lit(value as i64)])
                        .collect::<Vec<_>>();
                    let plan = LogicalPlanBuilder::values(rows)?.build()?;
                    return Ok(Some(plan));
                }
            }
        }

        Ok(None)
    }
}

struct DistinctPlanner;

impl SessionRelationPlanner for DistinctPlanner {
    fn plan_relation<'sql, 'state>(
        &self,
        relation: &TableFactor,
        sql_to_rel: &SessionSqlToRel<'sql, 'state>,
        planner_context: &mut PlannerContext,
    ) -> Result<Option<LogicalPlan>> {
        if let TableFactor::Table { name, .. } = relation {
            if let Some(base) = name.to_string().strip_suffix("_dedup") {
                let mut nested = relation.clone();
                if let TableFactor::Table { name, .. } = &mut nested {
                    *name = ObjectName(vec![Ident::new(base)]);
                }
                let plan = sql_to_rel.plan_table_factor(nested, planner_context)?;
                let deduped = LogicalPlanBuilder::from(plan).distinct()?.build()?;
                return Ok(Some(deduped));
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
        vec![Arc::new(Int64Array::from(vec![1_i64, 2, 2, 3, 3, 4]))],
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
    let numbers_table = build_numbers_table()?;

    let session = SessionContext::new();
    session.register_table("numbers", numbers_table)?;

    session.register_relation_planner(Arc::new(LimitWrapperPlanner { fetch: 2 }));
    session.register_relation_planner(Arc::new(DistinctPlanner));
    session.register_relation_planner(Arc::new(SeriesPlanner));

    let examples = [
        ("Synthetic series", "SELECT * FROM series_5 AS s"),
        (
            "Limit wrapper rewrites",
            "SELECT * FROM limit_numbers AS limited",
        ),
        (
            "Distinct table rewrites",
            "SELECT * FROM numbers_dedup AS deduped",
        ),
        ("Default table fallback", "SELECT * FROM numbers"),
    ];

    for (label, sql) in examples {
        let plan = plan_sql(&session, sql).await?;
        println!("{label}:\n{}\n", plan.display_indent());
    }

    Ok(())
}
