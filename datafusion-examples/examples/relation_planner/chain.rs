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

mod support;

use support::{plan_sql, InMemoryContextProvider};

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::{provider_as_source, MemTable, TableProvider};
use datafusion::error::Result;
use datafusion::logical_expr::{lit, LogicalPlan, LogicalPlanBuilder};
use datafusion::sql::planner::{
    PlannerContext, RelationPlanner, RelationPlannerDelegate, SqlToRel,
};
use datafusion::sql::sqlparser::ast::TableFactor;

/// Wraps every planned table relation in a LIMIT operator.
struct LimitWrapperPlanner {
    fetch: usize,
}

impl RelationPlanner<InMemoryContextProvider> for LimitWrapperPlanner {
    fn plan_relation(
        &self,
        relation: &TableFactor,
        _sql_to_rel: &SqlToRel<'_, InMemoryContextProvider>,
        delegate: &mut dyn RelationPlannerDelegate<'_, InMemoryContextProvider>,
        planner_context: &mut PlannerContext,
    ) -> Result<Option<LogicalPlan>> {
        if matches!(relation, TableFactor::Table { .. }) {
            if let Some(plan) = delegate.plan_next(relation, planner_context)? {
                let plan = LogicalPlanBuilder::from(plan)
                    .limit(0, Some(self.fetch))?
                    .build()?;
                return Ok(Some(plan));
            }

            let plan = delegate.plan_default(relation, planner_context)?;
            let plan = LogicalPlanBuilder::from(plan)
                .limit(0, Some(self.fetch))?
                .build()?;
            return Ok(Some(plan));
        }

        delegate.plan_next(relation, planner_context)
    }
}

/// Provides synthetic series tables such as `series_5` via VALUES clauses.
struct SeriesPlanner;

impl RelationPlanner<InMemoryContextProvider> for SeriesPlanner {
    fn plan_relation(
        &self,
        relation: &TableFactor,
        _sql_to_rel: &SqlToRel<'_, InMemoryContextProvider>,
        delegate: &mut dyn RelationPlannerDelegate<'_, InMemoryContextProvider>,
        planner_context: &mut PlannerContext,
    ) -> Result<Option<LogicalPlan>> {
        if let TableFactor::Table { name, .. } = relation {
            let ident = name.to_string();
            if let Some(size) = ident.strip_prefix("series_") {
                if let Ok(count) = size.parse::<usize>() {
                    if count == 0 {
                        return delegate.plan_next(relation, planner_context);
                    }
                    let rows = (0..count)
                        .map(|value| vec![lit(value as i64)])
                        .collect::<Vec<_>>();
                    let plan = LogicalPlanBuilder::values(rows)?.build()?;
                    return Ok(Some(plan));
                }
            }
        }

        delegate.plan_next(relation, planner_context)
    }
}

/// Delegates to the default planner and then de-duplicates results.
struct DistinctPlanner;

impl RelationPlanner<InMemoryContextProvider> for DistinctPlanner {
    fn plan_relation(
        &self,
        relation: &TableFactor,
        _sql_to_rel: &SqlToRel<'_, InMemoryContextProvider>,
        delegate: &mut dyn RelationPlannerDelegate<'_, InMemoryContextProvider>,
        planner_context: &mut PlannerContext,
    ) -> Result<Option<LogicalPlan>> {
        if let TableFactor::Table { name, .. } = relation {
            if name.to_string().ends_with("_dedup") {
                let plan = delegate.plan_default(relation, planner_context)?;
                let plan = LogicalPlanBuilder::from(plan).distinct()?.build()?;
                return Ok(Some(plan));
            }
        }

        delegate.plan_next(relation, planner_context)
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

fn main() -> Result<()> {
    let table = build_numbers_table()?;
    let table_source = provider_as_source(Arc::clone(&table));

    let mut provider = InMemoryContextProvider::default();
    provider.register_bare_table("numbers", Arc::clone(&table_source));
    provider.register_bare_table("numbers_dedup", table_source);

    let planners: Vec<Arc<dyn RelationPlanner<InMemoryContextProvider> + Send + Sync>> = vec![
        Arc::new(LimitWrapperPlanner { fetch: 2 }),
        Arc::new(DistinctPlanner),
        Arc::new(SeriesPlanner),
    ];
    let sql_to_rel = SqlToRel::new(&provider).with_relation_planners(planners);

    let examples = [
        ("Synthetic series", "SELECT * FROM series_5 AS s"),
        ("Distinct table rewrites", "SELECT * FROM numbers_dedup"),
        ("Default table fallback", "SELECT * FROM numbers"),
    ];

    for (label, sql) in examples {
        let plan = plan_sql(&sql_to_rel, sql)?;
        println!("{label}:\n{}\n", plan.display_indent());
    }

    Ok(())
}
