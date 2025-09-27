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

mod relation_planner_support;

use relation_planner_support::{plan_sql, InMemoryContextProvider};

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

/// Relation planner that materializes `synthetic_numbers` using VALUES rows.
struct SyntheticNumbersPlanner;

impl RelationPlanner<InMemoryContextProvider> for SyntheticNumbersPlanner {
    fn plan_relation(
        &self,
        relation: &TableFactor,
        _sql_to_rel: &SqlToRel<'_, InMemoryContextProvider>,
        delegate: &mut dyn RelationPlannerDelegate<'_, InMemoryContextProvider>,
        planner_context: &mut PlannerContext,
    ) -> Result<Option<LogicalPlan>> {
        if let TableFactor::Table { name, .. } = relation {
            if name.to_string() == "synthetic_numbers" {
                let rows = vec![vec![lit(1_i64)], vec![lit(2_i64)], vec![lit(3_i64)]];
                let plan = LogicalPlanBuilder::values(rows)?.build()?;
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
        vec![Arc::new(Int64Array::from(vec![10_i64, 20, 30]))],
    )?;
    MemTable::try_new(schema, vec![vec![batch]])
        .map(|table| Arc::new(table) as Arc<dyn TableProvider>)
}

fn main() -> Result<()> {
    let table = build_numbers_table()?;

    let mut provider = InMemoryContextProvider::default();
    provider.register_bare_table("numbers", provider_as_source(Arc::clone(&table)));

    let relation_planners: Vec<
        Arc<dyn RelationPlanner<InMemoryContextProvider> + Send + Sync>,
    > = vec![Arc::new(SyntheticNumbersPlanner)];
    let sql_to_rel = SqlToRel::new(&provider).with_relation_planners(relation_planners);

    let synthetic_plan = plan_sql(&sql_to_rel, "SELECT * FROM synthetic_numbers AS s")?;
    println!(
        "Synthetic relation planned by extension:\n{}\n",
        synthetic_plan.display_indent()
    );

    let default_plan = plan_sql(&sql_to_rel, "SELECT * FROM numbers")?;
    println!(
        "Fallback to default planner for regular table:\n{}\n",
        default_plan.display_indent()
    );

    Ok(())
}
