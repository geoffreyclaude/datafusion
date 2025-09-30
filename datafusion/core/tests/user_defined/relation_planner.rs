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

use std::sync::{Arc, Mutex};

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::memory::MemTable;
use datafusion::common::test_util::batches_to_string;
use datafusion::prelude::*;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::logical_plan::builder::LogicalPlanBuilder;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::planner::{RelationPlanner, RelationPlannerContext};
use datafusion_expr::Expr;
use datafusion_sql::sqlparser::ast::TableFactor;

#[derive(Debug)]
struct ValuesRelationPlanner;

impl RelationPlanner for ValuesRelationPlanner {
    fn plan_relation(
        &self,
        relation: &TableFactor,
        _context: &mut dyn RelationPlannerContext,
    ) -> Result<Option<LogicalPlan>> {
        if let TableFactor::Table { name, .. } = relation {
            if name.to_string().eq_ignore_ascii_case("custom_values") {
                let plan = LogicalPlanBuilder::values(vec![vec![Expr::Literal(
                    ScalarValue::Int64(Some(42)),
                    None,
                )]])?
                .build()?;
                return Ok(Some(plan));
            }
        }
        Ok(None)
    }
}

#[derive(Debug, Default)]
struct DelegatingPlanner {
    seen: Arc<Mutex<Vec<String>>>,
}

impl DelegatingPlanner {
    fn new(seen: Arc<Mutex<Vec<String>>>) -> Self {
        Self { seen }
    }
}

impl RelationPlanner for DelegatingPlanner {
    fn plan_relation(
        &self,
        relation: &TableFactor,
        context: &mut dyn RelationPlannerContext,
    ) -> Result<Option<LogicalPlan>> {
        self.seen.lock().unwrap().push(format!("{relation:?}"));

        match relation {
            TableFactor::Table { .. } => {
                let table_plan = context.plan_default(relation)?;
                Ok(Some(table_plan))
            }
            _ => Ok(context.plan_next(relation)?),
        }
    }
}

#[derive(Debug)]
struct JoinPlanner;

impl RelationPlanner for JoinPlanner {
    fn plan_relation(
        &self,
        relation: &TableFactor,
        context: &mut dyn RelationPlannerContext,
    ) -> Result<Option<LogicalPlan>> {
        if let TableFactor::NestedJoin {
            table_with_joins, ..
        } = relation
        {
            if table_with_joins.joins.len() != 1 {
                return Ok(None);
            }
            let left = context.plan_relation(&table_with_joins.relation)?;
            let right = context.plan_relation(&table_with_joins.joins[0].relation)?;
            let plan = LogicalPlanBuilder::from(left).cross_join(right)?.build()?;
            Ok(Some(plan))
        } else {
            Ok(None)
        }
    }
}

async fn collect_sql(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql).await.unwrap().collect().await.unwrap()
}

#[tokio::test]
async fn basic_relation_planner_uses_custom_plan() {
    let ctx = SessionContext::new();
    ctx.register_relation_planner(Arc::new(ValuesRelationPlanner))
        .unwrap();

    let results = collect_sql(&ctx, "SELECT * FROM custom_values").await;

    let expected = "\
+---------+\n\
| column1 |\n\
+---------+\n\
| 42      |\n\
+---------+";
    assert_eq!(batches_to_string(&results), expected);
}

#[tokio::test]
async fn relation_planner_chain_and_default() {
    let ctx = SessionContext::new();
    let seen = Arc::new(Mutex::new(vec![]));
    ctx.register_relation_planner(Arc::new(DelegatingPlanner::new(seen.clone())))
        .unwrap();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        true,
    )]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))])
            .unwrap();
    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("t", Arc::new(table)).unwrap();

    let results = collect_sql(&ctx, "SELECT * FROM t").await;
    let expected = "\
+-------+\n\
| value |\n\
+-------+\n\
| 1     |\n\
+-------+";
    assert_eq!(batches_to_string(&results), expected);

    assert!(!seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn relation_planner_recurses_with_extensions() {
    let ctx = SessionContext::new();
    ctx.register_relation_planner(Arc::new(ValuesRelationPlanner))
        .unwrap();
    ctx.register_relation_planner(Arc::new(JoinPlanner))
        .unwrap();

    let results = collect_sql(
        &ctx,
        "SELECT * FROM custom_values AS l JOIN custom_values AS r ON true",
    )
    .await;

    let expected = "\
+---------+---------+\n\
| column1 | column1 |\n\
+---------+---------+\n\
| 42      | 42      |\n\
+---------+---------+";
    assert_eq!(batches_to_string(&results), expected);
}
