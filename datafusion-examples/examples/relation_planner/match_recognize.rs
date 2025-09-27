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

use std::fmt;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::DFSchemaRef;
use datafusion::datasource::{provider_as_source, MemTable, TableProvider};
use datafusion::error::Result;
use datafusion::logical_expr::logical_plan::Extension;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::sql::planner::{
    PlannerContext, RelationPlanner, RelationPlannerDelegate, SqlToRel,
};
use datafusion::sql::sqlparser::ast::TableFactor;

/// Captures a minimal logical node that represents a planned MATCH_RECOGNIZE relation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
struct MatchRecognizeNode {
    input: LogicalPlan,
    partition_by: Vec<String>,
    order_by: Vec<String>,
    measures: Vec<String>,
    rows_per_match: Option<String>,
    after_match_skip: Option<String>,
    pattern: String,
    definitions: Vec<String>,
}

impl MatchRecognizeNode {
    fn new(
        input: LogicalPlan,
        partition_by: Vec<String>,
        order_by: Vec<String>,
        measures: Vec<String>,
        rows_per_match: Option<String>,
        after_match_skip: Option<String>,
        pattern: String,
        definitions: Vec<String>,
    ) -> Self {
        Self {
            input,
            partition_by,
            order_by,
            measures,
            rows_per_match,
            after_match_skip,
            pattern,
            definitions,
        }
    }
}

impl UserDefinedLogicalNodeCore for MatchRecognizeNode {
    fn name(&self) -> &str {
        "MatchRecognizeNode"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let format_list = |values: &[String]| values.join(", ");
        write!(
            f,
            "MatchRecognize(pattern={}, partition_by=[{}], order_by=[{}], measures=[{}]",
            self.pattern,
            format_list(&self.partition_by),
            format_list(&self.order_by),
            format_list(&self.measures)
        )?;
        if let Some(rows) = &self.rows_per_match {
            write!(f, ", rows_per_match={rows}")?;
        }
        if let Some(skip) = &self.after_match_skip {
            write!(f, ", {skip}")?;
        }
        if !self.definitions.is_empty() {
            write!(f, ", define=[{}]", format_list(&self.definitions))?;
        }
        write!(f, ")")
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        let mut cloned = self.clone();
        cloned.input = inputs
            .pop()
            .expect("MATCH_RECOGNIZE nodes expect a single input plan");
        Ok(cloned)
    }
}

/// Relation planner that rewrites MATCH_RECOGNIZE relations into the minimal
/// [`MatchRecognizeNode`] defined above.
struct MatchRecognizePlanner;

impl RelationPlanner<InMemoryContextProvider> for MatchRecognizePlanner {
    fn plan_relation(
        &self,
        relation: &TableFactor,
        _sql_to_rel: &SqlToRel<'_, InMemoryContextProvider>,
        delegate: &mut dyn RelationPlannerDelegate<'_, InMemoryContextProvider>,
        planner_context: &mut PlannerContext,
    ) -> Result<Option<LogicalPlan>> {
        if let TableFactor::MatchRecognize {
            table,
            partition_by,
            order_by,
            measures,
            rows_per_match,
            after_match_skip,
            pattern,
            symbols,
            ..
        } = relation
        {
            let input = delegate.plan_default(table.as_ref(), planner_context)?;
            let node = MatchRecognizeNode::new(
                input,
                partition_by.iter().map(ToString::to_string).collect(),
                order_by.iter().map(ToString::to_string).collect(),
                measures.iter().map(ToString::to_string).collect(),
                rows_per_match.as_ref().map(ToString::to_string),
                after_match_skip.as_ref().map(ToString::to_string),
                pattern.to_string(),
                symbols.iter().map(ToString::to_string).collect(),
            );
            let plan = LogicalPlan::Extension(Extension {
                node: Arc::new(node),
            });
            Ok(Some(plan))
        } else {
            delegate.plan_next(relation, planner_context)
        }
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
        vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4]))],
    )?;
    MemTable::try_new(schema, vec![vec![batch]])
        .map(|table| Arc::new(table) as Arc<dyn TableProvider>)
}

fn main() -> Result<()> {
    let table = build_numbers_table()?;
    let mut provider = InMemoryContextProvider::default();
    provider.register_bare_table("numbers", provider_as_source(table));

    let planners: Vec<Arc<dyn RelationPlanner<InMemoryContextProvider> + Send + Sync>> =
        vec![Arc::new(MatchRecognizePlanner)];
    let sql_to_rel = SqlToRel::new(&provider).with_relation_planners(planners);

    let match_recognize_sql = r#"
        SELECT *
        FROM numbers MATCH_RECOGNIZE (
            PARTITION BY value
            ORDER BY value
            MEASURES
                MATCH_NUMBER() AS match_num
            PATTERN (A+)
            DEFINE
                A AS value > 0
        ) AS mr
    "#;

    let plan = plan_sql(&sql_to_rel, match_recognize_sql)?;
    println!(
        "MATCH_RECOGNIZE relation planned via extension:\n{}\n",
        plan.display_indent()
    );

    let fallback_plan = plan_sql(&sql_to_rel, "SELECT * FROM numbers")?;
    println!(
        "Fallback to default planner for base table:\n{}\n",
        fallback_plan.display_indent()
    );

    Ok(())
}
