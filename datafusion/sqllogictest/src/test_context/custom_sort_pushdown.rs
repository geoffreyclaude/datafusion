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

use std::fmt::{self, Formatter};
use std::sync::Arc;

use arrow::array::record_batch;
use arrow::datatypes::{self as arrow_schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Result;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, Operator, TableType};
use datafusion::physical_expr::expressions::{binary, col, lit, negative};
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_expr::{PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::LocalLimitExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::test::TestMemoryExec;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    ReplaceChildrenOptions,
};
use datafusion::prelude::SessionContext;

/// Register custom plans whose native implementations are hidden from the optimizer.
pub(super) fn register_custom_sort_pushdown(ctx: &SessionContext) -> Result<()> {
    let batch = record_batch!(("a", Int32, [2, 1, 3]))?;
    let schema = batch.schema();
    let input = TestMemoryExec::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)?;
    let projection = ProjectionExec::try_new(
        [(negative(col("a", &schema)?, &schema)?, "a".to_string())],
        input,
    )?;
    ctx.register_table(
        "custom_negated_projection",
        Arc::new(CustomExec(Arc::new(projection))),
    )?;

    let batch = record_batch!(("a", Int32, [2, 1, 3]), ("b", Int32, [10, 30, 20]))?;
    let schema = batch.schema();
    let input = TestMemoryExec::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)?;
    let projection = ProjectionExec::try_new(
        [
            (col("b", &schema)?, "a".to_string()),
            (col("a", &schema)?, "b".to_string()),
        ],
        input,
    )?;
    ctx.register_table(
        "custom_reordered_projection",
        Arc::new(CustomExec(Arc::new(projection))),
    )?;

    let batch = record_batch!(("t", Int32, [1, 2, 3]), ("x", Int32, [0, 2, 4]))?;
    let schema = batch.schema();
    let input = TestMemoryExec::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)?;
    let predicate = binary(col("x", &schema)?, Operator::NotEq, lit(0i32), &schema)?;
    ctx.register_table(
        "custom_filtered_input",
        Arc::new(CustomExec(Arc::new(FilterExec::try_new(predicate, input)?))),
    )?;

    let batch = record_batch!(("a", Int32, [1, 2, 3]), ("b", Int32, [30, 20, 10]))?;
    let schema = batch.schema();
    let ordering = [PhysicalSortExpr::new_default(col("a", &schema)?)].into();
    let input = Arc::new(
        TestMemoryExec::try_new(&[vec![batch]], Arc::clone(&schema), None)?
            .try_with_sort_information(vec![ordering])?,
    );
    ctx.register_table(
        "custom_limited_input",
        Arc::new(CustomExec(Arc::new(LocalLimitExec::new(input, 2)))),
    )?;
    Ok(())
}

/// Exposes a native plan's children and properties under a distinct plan type,
/// exercising the generic custom-plan sort pushdown path.
#[derive(Debug, Clone)]
struct CustomExec(Arc<dyn ExecutionPlan>);

#[async_trait]
impl TableProvider for CustomExec {
    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _: &dyn Session,
        projection: Option<&[usize]>,
        _: &[Expr],
        _: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan: Arc<dyn ExecutionPlan> = Arc::new(self.clone());
        if let Some(projection) = projection {
            let expressions = ProjectionExprs::from_indices(projection, &self.0.schema());
            Ok(Arc::new(ProjectionExec::try_new(
                expressions.iter().cloned(),
                plan,
            )?))
        } else {
            Ok(plan)
        }
    }
}

impl DisplayAs for CustomExec {
    fn fmt_as(&self, display_type: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "Custom")?;
        self.0.fmt_as(display_type, f)
    }
}

impl ExecutionPlan for CustomExec {
    fn name(&self) -> &str {
        "CustomExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.0.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.0.children()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.0.maintains_input_order()
    }

    fn fetch(&self) -> Option<usize> {
        self.0.fetch()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self(Arc::clone(&self.0).replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?)))
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.0.apply_expressions(f)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.0.execute(partition, context)
    }
}
