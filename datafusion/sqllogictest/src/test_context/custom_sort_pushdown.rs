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
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{Column, col, negative};
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_expr_common::sort_expr::{
    LexRequirement, OrderingRequirements, PhysicalSortRequirement,
};
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::test::TestMemoryExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use datafusion::prelude::SessionContext;

/// Register custom projections with and without explicit sort propagation.
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
        Arc::new(CustomProjection {
            projection,
            enable_sort_pushdown: false,
        }),
    )?;
    let batch = record_batch!(
        ("a", Int32, [Some(2), Some(1), Some(3), None]),
        ("b", Int32, [4, 6, 5, 7])
    )?;
    let schema = batch.schema();
    let input = TestMemoryExec::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)?;
    let projection = ProjectionExec::try_new(
        [
            (negative(col("b", &schema)?, &schema)?, "neg_b".to_string()),
            (col("a", &schema)?, "key".to_string()),
        ],
        input,
    )?;
    ctx.register_table(
        "custom_mixed_projection",
        Arc::new(CustomProjection {
            projection,
            enable_sort_pushdown: true,
        }),
    )?;
    Ok(())
}

/// Keeping a distinct plan type exercises custom-plan sort propagation rather
/// than the optimizer's specialized handling for native projections.
#[derive(Debug, Clone)]
struct CustomProjection {
    projection: ProjectionExec,
    enable_sort_pushdown: bool,
}

#[async_trait]
impl TableProvider for CustomProjection {
    fn schema(&self) -> SchemaRef {
        self.projection.schema()
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
            let expressions =
                ProjectionExprs::from_indices(projection, &self.projection.schema());
            Ok(Arc::new(ProjectionExec::try_new(
                expressions.iter().cloned(),
                plan,
            )?))
        } else {
            Ok(plan)
        }
    }
}

impl DisplayAs for CustomProjection {
    fn fmt_as(&self, display_type: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "Custom")?;
        self.projection.fmt_as(display_type, f)
    }
}

impl ExecutionPlan for CustomProjection {
    fn name(&self) -> &str {
        "CustomProjectionExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.projection.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.projection.children()
    }

    // Preserving row positions alone does not identify safe sort expressions.
    fn maintains_input_order(&self) -> Vec<bool> {
        self.projection.maintains_input_order()
    }

    fn supports_limit_pushdown(&self) -> bool {
        self.projection.supports_limit_pushdown()
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        self.projection.cardinality_effect()
    }

    fn pushdown_sort_requirements(
        &self,
        required: &OrderingRequirements,
    ) -> Result<Option<Vec<Option<OrderingRequirements>>>> {
        if !self.enable_sort_pushdown {
            return Ok(None);
        }
        let (alternatives, soft) = required.clone().into_alternatives();
        let mapped = alternatives.into_iter().filter_map(|requirement| {
            let mapped = requirement
                .iter()
                .map(|sort| {
                    let output = sort.expr.downcast_ref::<Column>()?;
                    let input = &self.projection.expr().get(output.index())?.expr;
                    input.downcast_ref::<Column>()?;
                    Some(PhysicalSortRequirement::new(
                        Arc::clone(input),
                        sort.options,
                    ))
                })
                .collect::<Option<Vec<_>>>()?;
            LexRequirement::new(mapped)
        });
        Ok(OrderingRequirements::new_alternatives(mapped, soft)
            .map(|requirement| vec![Some(requirement)]))
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            projection: ProjectionExec::try_new(
                self.projection.expr().to_vec(),
                Arc::clone(&children[0]),
            )?,
            enable_sort_pushdown: self.enable_sort_pushdown,
        }))
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.projection.apply_expressions(f)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.projection.execute(partition, context)
    }
}
