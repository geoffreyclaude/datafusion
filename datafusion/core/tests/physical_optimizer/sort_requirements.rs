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

use arrow::array::{Int32Array, record_batch};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{JoinType, Result};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_optimizer::ensure_requirements::EnsureRequirements;
use datafusion_physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion_physical_optimizer::output_requirements::OutputRequirements;
use datafusion_physical_optimizer::sanity_checker::SanityCheckPlan;
use datafusion_physical_plan::limit::GlobalLimitExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, collect, get_plan_string,
};

use super::test_utils::{
    RequirementsTestExec, bounded_window_exec, create_test_schema3, hash_join_exec,
    memory_exec, sort_exec, sort_expr, union_exec,
};

#[test]
fn only_opted_in_wrappers_consolidate_compatible_sorts() -> Result<()> {
    let schema = create_test_schema3()?;
    let a: LexOrdering = [sort_expr("a", &schema)].into();
    let ab: LexOrdering = [sort_expr("a", &schema), sort_expr("b", &schema)].into();
    for opted_in in [false, true] {
        let mut wrapper =
            RequirementsTestExec::new(sort_exec(a.clone(), memory_exec(&schema)))
                .with_required_input_ordering(Some(a.clone()));
        if opted_in {
            wrapper = wrapper.with_sort_requirement_pushdown();
        }
        let optimized = optimize(sort_exec(ab.clone(), wrapper.into_arc()), 1)?;
        assert_eq!(sort_count(&optimized)?, if opted_in { 1 } else { 2 });
        if opted_in {
            assert!(optimized.is::<RequirementsTestExec>());
            assert_eq!(
                optimized.children()[0]
                    .downcast_ref::<SortExec>()
                    .unwrap()
                    .expr(),
                &ab
            );
        } else {
            assert_eq!(optimized.downcast_ref::<SortExec>().unwrap().expr(), &ab);
        }
    }
    Ok(())
}

#[test]
fn conflicting_mandatory_order_keeps_both_sorts() -> Result<()> {
    let schema = create_test_schema3()?;
    let ab: LexOrdering = [sort_expr("a", &schema), sort_expr("b", &schema)].into();
    let ac: LexOrdering = [sort_expr("a", &schema), sort_expr("c", &schema)].into();
    let wrapper = RequirementsTestExec::new(sort_exec(ab.clone(), memory_exec(&schema)))
        .with_required_input_ordering(Some(ab.clone()))
        .with_sort_requirement_pushdown()
        .into_arc();
    let optimized = optimize(sort_exec(ac.clone(), wrapper), 1)?;
    assert_eq!(optimized.downcast_ref::<SortExec>().unwrap().expr(), &ac);
    let wrapper = optimized.children()[0];
    assert!(wrapper.is::<RequirementsTestExec>());
    assert!(
        wrapper.children()[0]
            .equivalence_properties()
            .ordering_satisfy(ab)?
    );
    assert_eq!(sort_count(&optimized)?, 2);
    Ok(())
}

#[test]
fn hook_recovers_existing_native_sort_placement() -> Result<()> {
    let schema = create_test_schema3()?;
    let a: LexOrdering = [sort_expr("a", &schema)].into();
    let ab: LexOrdering = [sort_expr("a", &schema), sort_expr("b", &schema)].into();
    let join = hash_join_exec(
        memory_exec(&schema),
        memory_exec(&schema),
        vec![(col("a", &schema)?, col("a", &schema)?)],
        None,
        &JoinType::RightSemi,
    )?;
    let cases = [
        ("sort", sort_exec(a.clone(), memory_exec(&schema))),
        ("join", join),
        (
            "union",
            union_exec(vec![memory_exec(&schema), memory_exec(&schema)]),
        ),
        ("window", bounded_window_exec("a", a, memory_exec(&schema))),
    ];
    for (label, input) in cases {
        let wrapper = RequirementsTestExec::new(input)
            .with_sort_requirement_pushdown()
            .into_arc();
        let optimized = optimize(sort_exec(ab.clone(), wrapper), 2)?;
        let mut found = None;
        optimized.apply(|node| {
            if node.is::<RequirementsTestExec>() {
                found = Some(Arc::clone(node));
                return Ok(TreeNodeRecursion::Stop);
            }
            assert!(
                !node.is::<SortExec>(),
                "sort remained above {label} wrapper"
            );
            Ok(TreeNodeRecursion::Continue)
        })?;
        let wrapper = found.expect("the custom wrapper must remain");
        assert!(
            wrapper
                .equivalence_properties()
                .ordering_satisfy(ab.clone())?,
            "{label}"
        );
        assert!(
            sort_count(&wrapper)? > 0,
            "{label} must still sort its input"
        );
    }
    Ok(())
}

#[tokio::test]
async fn sort_requirement_pushdown_preserves_global_order() -> Result<()> {
    let first = record_batch!(("a", Int32, [4, 1]))?;
    let second = record_batch!(("a", Int32, [3, 2]))?;
    let schema = first.schema();
    let input = MemorySourceConfig::try_new_exec(
        &[vec![first], vec![second]],
        Arc::clone(&schema),
        None,
    )?;
    let wrapper = RequirementsTestExec::new(input)
        .with_sort_requirement_pushdown()
        .into_arc();
    let optimized = optimize(sort_exec([sort_expr("a", &schema)].into(), wrapper), 2)?;
    assert!(optimized.is::<SortPreservingMergeExec>());
    assert!(optimized.children()[0].is::<RequirementsTestExec>());
    assert_eq!(optimized.output_partitioning().partition_count(), 1);
    assert!(sort_count(&optimized)? > 0);
    assert_eq!(values(optimized).await?, [1, 2, 3, 4]);
    Ok(())
}

#[tokio::test]
async fn query_limit_keeps_bounded_sort_above_opted_in_wrapper() -> Result<()> {
    let batch = record_batch!(("a", Int32, [3, 1, 2]))?;
    let schema = batch.schema();
    let input =
        MemorySourceConfig::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)?;
    let wrapper = RequirementsTestExec::new(input)
        .with_sort_requirement_pushdown()
        .into_arc();
    assert!(!wrapper.supports_limit_pushdown());
    let sorted = sort_exec([sort_expr("a", &schema)].into(), wrapper);
    let mut plan: Arc<dyn ExecutionPlan> =
        Arc::new(GlobalLimitExec::new(sorted, 0, Some(2)));
    let mut config = ConfigOptions::new();
    config.execution.target_partitions = 1;
    for rule in PhysicalOptimizer::new().rules {
        plan = rule.optimize(plan, &config)?;
    }
    let mut bounded_sorts = 0;
    let mut found = false;
    plan.apply(|node| {
        if node.is::<RequirementsTestExec>() {
            assert_eq!(sort_count(node)?, 0);
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        if let Some(sort) = node.downcast_ref::<SortExec>() {
            assert_eq!(sort.fetch(), Some(2));
            bounded_sorts += 1;
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    assert!(found);
    assert_eq!(bounded_sorts, 1);
    assert_eq!(values(plan).await?, [1, 2]);
    Ok(())
}

fn optimize(
    plan: Arc<dyn ExecutionPlan>,
    partitions: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut config = ConfigOptions::new();
    config.execution.target_partitions = partitions;
    config.optimizer.repartition_sorts = true;
    let plan = OutputRequirements::new_add_mode().optimize(plan, &config)?;
    let optimized = EnsureRequirements::new().optimize(plan, &config)?;
    SanityCheckPlan::new().optimize(Arc::clone(&optimized), &config)?;
    let repeated = EnsureRequirements::new().optimize(Arc::clone(&optimized), &config)?;
    assert_eq!(get_plan_string(&optimized), get_plan_string(&repeated));
    OutputRequirements::new_remove_mode().optimize(optimized, &config)
}

fn sort_count(plan: &Arc<dyn ExecutionPlan>) -> Result<usize> {
    let mut count = 0;
    plan.apply(|node| {
        count += usize::from(node.is::<SortExec>());
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(count)
}

async fn values(plan: Arc<dyn ExecutionPlan>) -> Result<Vec<i32>> {
    let batches = collect(plan, Arc::new(TaskContext::default())).await?;
    Ok(batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect())
}
