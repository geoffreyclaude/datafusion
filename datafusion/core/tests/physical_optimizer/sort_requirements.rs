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

use arrow::array::{Int32Array, record_batch};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{JoinType, Result};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, OrderingRequirements};
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_optimizer::ensure_requirements::EnsureRequirements;
use datafusion_physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion_physical_optimizer::output_requirements::OutputRequirements;
use datafusion_physical_optimizer::sanity_checker::SanityCheckPlan;
use datafusion_physical_plan::execution_plan::CardinalityEffect;
use datafusion_physical_plan::limit::GlobalLimitExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    collect, get_plan_string,
};

use super::test_utils::{
    RequirementsTestExec, bounded_window_exec, create_test_schema3, hash_join_exec,
    memory_exec, sort_exec, sort_exec_with_fetch, sort_expr, union_exec,
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
        for fetch in [None, Some(2)] {
            let wrapper = RequirementsTestExec::new(Arc::clone(&input))
                .with_sort_requirement_pushdown()
                .with_limit_pushdown()
                .into_arc();
            let partitions = if fetch.is_some() { 1 } else { 2 };
            let optimized =
                optimize(sort_exec_with_fetch(ab.clone(), fetch, wrapper), partitions)?;
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
            if let Some(fetch) = fetch {
                let mut bounded_sort = false;
                wrapper.apply(|node| {
                    if let Some(sort) = node.downcast_ref::<SortExec>() {
                        bounded_sort |= sort.fetch() == Some(fetch);
                    }
                    Ok(TreeNodeRecursion::Continue)
                })?;
                assert!(bounded_sort, "{label} must retain a bounded sort");
            }
        }
    }
    Ok(())
}

#[test]
fn repartitioned_input_keeps_bounded_sort_above_wrapper() -> Result<()> {
    let schema = create_test_schema3()?;
    let join = hash_join_exec(
        memory_exec(&schema),
        memory_exec(&schema),
        vec![(col("a", &schema)?, col("a", &schema)?)],
        None,
        &JoinType::RightSemi,
    )?;
    let wrapper = RequirementsTestExec::new(join)
        .with_sort_requirement_pushdown()
        .with_limit_pushdown()
        .into_arc();
    let ordering = [sort_expr("a", &schema), sort_expr("b", &schema)].into();
    // The helper also verifies sanity and stability on a second requirements pass.
    let optimized = optimize(sort_exec_with_fetch(ordering, Some(2), wrapper), 2)?;
    let mut found_wrapper = false;
    let mut bounded_sort_above = false;
    optimized.apply(|node| {
        if node.is::<RequirementsTestExec>() {
            found_wrapper = true;
            assert_eq!(sort_count(node)?, 0);
            assert!(node.exists(|input| Ok(input.is::<RepartitionExec>()))?);
            return Ok(TreeNodeRecursion::Stop);
        }
        if let Some(sort) = node.downcast_ref::<SortExec>() {
            assert_eq!(sort.fetch(), Some(2));
            bounded_sort_above = true;
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    assert!(found_wrapper && bounded_sort_above);
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

#[tokio::test]
async fn query_topk_moves_below_limit_passthrough_wrapper() -> Result<()> {
    for partitions in [1, 2] {
        for skip in [0, 1] {
            let first = record_batch!(("a", Int32, [6, 1, 4]))?;
            let second = record_batch!(("a", Int32, [5, 2, 3]))?;
            let schema = first.schema();
            let batches = if partitions == 1 {
                vec![vec![first, second]]
            } else {
                vec![vec![first], vec![second]]
            };
            let input =
                MemorySourceConfig::try_new_exec(&batches, Arc::clone(&schema), None)?;
            let wrapper = RequirementsTestExec::new(input)
                .with_sort_requirement_pushdown()
                .with_limit_pushdown()
                .into_arc();
            let sorted = sort_exec([sort_expr("a", &schema)].into(), wrapper);
            let plan: Arc<dyn ExecutionPlan> =
                Arc::new(GlobalLimitExec::new(sorted, skip, Some(2)));
            let optimized = optimize_query(plan, partitions)?;
            let mut wrapper = None;
            let mut global_bound = false;
            optimized.apply(|node| {
                if node.is::<RequirementsTestExec>() {
                    wrapper = Some(Arc::clone(node));
                    return Ok(TreeNodeRecursion::Stop);
                }
                assert!(!node.is::<SortExec>(), "bounded sort stayed above wrapper");
                global_bound |= (node.is::<GlobalLimitExec>()
                    || node.is::<SortPreservingMergeExec>())
                    && node.fetch().is_some();
                Ok(TreeNodeRecursion::Continue)
            })?;
            let wrapper = wrapper.expect("custom wrapper must remain");
            let mut bounded_sorts = 0;
            wrapper.apply(|node| {
                if let Some(sort) = node.downcast_ref::<SortExec>() {
                    assert_eq!(sort.fetch(), Some(skip + 2));
                    bounded_sorts += 1;
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            assert!(bounded_sorts > 0, "TopK must remain bounded below wrapper");
            if partitions == 2 {
                assert!(global_bound, "local TopK needs a final global bound");
            }
            assert_eq!(optimized.output_partitioning().partition_count(), 1);
            let expected = if skip == 0 { vec![1, 2] } else { vec![2, 3] };
            assert_eq!(values(optimized).await?, expected);
        }
    }
    Ok(())
}

#[tokio::test]
async fn local_topk_preserves_each_partition_limit() -> Result<()> {
    let first = record_batch!(("a", Int32, [8, 1, 3]))?;
    let second = record_batch!(("a", Int32, [9, 2, 4]))?;
    let schema = first.schema();
    let input = MemorySourceConfig::try_new_exec(
        &[vec![first], vec![second]],
        Arc::clone(&schema),
        None,
    )?;
    let wrapper = RequirementsTestExec::new(input)
        .with_sort_requirement_pushdown()
        .with_limit_pushdown()
        .into_arc();
    let sorted: Arc<dyn ExecutionPlan> = Arc::new(
        SortExec::new([sort_expr("a", &schema)].into(), wrapper)
            .with_fetch(Some(2))
            .with_preserve_partitioning(true),
    );
    let optimized = optimize(sorted, 2)?;
    let mut found = false;
    optimized.apply(|node| {
        if node.is::<RequirementsTestExec>() {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        assert!(!node.is::<SortExec>(), "local TopK stayed above wrapper");
        Ok(TreeNodeRecursion::Continue)
    })?;
    assert!(found);
    assert_eq!(optimized.output_partitioning().partition_count(), 2);
    let mut bounded_sorts = 0;
    optimized.apply(|node| {
        assert!(!node.is::<SortPreservingMergeExec>());
        if let Some(sort) = node.downcast_ref::<SortExec>() {
            assert_eq!(sort.fetch(), Some(2));
            bounded_sorts += 1;
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    assert!(bounded_sorts > 0);
    let mut actual = values(optimized).await?;
    actual.sort_unstable();
    // Native TopK partitions share a pruning threshold. They may emit fewer
    // than four candidates, but a local bound must not cap the combined result at two.
    assert!((3..=4).contains(&actual.len()));
    assert!(actual.starts_with(&[1, 2]));
    assert!(actual.iter().all(|value| [1, 2, 3, 4].contains(value)));
    Ok(())
}

#[tokio::test]
async fn bounded_descendant_keeps_its_selected_rows() -> Result<()> {
    for offset_only in [false, true] {
        let (batch, expected) = if offset_only {
            (record_batch!(("a", Int32, [2, 1, 3]))?, 1)
        } else {
            (record_batch!(("a", Int32, [1, 4, 2, 3]))?, 3)
        };
        let schema = batch.schema();
        let input =
            MemorySourceConfig::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)?;
        let selected: Arc<dyn ExecutionPlan> = if offset_only {
            Arc::new(GlobalLimitExec::new(input, 1, None))
        } else {
            Arc::new(
                SortExec::new([sort_expr("a", &schema).reverse()].into(), input)
                    .with_fetch(Some(2)),
            )
        };
        let wrapper = RequirementsTestExec::new(selected)
            .with_sort_requirement_pushdown()
            .with_limit_pushdown()
            .into_arc();
        let sorted = sort_exec([sort_expr("a", &schema)].into(), wrapper);
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(sorted, 0, Some(1)));
        let optimized = optimize_query(plan, 1)?;
        let mut outer_topk = false;
        optimized.apply(|node| {
            if node.is::<RequirementsTestExec>() {
                assert!(outer_topk, "selection boundary must retain outer TopK");
                return Ok(TreeNodeRecursion::Stop);
            }
            if let Some(sort) = node.downcast_ref::<SortExec>() {
                assert_eq!(sort.fetch(), Some(1));
                outer_topk = true;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        let mut selection_retained = false;
        optimized.apply(|node| {
            if offset_only {
                if let Some(limit) = node.downcast_ref::<GlobalLimitExec>() {
                    selection_retained |= limit.skip() == 1;
                }
            } else if let Some(sort) = node.downcast_ref::<SortExec>() {
                selection_retained |=
                    sort.expr()[0].options.descending && sort.fetch() == Some(2);
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        assert!(selection_retained);
        assert_eq!(values(optimized).await?, [expected]);
    }
    Ok(())
}

#[tokio::test]
async fn partition_merging_custom_node_keeps_global_topk_above() -> Result<()> {
    let first = record_batch!(("a", Int32, [4, 1]), ("b", Int32, [0, 0]))?;
    let second = record_batch!(("a", Int32, [3, 2]), ("b", Int32, [0, 0]))?;
    let schema = first.schema();
    let input = MemorySourceConfig::try_new_exec(
        &[vec![first], vec![second]],
        Arc::clone(&schema),
        None,
    )?;
    let merge: Arc<dyn ExecutionPlan> = Arc::new(CustomMergeExec(
        SortPreservingMergeExec::new([sort_expr("a", &schema)].into(), input),
    ));
    let ordering = [sort_expr("a", &schema), sort_expr("b", &schema)].into();
    let plan: Arc<dyn ExecutionPlan> =
        Arc::new(GlobalLimitExec::new(sort_exec(ordering, merge), 0, Some(2)));
    let optimized = optimize_query(plan, 2)?;
    let mut bounded_sort_above = false;
    let mut found = false;
    optimized.apply(|node| {
        if node.is::<CustomMergeExec>() {
            assert!(
                bounded_sort_above,
                "merging custom node must retain outer TopK"
            );
            assert_eq!(node.output_partitioning().partition_count(), 1);
            assert_eq!(
                node.children()[0].output_partitioning().partition_count(),
                2
            );
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        if let Some(sort) = node.downcast_ref::<SortExec>() {
            assert_eq!(sort.fetch(), Some(2));
            bounded_sort_above = true;
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    assert!(found);
    assert_eq!(values(optimized).await?, [1, 2]);
    Ok(())
}

// Unlike a partition-preserving wrapper, this custom node combines all input
// partitions. Rebuilding follows the sorted child's complete ordering.
#[derive(Debug)]
struct CustomMergeExec(SortPreservingMergeExec);

impl DisplayAs for CustomMergeExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "CustomMergeExec")
    }
}

impl ExecutionPlan for CustomMergeExec {
    fn name(&self) -> &str {
        "CustomMergeExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.0.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![self.0.input()]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        self.0.required_input_ordering()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.0.maintains_input_order()
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn pushdown_sort_requirements(
        &self,
        ordering: &OrderingRequirements,
    ) -> Result<Option<Vec<Option<OrderingRequirements>>>> {
        Ok(Some(vec![Some(ordering.clone())]))
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input = children.swap_remove(0);
        let ordering = input
            .output_ordering()
            .cloned()
            .unwrap_or_else(|| self.0.expr().clone());
        Ok(Arc::new(Self(SortPreservingMergeExec::new(
            ordering, input,
        ))))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.0.execute(partition, context)
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.0.apply_expressions(f)
    }
}

fn optimize_query(
    mut plan: Arc<dyn ExecutionPlan>,
    partitions: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut config = ConfigOptions::new();
    config.execution.target_partitions = partitions;
    for rule in PhysicalOptimizer::new().rules {
        plan = rule.optimize(plan, &config)?;
    }
    // The requirements pass must remain stable after the full optimizer pipeline.
    let repeated = optimize(Arc::clone(&plan), partitions)?;
    assert_eq!(get_plan_string(&plan), get_plan_string(&repeated));
    Ok(plan)
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
