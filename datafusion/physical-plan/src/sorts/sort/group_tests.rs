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

use super::{ExternalSorter, sort_batch_chunked};
use crate::metrics::ExecutionPlanMetricsSet;
use crate::spill::get_record_batch_memory_size;
use crate::test::assert_is_pending;
use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use datafusion_common::config::SpillCompression;
use datafusion_common::{Result, internal_datafusion_err};
use datafusion_execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_physical_expr::PhysicalSortExpr;
use datafusion_physical_expr::expressions::Column;
use futures::FutureExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Stop a real group after its first run. It then owns prepared output, the
/// unsorted tail, and the input reservation shared by all three sliced runs.
async fn check_group_reservations_released(fail_second_run: bool) -> Result<()> {
    let pool = Arc::new(GreedyMemoryPool::new(1024 * 1024));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool) as Arc<dyn MemoryPool>)
        .build_arc()?;
    let rows_per_run = 4096;
    let parent = RecordBatch::try_from_iter([(
        "key",
        Arc::new(Int64Array::from_iter_values(
            (0..3 * rows_per_run).rev().map(|value| value as i64),
        )) as ArrayRef,
    )])?;
    let ordering = [PhysicalSortExpr::new_default(Arc::new(Column::new(
        "key", 0,
    )))]
    .into();
    let mut sorter = ExternalSorter::new(
        0,
        parent.schema(),
        ordering,
        1024,
        0,
        0,
        SpillCompression::Uncompressed,
        &ExecutionPlanMetricsSet::new(),
        runtime,
    )?;
    for run in 0..3 {
        sorter
            .insert_batch(parent.slice(run * rows_per_run, rows_per_run))
            .await?;
    }
    drop(parent);

    let runs = std::mem::take(&mut sorter.in_mem_batches);
    let first_output_bytes: usize =
        sort_batch_chunked(&runs[0], &sorter.expr, sorter.batch_size)?
            .iter()
            .map(get_record_batch_memory_size)
            .sum();
    let (sizes, shared_input_bytes) = sorter.sort_run_sizes(&runs)?;
    let shared_input_bytes = shared_input_bytes.expect("sliced runs share their input");
    let pending_input_bytes: usize = sizes[1..].iter().sum();
    let all_run_bytes: usize = sizes.iter().sum();
    let shared_input = Arc::new(sorter.reservation.split(shared_input_bytes));
    let shared_input_weak = Arc::downgrade(&shared_input);
    let reservation = Arc::new(sorter.reservation.take());
    let reservation_weak = Arc::downgrade(&reservation);
    let expressions = sorter.expr.clone();
    let batch_size = sorter.batch_size;
    let merge_pool = Arc::clone(&sorter.merge_pool);
    let elapsed_compute = sorter.metrics.baseline.elapsed_compute().clone();
    let started = Arc::new(AtomicUsize::new(0));
    let started_by_group = Arc::clone(&started);
    let sorts =
        runs.into_iter()
            .zip(sizes)
            .enumerate()
            .map(move |(index, (batch, size))| {
                started_by_group.fetch_add(1, Ordering::Relaxed);
                if fail_second_run && index == 1 {
                    return Err(internal_datafusion_err!("second run failed"));
                }
                ExternalSorter::sort_run_with_resources(
                    batch,
                    reservation.split(size),
                    Some(Arc::clone(&shared_input)),
                    expressions.clone(),
                    batch_size,
                    Arc::clone(&merge_pool),
                    elapsed_compute.clone(),
                )
            });
    let mut group = ExternalSorter::sort_run_group(sorts).boxed();
    // No sorter remains to release the group's reservations on its behalf.
    drop(sorter);
    assert_eq!(pool.reserved(), shared_input_bytes + all_run_bytes);

    assert_is_pending(&mut group);
    assert_eq!(started.load(Ordering::Relaxed), 1);
    assert_eq!(
        reservation_weak.upgrade().unwrap().size(),
        pending_input_bytes
    );
    assert_eq!(
        shared_input_weak.upgrade().unwrap().size(),
        shared_input_bytes
    );
    assert!(first_output_bytes > 0);
    assert_eq!(
        pool.reserved(),
        first_output_bytes + pending_input_bytes + shared_input_bytes
    );

    if fail_second_run {
        let error = group.await.err().expect("second run must fail");
        assert!(error.to_string().contains("second run failed"));
        assert_eq!(started.load(Ordering::Relaxed), 2);
    } else {
        drop(group);
        assert_eq!(started.load(Ordering::Relaxed), 1);
    }
    assert!(reservation_weak.upgrade().is_none());
    assert!(shared_input_weak.upgrade().is_none());
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn sort_run_group_drop_releases_prepared_and_pending_reservations() -> Result<()> {
    check_group_reservations_released(false).await
}

#[tokio::test]
async fn sort_run_group_error_releases_prepared_and_pending_reservations() -> Result<()> {
    check_group_reservations_released(true).await
}
