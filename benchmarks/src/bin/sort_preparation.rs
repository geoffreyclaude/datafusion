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

//! Heavily AI-assisted experiment measuring sort throughput and responsiveness
//! to another query on the same four-worker Tokio runtime.
//!
//! Q5 and Q10 are labels for the narrow and wide sort fixtures used in this
//! experiment, not implementations of the corresponding TPC-H queries. The SQL
//! reconstructs sorting shapes from the original discussion; the original exact
//! SQL is unknown. The six cases combine these fixtures with Parquet input,
//! deterministically preloaded input, or one synthetic skewed partition.
//!
//! Build with `cargo build --profile release-nonlto -p datafusion-benchmarks
//! --bin sort_preparation`. Run the executable with `--help` for options.
//! Synthetic generation, optional Parquet preloading, and planning happen before
//! timing; direct Parquet scans remain timed. An OS thread submits one `SELECT 1`
//! probe at a time, sleeping after each response. Sampling is approximate;
//! unavailable resident-memory measurements are reported as null.
//!
//! `--validate` runs separately without timing or probes. It checks global key
//! order and compares row counts plus an order-independent fingerprint of every
//! projected column against the unsorted input. Equal-key rows may be reordered.
//! No hashing or per-row validation is performed during measured runs.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier, mpsc};
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array};
use arrow::compute::SortOptions;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow::row::{OwnedRow, RowConverter, SortField};
use clap::{Parser, ValueEnum};
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::{displayable, execute_stream};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion_common::instant::Instant;
use datafusion_common_runtime::SpawnedTask;
use futures::TryStreamExt;
use rand::{Rng, SeedableRng, rngs::StdRng};
use serde_json::json;

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;
#[cfg(all(not(feature = "snmalloc"), feature = "mimalloc"))]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Query {
    Q5,
    Q10,
}

impl Query {
    fn sql(self) -> &'static str {
        match self {
            Self::Q5 => {
                "SELECT l_linenumber, l_suppkey, l_orderkey FROM lineitem ORDER BY l_linenumber, l_suppkey, l_orderkey"
            }
            Self::Q10 => {
                "SELECT l_orderkey, l_suppkey, l_linenumber, l_comment, l_partkey, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem ORDER BY l_orderkey, l_suppkey, l_linenumber, l_comment"
            }
        }
    }
}

#[derive(Parser)]
struct Options {
    /// Parquet file or directory for lineitem, generated with tpchgen-cli SF1.
    #[arg(long, required_unless_present = "synthetic_skew")]
    lineitem: Option<PathBuf>,
    /// One partition of 6,001,215 rows; only the first quarter of batches has random keys.
    /// Uses 8,192-row batches and a zero in-place threshold regardless of --batch-size.
    #[arg(long)]
    synthetic_skew: bool,
    /// Load Parquet before timing, then feed zero-copy input slices into the sort.
    /// --batch-size controls the input slices; --output-batch-size controls execution.
    #[arg(long, conflicts_with = "synthetic_skew")]
    preload: bool,
    /// Check global key order and a complete-row fingerprint without benchmark timing.
    #[arg(long)]
    validate: bool,
    #[arg(long, default_value_t = 8192)]
    output_batch_size: usize,
    #[arg(long, default_value = "unlabelled")]
    label: String,
    #[arg(long, default_value_t = 32)]
    batch_size: usize,
    #[arg(long, value_enum, default_value_t = Query::Q5)]
    query: Query,
    #[arg(long, default_value_t = 2)]
    iterations: usize,
    #[arg(long, default_value_t = 1)]
    sample_ms: u64,
    #[arg(long, default_value_t = 10)]
    probe_ms: u64,
}

fn rss_bytes() -> Option<usize> {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status.lines().find_map(|line| {
                line.strip_prefix("VmRSS:")?
                    .split_whitespace()
                    .next()?
                    .parse::<usize>()
                    .ok()
                    .and_then(|kb| kb.checked_mul(1024))
            })
        })
}

fn percentile(sorted: &[f64], quantile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    sorted[((sorted.len() as f64 * quantile).ceil() as usize)
        .saturating_sub(1)
        .min(sorted.len() - 1)]
}

const SYNTHETIC_ROWS: usize = 6_001_215;
const SYNTHETIC_BATCH_SIZE: usize = 8192;
const SYNTHETIC_SQL: &str = "SELECT a, b, payload FROM skew ORDER BY a, b";

struct PreparedInput {
    table: MemTable,
    partition_rows: Vec<usize>,
    partition_batches: Vec<usize>,
}

impl PreparedInput {
    fn new(
        schema: SchemaRef,
        partitions: Vec<Vec<RecordBatch>>,
    ) -> Result<Self, DataFusionError> {
        let partition_rows = partitions
            .iter()
            .map(|partition| partition.iter().map(RecordBatch::num_rows).sum())
            .collect();
        let partition_batches = partitions.iter().map(Vec::len).collect();
        Ok(Self {
            table: MemTable::try_new(schema, partitions)?,
            partition_rows,
            partition_batches,
        })
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct RowDigest {
    rows: usize,
    hash_sum: u64,
    hash_xor: u64,
}

impl RowDigest {
    fn add_batch(
        &mut self,
        converter: &RowConverter,
        batch: &RecordBatch,
    ) -> Result<(), DataFusionError> {
        let rows = converter.convert_columns(batch.columns())?;
        for row in rows.iter() {
            let mut hasher = DefaultHasher::new();
            row.hash(&mut hasher);
            let hash = hasher.finish();
            self.hash_sum = self.hash_sum.wrapping_add(hash);
            self.hash_xor ^= hash;
        }
        self.rows += batch.num_rows();
        Ok(())
    }
}

async fn validate_query(
    context: &SessionContext,
    sql: &str,
    key_columns: usize,
) -> Result<RowDigest, DataFusionError> {
    let input_sql = sql.split_once(" ORDER BY ").unwrap().0;
    let input = context.sql(input_sql).await?;
    let schema = Arc::clone(input.schema().inner());
    let full_rows = RowConverter::new(
        schema
            .fields()
            .iter()
            .map(|field| SortField::new(field.data_type().clone()))
            .collect(),
    )?;
    let key_rows = RowConverter::new(
        schema.fields()[..key_columns]
            .iter()
            .map(|field| {
                SortField::new_with_options(
                    field.data_type().clone(),
                    SortOptions {
                        descending: false,
                        nulls_first: false,
                    },
                )
            })
            .collect(),
    )?;
    let mut expected = RowDigest::default();
    let mut input =
        execute_stream(input.create_physical_plan().await?, context.task_ctx())?;
    while let Some(batch) = input.try_next().await? {
        expected.add_batch(&full_rows, &batch)?;
    }

    let plan = context.sql(sql).await?.create_physical_plan().await?;
    eprintln!(
        "Validation plan:\n{}",
        displayable(plan.as_ref()).indent(true)
    );
    let mut output = execute_stream(plan, context.task_ctx())?;
    let mut actual = RowDigest::default();
    let mut previous: Option<OwnedRow> = None;
    while let Some(batch) = output.try_next().await? {
        let keys = key_rows.convert_columns(&batch.columns()[..key_columns])?;
        if batch.num_rows() != 0 {
            if let Some(previous) = previous.as_ref() {
                assert!(
                    previous.row() <= keys.row(0),
                    "keys out of order between output batches"
                );
            }
            for index in 1..batch.num_rows() {
                assert!(
                    keys.row(index - 1) <= keys.row(index),
                    "keys out of order within output batch"
                );
            }
            previous = Some(keys.row(batch.num_rows() - 1).owned());
        }
        actual.add_batch(&full_rows, &batch)?;
    }
    assert_eq!(actual, expected, "sort changed the complete-row multiset");
    Ok(actual)
}

fn synthetic_skew_table() -> Result<PreparedInput, DataFusionError> {
    let batch_count = SYNTHETIC_ROWS.div_ceil(SYNTHETIC_BATCH_SIZE);
    let random_batches = batch_count / 4;
    let mut rng = StdRng::seed_from_u64(0x26242);
    let mut batches = Vec::with_capacity(batch_count);
    for (batch_index, offset) in (0..SYNTHETIC_ROWS)
        .step_by(SYNTHETIC_BATCH_SIZE)
        .enumerate()
    {
        let rows = (SYNTHETIC_ROWS - offset).min(SYNTHETIC_BATCH_SIZE);
        let (a, b) = if batch_index < random_batches {
            (
                (0..rows).map(|_| rng.random::<i64>()).collect::<Vec<_>>(),
                (0..rows).map(|_| rng.random::<i64>()).collect::<Vec<_>>(),
            )
        } else {
            (vec![0_i64; rows], vec![0_i64; rows])
        };
        let payload =
            Int64Array::from_iter_values((offset..offset + rows).map(|i| i as i64));
        batches.push(RecordBatch::try_from_iter([
            ("a", Arc::new(Int64Array::from(a)) as ArrayRef),
            ("b", Arc::new(Int64Array::from(b)) as ArrayRef),
            ("payload", Arc::new(payload) as ArrayRef),
        ])?);
    }
    PreparedInput::new(batches[0].schema(), vec![batches])
}

async fn preloaded_table(
    path: &str,
    input_batch_size: usize,
) -> Result<PreparedInput, DataFusionError> {
    let loader = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(1)
            .with_batch_size(8192)
            .with_coalesce_batches(false),
    );
    loader
        .register_parquet("lineitem", path, ParquetReadOptions::default())
        .await?;
    let input = loader.table("lineitem").await?;
    let schema = Arc::clone(input.schema().inner());
    // Load a single ordered partition, then distribute whole batches explicitly.
    // A parallel repartition operator can assign batches according to task timing.
    let mut loaded = input.collect_partitioned().await?;
    assert_eq!(loaded.len(), 1);
    let mut partitions = vec![Vec::new(); 4];
    for (batch_index, batch) in loaded.pop().unwrap().into_iter().enumerate() {
        for offset in (0..batch.num_rows()).step_by(input_batch_size) {
            partitions[batch_index % 4].push(
                batch.slice(offset, (batch.num_rows() - offset).min(input_batch_size)),
            );
        }
    }
    assert!(partitions.iter().all(|partition| !partition.is_empty()));
    PreparedInput::new(schema, partitions)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = Options::parse();
    assert!(
        options.batch_size > 0
            && options.output_batch_size > 0
            && options.sample_ms > 0
            && options.probe_ms > 0
    );
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;
    let partitions = if options.synthetic_skew { 1 } else { 4 };
    let batch_size = if options.synthetic_skew {
        SYNTHETIC_BATCH_SIZE
    } else if options.preload {
        options.output_batch_size
    } else {
        options.batch_size
    };
    let sql = if options.synthetic_skew {
        SYNTHETIC_SQL
    } else {
        options.query.sql()
    };
    let mut config = SessionConfig::new()
        .with_target_partitions(partitions)
        .with_batch_size(batch_size)
        .with_coalesce_batches(false);
    if options.synthetic_skew {
        config = config.with_sort_in_place_threshold_bytes(0);
    }
    // RuntimeEnv's default pool is unbounded; the sort must not spill.
    let context = Arc::new(SessionContext::new_with_config(config));
    let mut input_partition_rows = None;
    let mut input_partition_batches = None;
    if options.synthetic_skew {
        // Construct all arrays before planning, starting monitors, or timing the query.
        let input = synthetic_skew_table()?;
        input_partition_rows = Some(input.partition_rows);
        input_partition_batches = Some(input.partition_batches);
        context.register_table("skew", Arc::new(input.table))?;
    } else if options.preload {
        let table = runtime.block_on(preloaded_table(
            options
                .lineitem
                .as_ref()
                .unwrap()
                .to_string_lossy()
                .as_ref(),
            options.batch_size,
        ))?;
        input_partition_rows = Some(table.partition_rows);
        input_partition_batches = Some(table.partition_batches);
        context.register_table("lineitem", Arc::new(table.table))?;
    } else {
        runtime.block_on(
            context.register_parquet(
                "lineitem",
                options
                    .lineitem
                    .as_ref()
                    .unwrap()
                    .to_string_lossy()
                    .as_ref(),
                ParquetReadOptions::default(),
            ),
        )?;
    }
    // Warm the probe's SQL and execution paths before measurement.
    runtime.block_on(async { context.sql("SELECT 1").await?.collect().await })?;

    if options.validate {
        let key_columns = if options.synthetic_skew {
            2
        } else {
            match options.query {
                Query::Q5 => 3,
                Query::Q10 => 4,
            }
        };
        let digest = runtime.block_on(validate_query(&context, sql, key_columns))?;
        println!(
            "{}",
            json!({
                "label": options.label,
                "query": if options.synthetic_skew { "synthetic_skew".to_string() } else { format!("{:?}", options.query) },
                "validate": true,
                "validation_passed": true,
                "validation_global_key_order": true,
                "validation_key_columns": key_columns,
                "validation_rows": digest.rows,
                "validation_hash_sum": format!("{:016x}", digest.hash_sum),
                "validation_hash_xor": format!("{:016x}", digest.hash_xor),
                "validation_hash_algorithm": "DefaultHasher::new per complete Arrow row; wrapping sum and xor",
                "preload": options.preload,
                "synthetic_skew": options.synthetic_skew,
                "sql": sql,
                "batch_size": batch_size,
                "input_batch_size": if options.synthetic_skew { SYNTHETIC_BATCH_SIZE } else { options.batch_size },
                "workers": 4,
                "partitions": partitions,
                "input_partition_rows": input_partition_rows,
                "input_partition_batches": input_partition_batches,
            })
        );
        return Ok(());
    }

    for iteration in 0..options.iterations {
        let plan = runtime
            .block_on(async { context.sql(sql).await?.create_physical_plan().await })?;
        let plan_text = format!("{}", displayable(plan.as_ref()).indent(true));
        eprintln!("{} iteration {iteration}:\n{plan_text}", options.label);
        assert!(plan_text.contains("SortExec"));
        assert!(!plan_text.contains("fetch="), "TopK must not be selected");

        let stopped = Arc::new(AtomicBool::new(false));
        let start_barrier = Arc::new(Barrier::new(3));
        let trace_origin = Instant::now();
        let metrics = runtime.handle().metrics();
        let sample_stop = Arc::clone(&stopped);
        let sample_start = Arc::clone(&start_barrier);
        let pool = Arc::clone(&context.runtime_env().memory_pool);
        let sample_interval = Duration::from_millis(options.sample_ms);
        let sampler = std::thread::spawn(move || {
            let mut peak_tasks = 0;
            let mut peak_rss = None;
            let mut peak_reserved = 0;
            let mut samples = 0;
            sample_start.wait();
            while !sample_stop.load(Ordering::Acquire) {
                peak_tasks = peak_tasks.max(metrics.num_alive_tasks());
                peak_rss = peak_rss.max(rss_bytes());
                peak_reserved = peak_reserved.max(pool.reserved());
                samples += 1;
                std::thread::sleep(sample_interval);
            }
            (peak_tasks, peak_rss, peak_reserved, samples)
        });

        let probe_stop = Arc::clone(&stopped);
        let probe_start = Arc::clone(&start_barrier);
        let probe_context = Arc::clone(&context);
        let handle = runtime.handle().clone();
        let probe_interval = Duration::from_millis(options.probe_ms);
        let probes = std::thread::spawn(move || {
            let mut latencies = Vec::new();
            let mut execution_latencies = Vec::new();
            let mut probe_trace = Vec::new();
            probe_start.wait();
            while !probe_stop.load(Ordering::Acquire) {
                let submitted = Instant::now();
                let (send, receive) = mpsc::sync_channel(1);
                let context = Arc::clone(&probe_context);
                handle.spawn(async move {
                    let execution_started = Instant::now();
                    let result = async {
                        let output = context.sql("SELECT 1").await?.collect().await?;
                        assert_eq!(
                            output.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                            1
                        );
                        Ok::<_, DataFusionError>(())
                    }
                    .await;
                    let _ = send.send((
                        submitted.elapsed().as_secs_f64() * 1000.0,
                        execution_started.elapsed().as_secs_f64() * 1000.0,
                        execution_started.duration_since(submitted).as_secs_f64()
                            * 1000.0,
                        result.map_err(|error| error.to_string()),
                    ));
                });
                let (latency, execution_latency, queue_delay, result) =
                    receive.recv().expect("probe task must respond");
                result.expect("SELECT 1 probe must succeed");
                latencies.push(latency);
                execution_latencies.push(execution_latency);
                probe_trace.push(json!({
                    "submitted_offset_ms": submitted.duration_since(trace_origin).as_secs_f64() * 1000.0,
                    "latency_ms": latency,
                    "execution_ms": execution_latency,
                    "initial_queue_delay_ms": queue_delay,
                }));
                std::thread::sleep(probe_interval);
            }
            (latencies, execution_latencies, probe_trace)
        });

        let rss_before = rss_bytes();
        let task_context = context.task_ctx();
        start_barrier.wait();
        let started = Instant::now();
        // Spawn the query so all of its execution uses the four worker threads.
        let output = runtime.block_on(async move {
            SpawnedTask::spawn(async move {
                let mut stream = execute_stream(plan, task_context)?;
                let mut rows = 0usize;
                let mut batches = 0usize;
                let mut first_output_ms = None;
                while let Some(batch) = stream.try_next().await? {
                    first_output_ms
                        .get_or_insert_with(|| started.elapsed().as_secs_f64() * 1000.0);
                    rows += batch.num_rows();
                    batches += 1;
                }
                Ok::<_, DataFusionError>((rows, batches, first_output_ms))
            })
            .await
        });
        let wall_ms = started.elapsed().as_secs_f64() * 1000.0;
        stopped.store(true, Ordering::Release);
        let (peak_tasks, peak_rss, peak_reserved, samples) = sampler.join().unwrap();
        let (mut latencies, mut execution_latencies, probe_trace) =
            probes.join().unwrap();
        let (rows, batches, first_output_ms) = output??;
        latencies.sort_by(f64::total_cmp);
        execution_latencies.sort_by(f64::total_cmp);
        println!(
            "{}",
            json!({
                "label": options.label,
                "iteration": iteration,
                "query": if options.synthetic_skew { "synthetic_skew".to_string() } else { format!("{:?}", options.query) },
                "synthetic_skew": options.synthetic_skew,
                "preload": options.preload,
                "validate": false,
                "validation_passed": null,
                "input_partition_rows": input_partition_rows,
                "input_partition_batches": input_partition_batches,
                "sql": sql,
                "batch_size": batch_size,
                "input_batch_size": if options.synthetic_skew { SYNTHETIC_BATCH_SIZE } else { options.batch_size },
                "workers": 4,
                "partitions": partitions,
                "coalesce_batches": false,
                "memory_pool": "unbounded",
                "rows": rows,
                "batches": batches,
                "wall_ms": wall_ms,
                "first_output_ms": first_output_ms,
                "query_started_offset_ms": started.duration_since(trace_origin).as_secs_f64() * 1000.0,
                "query_finished_offset_ms": started.duration_since(trace_origin).as_secs_f64() * 1000.0 + wall_ms,
                "sample_interval_ms": options.sample_ms,
                "samples": samples,
                "peak_live_tasks": peak_tasks,
                "rss_before_bytes": rss_before,
                "sampled_peak_rss_bytes": peak_rss,
                "sampled_peak_reserved_bytes": peak_reserved,
                "probe_gap_ms": options.probe_ms,
                "probe_policy": "one outstanding probe; OS thread sleeps after each response",
                "probe_count": latencies.len(),
                "probe_p50_ms": percentile(&latencies, 0.50),
                "probe_p95_ms": percentile(&latencies, 0.95),
                "probe_p99_ms": percentile(&latencies, 0.99),
                "probe_max_ms": latencies.last().copied().unwrap_or_default(),
                "probe_execution_p50_ms": percentile(&execution_latencies, 0.50),
                "probe_execution_p95_ms": percentile(&execution_latencies, 0.95),
                "probe_execution_p99_ms": percentile(&execution_latencies, 0.99),
                "probe_execution_max_ms": execution_latencies.last().copied().unwrap_or_default(),
                "probe_latencies_ms": latencies,
                "probe_trace": probe_trace,
            })
        );
    }
    Ok(())
}
