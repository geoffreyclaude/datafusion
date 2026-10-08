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

//! Batched slice search benchmarks.
//!
//! Each benchmark processes BATCH_SIZE lookups at once, representative of
//! production IN LIST processing where we check many values against a haystack.
//! Results are reported as total time for the batch; divide by BATCH_SIZE for
//! per-lookup cost.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

/// Set the current thread to highest QoS class on macOS (Apple Silicon).
/// This strongly encourages the scheduler to run on Performance cores.
#[cfg(target_os = "macos")]
fn prefer_performance_cores() {
    unsafe {
        // QOS_CLASS_USER_INTERACTIVE is the highest priority, strongly prefers P-cores
        libc::pthread_set_qos_class_self_np(
            libc::qos_class_t::QOS_CLASS_USER_INTERACTIVE,
            0,
        );
    }
}

#[cfg(not(target_os = "macos"))]
fn prefer_performance_cores() {
    // No-op on non-macOS platforms
}
use hashbrown::HashSet;
use slice_search_bench::branchless::branchless_batch_lookup;
use slice_search_bench::{
    generate_even_strings, generate_numeric_even, LENGTHS_128, LENGTHS_16, LENGTHS_256,
    LENGTHS_64,
};
use std::hash::Hash;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

/// Number of lookups per benchmark iteration.
/// Matches typical Arrow array batch sizes.
const BATCH_SIZE: usize = 8192;

/// Generate needles for batch lookup.
/// Mix of values that will hit (even) and miss (odd) in the haystack.
fn generate_numeric_needles<T, F>(count: usize, max_value: usize, to_type: F) -> Vec<T>
where
    F: Fn(usize) -> T,
{
    (0..count)
        .map(|i| {
            // Alternate between hits (even values in haystack) and misses (odd values)
            let v = (i * 7) % (max_value * 2); // spread across range, ~50% hit rate
            to_type(v)
        })
        .collect()
}

fn generate_string_needles(count: usize, max_value: usize) -> Vec<String> {
    (0..count)
        .map(|i| {
            let v = (i * 7) % (max_value * 2);
            // Match haystack format: "item_padding_________________{:08}" (32 chars)
            format!("item_padding_________________{v:08}")
        })
        .collect()
}

fn bench_numeric<T, F>(c: &mut Criterion, type_name: &str, lengths: &[usize], to_type: F)
where
    T: Ord + Copy + Hash + Default + Send + Sync + 'static,
    F: Fn(usize) -> T + Copy,
{
    let mut group = c.benchmark_group(format!("{type_name}_slice_search"));
    // Minimal config for fast benchmarks with meaningful stats
    group.sample_size(10); // Minimum for statistical significance
    group.measurement_time(Duration::from_millis(100));
    group.warm_up_time(Duration::from_millis(50));

    for &len in lengths {
        let (haystack_vec, _) = generate_numeric_even(len, to_type);
        let haystack = Arc::new(haystack_vec);
        let hashset: Arc<HashSet<T>> = Arc::new(haystack.iter().cloned().collect());
        let needles: Arc<Vec<T>> =
            Arc::new(generate_numeric_needles(BATCH_SIZE, len * 2, to_type));

        // Only run linear/binary_search for small sizes (not competitive above 64)
        if len <= 64 {
            group.bench_with_input(
                BenchmarkId::new("contains", len),
                &len,
                |b, &_len| {
                    let haystack = Arc::clone(&haystack);
                    let needles = Arc::clone(&needles);
                    b.iter(|| {
                        let haystack = black_box(haystack.as_slice());
                        black_box(&*needles)
                            .iter()
                            .filter(|needle| haystack.contains(needle))
                            .count()
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new("binary_search", len),
                &len,
                |b, &_len| {
                    let haystack = Arc::clone(&haystack);
                    let needles = Arc::clone(&needles);
                    b.iter(|| {
                        let haystack = black_box(haystack.as_slice());
                        black_box(&*needles)
                            .iter()
                            .filter(|needle| haystack.binary_search(needle).is_ok())
                            .count()
                    });
                },
            );
        }

        group.bench_with_input(BenchmarkId::new("hashset", len), &len, |b, &_len| {
            let hashset = Arc::clone(&hashset);
            let needles = Arc::clone(&needles);
            b.iter(|| {
                let hashset = black_box(&*hashset);
                black_box(&*needles)
                    .iter()
                    .filter(|needle| hashset.contains(*needle))
                    .count()
            });
        });

        group.bench_with_input(BenchmarkId::new("branchless", len), &len, |b, &_len| {
            let haystack = Arc::clone(&haystack);
            let needles = Arc::clone(&needles);
            b.iter(|| {
                branchless_batch_lookup(
                    black_box(haystack.as_slice()),
                    black_box(&needles),
                )
                .expect("LENGTHS should only contain supported sizes")
            });
        });
    }

    group.finish();
}

fn bench_strings(c: &mut Criterion) {
    let mut group = c.benchmark_group("str_slice_search");
    // Minimal config for fast benchmarks with meaningful stats
    group.sample_size(10); // Minimum for statistical significance
    group.measurement_time(Duration::from_millis(100));
    group.warm_up_time(Duration::from_millis(50));

    for &len in &LENGTHS_16 {
        let (haystack_vec, _) = generate_even_strings(len);
        let haystack = Arc::new(haystack_vec);
        let hashset: Arc<HashSet<String>> = Arc::new(haystack.iter().cloned().collect());
        let needles: Arc<Vec<String>> =
            Arc::new(generate_string_needles(BATCH_SIZE, len * 2));

        group.bench_with_input(BenchmarkId::new("contains", len), &len, |b, &_len| {
            let haystack = Arc::clone(&haystack);
            let needles = Arc::clone(&needles);
            b.iter(|| {
                let haystack = black_box(haystack.as_slice());
                black_box(&*needles)
                    .iter()
                    .filter(|needle| haystack.contains(needle))
                    .count()
            });
        });

        group.bench_with_input(
            BenchmarkId::new("binary_search", len),
            &len,
            |b, &_len| {
                let haystack = Arc::clone(&haystack);
                let needles = Arc::clone(&needles);
                b.iter(|| {
                    let haystack = black_box(haystack.as_slice());
                    black_box(&*needles)
                        .iter()
                        .filter(|needle| haystack.binary_search(needle).is_ok())
                        .count()
                });
            },
        );

        group.bench_with_input(BenchmarkId::new("hashset", len), &len, |b, &_len| {
            let hashset = Arc::clone(&hashset);
            let needles = Arc::clone(&needles);
            b.iter(|| {
                let hashset = black_box(&*hashset);
                black_box(&*needles)
                    .iter()
                    .filter(|needle| hashset.contains(needle.as_str()))
                    .count()
            });
        });

        // Note: branchless not implemented for strings (requires Copy trait)
    }

    group.finish();
}

fn slice_search(c: &mut Criterion) {
    // On Apple Silicon, prefer Performance cores for consistent results
    prefer_performance_cores();

    // i8: benchmark up to 256 (extended range for cutoff analysis)
    bench_numeric::<i8, _>(c, "i8", &LENGTHS_256, |v| v as i8);
    // i16: benchmark up to 128
    bench_numeric::<i16, _>(c, "i16", &LENGTHS_128, |v| v as i16);
    // i32/i64: benchmark up to 64
    bench_numeric::<i32, _>(c, "i32", &LENGTHS_64, |v| v as i32);
    bench_numeric::<i64, _>(c, "i64", &LENGTHS_64, |v| v as i64);
    // i128: benchmark up to 16
    bench_numeric::<i128, _>(c, "i128", &LENGTHS_16, |v| v as i128);
    // str: benchmark up to 16 (uses LENGTHS_16 internally)
    bench_strings(c);
}

criterion_group!(benches, slice_search);
criterion_main!(benches);
