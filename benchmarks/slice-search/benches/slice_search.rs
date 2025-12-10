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

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use slice_search_bench::{generate_even_strings, generate_numeric_even, LENGTHS};
use std::sync::Arc;
use std::time::Duration;

fn bench_numeric<T, F>(c: &mut Criterion, type_name: &str, to_type: F)
where
    T: Ord + Clone + Send + Sync + 'static,
    F: Fn(usize) -> T + Copy,
{
    let mut group = c.benchmark_group(format!("{type_name}_slice_search"));
    group.measurement_time(Duration::from_secs(2));
    group.sample_size(50);
    group.warm_up_time(Duration::from_secs(1));

    for &len in &LENGTHS {
        let (data_vec, target_value) = generate_numeric_even(len, to_type);
        let data = Arc::new(data_vec);

        group.bench_with_input(BenchmarkId::new("contains", len), &len, |b, &_len| {
            let data = Arc::clone(&data);
            let target = target_value.clone();
            b.iter(|| black_box(data.as_slice()).contains(black_box(&target)));
        });

        group.bench_with_input(
            BenchmarkId::new("binary_search", len),
            &len,
            |b, &_len| {
                let data = Arc::clone(&data);
                let target = target_value.clone();
                b.iter(|| {
                    black_box(data.as_slice())
                        .binary_search(black_box(&target))
                        .is_ok()
                });
            },
        );
    }

    group.finish();
}

fn bench_strings(c: &mut Criterion) {
    let mut group = c.benchmark_group("str_slice_search");
    group.measurement_time(Duration::from_secs(2));
    group.sample_size(50);
    group.warm_up_time(Duration::from_secs(1));

    for &len in &LENGTHS {
        let (data_vec, target_value) = generate_even_strings(len);
        let data = Arc::new(data_vec);

        group.bench_with_input(BenchmarkId::new("contains", len), &len, |b, &_len| {
            let data = Arc::clone(&data);
            let target = target_value.clone();
            b.iter(|| black_box(data.as_slice()).contains(black_box(&target)));
        });

        group.bench_with_input(
            BenchmarkId::new("binary_search", len),
            &len,
            |b, &_len| {
                let data = Arc::clone(&data);
                let target = target_value.clone();
                b.iter(|| {
                    black_box(data.as_slice())
                        .binary_search(black_box(&target))
                        .is_ok()
                });
            },
        );
    }

    group.finish();
}

fn slice_search(c: &mut Criterion) {
    bench_numeric::<i32, _>(c, "i32", |v| v as i32);
    bench_numeric::<i64, _>(c, "i64", |v| v as i64);
    bench_strings(c);
}

criterion_group!(benches, slice_search);
criterion_main!(benches);
