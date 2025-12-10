# Slice search benchmark analysis

## Setup
- Benchmarks live in a dedicated `slice-search-bench` crate that only depends on Criterion. Each benchmark group measures `contains` and `binary_search` across a length set that is dense below 32 (1, 2, 4, 8, 12, 16, 24, 32) and continues with powers of two up to 1024 for `i32`, `i64`, and `String` data.【F:benchmarks/slice-search/src/lib.rs†L18-L49】【F:benchmarks/slice-search/benches/slice_search.rs†L23-L101】
- Criterion settings were shortened to speed iteration: 100ms warmup, 200ms measurement time, and 20 samples per benchmark.【F:benchmarks/slice-search/benches/slice_search.rs†L23-L101】
- Data generators create even-numbered sequences with a missing midpoint value so the target is absent and sits between the central elements, driving binary search down its deepest path.【F:benchmarks/slice-search/src/lib.rs†L23-L49】

## Findings (i64 slices up to length 32)
- For a single-element slice, `binary_search` is ~24% faster (1.21ns vs 1.56ns).【F:benchmarks/slice-search/results/slice_search_i64_len32.txt†L1-L9】
- Linear `contains` pulls ahead on tiny slices: at lengths 2, 4, and 8 it holds a 5–10% lead (e.g., 1.81ns vs 1.93ns at length 2).【F:benchmarks/slice-search/results/slice_search_i64_len32.txt†L10-L30】
- The crossover happens between lengths 8 and 12; by length 12 `binary_search` is ~8% faster (3.77ns vs 4.01ns) and the gap widens with size. At length 32, `binary_search` is ~40% faster (4.25ns vs 7.23ns).【F:benchmarks/slice-search/results/slice_search_i64_len32.txt†L31-L67】

The raw Criterion output for this run is captured in `results/slice_search_i64_len32.txt`.【F:benchmarks/slice-search/results/slice_search_i64_len32.txt†L1-L68】
