# Slice search benchmark analysis

## Setup
- Benchmarks live in a dedicated `slice-search-bench` crate that only depends on Criterion, keeping compile times small. Each benchmark group measures `contains` and `binary_search` across power-of-two lengths from 1 to 1024 for `i32`, `i64`, and `String` data. Measurement time is 2s with 50 samples and a 1s warmup to balance stability and runtime.【F:benchmarks/slice-search/benches/slice_search.rs†L23-L101】
- Data generators create even-numbered sequences with a missing midpoint value so the target is absent and sits between the central elements, driving binary search down its deepest path.【F:benchmarks/slice-search/src/lib.rs†L18-L46】

## Findings
- For integer slices, binary search stays roughly constant while `contains` scales linearly. At length 1024, `contains` takes ~103ns for `i32` and ~275ns for `i64`, compared to ~15–17ns for `binary_search`.【F:benchmarks/slice-search/results/slice_search_benchmark.txt†L196-L210】【F:benchmarks/slice-search/results/slice_search_benchmark.txt†L381-L395】
- On tiny integer slices the gap is narrow (e.g., length 1: `contains` ~2.9ns vs `binary_search` ~1.7ns for `i32`), but the logarithmic behavior wins quickly as lengths grow.【F:benchmarks/slice-search/results/slice_search_benchmark.txt†L21-L38】
- String lookups show the largest divergence: at length 1024 the missing-target `contains` walk costs ~3.18µs while `binary_search` finishes in ~128ns, over an order of magnitude faster. Small slices remain similar (length 1: ~4.7ns each).【F:benchmarks/slice-search/results/slice_search_benchmark.txt†L400-L414】【F:benchmarks/slice-search/results/slice_search_benchmark.txt†L570-L584】

The raw Criterion output is captured in `results/slice_search_benchmark.txt` for full detail.【F:benchmarks/slice-search/results/slice_search_benchmark.txt†L1-L585】
