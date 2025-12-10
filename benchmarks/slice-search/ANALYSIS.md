# Slice search benchmark analysis

## Setup
- Benchmarks live in a dedicated `slice-search-bench` crate that only depends on Criterion. Each benchmark group measures `contains` and `binary_search` across a length set that is dense below 32 (1, 2, 4, 8, 12, 16, 24, 32) and continues with powers of two up to 1024 for `i32`, `i64`, and `String` data.【F:benchmarks/slice-search/src/lib.rs†L18-L49】【F:benchmarks/slice-search/benches/slice_search.rs†L23-L101】
- Criterion settings were shortened to speed iteration: 100ms warmup, 200ms measurement time, and 20 samples per benchmark.【F:benchmarks/slice-search/benches/slice_search.rs†L23-L101】
- Data generators create even-numbered sequences with a missing midpoint value so the target is absent and sits between the central elements, driving binary search down its deepest path.【F:benchmarks/slice-search/src/lib.rs†L23-L49】
- A fresh full run now lives in `results/slice_search_results.txt`, covering all three data types. The `plot_results.py` helper consumes Criterion's JSON output to draw `contains` vs `binary_search` curves per type and saves `results/slice_search_results.png` for quick inspection.【F:benchmarks/slice-search/results/slice_search_results.txt†L6-L307】【F:benchmarks/slice-search/results/plot_results.py†L1-L81】

## Findings
- `i32`: Binary search generally leads, but `contains` still wins at lengths 16 and 32 (4.04ns vs 5.70ns; 5.18ns vs 6.60ns). Beyond those dips, binary search regains the edge and is 3–9x faster by lengths 256–1024.【F:benchmarks/slice-search/results/slice_search_results.txt†L47-L103】
- `i64`: Linear search is ahead only through length 8 (3.78ns vs 4.22ns). Binary search overtakes at length 12 and is ~40% faster by length 32 (6.11ns vs 10.63ns), widening to an order-of-magnitude lead at 256+ elements.【F:benchmarks/slice-search/results/slice_search_results.txt†L125-L205】
- `String`: Linear search holds a small lead up to length 8, but binary search pulls ahead from length 12 onward—roughly 2x faster by length 32 (50.96ns vs 102.50ns) and over 20x faster by length 1024 (110.94ns vs 3.13µs).【F:benchmarks/slice-search/results/slice_search_results.txt†L208-L307】
