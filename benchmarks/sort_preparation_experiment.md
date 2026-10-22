<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Sort preparation experiments

These fork-only experiments compare the bounded sorting tasks in
[DataFusion #26242](https://github.com/apache/datafusion/pull/26242) with:

- [Fixed groups (#21)](https://github.com/geoffreyclaude/datafusion/pull/21):
  divide each partition's runs into a fixed number of sorting tasks.
- [Small-run coalescing (#22)](https://github.com/geoffreyclaude/datafusion/pull/22):
  concatenate small batches before sorting, giving the merge fewer inputs.

A **run** is an independently sorted piece of input. The final merge combines
the runs into ordered output. Both experiments keep the shared task limit.

## Build and run

Build `sort_preparation` from the common baseline branch and the experiment's
branch, saving each executable before switching branches:

```bash
cargo build --locked --profile release-nonlto -p datafusion-benchmarks --bin sort_preparation
```

Use the same build options for both. With TPC-H SF1 `lineitem` Parquet data:

```bash
python3 benchmarks/sort_preparation_experiment.py \
  --baseline /path/to/baseline --grouped /path/to/grouped \
  --lineitem /path/to/lineitem.parquet --output-dir /tmp/sort-validation --validate

python3 benchmarks/sort_preparation_experiment.py \
  --baseline /path/to/baseline --grouped /path/to/grouped \
  --lineitem /path/to/lineitem.parquet --output-dir /tmp/sort-timing \
  --repetitions 12 --warmups 1
```

For #22, replace `--grouped` with `--coalesced`. Pass both to compare all three.
Each output directory must be new. Run after builds finish, on an otherwise
quiet machine. The runner records binary and dataset hashes and balances run
order. Validation checks key order and a full-row fingerprint separately from timing.

## Reading the results

The cases use 6,001,215 rows and four Tokio workers. `q5` and `q10` name
custom narrow-integer and wide-with-text sorts, respectively. Sort timings exclude
planning. Preloaded cases also exclude reading; Parquet cases include reading.
Tiny input batches stress run count with a modest dataset. The output batch
target is 8,192 rows.
The skew case places expensive sorts at the start of one partition to test
uneven work across fixed groups.

Results include complete execution time, first output, DataFusion memory
reservations, and delays of unrelated `SELECT 1` queries. These probes include
planning and execution. They run one at a time with a 10 ms pause after each
response, so short runs provide few tail samples. RSS sampling is available on
Linux. The pool is unbounded in timing
runs; focused tests exercise spilling.
