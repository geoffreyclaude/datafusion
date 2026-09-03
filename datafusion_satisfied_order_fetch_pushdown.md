# DataFusion Satisfied-Order Fetch Pushdown Follow-up

## Outcome

- [ ] Fix the satisfied-order fetch propagation defect in Apache DataFusion.
- [ ] Add one native DataFusion regression that proves a fetched output sort
      cannot become a fetch below a cardinality-changing operator.
- [ ] Verify the fix against this repository's four-partition
      `MATCH_RECOGNIZE` regression.
- [ ] Upgrade to the first fixed DataFusion release and remove the local output
      boundary when doing so is safe.

This is follow-up work, not part of the matcher output-pipeline refactor. The
current repository must retain `MatchOutputBoundaryExec` until the minimum
supported DataFusion version contains and passes the generic fix.

## Severity and invariant

This is a wrong-results bug in DataFusion 55.0.0. A fetched sort represents a
limit on rows produced by its input. Moving that fetch below an operator is safe
only when the operator cannot change how many rows flow through it.

The invariant is:

> An output fetch must not cross an `ExecutionPlan` whose cardinality is not
> exactly equal, unless that operator explicitly absorbs the fetch on its own
> output.

Ordering and cardinality are separate facts. An operator can preserve or
produce the requested order while filtering, grouping, joining, or matching a
different number of rows. Satisfying an ordering requirement therefore does not
make fetch propagation safe.

## Repository reproduction

The existing compatibility case is
[`rows_per_match__comparison_no_measures.slt`](../../match-recognize-testing-e2e/test_files/compatibility/trino/execution/clause/rows_per_match/rows_per_match__comparison_no_measures.slt).
It partitions by `l_orderkey`, emits one row per match, orders the result, and
applies `LIMIT 20`.

With four target partitions before the local fix, the result contained only
four partition keys: `1`, `3`, `5`, and `7`. The relevant bad plan shape was:

```text
SortPreservingMergeExec: [...], fetch=20
  MatchRecognizePatternExec: ...
    SortExec: TopK(fetch=20), ...
      RepartitionExec: ...
        SortPreservingMergeExec: [...], fetch=20
```

The outer fetch asks for 20 completed matches. The generated inner fetch keeps
only 20 source rows before matching, which is not equivalent: one match can
consume any number of source rows.

## Native DataFusion reproductions

The defect does not depend on an extension node. A temporary, uncommitted Rust
probe reproduced it with only DataFusion 55.0.0 physical nodes.

### Isolated sort-pushdown reproduction

Construct one partition-preserving sorted stream per input partition, then use
this physical plan:

```text
SortExec: TopK(fetch=10), x ASC
  FilterExec: x > 49
    SortExec: x ASC, preserve_partitioning=true
      DataSourceExec: values 0..149 across four partitions
```

Run `assign_initial_requirements` followed by `pushdown_sorts`. The optimized
plan is:

```text
FilterExec: x > 49
  SortExec: TopK(fetch=10), x ASC, preserve_partitioning=true
    DataSourceExec: four partitions
```

The original plan returns `50..59`. The optimized plan sorts and retains the
first ten input values before filtering, so it returns no rows. This isolates
the faulty satisfied-order branch directly.

### Full `EnsureRequirements` reproduction

The stronger reproduction uses a native sorted `AggregateExec` over 100 group
keys, with 20 rows per key and four hash partitions:

```text
SortExec: TopK(fetch=10), x ASC
  AggregateExec: grouped by x, ordering_mode=Sorted
    SortExec: x ASC, preserve_partitioning=true
      RepartitionExec: Hash(x, 4)
        DataSourceExec: four partitions
```

Run the same relevant sequence as DataFusion's recommended physical optimizer:

1. `OutputRequirements::new_add_mode()`
2. `EnsureRequirements::new()`
3. `OutputRequirements::new_remove_mode()`

The resulting plan is:

```text
SortPreservingMergeExec: x ASC, fetch=10
  AggregateExec: grouped by x, ordering_mode=Sorted
    SortExec: TopK(fetch=10), x ASC, preserve_partitioning=true
      RepartitionExec: Hash(x, 4)
        DataSourceExec: four partitions
```

The correct first ten groups are `0..9`. The optimized plan returns only
`0..2`, because limiting each aggregate input partition to ten rows removes
groups before aggregation. This proves the issue is reachable through the
normal requirement-enforcement flow with only native nodes.

Running the same plan through every rule in `PhysicalOptimizer::new().rules`
confirms the end-to-end impact: later rules add another input-side fetch, and
the final result contains only group `0` instead of `0..9`.

A simple SQL filter query may still produce a correct plan because
`FilterExec::with_fetch` can absorb a limit on the filter's own output. That
does not repair the generic branch, and it does not protect nodes such as a
sorted aggregate or `MatchRecognizePatternExec` that cannot absorb the fetch.

## Faulty optimizer path

DataFusion's recommended physical rule order places `EnsureRequirements`
before `LimitPushdown`. `OutputRequirements` first preserves root ordering and
fetch information; `EnsureRequirements` then enforces distribution and sorting
and performs sort pushdown.

The defect is in
`datafusion-physical-optimizer/src/ensure_requirements/enforce_sorting/sort_pushdown.rs`,
inside `pushdown_sorts_helper`:

1. A fetched sort establishes `parent_fetch` and an ordering requirement.
2. The current non-sort operator's output equivalence properties already
   satisfy that ordering.
3. The `satisfy_parent` branch removes the redundant sort.
4. For each child it assigns the operator's required input ordering and
   `min_fetch(parent_fetch, child_fetch)`.
5. A child sort is rebuilt as a `TopK` with that fetch.

Step 4 does not consult `supports_limit_pushdown`, `with_fetch`, `fetch`,
`cardinality_effect`, or `try_pushdown_sort`.

The adjacent `pushdown_requirement_to_children` path already implements the
missing safety rule. When a fetch is present, it rejects propagation if
`supports_limit_pushdown()` is false or `cardinality_effect()` is not
`CardinalityEffect::Equal`. That helper is called only when the current output
does *not* already satisfy the parent ordering, so the satisfied-order path
bypasses its guards.

The later `LimitPushdown` rule cannot recover rows discarded by the generated
child `TopK`.

## `ExecutionPlan` contract audit

`MatchRecognizePatternExec` already gives DataFusion the information required
to reject this transformation:

| Method | Matcher value | Meaning | Consulted by faulty branch |
| --- | --- | --- | --- |
| `supports_limit_pushdown()` | inherited `false` | A result limit cannot pass to matcher input. | No |
| `with_fetch()` | inherited `None` | The matcher has no fetch-bearing variant. | No |
| `fetch()` | inherited `None` | The matcher itself owns no fetch. | No |
| `cardinality_effect()` | `LowerEqual` for `PAST LAST ROW`; otherwise `GreaterEqual` | Matching does not preserve row count. | No |
| `try_pushdown_sort()` | inherited `Unsupported` | The matcher does not implement custom source-style sort pushdown. | No |
| `required_input_ordering()` | partition-first alternatives followed by match order | The matcher needs ordered input for correctness. | Yes, but only as the route used to forward the fetch. |
| `input_distribution_requirements()` | single or key-partitioned input | Rows for a match partition must meet on one execution partition. | Yes for distribution, not fetch safety. |
| `maintains_input_order()` | true only for output modes that really preserve it | Some matcher outputs retain input order. | Not used as a fetch-safety guard. |
| `properties()` | truthful output ordering and partitioning | Allows removal of genuinely redundant output sorts. | Used to decide `satisfy_parent`. |

The rest of the trait does not supply a hidden limit fence. Child replacement,
statistics, metrics, execution, projection swapping, filter pushdown, dynamic
expressions, state injection, serialization, and downcast delegation do not
control this sort-pushdown branch.

Making any truthful matcher method stricter does not fix the defect:

- Explicitly overriding `supports_limit_pushdown()` with `false` repeats the
  inherited value and remains ignored.
- Reporting no output ordering retains redundant result sorts and loses valid
  optimization.
- Reporting `maintains_input_order=false` does not gate the satisfied-order
  branch and suppresses valid order preservation.
- Clearing required input order or distribution breaks matching correctness or
  partition parallelism.
- Implementing `with_fetch` could let a later rule stop after producing a safe
  number of matches, but the earlier faulty branch never calls it.

## Current local containment

This repository places one executable, zero-work `MatchOutputBoundaryExec`
around the complete ordinary or Halo matcher pipeline. It delegates schema,
statistics, output properties, and execution unchanged. Its input slot has no
ordering requirement, so the satisfied-order branch has no ordering-plus-fetch
carrier through which to reach matcher input. The matcher keeps its truthful
ordering and distribution requirements below the boundary.

The boundary is intentionally permanent in optimized plans. It protects a
second optimizer pass and adaptive hosts as well as initial planning. It must
not erase explicit SQL limits inside the matcher input relation.

## Upstream fix shape

Keep the upstream change inside the existing sort-pushdown rule. Do not add a
new optimizer pass or special-case `MATCH_RECOGNIZE`.

The `satisfy_parent` branch must propagate `parent_fetch` only when the same
conditions used by `pushdown_requirement_to_children` permit it:

```text
no parent fetch
OR
(plan supports limit pushdown AND plan cardinality effect is Equal)
```

When ordering is satisfied but fetch propagation is unsafe, retain or
materialize the fetched operation above the current node. Reuse one shared
predicate/helper so the satisfied and unsatisfied ordering paths cannot drift
again.

## Focused upstream validation

- [ ] First check the current DataFusion main branch; do not file or patch a
      defect that has already been fixed after 55.0.0.
- [ ] Add the native sorted-aggregate case to the nearest existing
      `EnsureRequirements` sort-pushdown tests. Assert both plan placement and
      rows so a plausible-looking wrong plan cannot pass.
- [ ] Keep one equal-cardinality control showing that a safe TopK still moves
      down. This protects the optimization rather than disabling it globally.
- [ ] Run the containing DataFusion optimizer test target and its standard
      formatting/lint checks.
- [ ] Run this repository's focused partition-key limit test and the exact
      four-partition compatibility case against the patched dependency.
- [ ] Compare representative matcher plans and the existing partitioned row-loop
      benchmark before removing `MatchOutputBoundaryExec`.

Do not build a matrix across every cardinality-changing operator. One native
wrong-results regression, one safe control, and this repository's production
reproduction cover the invariant.

## Retirement sequence

- [ ] Land the generic DataFusion fix and record its issue/PR and first release.
- [ ] Raise this repository's minimum DataFusion version to that release.
- [ ] Remove `MatchOutputBoundaryExec`, its focused idempotence assertions, and
      only the resulting `EXPLAIN` lines.
- [ ] Keep the end-to-end wrong-results regression permanently.
- [ ] Move any lasting dependency constraint to the owning architecture or
      compatibility documentation, then delete this plan.
