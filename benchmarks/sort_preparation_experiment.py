#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Compare baseline sort preparation with the two fork experiments.

Run baseline plus grouped, coalesced, or both using sort_preparation probe binaries
built from the identical Rust fixture. Validation is separate from timing. Fresh
processes run sequentially in balanced orders; warmups excluded, all measured
runs retained. --summarize accepts a CSV, results.jsonl, or a run
directory. No builds are performed. Python standard library only.
"""

import argparse
import csv
import hashlib
import itertools
import json
import math
import os
import platform
import random
import statistics
import subprocess
import sys
import time
from pathlib import Path

LABELS = ("baseline", "grouped", "coalesced")
ROWS = 6_001_215
CASES = {
    "preload-q5-in32-out8192": ["--preload", "--query", "q5", "--batch-size", "32", "--output-batch-size", "8192"],
    "preload-q5-in256-out8192": ["--preload", "--query", "q5", "--batch-size", "256", "--output-batch-size", "8192"],
    "preload-q10-in256-out8192": ["--preload", "--query", "q10", "--batch-size", "256", "--output-batch-size", "8192"],
    "parquet-q5-in8192-out8192": ["--query", "q5", "--batch-size", "8192"],
    "parquet-q10-in8192-out8192": ["--query", "q10", "--batch-size", "8192"],
    "synthetic-skew-in8192-out8192": ["--synthetic-skew"],
    "preload-q10-in8192-out8192": ["--preload", "--query", "q10", "--batch-size", "8192", "--output-batch-size", "8192"],
}
METRICS = ("wall_ms", "first_output_ms", "probe_p50_ms", "probe_p99_ms", "probe_max_ms",
           "probe_count", "peak_live_tasks", "sampled_peak_rss_bytes", "sampled_peak_reserved_bytes")


def identity(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return {"path": str(path), "bytes": path.stat().st_size, "sha256": digest.hexdigest()}


def write_json(path, value):
    with path.open("x") as sink:
        json.dump(value, sink, indent=2, allow_nan=False)
        sink.write("\n")


def metric_ok(row, name):
    value = row.get(name)
    return name in row and ((name == "sampled_peak_rss_bytes" and value is None)
                           or isinstance(value, (int, float)) and math.isfinite(value) and value >= 0)


def read_records(path):
    schedule_path = path / "schedule.json" if path.is_dir() else None
    if path.is_dir():
        path = path / "results.jsonl"
    if path.suffix.lower() == ".csv":
        with path.open(newline="") as source:
            rows = list(csv.DictReader(source))
    else:
        rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    if schedule_path and schedule_path.exists() and len(rows) != len(json.loads(schedule_path.read_text())):
        raise ValueError("Run is incomplete: result count differs from its frozen schedule")
    for row in rows:
        row.update(row.pop("measurement", {}))
        row["warmup"] = str(row.get("warmup", False)).lower() in ("true", "1")
        row["repetition"] = int(row["repetition"])
        for metric in METRICS:
            if metric in row:
                row[metric] = (None if metric == "sampled_peak_rss_bytes" and row[metric] in (None, "", "null", "None")
                               else float(row[metric]))
    return rows


def interval(ratios, seed, samples):
    if len(ratios) < 6 or not samples:
        return None
    rng = random.Random(seed)
    medians = sorted(statistics.median(rng.choices(ratios, k=len(ratios))) for _ in range(samples))
    def quantile(p):
        offset = (len(medians) - 1) * p
        low, high = math.floor(offset), math.ceil(offset)
        return medians[low] + (medians[high] - medians[low]) * (offset - low)
    return [100 * (quantile(p) - 1) for p in (0.025, 0.975)]


def summarize(rows, selected, seed, samples):
    rows = [row for row in rows if row["variant"] in selected]
    warmups = sum(row["warmup"] for row in rows)
    measured = [row for row in rows if not row["warmup"]]
    if not measured or any(row.get("status", "ok") != "ok" or row.get("validate") for row in measured):
        raise ValueError("Missing or failed measurements; inspect results.jsonl and logs")
    if any(any(not metric_ok(row, key) for key in METRICS) or row['wall_ms'] <= 0 for row in measured):
        raise ValueError("Invalid/missing metrics; wall times must be positive and finite")
    summary = {"variants": selected, "warmups_excluded": warmups, "measured_records": len(measured),
               "method": "Median per-repetition percentage change; descriptive 95% paired bootstrap interval",
               "caveats": "No outliers excluded. Small samples on one host; probe counts vary. Not population proof.",
               "cases": {}}
    table = ["Variant order in rows: " + ", ".join(selected), "Warmups excluded; no outliers excluded.", ""]
    for case in dict.fromkeys(row["case"] for row in measured):
        records = [row for row in measured if row["case"] == case]
        indexed = {(row["repetition"], row["variant"]): row for row in records}
        repetitions = sorted({row["repetition"] for row in records})
        if len(indexed) != len(records) or any((rep, label) not in indexed for rep in repetitions for label in selected):
            raise ValueError(f"{case}: duplicate or incomplete matched repetitions")
        values = {label: {metric: statistics.median(available) if (available := [indexed[(rep, label)][metric]
                          for rep in repetitions if indexed[(rep, label)][metric] is not None]) else None
                          for metric in METRICS} for label in selected}
        contrasts = {}
        table += [case, "variant       n    wall ms   first ms   probe p99 ms   probe max ms"]
        for label in selected:
            v = values[label]
            table.append(f"{label:10s} {len(repetitions):4d} {v['wall_ms']:10.2f} {v['first_output_ms']:10.2f} "
                         f"{v['probe_p99_ms']:14.2f} {v['probe_max_ms']:14.2f}")
        for position, label in enumerate(selected):
            for control in selected[:position]:
                ratios = [indexed[(rep, label)]["wall_ms"] / indexed[(rep, control)]["wall_ms"] for rep in repetitions]
                name = f"{label}_vs_{control}"
                material = "\0".join((str(seed), case, name, "wall_ms")).encode()
                ci = interval(ratios, int.from_bytes(hashlib.sha256(material).digest()[:8], "big"), samples)
                change = statistics.median(100 * (ratio - 1) for ratio in ratios)
                contrasts[name] = {"n_pairs": len(ratios), "median_paired_change_pct": change,
                                   "ratio_of_medians_change_pct": 100 * (values[label]['wall_ms'] / values[control]['wall_ms'] - 1),
                                   "paired_change_95pct_interval": ci, "paired_ratios": ratios}
                bounds = f"[{ci[0]:+.2f}%, {ci[1]:+.2f}%]" if ci else "interval unavailable"
                table.append(f"  {name}: median paired wall {change:+.2f}% {bounds}")
        summary["cases"][case] = {"n": len(repetitions), "medians": values, "comparisons": contrasts}
        table.append("")
    table += [summary["method"], summary["caveats"]]
    return summary, "\n".join(table) + "\n"


def run_probe(command, output, stem, timeout):
    stdout_path, stderr_path = output / f"{stem}.stdout", output / f"{stem}.stderr"
    with stdout_path.open("x") as stdout, stderr_path.open("x") as stderr:
        process = subprocess.run(command, stdout=stdout, stderr=stderr, timeout=timeout, check=False)
    if process.returncode:
        raise ValueError(f"Probe exited {process.returncode}; see {stderr_path}")
    records = [json.loads(line) for line in stdout_path.read_text().splitlines() if line.lstrip().startswith("{")]
    if len(records) != 1:
        raise ValueError(f"Expected one JSON result; see {stdout_path}")
    return records[0]


def schedule(cases, selected, repetitions, warmups, seed, validate):
    rng = random.Random(seed)
    permutations = list(itertools.permutations(selected))
    orders = {}
    for case in cases:
        orders[case] = []
        while len(orders[case]) < repetitions:
            block = permutations.copy()
            rng.shuffle(block)
            orders[case].extend(block)
    tasks = []
    phases = [("validation", 1)] if validate else [("warmup", warmups), ("measure", repetitions)]
    for phase, count in phases:
        for rep in range(count):
            case_order = list(cases)
            rng.shuffle(case_order)
            for case in case_order:
                order = orders[case][rep] if phase == "measure" else rng.choice(permutations)
                for position, label in enumerate(order):
                    tasks.append({"case": case, "variant": label, "repetition": rep,
                                  "warmup": phase == "warmup", "phase": phase,
                                  "position_in_order": position, "variant_order": order})
    return tasks


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for label in LABELS:
        parser.add_argument("--" + label, type=Path, help="Probe binary; baseline plus at least one alternative")
    parser.add_argument("--lineitem", type=Path, help="TPC-H SF1 lineitem Parquet file/directory")
    parser.add_argument("--output-dir", type=Path, help="New directory; never overwrites artifacts")
    parser.add_argument("--fixture", type=Path,
                        default=Path(__file__).resolve().parent / "src/bin/sort_preparation.rs",
                        help="Identical Rust fixture source to hash (defaults to the sibling benchmark)")
    parser.add_argument("--repetitions", type=int, default=12)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--seed", type=int, default=26242)
    parser.add_argument("--cases", nargs="+", choices=list(CASES), default=list(CASES))
    parser.add_argument("--timeout", type=float, default=240, help="Per-process timeout in seconds")
    parser.add_argument("--bootstrap-samples", type=int, default=10_000)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--validate", action="store_true", help="Check ordering and row fingerprints separately from timing")
    mode.add_argument("--summarize", type=Path, metavar="CSV_OR_RESULTS", help="Summarize without launching binaries")
    args = parser.parse_args()
    selected = [label for label in LABELS if label == "baseline" or getattr(args, label) is not None]
    if args.bootstrap_samples < 0 or args.warmups < 0 or args.timeout <= 0:
        parser.error("Bootstrap samples/warmups must be nonnegative; timeout must be positive")
    if args.summarize:
        rows = [row for row in read_records(args.summarize) if row['case'] in args.cases]
        if len(selected) == 1:
            selected = [label for label in LABELS if any(row["variant"] == label for row in rows)]
        if len(selected) < 2 or "baseline" not in selected:
            parser.error("Summary needs baseline and at least one alternative")
        summary, text = summarize(rows, selected, args.seed, args.bootstrap_samples)
        if args.output_dir:
            args.output_dir.mkdir(parents=True, exist_ok=False)
            write_json(args.output_dir / "summary.json", summary)
            (args.output_dir / "summary.txt").write_text(text)
        print(text, end="")
        return
    order_count = math.factorial(len(selected))
    if not args.baseline or len(selected) < 2 or not args.output_dir:
        parser.error("Run/validation requires --baseline, --grouped and/or --coalesced, and --output-dir")
    if args.repetitions <= 0 or args.repetitions % order_count:
        parser.error(f"Repetitions must be a positive multiple of {order_count} for balanced orders")
    cases = list(dict.fromkeys(args.cases))
    if any(not case.startswith("synthetic-") for case in cases) and args.lineitem is None:
        parser.error("--lineitem is required for Parquet/preloaded cases")
    binaries = {label: getattr(args, label).resolve() for label in selected}
    dataset = args.lineitem.resolve() if args.lineitem else None
    data_files = ([dataset] if dataset.is_file() else sorted(dataset.rglob("*.parquet"))) if dataset else []
    if dataset and not data_files:
        raise ValueError(f"No Parquet files found: {dataset}")
    manifest = {"args": {key: str(value) if isinstance(value, Path) else value for key, value in vars(args).items()},
                "variants": selected, "python": sys.version, "platform": platform.platform(),
                "logical_cpus": os.cpu_count(),
                "binaries": {label: identity(path) for label, path in binaries.items()},
                "dataset": [identity(path) for path in data_files], "gap_seconds": 0.5,
                "fixture": identity(args.fixture.resolve()) if args.fixture else None,
                "policy": "Fresh sequential processes; balanced orders; warmups separate; no outlier exclusions"}
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=False)
    tasks = schedule(cases, selected, args.repetitions, args.warmups, args.seed, args.validate)
    write_json(output / "manifest.json", manifest)
    write_json(output / "schedule.json", tasks)
    records, fingerprints = [], {}
    with (output / "results.jsonl").open("x") as sink:
        for index, task in enumerate(tasks):
            command = [str(binaries[task['variant']]), "--label", task['variant'], "--iterations", "1"]
            if not task['case'].startswith("synthetic-"):
                command += ["--lineitem", str(args.lineitem.resolve())]
            command += CASES[task['case']] + (["--validate"] if args.validate else [])
            stem = f"{index:04d}-{task['phase']}-{task['case']}-{task['variant']}"
            row = {**task, "command": command, "status": "ok"}
            try:
                result = run_probe(command, output, stem, args.timeout)
                expected = {"label": task['variant'], "workers": 4, "batch_size": 8192,
                            "input_batch_size": int(task['case'].split('-in')[1].split('-out')[0]),
                            "preload": task['case'].startswith('preload-'),
                            "synthetic_skew": task['case'].startswith('synthetic-')}
                if any(result.get(key) != value for key, value in expected.items()):
                    raise ValueError("Probe result does not match the requested case/runtime")
                if args.validate:
                    if not (result.get("validation_passed") and result.get("validation_global_key_order")
                            and result.get("validation_rows") == ROWS):
                        raise ValueError("Validation/order/row-count check failed")
                    fingerprint = tuple(result[key] for key in ("validation_rows", "validation_hash_sum", "validation_hash_xor"))
                    if task['case'] in fingerprints and fingerprints[task['case']] != fingerprint:
                        raise ValueError(f"{task['case']}: row fingerprints differ across variants")
                    fingerprints[task['case']] = fingerprint
                else:
                    if result.get("rows") != ROWS or result.get("validate"):
                        raise ValueError("Expected a non-validation SF1 timing result")
                    if any(not metric_ok(result, key) for key in METRICS) or result['wall_ms'] <= 0:
                        raise ValueError("Missing/nonfinite/negative measurement")
                row.update(result)
            except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
                row.update(status="error", error=str(error))
                sink.write(json.dumps(row) + "\n"); sink.flush()
                raise ValueError(f"{stem}: {error}; artifacts retained in {output}") from error
            sink.write(json.dumps(row, allow_nan=False) + "\n"); sink.flush()
            records.append(row)
            print(f"[{index + 1}/{len(tasks)}] {stem}", flush=True)
            if index + 1 < len(tasks):
                time.sleep(0.5)
    if args.validate:
        write_json(output / "validation.json", {"passed": True, "cases": fingerprints, "records": len(records)})
    else:
        summary, text = summarize(records, selected, args.seed, args.bootstrap_samples)
        write_json(output / "summary.json", summary)
        (output / "summary.txt").write_text(text)
        print(text, end="")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError) as error:
        sys.exit(str(error))
