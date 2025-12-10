"""Build plots from the Criterion estimates under target/criterion.

This script reads the per-benchmark `estimates.json` files emitted by Criterion
and produces a PNG showing average nanoseconds per lookup for
`contains` vs `binary_search` across the measured slice lengths and data types.
"""
from pathlib import Path
import json
import pandas as pd
import matplotlib.pyplot as plt

REPO_ROOT = Path(__file__).resolve().parents[3]
CRITERION_DIR = REPO_ROOT / "target" / "criterion"
OUTPUT_PATH = Path(__file__).with_name("slice_search_results.png")


def load_results():
    records = []
    for est_path in CRITERION_DIR.glob("*_slice_search/*/*/new/estimates.json"):
        try:
            length = int(est_path.parent.parent.name)
        except ValueError:
            continue
        method = est_path.parent.parent.parent.name
        type_group = est_path.parent.parent.parent.parent.name
        type_name = type_group.split("_")[0]

        with est_path.open() as f:
            estimates = json.load(f)
        mean_seconds = estimates["mean"]["point_estimate"]
        records.append(
            {
                "type": type_name,
                "method": method,
                "length": length,
                "mean_ns": mean_seconds * 1e9,
            }
        )

    return pd.DataFrame(records)


def plot(df: pd.DataFrame):
    types = sorted(df["type"].unique())
    fig, axes = plt.subplots(len(types), 1, figsize=(8, 10), sharex=True)

    for ax, t in zip(axes, types):
        subset = df[df["type"] == t]
        for method, style in [
            ("contains", {"color": "tab:blue", "marker": "o"}),
            ("binary_search", {"color": "tab:orange", "marker": "s"}),
        ]:
            data = subset[subset["method"] == method].sort_values("length")
            ax.plot(
                data["length"],
                data["mean_ns"],
                label=method.replace("_", " "),
                **style,
            )

        ax.set_title(f"{t} slices")
        ax.set_ylabel("ns per lookup")
        ax.set_xscale("log", base=2)
        ax.legend()
        ax.grid(True, which="both", linestyle="--", alpha=0.4)

    axes[-1].set_xlabel("slice length (log2 scale)")
    fig.tight_layout()
    fig.savefig(OUTPUT_PATH)
    print(f"Saved plot to {OUTPUT_PATH}")


def main():
    df = load_results()
    if df.empty:
        raise SystemExit("No criterion results found; run cargo bench first.")
    plot(df)


if __name__ == "__main__":
    main()
