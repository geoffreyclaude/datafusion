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

"""Build plots from the Criterion sample data under target/criterion.

This script reads the per-benchmark `sample.json` files emitted by Criterion
and produces PNGs showing **minimum** time to process a batch of 8192 lookups
for `contains` vs `binary_search` vs `hashset` vs `branchless` across the
measured slice lengths and data types.

We use min (not mean) because noise can only add time, never subtract.
The minimum represents the code's intrinsic cost when the system isn't
doing other things.

Results are saved to a CPU-specific subfolder (e.g., results/Apple_M1_Pro/).

Style: SciencePlots (https://github.com/garrettj403/SciencePlots)
"""
from pathlib import Path
import json
import platform
import re
import subprocess
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import scienceplots  # noqa: F401 - required for plt.style.use('science')

# Use SciencePlots science style (no LaTeX required) with sans-serif fonts
plt.style.use(['science', 'no-latex', 'grid'])
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = ['Arial', 'Helvetica', 'DejaVu Sans']


def get_cpu_model() -> str:
    """Get the CPU model name, formatted for use as a folder name."""
    system = platform.system()
    
    try:
        if system == "Darwin":  # macOS
            result = subprocess.run(
                ["sysctl", "-n", "machdep.cpu.brand_string"],
                capture_output=True, text=True, check=True
            )
            cpu_name = result.stdout.strip()
        elif system == "Linux":
            with open("/proc/cpuinfo") as f:
                for line in f:
                    if line.startswith("model name"):
                        cpu_name = line.split(":")[1].strip()
                        break
                else:
                    cpu_name = "Unknown_CPU"
        else:
            cpu_name = platform.processor() or "Unknown_CPU"
    except Exception:
        cpu_name = "Unknown_CPU"
    
    # Sanitize for folder name: replace spaces/special chars with underscores
    cpu_name = re.sub(r'[^\w\-]', '_', cpu_name)
    cpu_name = re.sub(r'_+', '_', cpu_name)  # Collapse multiple underscores
    cpu_name = cpu_name.strip('_')
    
    return cpu_name


REPO_ROOT = Path(__file__).resolve().parents[3]
CRITERION_DIR = REPO_ROOT / "target" / "criterion"
RESULTS_BASE_DIR = Path(__file__).parent
CPU_MODEL = get_cpu_model()
RESULTS_DIR = RESULTS_BASE_DIR / CPU_MODEL
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# Display names for data types
TYPE_DISPLAY_NAMES = {
    "i8": "Int8",
    "i16": "Int16",
    "i32": "Int32",
    "i64": "Int64",
    "i128": "Int128",
    "str": "String",
}

# Bins for cutoff visualization (slice size ranges)
CUTOFF_BINS = ["2", "3-4", "5-8", "9-16", "17-32", "33-64", "65-128", "129-256"]
CUTOFF_BIN_RANGES = [(2, 2), (3, 4), (5, 8), (9, 16), (17, 32), (33, 64), (65, 128), (129, 256)]


def load_results():
    records = []
    for sample_path in CRITERION_DIR.glob("*_slice_search/*/*/new/sample.json"):
        try:
            length = int(sample_path.parent.parent.name)
        except ValueError:
            continue
        method = sample_path.parent.parent.parent.name
        type_group = sample_path.parent.parent.parent.parent.name
        type_name = type_group.split("_")[0]

        with sample_path.open() as f:
            sample = json.load(f)
        # Compute per-iteration time and take the minimum (convert ns to µs)
        iters = sample["iters"]
        times = sample["times"]
        per_iter_ns = [t / i for t, i in zip(times, iters)]
        min_us = min(per_iter_ns) / 1e3  # ns to µs
        records.append(
            {
                "type": type_name,
                "method": method,
                "length": length,
                "min_us": min_us,
            }
        )

    return pd.DataFrame(records)


def plot_consolidated(df: pd.DataFrame, output_path: Path):
    """Generate a single consolidated plot organized by max slice length sections.
    
    Uses subplot_mosaic for declarative layout definition.
    """
    # Define layout using subplot_mosaic
    # Each unique string becomes a subplot; '.' creates empty space
    # Using 6-column grid for flexible spanning
    mosaic = [
        ['i8_256',  'i8_256',  'i8_256',  'i16_128', 'i16_128', 'i16_128'],  # Section 1
        ['.',       '.',       '.',       '.',       '.',       '.'],        # Gap between sections
        ['i32_64',  'i32_64',  'i32_64',  'i64_64',  'i64_64',  'i64_64'],   # Section 2
        ['.',       '.',       '.',       '.',       '.',       '.'],        # Gap between sections
        ['i8_16',   'i8_16',   'i16_16',  'i16_16',  'i32_16',  'i32_16'],   # Section 3 top
        ['.',       '.',       '.',       '.',       '.',       '.'],        # Gap within section 3
        ['i64_16',  'i64_16',  'i128_16', 'i128_16', 'str_16',  'str_16'],   # Section 3 bottom
        ['.',       '.',       '.',       '.',       '.',       '.'],        # Gap between sections
        ['cutoffs', 'cutoffs', 'cutoffs', 'cutoffs', 'cutoffs', 'cutoffs'],  # Section 4
    ]
    
    # Create figure with mosaic layout
    fig, axd = plt.subplot_mosaic(
        mosaic,
        figsize=(12, 17),
        height_ratios=[1, 0.2, 1, 0.2, 1, 0.12, 1, 0.2, 0.6],
        gridspec_kw={'hspace': 0.25, 'wspace': 0.3},
    )
    
    # Define what each subplot shows: (type, max_length)
    subplot_config = {
        'i8_256': ('i8', 256), 'i16_128': ('i16', 128),
        'i32_64': ('i32', 64), 'i64_64': ('i64', 64),
        'i8_16': ('i8', 16), 'i16_16': ('i16', 16), 'i32_16': ('i32', 16),
        'i64_16': ('i64', 16), 'i128_16': ('i128', 16), 'str_16': ('str', 16),
    }
    
    # Define sections for borders and titles
    sections = [
        ("Max Slice Length of 256/128", ['i8_256', 'i16_128']),
        ("Max Slice Length of 64", ['i32_64', 'i64_64']),
        ("Max Slice Length of 16", ['i8_16', 'i16_16', 'i32_16', 'i64_16', 'i128_16', 'str_16']),
        ("Recommended Algorithm Cutoffs", ['cutoffs']),
    ]
    
    methods = ["contains", "binary_search", "hashset", "branchless"]
    markers = ["o", "s", "^", "d"]
    
    # Plot data on all benchmark axes using default color cycle
    handles, labels = [], []
    algo_colors = {}  # Will capture colors from first subplot for cutoffs
    for idx, (subplot_name, (t, max_len)) in enumerate(subplot_config.items()):
        ax = axd[subplot_name]
        subset = df[(df["type"] == t) & (df["length"] <= max_len)]
        
        for method, marker in zip(methods, markers):
            data = subset[subset["method"] == method].sort_values("length")
            if data.empty:
                continue
            label = "linear" if method == "contains" else method.replace("_", " ")
            line, = ax.plot(
                data["length"],
                data["min_us"],
                label=label,
                marker=marker,
                markersize=2.5,
            )
            
            # Capture handles/labels/colors from first subplot
            if idx == 0:
                handles.append(line)
                labels.append(label)
                algo_colors[method] = line.get_color()
        
        ax.set_title(TYPE_DISPLAY_NAMES.get(t, t))
    
    # Get cutoffs axes
    cutoffs_ax = axd['cutoffs']
    
    # Plot cutoffs on the dedicated axes
    cutoffs_data = find_best_cutoffs(df)
    type_order = ["i8", "i16", "i32", "i64", "i128", "str"]
    types = [t for t in type_order if t in cutoffs_data]
    
    def get_algo_for_size(ranges, size):
        for algo, start, end in ranges:
            if start <= size <= end:
                return algo
        return ranges[-1][0]
    
    # Short labels for algorithms (to fit in bars)
    algo_labels = {
        "branchless": "branchless",
        "hashset": "hashset", 
        "contains": "linear",
        "binary_search": "binary",
    }
    
    bar_width = 0.8
    for y, dtype in enumerate(types):
        ranges = cutoffs_data[dtype]
        
        # Group consecutive bins by algorithm to place centered labels
        current_algo = None
        algo_start_x = 0
        
        for x, (bin_label, (bin_start, bin_end)) in enumerate(zip(CUTOFF_BINS, CUTOFF_BIN_RANGES)):
            algo = get_algo_for_size(ranges, bin_start)
            color = algo_colors.get(algo, "#95a5a6")
            cutoffs_ax.barh(y, 1, left=x, height=bar_width, color=color, edgecolor='white', linewidth=0.5)
            
            # Track algorithm spans for labeling
            if algo != current_algo:
                # Label the previous span (left-aligned with small padding)
                if current_algo is not None:
                    cutoffs_ax.text(algo_start_x + 0.1, y, algo_labels.get(current_algo, current_algo),
                                   ha='left', va='center', color='white', fontsize=8, fontweight='bold')
                current_algo = algo
                algo_start_x = x
        
        # Label the final span (left-aligned with small padding)
        if current_algo is not None:
            cutoffs_ax.text(algo_start_x + 0.1, y, algo_labels.get(current_algo, current_algo),
                           ha='left', va='center', color='white', fontsize=8, fontweight='bold')
    
    cutoffs_ax.set_yticks(range(len(types)))
    cutoffs_ax.set_yticklabels([TYPE_DISPLAY_NAMES.get(t, t) for t in types])
    cutoffs_ax.set_xticks([i + 0.5 for i in range(len(CUTOFF_BINS))], minor=True)
    cutoffs_ax.set_xticklabels(CUTOFF_BINS, fontsize=9, minor=True)
    cutoffs_ax.set_xticks(range(len(CUTOFF_BINS) + 1), minor=False)
    cutoffs_ax.set_xticklabels([], minor=False)
    cutoffs_ax.tick_params(axis='x', which='minor', length=0)
    cutoffs_ax.tick_params(axis='x', which='major', length=4, color='#888888')
    cutoffs_ax.tick_params(axis='y', length=0, which='both')
    cutoffs_ax.set_xlim(0, len(CUTOFF_BINS))
    cutoffs_ax.grid(False)
    cutoffs_ax.invert_yaxis()
    
    # Add section titles and borders
    fig.canvas.draw()  # Need to draw first to get accurate positions
    
    # Padding for borders
    pad_left = 0.045  # Sized for widest labels (Int8, String, etc.)
    pad_right = 0.012
    pad_bottom = 0.012
    pad_top = 0.035
    
    # Compute consistent left boundary across all sections
    left_boundary = None
    for _, subplot_names in sections:
        positions = [axd[name].get_position() for name in subplot_names]
        x0 = min(p.x0 for p in positions) - pad_left
        if left_boundary is None or x0 < left_boundary:
            left_boundary = x0
    
    # Draw section titles and borders
    for title, subplot_names in sections:
        axes_list = [axd[name] for name in subplot_names]
        positions = [ax.get_position() for ax in axes_list]
        
        # Section title above first axis
        axes_list[0].annotate(
            title,
            xy=(0, 1.18), xycoords='axes fraction',
            fontsize=11, fontweight='bold',
            ha='left', va='bottom'
        )
        
        # Section border
        y0 = min(p.y0 for p in positions) - pad_bottom
        x1 = max(p.x1 for p in positions) + pad_right
        y1 = max(p.y1 for p in positions) + pad_top
        
        rect = mpatches.FancyBboxPatch(
            (left_boundary, y0), x1 - left_boundary, y1 - y0,
            boxstyle="square,pad=0",
            linewidth=1, edgecolor='#cccccc', facecolor='none',
            transform=fig.transFigure, clip_on=False
        )
        fig.add_artist(rect)
    
    # Title and legend at top, left-aligned to section boundaries
    fig.suptitle("Slice search benchmark — time in µs for 8192 lookups vs slice length", 
                 fontsize=14, fontweight='bold', x=left_boundary, ha='left', y=0.98)
    fig.legend(handles, labels, loc='upper left', ncol=4, frameon=True, 
               bbox_to_anchor=(left_boundary, 0.96), bbox_transform=fig.transFigure)
    
    fig.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close(fig)
    print(f"Saved plot to {output_path}")


def find_best_cutoffs(df: pd.DataFrame) -> dict:
    """Find the best algorithm cutoffs for each data type.
    
    Uses dynamic programming to find optimal power-of-2 cutoffs that minimize
    total cost while keeping algorithm ranges contiguous.
    
    Returns a dict mapping type -> list of (algorithm, start, end) tuples.
    """
    # Powers of 2 we use as cutoff boundaries
    powers_of_2 = [2, 4, 8, 16, 32, 64, 128, 256]
    
    results = {}
    
    for dtype in df["type"].unique():
        type_df = df[df["type"] == dtype]
        lengths = sorted(type_df["length"].unique())
        max_len = max(lengths)
        
        # Get available algorithms for this type
        algorithms = list(type_df["method"].unique())
        
        # Build cost matrix: cost[algo][length] = time for that algo at that length
        algo_costs = {}
        for algo in algorithms:
            algo_df = type_df[type_df["method"] == algo]
            algo_costs[algo] = dict(zip(algo_df["length"], algo_df["min_us"]))
        
        # Find best power-of-2 aligned cutoffs
        # Try all possible 1, 2, or 3 algorithm configurations
        best_config = find_best_power2_config(lengths, algorithms, algo_costs, powers_of_2, max_len)
        results[dtype] = best_config
    
    return results


def find_best_power2_config(lengths: list, algorithms: list, algo_costs: dict, 
                             powers_of_2: list, max_len: int) -> list:
    """Find the best algorithm configuration using power-of-2 boundaries."""
    
    def range_cost(algo: str, start: int, end: int) -> float:
        """Total cost of using algo for lengths in [start, end]."""
        cost = 0
        for length in lengths:
            if start <= length <= end:
                if length in algo_costs.get(algo, {}):
                    cost += algo_costs[algo][length]
                else:
                    cost += float('inf')  # Algorithm not available at this length
        return cost
    
    # Valid cutoffs (powers of 2 within our range)
    cutoffs = [p for p in powers_of_2 if p <= max_len]
    
    best_cost = float('inf')
    best_config = [(algorithms[0], 1, max_len)]
    
    # Try single algorithm (no cutoffs)
    for algo in algorithms:
        cost = range_cost(algo, 1, max_len)
        if cost < best_cost:
            best_cost = cost
            best_config = [(algo, 1, max_len)]
    
    # Try two algorithms with one cutoff
    for cutoff in cutoffs:
        if cutoff >= max_len:
            continue
        for algo1 in algorithms:
            for algo2 in algorithms:
                if algo1 == algo2:
                    continue
                cost = range_cost(algo1, 1, cutoff) + range_cost(algo2, cutoff + 1, max_len)
                if cost < best_cost:
                    best_cost = cost
                    best_config = [(algo1, 1, cutoff), (algo2, cutoff + 1, max_len)]
    
    # Try three algorithms with two cutoffs
    for i, cutoff1 in enumerate(cutoffs):
        for cutoff2 in cutoffs[i+1:]:
            if cutoff2 >= max_len:
                continue
            for algo1 in algorithms:
                for algo2 in algorithms:
                    for algo3 in algorithms:
                        if algo1 == algo2 or algo2 == algo3:
                            continue
                        cost = (range_cost(algo1, 1, cutoff1) + 
                                range_cost(algo2, cutoff1 + 1, cutoff2) +
                                range_cost(algo3, cutoff2 + 1, max_len))
                        if cost < best_cost:
                            best_cost = cost
                            best_config = [
                                (algo1, 1, cutoff1),
                                (algo2, cutoff1 + 1, cutoff2),
                                (algo3, cutoff2 + 1, max_len)
                            ]
    
    return best_config


def generate_cutoff_table(df: pd.DataFrame) -> str:
    """Generate a Markdown summary of algorithm cutoffs per type."""
    cutoffs = find_best_cutoffs(df)
    
    # Order types by byte size
    type_order = ["i8", "i16", "i32", "i64", "i128", "str"]
    types = [t for t in type_order if t in cutoffs]
    
    lines = [
        "# Recommended Search Algorithm Cutoffs",
        "",
        "Based on minimum batch time (8192 lookups per batch).",
        "",
    ]
    
    # Generate readable decision logic
    for dtype in types:
        ranges = cutoffs[dtype]
        display_name = TYPE_DISPLAY_NAMES.get(dtype, dtype)
        
        if len(ranges) == 1:
            algo = "linear" if ranges[0][0] == "contains" else ranges[0][0]
            lines.append(f"- **{display_name}**: always use {algo}")
        elif len(ranges) == 2:
            algo1 = "linear" if ranges[0][0] == "contains" else ranges[0][0]
            algo2 = "linear" if ranges[1][0] == "contains" else ranges[1][0]
            cutoff = ranges[0][2]
            lines.append(f"- **{display_name}**: use {algo1} up to {cutoff}, then {algo2}")
        else:
            parts = []
            for algo, start, end in ranges:
                algo_name = "linear" if algo == "contains" else algo
                parts.append(f"{algo_name} ({start}-{end})")
            lines.append(f"- **{display_name}**: {', '.join(parts)}")
    
    return "\n".join(lines)


def main():
    df = load_results()
    if df.empty:
        raise SystemExit("No criterion results found; run cargo bench first.")

    # Single consolidated plot with benchmark results and cutoffs
    plot_consolidated(df, output_path=RESULTS_DIR / "slice_search.png")
    
    # Generate and save cutoff recommendations
    table = generate_cutoff_table(df)
    print("\n" + table)
    
    cutoff_path = RESULTS_DIR / "CUTOFFS.md"
    cutoff_path.write_text(table + "\n")
    print(f"\nSaved cutoff recommendations to {cutoff_path}")


if __name__ == "__main__":
    main()
