#!/usr/bin/env python3
"""Visualize sort benchmark input data patterns."""

import os
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

DATASETS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datasets", "sort_bench")

PATTERNS = ["random", "few_unique", "nearly_sorted", "rev_nearly_sorted"]
PATTERN_LABELS = {
    "random": "Random",
    "few_unique": "Few Unique",
    "nearly_sorted": "Nearly Sorted",
    "rev_nearly_sorted": "Reversed Nearly Sorted",
}

DTYPES = ["u8", "i16", "i32", "i64", "f64", "sym", "str8", "str16"]
DTYPE_COLORS = {
    "u8": "#1f77b4",
    "i16": "#aec7e8",
    "i32": "#ff7f0e",
    "i64": "#ffbb78",
    "f64": "#2ca02c",
    "sym": "#d62728",
    "str8": "#9467bd",
    "str16": "#c5b0d5",
}

N = 1000


def load_csv(pattern, dtype):
    path = os.path.join(DATASETS, pattern, dtype, f"{N}.csv")
    vals = []
    with open(path) as f:
        f.readline()  # skip header
        for line in f:
            v = line.strip()
            if not v:
                continue
            if dtype in ("sym", "str8", "str16"):
                vals.append(v)
            elif dtype == "f64":
                vals.append(float(v))
            else:
                vals.append(int(v))
    return vals


def make_numeric_trace(vals, name, color, row, col):
    return go.Scattergl(
        x=list(range(len(vals))),
        y=vals,
        mode="markers",
        marker=dict(size=2, color=color, opacity=0.6),
        name=name,
        legendgroup=name,
        showlegend=(row == 1 and col == 1),
    )


def make_string_trace(vals, name, color, row, col):
    # For strings, show ordinal position (rank) as y value
    sorted_unique = sorted(set(vals))
    rank = {v: i for i, v in enumerate(sorted_unique)}
    y = [rank[v] for v in vals]
    return go.Scattergl(
        x=list(range(len(vals))),
        y=y,
        mode="markers",
        marker=dict(size=2, color=color, opacity=0.6),
        name=name,
        legendgroup=name,
        showlegend=(row == 1 and col == 1),
    )


def main():
    # 4 patterns x 4 dtypes = 16 subplots (4 rows x 4 cols)
    # rows = patterns, cols = dtypes
    fig = make_subplots(
        rows=4, cols=len(DTYPES),
        subplot_titles=[
            f"{PATTERN_LABELS[p]} / {d}" for p in PATTERNS for d in DTYPES
        ],
        vertical_spacing=0.06,
        horizontal_spacing=0.03,
    )

    for pi, pattern in enumerate(PATTERNS):
        for di, dtype in enumerate(DTYPES):
            row = pi + 1
            col = di + 1
            vals = load_csv(pattern, dtype)

            if dtype in ("sym", "str8", "str16"):
                trace = make_string_trace(vals, dtype, DTYPE_COLORS[dtype], row, col)
            else:
                trace = make_numeric_trace(vals, dtype, DTYPE_COLORS[dtype], row, col)

            fig.add_trace(trace, row=row, col=col)

    fig.update_layout(
        title=f"Sort Benchmark Input Data Patterns ({N} elements)",
        height=1200,
        width=len(DTYPES) * 250,
        showlegend=True,
        template="plotly_white",
        font=dict(size=10),
    )

    fig.update_xaxes(title_text="index", row=4)
    for pi in range(4):
        fig.update_yaxes(title_text=PATTERN_LABELS[PATTERNS[pi]], row=pi + 1, col=1)

    # Wrap in scrollable div
    results_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
    os.makedirs(results_dir, exist_ok=True)
    out = os.path.join(results_dir, "sort_data_viz.html")
    html = fig.to_html(include_plotlyjs="cdn", full_html=False)
    with open(out, "w") as f:
        f.write(f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Sort Data Patterns</title></head>
<body style="margin:0;padding:0;overflow-x:auto">
<div style="min-width:{len(DTYPES) * 250}px">
{html}
</div></body></html>""")
    print(f"Written: {out}")
    return



if __name__ == "__main__":
    main()
