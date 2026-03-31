#!/usr/bin/env python3
"""Interactive sort benchmark visualization with filter controls."""

import json
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(SCRIPT_DIR, "results")


def main():
    results_path = os.path.join(RESULTS_DIR, "sort_results.json")
    if not os.path.exists(results_path):
        print(f"No results found at {results_path}")
        sys.exit(1)

    with open(results_path) as f:
        data = json.load(f)

    results = data["results"]

    # Build lookup: {engine: {pattern: {dtype: [{length, median_ms}]}}}
    engines = sorted(set(r["engine"] for r in results))
    patterns_order = ["random", "few_unique", "nearly_sorted", "rev_nearly_sorted"]
    dtypes_order = ["u8", "i16", "i32", "i64", "f64", "sym", "str8", "str16"]
    patterns = [p for p in patterns_order if any(r["pattern"] == p for r in results)]
    dtypes = [d for d in dtypes_order if any(r["dtype"] == d for r in results)]

    # Group data
    series = []
    for engine in engines:
        for pattern in patterns:
            for dtype in dtypes:
                points = sorted(
                    [r for r in results
                     if r["engine"] == engine and r["pattern"] == pattern and r["dtype"] == dtype],
                    key=lambda r: r["length"]
                )
                pts = [(p["length"], p["median_ms"]) for p in points if p["length"] > 0 and p["median_ms"] > 0]
                if pts:
                    series.append({
                        "engine": engine,
                        "pattern": pattern,
                        "dtype": dtype,
                        "x": [p[0] for p in pts],
                        "y": [p[1] for p in pts],
                    })

    series_json = json.dumps(series)
    engines_json = json.dumps(engines)
    patterns_json = json.dumps(patterns)
    dtypes_json = json.dumps(dtypes)

    pattern_labels = {
        "random": "Random",
        "few_unique": "Few Unique",
        "nearly_sorted": "Nearly Sorted",
        "rev_nearly_sorted": "Rev. Nearly Sorted",
    }
    pattern_labels_json = json.dumps(pattern_labels)

    engine_colors = {
        "duckdb": "#1f77b4",
        "teide": "#d62728",
        "polars": "#2ca02c",
        "rayforce": "#ff7f0e",
    }
    engine_colors_json = json.dumps(engine_colors)

    dtype_dashes = {
        "u8": None,
        "i16": "dash",
        "i32": "dot",
        "i64": "dashdot",
        "f64": "longdash",
        "sym": "longdashdot",
        "str8": None,
        "str16": "dash",
    }
    dtype_dashes_json = json.dumps(dtype_dashes)

    dtype_markers = {
        "u8": "circle",
        "i16": "square",
        "i32": "diamond",
        "i64": "triangle-up",
        "f64": "x",
        "sym": "star",
        "str8": "cross",
        "str16": "hexagon",
    }
    dtype_markers_json = json.dumps(dtype_markers)

    pattern_widths = {
        "random": 2.5,
        "few_unique": 2,
        "nearly_sorted": 1.5,
        "rev_nearly_sorted": 1.5,
    }
    pattern_widths_json = json.dumps(pattern_widths)

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Sort Benchmark</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; margin: 0; padding: 20px; background: #fafafa; }}
  .controls {{ display: flex; gap: 30px; flex-wrap: wrap; margin-bottom: 20px; padding: 15px; background: white; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
  .control-group {{ display: flex; flex-direction: column; gap: 4px; }}
  .control-group h3 {{ margin: 0 0 6px 0; font-size: 13px; color: #666; text-transform: uppercase; letter-spacing: 0.5px; }}
  .control-group label {{ font-size: 13px; cursor: pointer; display: flex; align-items: center; gap: 4px; }}
  .control-group input[type=checkbox] {{ cursor: pointer; }}
  .swatch {{ display: inline-block; width: 14px; height: 3px; border-radius: 1px; vertical-align: middle; }}
  #chart {{ background: white; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
  .btn-row {{ display: flex; gap: 6px; margin-bottom: 6px; }}
  .btn-row button {{ font-size: 11px; padding: 2px 8px; cursor: pointer; border: 1px solid #ccc; border-radius: 3px; background: #f5f5f5; }}
  .btn-row button:hover {{ background: #e0e0e0; }}
</style>
</head>
<body>

<h2>Sort Benchmark</h2>

<div class="controls">
  <div class="control-group">
    <h3>Engine</h3>
    <div class="btn-row">
      <button onclick="toggleAll('engine', true)">All</button>
      <button onclick="toggleAll('engine', false)">None</button>
    </div>
    <div id="engine-checks"></div>
  </div>
  <div class="control-group">
    <h3>Pattern</h3>
    <div class="btn-row">
      <button onclick="toggleAll('pattern', true)">All</button>
      <button onclick="toggleAll('pattern', false)">None</button>
    </div>
    <div id="pattern-checks"></div>
  </div>
  <div class="control-group">
    <h3>Data Type</h3>
    <div class="btn-row">
      <button onclick="toggleAll('dtype', true)">All</button>
      <button onclick="toggleAll('dtype', false)">None</button>
    </div>
    <div id="dtype-checks"></div>
  </div>
</div>

<div id="chart"></div>

<script>
const ALL_SERIES = {series_json};
const ENGINES = {engines_json};
const PATTERNS = {patterns_json};
const DTYPES = {dtypes_json};
const PATTERN_LABELS = {pattern_labels_json};
const ENGINE_COLORS = {engine_colors_json};
const DTYPE_DASHES = {dtype_dashes_json};
const DTYPE_MARKERS = {dtype_markers_json};
const PATTERN_WIDTHS = {pattern_widths_json};

// State
let enabled = {{
  engine: {{}},
  pattern: {{}},
  dtype: {{}},
}};
ENGINES.forEach(e => enabled.engine[e] = true);
PATTERNS.forEach(p => enabled.pattern[p] = true);
DTYPES.forEach(d => enabled.dtype[d] = true);

// Build checkboxes
function buildChecks(containerId, items, category, labelFn, swatchFn) {{
  const el = document.getElementById(containerId);
  items.forEach(item => {{
    const label = document.createElement('label');
    const cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.checked = true;
    cb.dataset.category = category;
    cb.dataset.item = item;
    cb.addEventListener('change', () => {{
      enabled[category][item] = cb.checked;
      redraw();
    }});
    label.appendChild(cb);
    if (swatchFn) {{
      const swatch = document.createElement('span');
      swatch.className = 'swatch';
      swatch.style.background = swatchFn(item);
      label.appendChild(swatch);
    }}
    label.appendChild(document.createTextNode(' ' + labelFn(item)));
    el.appendChild(label);
  }});
}}

buildChecks('engine-checks', ENGINES, 'engine', e => e, e => ENGINE_COLORS[e] || '#333');
buildChecks('pattern-checks', PATTERNS, 'pattern', p => PATTERN_LABELS[p] || p, null);
buildChecks('dtype-checks', DTYPES, 'dtype', d => d, null);

function toggleAll(category, val) {{
  const items = category === 'engine' ? ENGINES : category === 'pattern' ? PATTERNS : DTYPES;
  items.forEach(item => enabled[category][item] = val);
  document.querySelectorAll(`input[data-category="${{category}}"]`).forEach(cb => cb.checked = val);
  redraw();
}}

function redraw() {{
  const filtered = ALL_SERIES.filter(s =>
    enabled.engine[s.engine] && enabled.pattern[s.pattern] && enabled.dtype[s.dtype]
  );

  const traces = filtered.map(s => ({{
    x: s.x,
    y: s.y,
    mode: 'lines+markers',
    name: `${{s.engine}} / ${{PATTERN_LABELS[s.pattern] || s.pattern}} / ${{s.dtype}}`,
    line: {{
      color: ENGINE_COLORS[s.engine] || '#333',
      dash: DTYPE_DASHES[s.dtype] || 'solid',
      width: PATTERN_WIDTHS[s.pattern] || 2,
    }},
    marker: {{
      symbol: DTYPE_MARKERS[s.dtype] || 'circle',
      size: 6,
    }},
    hovertemplate: `<b>${{s.engine}} / ${{s.dtype}}</b><br>${{PATTERN_LABELS[s.pattern] || s.pattern}}<br>Length: %{{x:,}}<br>Time: %{{y:.2f}} ms<extra></extra>`,
  }}));

  const layout = {{
    height: 600,
    xaxis: {{ type: 'log', title: 'Vector length', exponentformat: 'power' }},
    yaxis: {{
      type: 'log',
      title: 'Time (ms)',
      tickmode: 'array',
      tickvals: (function() {{
        const vals = [];
        [0.001, 0.01, 0.1, 1, 10, 100, 1000, 10000].forEach(base => {{
          [1, 2, 5].forEach(m => vals.push(base * m));
        }});
        return vals;
      }})(),
      ticktext: (function() {{
        const texts = [];
        [0.001, 0.01, 0.1, 1, 10, 100, 1000, 10000].forEach(base => {{
          [1, 2, 5].forEach(m => {{
            const v = base * m;
            if (v >= 1000) texts.push((v/1000) + 's');
            else if (v >= 1) texts.push(v + 'ms');
            else texts.push((v*1000).toFixed(0) + 'us');
          }});
        }});
        return texts;
      }})(),
    }},
    template: 'plotly_white',
    hovermode: 'closest',
    margin: {{ t: 30 }},
    legend: {{ font: {{ size: 10 }} }},
  }};

  Plotly.react('chart', traces, layout, {{ responsive: true }});
}}

redraw();
</script>
</body>
</html>"""

    out_path = os.path.join(RESULTS_DIR, "sort_bench.html")
    with open(out_path, "w") as f:
        f.write(html)
    print(f"Written: {out_path}")


if __name__ == "__main__":
    main()
