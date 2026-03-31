#!/usr/bin/env python3
"""Sort benchmark: measure single-column sort across engines, patterns, types, lengths."""

import argparse
import json
import os
import platform
import subprocess
import sys
import tempfile
import time

import numpy as np

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATASETS = os.path.join(SCRIPT_DIR, "datasets", "sort_bench")
RESULTS_DIR = os.path.join(SCRIPT_DIR, "results")

from engine_utils import build_engine, engine_label, resolve_source

PATTERNS = ["random", "few_unique", "nearly_sorted", "rev_nearly_sorted"]
DTYPES = ["u8", "i16", "i32", "i64", "f64", "sym", "str8", "str16"]
def _make_lengths():
    pts = [0]
    for exp in range(7):
        base = 10 ** exp
        for m in range(1, 10):
            pts.append(base * m)
    pts.append(10_000_000)
    pts.append(100_000_000)
    return sorted(set(pts))

LENGTHS = _make_lengths()


def median(lst):
    s = sorted(lst)
    return s[len(s) // 2]


def fmt(ms):
    if ms < 0.001:
        return "0"
    if ms < 1:
        return f"{ms*1000:.0f}us"
    if ms < 1000:
        return f"{ms:.1f}ms"
    return f"{ms/1000:.2f}s"


def iter_counts(length):
    """More iterations for small vectors, fewer for large."""
    if length <= 100:
        return 21, 5
    if length <= 100_000:
        return 7, 3
    if length <= 10_000_000:
        return 5, 2
    return 3, 1


def csv_path(pattern, dtype, length):
    return os.path.join(DATASETS, pattern, dtype, f"{length}.csv")


# ── DuckDB ──

DUCKDB_TYPES = {
    "u8": "UTINYINT", "i16": "SMALLINT", "i32": "INTEGER",
    "i64": "BIGINT", "f64": "DOUBLE",
    "sym": "VARCHAR", "str8": "VARCHAR", "str16": "VARCHAR",
}


def bench_duckdb(pattern, dtype, length):
    import duckdb

    path = csv_path(pattern, dtype, length)
    cast = DUCKDB_TYPES[dtype]
    n_iter, n_warmup = iter_counts(length)

    con = duckdb.connect()
    con.execute(f"CREATE TABLE data AS SELECT CAST(v AS {cast}) AS v FROM read_csv_auto('{path}')")
    nrows = con.execute("SELECT COUNT(*) FROM data").fetchone()[0]

    for _ in range(n_warmup):
        con.execute("CREATE OR REPLACE TABLE _r AS SELECT * FROM data ORDER BY v")

    times = []
    for _ in range(n_iter):
        con.execute("DROP TABLE IF EXISTS _r")
        t0 = time.perf_counter()
        con.execute("CREATE OR REPLACE TABLE _r AS SELECT * FROM data ORDER BY v")
        times.append((time.perf_counter() - t0) * 1000)

    con.execute("DROP TABLE IF EXISTS _r")
    con.execute("DROP TABLE IF EXISTS data")
    con.close()

    return {
        "median_ms": median(times),
        "times_ms": times,
        "rows": nrows,
        "version": duckdb.__version__,
    }


# ── Teide ──

def bench_teide(pattern, dtype, length):
    from teide.api import Context

    path = csv_path(pattern, dtype, length)
    n_iter, n_warmup = iter_counts(length)

    import teide
    version = getattr(teide, "__version__", "dev")

    with Context() as ctx:
        df = ctx.read_csv(path)
        nrows = len(df)

        for _ in range(n_warmup):
            df.sort("v").collect()

        times = []
        for _ in range(n_iter):
            t0 = time.perf_counter()
            df.sort("v").collect()
            times.append((time.perf_counter() - t0) * 1000)

    return {
        "median_ms": median(times),
        "times_ms": times,
        "rows": nrows,
        "version": version,
    }


# ── Polars ──

POLARS_DTYPES = {
    "u8": "UInt8", "i16": "Int16", "i32": "Int32",
    "i64": "Int64", "f64": "Float64",
    "sym": "Utf8", "str8": "Utf8", "str16": "Utf8",
}


def bench_polars(pattern, dtype, length):
    import polars as pl

    path = csv_path(pattern, dtype, length)
    n_iter, n_warmup = iter_counts(length)

    pl_dtype = getattr(pl, POLARS_DTYPES[dtype])
    df = pl.read_csv(path, schema={"v": pl_dtype})
    nrows = df.height

    for _ in range(n_warmup):
        df.sort("v")

    times = []
    for _ in range(n_iter):
        t0 = time.perf_counter()
        df.sort("v")
        times.append((time.perf_counter() - t0) * 1000)

    return {
        "median_ms": median(times),
        "times_ms": times,
        "rows": nrows,
        "version": pl.__version__,
    }


# ── RayForce ──

RF_TYPES = {
    "u8": "U8", "i16": "I16", "i32": "I32",
    "i64": "I64", "f64": "F64",
    "sym": "Symbol", "str8": "Symbol", "str16": "Symbol",
}


def bench_rayforce(pattern, dtype, length):
    import rayforce as rf

    path = csv_path(pattern, dtype, length)
    n_iter, n_warmup = iter_counts(length)

    rf_type = getattr(rf, RF_TYPES[dtype])
    t = rf.Table.from_csv([rf_type], path)
    nrows = len(t)

    for _ in range(n_warmup):
        t.order_by("v").execute()

    times = []
    for _ in range(n_iter):
        t0 = time.perf_counter()
        t.order_by("v").execute()
        times.append((time.perf_counter() - t0) * 1000)

    # Write result before cleanup — rayforce-py may segfault on exit
    result = {
        "median_ms": median(times),
        "times_ms": times,
        "rows": nrows,
        "version": rf.version,
    }
    return result


# ── Engine registry ──

ENGINES = {
    "duckdb": bench_duckdb,
    "teide": bench_teide,
    "polars": bench_polars,
    "rayforce": bench_rayforce,
}


# ── Main ──

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Sort benchmark")
    ap.add_argument("--engines", "-e", default="duckdb,teide,polars,rayforce")
    ap.add_argument("--patterns", default=",".join(PATTERNS))
    ap.add_argument("--types", default=",".join(DTYPES))
    ap.add_argument("--lengths", default=",".join(str(x) for x in LENGTHS))
    ap.add_argument("--max-length", type=int, default=None)
    ap.add_argument("--outdir", default=RESULTS_DIR)
    ap.add_argument("--rayforce-dir", default=None,
                    help="Build rayforce from this directory")
    ap.add_argument("--rayforce-branch", default=None,
                    help="Clone and build rayforce from this git branch")
    ap.add_argument("--teide-dir", default=None,
                    help="Build teide from this directory")
    ap.add_argument("--teide-branch", default=None,
                    help="Clone and build teide from this git branch")
    # Internal subprocess mode
    ap.add_argument("--_engine", help=argparse.SUPPRESS)
    ap.add_argument("--_pattern", help=argparse.SUPPRESS)
    ap.add_argument("--_dtype", help=argparse.SUPPRESS)
    ap.add_argument("--_length", type=int, help=argparse.SUPPRESS)
    ap.add_argument("--_result", help=argparse.SUPPRESS)
    args = ap.parse_args()

    # ── Subprocess mode ──
    if args._engine:
        data = ENGINES[args._engine](args._pattern, args._dtype, args._length)
        with open(args._result, "w") as f:
            json.dump(data, f)
        os._exit(0)  # skip Python cleanup — avoids rayforce segfault on exit

    # ── Orchestrator mode ──
    engines = [e.strip() for e in args.engines.split(",")]
    patterns = [p.strip() for p in args.patterns.split(",")]
    dtypes = [d.strip() for d in args.types.split(",")]
    lengths = sorted(int(x.strip()) for x in args.lengths.split(","))

    if args.max_length:
        lengths = [l for l in lengths if l <= args.max_length]

    # Source directories for engines (--dir takes precedence over --branch)
    src_dirs = {}
    for eng, d, b in [("rayforce", args.rayforce_dir, args.rayforce_branch),
                       ("teide", args.teide_dir, args.teide_branch)]:
        resolved = resolve_source(eng, d, b)
        if resolved:
            src_dirs[eng] = resolved

    # Build engines from custom directories
    for eng, src in src_dirs.items():
        if eng in engines:
            print(f"Building {eng} from {src}...")
            build_engine(eng, src)

    # Compute engine labels
    engine_labels = {}
    for eng in engines:
        engine_labels[eng] = engine_label(eng, src_dirs.get(eng))

    # Verify CSVs exist
    missing = []
    for p in patterns:
        for d in dtypes:
            for n in lengths:
                if not os.path.exists(csv_path(p, d, n)):
                    missing.append(f"{p}/{d}/{n}")
    if missing:
        print(f"Missing {len(missing)} CSV files. Run gen/gen_sort.py first.")
        for m in missing[:10]:
            print(f"  {m}")
        if len(missing) > 10:
            print(f"  ... and {len(missing) - 10} more")
        sys.exit(1)

    os.makedirs(args.outdir, exist_ok=True)

    total = len(engines) * len(patterns) * len(dtypes) * len(lengths)
    print(f"Sort benchmark: {total} tasks "
          f"({len(engines)} engines x {len(patterns)} patterns x {len(dtypes)} types x {len(lengths)} lengths)")

    results = []
    done = 0

    for engine in engines:
        if engine not in ENGINES:
            print(f"Unknown engine: {engine}")
            continue

        label_name = engine_labels.get(engine, engine)
        print(f"\n=== {label_name} ===")

        for pattern in patterns:
            for dtype in dtypes:
                for length in lengths:
                    done += 1
                    label = f"  {pattern}/{dtype}/{length}"

                    fd, result_path = tempfile.mkstemp(suffix=".json")
                    os.close(fd)
                    try:
                        proc = subprocess.run(
                            [sys.executable, os.path.abspath(__file__),
                             "--_engine", engine,
                             "--_pattern", pattern,
                             "--_dtype", dtype,
                             "--_length", str(length),
                             "--_result", result_path],
                            timeout=600,
                        )
                        if proc.returncode != 0:
                            print(f"{label:40s} ERROR (exit {proc.returncode})")
                            continue
                        with open(result_path) as f:
                            data = json.load(f)
                        ms = data["median_ms"]
                        print(f"{label:40s} {fmt(ms):>10s}  [{done}/{total}]")
                        results.append({
                            "engine": label_name,
                            "version": data.get("version", ""),
                            "pattern": pattern,
                            "dtype": dtype,
                            "length": length,
                            "median_ms": ms,
                            "times_ms": data["times_ms"],
                        })
                    except subprocess.TimeoutExpired:
                        print(f"{label:40s} TIMEOUT")
                    except Exception as e:
                        print(f"{label:40s} ERROR ({e})")
                    finally:
                        if os.path.exists(result_path):
                            os.unlink(result_path)

    # Merge with existing results
    out_path = os.path.join(args.outdir, "sort_results.json")
    existing = []
    if os.path.exists(out_path):
        with open(out_path) as f:
            existing = json.load(f).get("results", [])

    # Remove old entries that we just re-measured
    new_keys = set()
    for r in results:
        new_keys.add((r["engine"], r["pattern"], r["dtype"], r["length"]))
    merged = [r for r in existing
              if (r["engine"], r["pattern"], r["dtype"], r["length"]) not in new_keys]
    merged.extend(results)

    out = {
        "meta": {
            "cpu": platform.processor(),
            "os": platform.platform(),
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        },
        "results": merged,
    }
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2)
    print(f"\nResults saved to {out_path} ({len(merged)} total, {len(results)} new)")
