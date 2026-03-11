#!/usr/bin/env python3
"""H2O.ai benchmark: Teide vs DuckDB vs Polars vs GlareDB vs RayForce."""

import argparse
import gc
import json
import os
import platform
import subprocess
import sys
import tempfile
import time

import numpy as np

# Reuse path helpers from the generator
from gen.generate import n_label, dataset_prefix, join_dir_name

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATASETS = os.path.join(SCRIPT_DIR, "datasets")

N_ITER = 7
N_ITER_JOIN = 5
N_WARMUP = 3

N_ITER_CSV  = 3
N_WARMUP_CSV = 1

# CSV load dataset: 7 cols — i64, f64, str, date, timestamp, time, uuid
_CSV_HEADER = "i0,f0,s0,dt,ts,tm,g0\n"
_CSV_ALPHA  = np.frombuffer(b"abcdefghijklmnopqrstuvwxyz0123456789", dtype=np.uint8)
_CSV_HEX    = np.frombuffer(b"0123456789abcdef", dtype=np.uint8)


def median(lst):
    s = sorted(lst)
    return s[len(s) // 2]


def fmt(ms):
    if ms < 1:
        return f"{ms*1000:.0f}us"
    if ms < 1000:
        return f"{ms:.1f}ms"
    return f"{ms/1000:.2f}s"


# ── CSV load helpers ──

def csv_load_paths(n):
    d = os.path.join(DATASETS, f"csv_load_{n_label(n)}")
    return os.path.join(d, "s8.csv"), os.path.join(d, "s16.csv")


def _csv_gen_file(path, n_rows, str_len, seed):
    rng   = np.random.default_rng(seed)
    chunk = 500_000
    os.makedirs(os.path.dirname(path), exist_ok=True)
    print(f"  generating {os.path.basename(path)} ({n_rows:,} rows, str_len={str_len}) ...",
          flush=True)
    t0 = time.perf_counter()
    with open(path, "w", buffering=16 * 1024 * 1024) as fh:
        fh.write(_CSV_HEADER)
        for start in range(0, n_rows, chunk):
            n = min(chunk, n_rows - start)
            i0 = rng.integers(-(1 << 62), 1 << 62, n, dtype=np.int64).tolist()
            f0 = rng.uniform(-1e15, 1e15, n).tolist()
            # str cols: fixed-length a-z0-9
            s0 = _CSV_ALPHA[rng.integers(0, len(_CSV_ALPHA), (n, str_len), dtype=np.uint8)]
            s0 = s0.view(f"S{str_len}").ravel().tolist()
            # date: YYYY-MM-DD
            base_d = np.datetime64("1970-01-01", "D")
            dt = (base_d + rng.integers(0, 100_000, n).astype("timedelta64[D]")
                  ).astype("U10").tolist()
            # timestamp: YYYY-MM-DD HH:MM:SS  (~1.6B unique values)
            base_ts = np.datetime64("1970-01-01T00:00:00", "s")
            ts_arr = (base_ts + rng.integers(0, 1_600_000_000, n,
                      dtype=np.int64).astype("timedelta64[s]")).astype("U19").tolist()
            ts = [s.replace("T", " ") for s in ts_arr]
            # time: HH:MM:SS.ffffff  (~86.4B unique values)
            us = rng.integers(0, 86_400_000_000, n, dtype=np.int64).tolist()
            tm = [f"{u//3600000000:02d}:{(u//60000000)%60:02d}:"
                  f"{(u//1000000)%60:02d}.{u%1000000:06d}" for u in us]
            # uuid: vectorised hex, zero Python loop over chars
            raw  = rng.integers(0, 256, (n, 16), dtype=np.uint8)
            hi   = _CSV_HEX[raw >> 4]; lo = _CSV_HEX[raw & 0xf]
            h32  = np.empty((n, 32), dtype=np.uint8)
            h32[:, 0::2] = hi; h32[:, 1::2] = lo
            D = ord("-")
            ubuf = np.empty((n, 36), dtype=np.uint8)
            ubuf[:,  0: 8] = h32[:,  0: 8]; ubuf[:,  8] = D
            ubuf[:,  9:13] = h32[:,  8:12]; ubuf[:, 13] = D
            ubuf[:, 14:18] = h32[:, 12:16]; ubuf[:, 18] = D
            ubuf[:, 19:23] = h32[:, 16:20]; ubuf[:, 23] = D
            ubuf[:, 24:36] = h32[:, 20:32]
            g0 = ubuf.view("S36").ravel().tolist()
            fh.write("".join(
                f"{a},{b},{c.decode()},{d},{e},{f_},{u.decode()}\n"
                for a, b, c, d, e, f_, u
                in zip(i0, f0, s0, dt, ts, tm, g0)
            ))
            if (start + n) % 2_000_000 == 0:
                print(f"    {start + n:,} / {n_rows:,}", flush=True)
    elapsed = time.perf_counter() - t0
    size_mb = os.path.getsize(path) / 1024 ** 2
    print(f"  done: {size_mb:.0f} MB in {elapsed:.1f}s", flush=True)


def ensure_csv_load(n, seed):
    """Generate CSV load datasets for row count n if not present."""
    for path, str_len in zip(csv_load_paths(n), (8, 16)):
        ok = False
        if os.path.exists(path):
            try:
                with open(path) as fh:
                    ok = fh.readline().strip() == _CSV_HEADER.strip()
            except OSError:
                pass
        if not ok:
            if os.path.exists(path):
                print(f"  [regen] {os.path.basename(path)} — schema mismatch")
                os.remove(path)
            _csv_gen_file(path, n, str_len, seed)
        else:
            size_mb = os.path.getsize(path) / 1024 ** 2
            print(f"  [skip] {os.path.basename(path)} ({size_mb:.0f} MB)")


def csv_paths(n, k, seed):
    prefix = dataset_prefix(n, k, seed)
    ns = n_label(n)
    jd = join_dir_name(n)
    gb = os.path.join(DATASETS, prefix, f"{prefix}.csv")
    jx = os.path.join(DATASETS, jd, f"J1_{ns}_NA_0_0.csv")
    jy = os.path.join(DATASETS, jd, f"J1_{ns}_{ns}_0_0.csv")
    return gb, jx, jy


# ── DuckDB ──

def bench_duckdb(n, k, seed):
    import duckdb
    gb, jx, jy = csv_paths(n, k, seed)

    con = duckdb.connect()
    con.execute("RESET threads")
    nthreads = con.execute("SELECT current_setting('threads')").fetchone()[0]
    print(f"\n=== DuckDB {duckdb.__version__} ({nthreads} threads) ===")

    con.execute(f"CREATE TABLE df AS SELECT * FROM read_csv_auto('{gb}')")
    nrows = con.execute("SELECT COUNT(*) FROM df").fetchone()[0]
    print(f"  {nrows:,} rows loaded")

    def run(label, sql, n_iter=N_ITER):
        for _ in range(N_WARMUP):
            con.execute(f"CREATE OR REPLACE TABLE _r AS {sql}")
        times = []
        for _ in range(n_iter):
            t0 = time.perf_counter()
            con.execute(f"CREATE OR REPLACE TABLE _r AS {sql}")
            times.append((time.perf_counter() - t0) * 1000)
        con.execute("DROP TABLE IF EXISTS _r")
        ms = median(times)
        print(f"  {label:30s} {fmt(ms):>10s}")
        return ms

    res = {}
    res["q1"] = run("q1 - id1, SUM v1",
        "SELECT id1, SUM(v1) AS v1 FROM df GROUP BY id1")
    res["q2"] = run("q2 - id1+id2, SUM v1",
        "SELECT id1, id2, SUM(v1) AS v1 FROM df GROUP BY id1, id2")
    res["q3"] = run("q3 - id3, SUM+AVG",
        "SELECT id3, SUM(v1) AS v1, AVG(v3) AS v3 FROM df GROUP BY id3")
    res["q5"] = run("q5 - id6, 3xSUM",
        "SELECT id6, SUM(v1) AS v1, SUM(v2) AS v2, SUM(v3) AS v3 FROM df GROUP BY id6")
    res["q7"] = run("q7 - 6-key, SUM+COUNT",
        "SELECT id1,id2,id3,id4,id5,id6, SUM(v3) AS v3, COUNT(*) AS cnt "
        "FROM df GROUP BY id1,id2,id3,id4,id5,id6")

    res["s1"] = run("sort s1 - id1 ASC",
        "SELECT * FROM df ORDER BY id1")
    res["s6"] = run("sort s6 - 3-key ASC",
        "SELECT * FROM df ORDER BY id1, id2, id3")

    # df no longer needed — free before loading join tables
    con.execute("DROP TABLE df")

    con.execute(f"CREATE TABLE x AS SELECT * FROM read_csv_auto('{jx}')")
    con.execute(f"CREATE TABLE y AS SELECT * FROM read_csv_auto('{jy}')")
    res["j1"] = run("join j1 - inner, 3-key",
        "SELECT x.id1,x.id2,x.id3,x.v1,y.v2 FROM x "
        "INNER JOIN y ON x.id1=y.id1 AND x.id2=y.id2 AND x.id3=y.id3",
        n_iter=N_ITER_JOIN)

    # join tables no longer needed — free before CSV load
    con.execute("DROP TABLE x")
    con.execute("DROP TABLE y")

    s8, s16 = csv_load_paths(n)
    for key, path in [("csv_s8", s8), ("csv_s16", s16)]:
        for _ in range(N_WARMUP_CSV):
            con.execute(f"CREATE OR REPLACE TABLE _csv AS SELECT * FROM read_csv_auto('{path}')")
        times = []
        for _ in range(N_ITER_CSV):
            t0 = time.perf_counter()
            con.execute(f"CREATE OR REPLACE TABLE _csv AS SELECT * FROM read_csv_auto('{path}')")
            times.append((time.perf_counter() - t0) * 1000)
        con.execute("DROP TABLE IF EXISTS _csv")
        ms = median(times)
        print(f"  {'read_csv ' + key[4:]:30s} {fmt(ms):>10s}")
        res[key] = ms

    con.close()
    return {"results": res, "version": duckdb.__version__, "threads": nthreads}


# ── Polars ──

def bench_polars(n, k, seed):
    import polars as pl
    gb, jx, jy = csv_paths(n, k, seed)

    nthreads = pl.thread_pool_size()
    print(f"\n=== Polars {pl.__version__} ({nthreads} threads) ===")

    df = pl.read_csv(gb)
    print(f"  {df.height:,} rows loaded")

    def run(label, fn, n_iter=N_ITER):
        for _ in range(N_WARMUP):
            fn()
        times = []
        for _ in range(n_iter):
            t0 = time.perf_counter()
            fn()
            times.append((time.perf_counter() - t0) * 1000)
        ms = median(times)
        print(f"  {label:30s} {fmt(ms):>10s}")
        return ms

    res = {}
    res["q1"] = run("q1 - id1, SUM v1",
        lambda: df.group_by("id1").agg(pl.col("v1").sum()))
    res["q2"] = run("q2 - id1+id2, SUM v1",
        lambda: df.group_by(["id1", "id2"]).agg(pl.col("v1").sum()))
    res["q3"] = run("q3 - id3, SUM+AVG",
        lambda: df.group_by("id3").agg(pl.col("v1").sum(), pl.col("v3").mean()))
    res["q5"] = run("q5 - id6, 3xSUM",
        lambda: df.group_by("id6").agg(
            pl.col("v1").sum(), pl.col("v2").sum(), pl.col("v3").sum()))
    res["q7"] = run("q7 - 6-key, SUM+COUNT",
        lambda: df.group_by(["id1","id2","id3","id4","id5","id6"]).agg(
            pl.col("v3").sum(), pl.len()))

    res["s1"] = run("sort s1 - id1 ASC", lambda: df.sort("id1"))
    res["s6"] = run("sort s6 - 3-key ASC", lambda: df.sort(["id1", "id2", "id3"]))

    # df no longer needed — free before loading join tables
    del df
    gc.collect()

    x = pl.read_csv(jx)
    y = pl.read_csv(jy)
    res["j1"] = run("join j1 - inner, 3-key",
        lambda: x.join(y, on=["id1", "id2", "id3"], how="inner"),
        n_iter=N_ITER_JOIN)

    # join tables no longer needed — free before CSV load
    del x, y
    gc.collect()

    s8, s16 = csv_load_paths(n)
    for key, path in [("csv_s8", s8), ("csv_s16", s16)]:
        for _ in range(N_WARMUP_CSV):
            pl.read_csv(path, try_parse_dates=True)
        times = []
        for _ in range(N_ITER_CSV):
            t0 = time.perf_counter()
            pl.read_csv(path, try_parse_dates=True)
            times.append((time.perf_counter() - t0) * 1000)
        ms = median(times)
        print(f"  {'read_csv ' + key[4:]:30s} {fmt(ms):>10s}")
        res[key] = ms

    return {"results": res, "version": pl.__version__, "threads": nthreads}


# ── GlareDB ──

def bench_glaredb(n, k, seed):
    import glaredb
    gb, jx, jy = csv_paths(n, k, seed)

    con = glaredb.connect()
    version = "25.6.3"
    print(f"\n=== GlareDB {version} ===")

    con.sql(f"CREATE TEMP TABLE df AS SELECT * FROM read_csv('{gb}')")
    nrows_r = con.sql("SELECT COUNT(*) AS cnt FROM df")
    # GlareDB show() prints; parse count from a fresh query
    con.sql("SELECT 1")  # dummy
    print(f"  {n:,} rows loaded")

    def run(label, sql, n_iter=N_ITER):
        for _ in range(N_WARMUP):
            con.sql("DROP TABLE IF EXISTS _r")
            con.sql(f"CREATE TEMP TABLE _r AS {sql}")
        times = []
        for _ in range(n_iter):
            con.sql("DROP TABLE IF EXISTS _r")
            t0 = time.perf_counter()
            con.sql(f"CREATE TEMP TABLE _r AS {sql}")
            times.append((time.perf_counter() - t0) * 1000)
        con.sql("DROP TABLE IF EXISTS _r")
        ms = median(times)
        print(f"  {label:30s} {fmt(ms):>10s}")
        return ms

    res = {}
    res["q1"] = run("q1 - id1, SUM v1",
        "SELECT id1, SUM(v1) AS v1 FROM df GROUP BY id1")
    res["q2"] = run("q2 - id1+id2, SUM v1",
        "SELECT id1, id2, SUM(v1) AS v1 FROM df GROUP BY id1, id2")
    res["q3"] = run("q3 - id3, SUM+AVG",
        "SELECT id3, SUM(v1) AS v1, AVG(v3) AS v3 FROM df GROUP BY id3")
    res["q5"] = run("q5 - id6, 3xSUM",
        "SELECT id6, SUM(v1) AS v1, SUM(v2) AS v2, SUM(v3) AS v3 FROM df GROUP BY id6")
    res["q7"] = run("q7 - 6-key, SUM+COUNT",
        "SELECT id1,id2,id3,id4,id5,id6, SUM(v3) AS v3, COUNT(*) AS cnt "
        "FROM df GROUP BY id1,id2,id3,id4,id5,id6")

    res["s1"] = run("sort s1 - id1 ASC",
        "SELECT * FROM df ORDER BY id1")
    res["s6"] = run("sort s6 - 3-key ASC",
        "SELECT * FROM df ORDER BY id1, id2, id3")

    # df no longer needed — free before loading join tables
    con.sql("DROP TABLE IF EXISTS df")

    con.sql(f"CREATE TEMP TABLE x AS SELECT * FROM read_csv('{jx}')")
    con.sql(f"CREATE TEMP TABLE y AS SELECT * FROM read_csv('{jy}')")
    res["j1"] = run("join j1 - inner, 3-key",
        "SELECT x.id1,x.id2,x.id3,x.v1,y.v2 FROM x "
        "INNER JOIN y ON x.id1=y.id1 AND x.id2=y.id2 AND x.id3=y.id3",
        n_iter=N_ITER_JOIN)

    # join tables no longer needed — free before CSV load
    con.sql("DROP TABLE IF EXISTS x")
    con.sql("DROP TABLE IF EXISTS y")

    s8, s16 = csv_load_paths(n)
    for key, path in [("csv_s8", s8), ("csv_s16", s16)]:
        for _ in range(N_WARMUP_CSV):
            con.sql("DROP TABLE IF EXISTS _csv")
            con.sql(f"CREATE TEMP TABLE _csv AS SELECT * FROM read_csv('{path}')")
        times = []
        for _ in range(N_ITER_CSV):
            con.sql("DROP TABLE IF EXISTS _csv")
            t0 = time.perf_counter()
            con.sql(f"CREATE TEMP TABLE _csv AS SELECT * FROM read_csv('{path}')")
            times.append((time.perf_counter() - t0) * 1000)
        con.sql("DROP TABLE IF EXISTS _csv")
        ms = median(times)
        print(f"  {'read_csv ' + key[4:]:30s} {fmt(ms):>10s}")
        res[key] = ms

    con.close()
    return {"results": res, "version": version}


# ── Teide ──

def bench_teide(n, k, seed):
    import teide
    from teide.api import Context, col
    gb, jx, jy = csv_paths(n, k, seed)

    version = getattr(teide, "__version__", "dev")
    print(f"\n=== Teide {version} ===")

    res = {}

    # Context 1: groupby + sort + join — closed before CSV load
    with Context() as ctx:
        df = ctx.read_csv(gb)
        nrows = len(df)
        print(f"  {nrows:,} rows loaded")

        def run(label, fn, n_iter=N_ITER):
            for _ in range(N_WARMUP):
                fn()
            times = []
            for _ in range(n_iter):
                t0 = time.perf_counter()
                fn()
                times.append((time.perf_counter() - t0) * 1000)
            ms = median(times)
            print(f"  {label:30s} {fmt(ms):>10s}")
            return ms

        res["q1"] = run("q1 - id1, SUM v1",
            lambda: df.group_by("id1").agg(col("v1").sum()).collect())
        res["q2"] = run("q2 - id1+id2, SUM v1",
            lambda: df.group_by("id1", "id2").agg(col("v1").sum()).collect())
        res["q3"] = run("q3 - id3, SUM+AVG",
            lambda: df.group_by("id3").agg(
                col("v1").sum(), col("v3").mean()).collect())
        res["q5"] = run("q5 - id6, 3xSUM",
            lambda: df.group_by("id6").agg(
                col("v1").sum(), col("v2").sum(), col("v3").sum()).collect())
        res["q7"] = run("q7 - 6-key, SUM+COUNT",
            lambda: df.group_by("id1","id2","id3","id4","id5","id6").agg(
                col("v3").sum(), col("v1").count()).collect())

        res["s1"] = run("sort s1 - id1 ASC",
            lambda: df.sort("id1").collect())
        res["s6"] = run("sort s6 - 3-key ASC",
            lambda: df.sort("id1", "id2", "id3").collect())

        x = ctx.read_csv(jx)
        y = ctx.read_csv(jy)
        res["j1"] = run("join j1 - inner, 3-key",
            lambda: x.join(y, on=["id1", "id2", "id3"]),
            n_iter=N_ITER_JOIN)
    # Context 1 closed — all H2O.ai data freed

    s8, s16 = csv_load_paths(n)

    # Context 2: CSV s8
    with Context() as ctx:
        for _ in range(N_WARMUP_CSV):
            ctx.read_csv(s8)
        times = []
        for _ in range(N_ITER_CSV):
            t0 = time.perf_counter()
            ctx.read_csv(s8)
            times.append((time.perf_counter() - t0) * 1000)
    res["csv_s8"] = median(times)
    print(f"  {'read_csv s8':30s} {fmt(res['csv_s8']):>10s}")

    # Context 3: CSV s16
    with Context() as ctx:
        for _ in range(N_WARMUP_CSV):
            ctx.read_csv(s16)
        times = []
        for _ in range(N_ITER_CSV):
            t0 = time.perf_counter()
            ctx.read_csv(s16)
            times.append((time.perf_counter() - t0) * 1000)
    res["csv_s16"] = median(times)
    print(f"  {'read_csv s16':30s} {fmt(res['csv_s16']):>10s}")

    return {"results": res, "version": version}


# ── RayForce ──

def bench_rayforce(n, k, seed):
    import rayforce as rf
    print(f"\n=== RayForce {rf.version} ===")

    col_types = [
        rf.I64,
        rf.F64,
        rf.Symbol,
        rf.Date, rf.Timestamp, rf.Time,
        rf.GUID,
    ]

    res = {}
    s8, s16 = csv_load_paths(n)
    for key, path in [("csv_s8", s8), ("csv_s16", s16)]:
        for _ in range(N_WARMUP_CSV):
            rf.Table.from_csv(col_types, path)
        times = []
        for _ in range(N_ITER_CSV):
            t0 = time.perf_counter()
            rf.Table.from_csv(col_types, path)
            times.append((time.perf_counter() - t0) * 1000)
        ms = median(times)
        print(f"  {'read_csv ' + key[4:]:30s} {fmt(ms):>10s}")
        res[key] = ms

    return {"results": res, "version": rf.version}


# ── Main ──

ENGINES = {"duckdb": bench_duckdb, "polars": bench_polars, "glaredb": bench_glaredb,
           "teide": bench_teide, "rayforce": bench_rayforce}

QUERIES = [
    ("q1 - id1, SUM v1", "q1"),
    ("q2 - id1+id2, SUM v1", "q2"),
    ("q3 - id3, SUM+AVG", "q3"),
    ("q5 - id6, 3xSUM", "q5"),
    ("q7 - 6-key, SUM+COUNT", "q7"),
    ("sort s1 - id1 ASC", "s1"),
    ("sort s6 - 3-key ASC", "s6"),
    ("join j1 - inner, 3-key", "j1"),
    ("read_csv - str8+temporal+uuid", "csv_s8"),
    ("read_csv - str16+temporal+uuid", "csv_s16"),
]


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="H2O.ai benchmark: Teide vs DuckDB vs Polars")
    ap.add_argument("--rows", "-n", default="1e7", help="Row count (default: 1e7)")
    ap.add_argument("--k", "-K", type=int, default=100, help="Group cardinality (default: 100)")
    ap.add_argument("--seed", "-s", type=int, default=0, help="Dataset seed (default: 0)")
    ap.add_argument("--engines", "-e", default="duckdb,polars,glaredb,teide,rayforce",
                    help="Comma-separated engines (default: duckdb,polars,glaredb,teide,rayforce)")
    # Internal: single-engine subprocess mode
    ap.add_argument("--_engine", help=argparse.SUPPRESS)
    ap.add_argument("--_result", help=argparse.SUPPRESS)
    args = ap.parse_args()

    from gen.generate import parse_sci
    n = parse_sci(args.rows)
    k = args.k
    seed = args.seed

    # ── Subprocess mode: run one engine, write JSON result, exit ──
    if args._engine:
        data = ENGINES[args._engine](n, k, seed)
        with open(args._result, "w") as f:
            json.dump(data, f)
        sys.exit(0)

    # ── Orchestrator mode ──
    engines = [e.strip() for e in args.engines.split(",")]

    # Check H2O.ai datasets exist
    gb, jx, jy = csv_paths(n, k, seed)
    for p in [gb, jx, jy]:
        if not os.path.exists(p):
            print(f"Dataset not found: {p}")
            print(f"Run: python gen/generate.py --rows {args.rows} --k {k} --seed {seed}")
            exit(1)

    # Generate CSV load datasets if needed
    print("Preparing CSV load datasets...")
    ensure_csv_load(n, seed)

    all_results = {
        "rows": n, "k": k, "seed": seed,
        "cpu": platform.processor(), "os": platform.platform(),
    }

    for name in engines:
        if name not in ENGINES:
            print(f"Unknown engine: {name}")
            continue

        fd, result_path = tempfile.mkstemp(suffix=".json")
        os.close(fd)
        try:
            proc = subprocess.run(
                [sys.executable, os.path.abspath(__file__),
                 "--rows", args.rows, "--k", str(k), "--seed", str(seed),
                 "--_engine", name, "--_result", result_path],
            )
            if proc.returncode != 0:
                print(f"  [{name}] subprocess exited with code {proc.returncode}")
                continue
            with open(result_path) as f:
                all_results[name] = json.load(f)
        except ImportError:
            print(f"\n[skip] {name} not installed")
        finally:
            if os.path.exists(result_path):
                os.unlink(result_path)

    # Summary
    print(f"\n{'='*80}")
    print(f"SUMMARY - H2O.ai {n:,} rows")
    print(f"{'='*80}")

    header = f"  {'Query':<30s}"
    for name in engines:
        header += f" {name:>10s}"
    print(f"\n{header}")
    print(f"  {'-'*30}" + f" {'-'*10}" * len(engines))

    for label, key in QUERIES:
        line = f"  {label:<30s}"
        for name in engines:
            r = all_results.get(name, {}).get("results", {})
            ms = r.get(key)
            line += f" {fmt(ms) if ms else 'N/A':>10s}"
        print(line)

    out = os.path.join(SCRIPT_DIR, "results.json")
    with open(out, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\nResults saved to {out}")
