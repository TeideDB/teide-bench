#!/usr/bin/env python3
"""H2O.ai benchmark: Teide vs DuckDB vs Polars."""

import argparse
import json
import os
import platform
import time

# Reuse path helpers from the generator
from gen.generate import n_label, dataset_prefix, join_dir_name

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATASETS = os.path.join(SCRIPT_DIR, "datasets")

N_ITER = 7
N_ITER_JOIN = 5
N_WARMUP = 3


def median(lst):
    s = sorted(lst)
    return s[len(s) // 2]


def fmt(ms):
    if ms < 1:
        return f"{ms*1000:.0f}us"
    if ms < 1000:
        return f"{ms:.1f}ms"
    return f"{ms/1000:.2f}s"


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

    con.execute(f"CREATE TABLE x AS SELECT * FROM read_csv_auto('{jx}')")
    con.execute(f"CREATE TABLE y AS SELECT * FROM read_csv_auto('{jy}')")
    res["j1"] = run("join j1 - inner, 3-key",
        "SELECT x.id1,x.id2,x.id3,x.v1,y.v2 FROM x "
        "INNER JOIN y ON x.id1=y.id1 AND x.id2=y.id2 AND x.id3=y.id3",
        n_iter=N_ITER_JOIN)

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

    x = pl.read_csv(jx)
    y = pl.read_csv(jy)
    res["j1"] = run("join j1 - inner, 3-key",
        lambda: x.join(y, on=["id1", "id2", "id3"], how="inner"),
        n_iter=N_ITER_JOIN)

    return {"results": res, "version": pl.__version__, "threads": nthreads}


# ── Teide ──

def bench_teide(n, k, seed):
    import teide
    from teide.api import Context, col
    gb, jx, jy = csv_paths(n, k, seed)

    version = getattr(teide, "__version__", "dev")
    print(f"\n=== Teide {version} ===")

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

        res = {}
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

        def join_fn():
            return x.join(y, on=["id1", "id2", "id3"])

        res["j1"] = run("join j1 - inner, 3-key", join_fn, n_iter=N_ITER_JOIN)

    return {"results": res, "version": version}


# ── Main ──

ENGINES = {"duckdb": bench_duckdb, "polars": bench_polars, "teide": bench_teide}

QUERIES = [
    ("q1 - id1, SUM v1", "q1"),
    ("q2 - id1+id2, SUM v1", "q2"),
    ("q3 - id3, SUM+AVG", "q3"),
    ("q5 - id6, 3xSUM", "q5"),
    ("q7 - 6-key, SUM+COUNT", "q7"),
    ("sort s1 - id1 ASC", "s1"),
    ("sort s6 - 3-key ASC", "s6"),
    ("join j1 - inner, 3-key", "j1"),
]


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="H2O.ai benchmark: Teide vs DuckDB vs Polars")
    ap.add_argument("--rows", "-n", default="1e7", help="Row count (default: 1e7)")
    ap.add_argument("--k", "-K", type=int, default=100, help="Group cardinality (default: 100)")
    ap.add_argument("--seed", "-s", type=int, default=0, help="Dataset seed (default: 0)")
    ap.add_argument("--engines", "-e", default="duckdb,polars,teide",
                    help="Comma-separated engines (default: duckdb,polars,teide)")
    args = ap.parse_args()

    from gen.generate import parse_sci
    n = parse_sci(args.rows)
    k = args.k
    seed = args.seed
    engines = [e.strip() for e in args.engines.split(",")]

    # Check datasets exist
    gb, jx, jy = csv_paths(n, k, seed)
    for p in [gb, jx, jy]:
        if not os.path.exists(p):
            print(f"Dataset not found: {p}")
            print(f"Run: python gen/generate.py --rows {args.rows} --k {k} --seed {seed}")
            exit(1)

    all_results = {
        "rows": n, "k": k, "seed": seed,
        "cpu": platform.processor(), "os": platform.platform(),
    }

    for name in engines:
        if name not in ENGINES:
            print(f"Unknown engine: {name}")
            continue
        data = ENGINES[name](n, k, seed)
        all_results[name] = data

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
