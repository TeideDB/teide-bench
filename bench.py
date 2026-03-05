#!/usr/bin/env python3
"""H2O.ai benchmark: Teide vs DuckDB vs Polars."""

import argparse
import json
import os
import platform
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATASETS = os.path.join(SCRIPT_DIR, "datasets")

N_ITER = 7
N_ITER_JOIN = 5
N_WARMUP = 3


def parse_sci(s):
    s = s.lower().strip()
    if s.startswith("1e"):
        return int(10 ** int(s[2:]))
    return int(float(s))


def n_label(n):
    for exp in range(9, 0, -1):
        if n >= 10**exp:
            return f"1e{exp}"
    return str(n)


def median(lst):
    s = sorted(lst)
    return s[len(s) // 2]


def fmt(ms):
    if ms < 1:
        return f"{ms*1000:.0f}us"
    if ms < 1000:
        return f"{ms:.1f}ms"
    return f"{ms/1000:.2f}s"


def csv_paths(n):
    ns = n_label(n)
    ks = "1e2"
    gb = os.path.join(DATASETS, f"G1_{ns}_{ks}_0_0", f"G1_{ns}_{ks}_0_0.csv")
    jx = os.path.join(DATASETS, f"J1_{ns}", f"J1_{ns}_NA_0_0.csv")
    jy = os.path.join(DATASETS, f"J1_{ns}", f"J1_{ns}_{ns}_0_0.csv")
    return gb, jx, jy


# ── DuckDB ──

def bench_duckdb(n):
    import duckdb
    gb, jx, jy = csv_paths(n)

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
    con.execute(f"CREATE TABLE small AS SELECT * FROM read_csv_auto('{jy}')")
    res["j1"] = run("join j1 - inner, 3-key",
        "SELECT x.id1,x.id2,x.id3,x.v1,small.v2 FROM x "
        "INNER JOIN small ON x.id1=small.id1 AND x.id2=small.id2 AND x.id3=small.id3",
        n_iter=N_ITER_JOIN)

    con.close()
    return {"results": res, "version": duckdb.__version__, "threads": nthreads}


# ── Polars ──

def bench_polars(n):
    import polars as pl
    gb, jx, jy = csv_paths(n)

    print(f"\n=== Polars {pl.__version__} ===")

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

    return {"results": res, "version": pl.__version__}


# ── Teide ──

def bench_teide(n):
    from teide.api import Context, col
    gb, jx, jy = csv_paths(n)

    print(f"\n=== Teide ===")

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

    return {"results": res}


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
    ap.add_argument("--engines", "-e", default="duckdb,polars,teide",
                    help="Comma-separated engines (default: duckdb,polars,teide)")
    args = ap.parse_args()

    n = parse_sci(args.rows)
    engines = [e.strip() for e in args.engines.split(",")]

    # Check datasets exist
    gb, jx, jy = csv_paths(n)
    for p in [gb, jx, jy]:
        if not os.path.exists(p):
            print(f"Dataset not found: {p}")
            print(f"Run: python gen/generate.py --rows {args.rows}")
            exit(1)

    all_results = {"rows": n, "cpu": platform.processor(), "os": platform.platform()}

    for name in engines:
        if name not in ENGINES:
            print(f"Unknown engine: {name}")
            continue
        data = ENGINES[name](n)
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
