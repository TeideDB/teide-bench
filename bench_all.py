#!/usr/bin/env python3
"""Run H2O.ai benchmarks for Teide, DuckDB, and Polars. Output JSON results."""

import json
import time
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATASETS = os.path.join(SCRIPT_DIR, "datasets")
CSV_GROUPBY = os.path.join(DATASETS, "G1_1e7_1e2_0_0", "G1_1e7_1e2_0_0.csv")
CSV_JOIN_X = os.path.join(DATASETS, "h2oai_join_1e7", "J1_1e7_NA_0_0.csv")
CSV_JOIN_Y = os.path.join(DATASETS, "h2oai_join_1e7", "J1_1e7_1e7_0_0.csv")

N_ITER = 7
N_ITER_JOIN = 5
N_WARMUP = 3

results = {}


def median(lst):
    s = sorted(lst)
    return s[len(s) // 2]


def fmt(ms):
    if ms < 1:
        return f"{ms*1000:.0f}us"
    if ms < 1000:
        return f"{ms:.1f}ms"
    return f"{ms/1000:.2f}s"


# ─────────────────── DuckDB ───────────────────

def bench_duckdb():
    import duckdb

    con = duckdb.connect()
    con.execute("RESET threads")
    nthreads = con.execute("SELECT current_setting('threads')").fetchone()[0]
    print(f"\n=== DuckDB {duckdb.__version__} ({nthreads} threads) ===")

    # Load groupby data
    print(f"Loading groupby CSV...")
    t0 = time.perf_counter()
    con.execute(f"CREATE TABLE df AS SELECT * FROM read_csv_auto('{CSV_GROUPBY}')")
    load_ms = (time.perf_counter() - t0) * 1000
    nrows = con.execute("SELECT COUNT(*) FROM df").fetchone()[0]
    print(f"  {nrows:,} rows in {load_ms:.0f}ms")

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
        print(f"  {label:24s} {fmt(ms):>10s}")
        return ms

    res = {}

    # Groupby
    res["q1"] = run("q1 — id1, SUM",
        "SELECT id1, SUM(v1) AS v1 FROM df GROUP BY id1")
    res["q2"] = run("q2 — id1+id2, SUM",
        "SELECT id1, id2, SUM(v1) AS v1 FROM df GROUP BY id1, id2")
    res["q3"] = run("q3 — id3, SUM+AVG",
        "SELECT id3, SUM(v1) AS v1, AVG(v3) AS v3 FROM df GROUP BY id3")
    res["q5"] = run("q5 — id6, 3xSUM",
        "SELECT id6, SUM(v1) AS v1, SUM(v2) AS v2, SUM(v3) AS v3 FROM df GROUP BY id6")
    res["q7"] = run("q7 — 6-key, SUM+COUNT",
        "SELECT id1,id2,id3,id4,id5,id6, SUM(v3) AS v3, COUNT(*) AS cnt FROM df GROUP BY id1,id2,id3,id4,id5,id6")

    # Sort
    res["s1"] = run("sort s1 — id1 ASC",
        "SELECT * FROM df ORDER BY id1")
    res["s6"] = run("sort s6 — 3-key ASC",
        "SELECT * FROM df ORDER BY id1, id2, id3")

    # Join
    print(f"  Loading join CSVs...")
    con.execute(f"CREATE TABLE x AS SELECT * FROM read_csv_auto('{CSV_JOIN_X}')")
    con.execute(f"CREATE TABLE small AS SELECT * FROM read_csv_auto('{CSV_JOIN_Y}')")

    res["j1"] = run("join j1 — inner, 3-key",
        "SELECT x.id1,x.id2,x.id3,x.v1,small.v2 FROM x INNER JOIN small ON x.id1=small.id1 AND x.id2=small.id2 AND x.id3=small.id3",
        n_iter=N_ITER_JOIN)

    con.close()
    results["duckdb"] = res
    results["duckdb_version"] = duckdb.__version__
    results["duckdb_threads"] = nthreads
    return res


# ─────────────────── Polars ───────────────────

def bench_polars():
    import polars as pl

    print(f"\n=== Polars {pl.__version__} ===")

    print(f"Loading groupby CSV...")
    t0 = time.perf_counter()
    df = pl.read_csv(CSV_GROUPBY)
    load_ms = (time.perf_counter() - t0) * 1000
    print(f"  {df.height:,} rows in {load_ms:.0f}ms")

    def run(label, query_fn, n_iter=N_ITER):
        for _ in range(N_WARMUP):
            query_fn(df)
        times = []
        for _ in range(n_iter):
            t0 = time.perf_counter()
            query_fn(df)
            times.append((time.perf_counter() - t0) * 1000)
        ms = median(times)
        print(f"  {label:24s} {fmt(ms):>10s}")
        return ms

    res = {}

    res["q1"] = run("q1 — id1, SUM",
        lambda d: d.group_by("id1").agg(pl.col("v1").sum()))
    res["q2"] = run("q2 — id1+id2, SUM",
        lambda d: d.group_by(["id1", "id2"]).agg(pl.col("v1").sum()))
    res["q3"] = run("q3 — id3, SUM+AVG",
        lambda d: d.group_by("id3").agg(pl.col("v1").sum(), pl.col("v3").mean()))
    res["q5"] = run("q5 — id6, 3xSUM",
        lambda d: d.group_by("id6").agg(pl.col("v1").sum(), pl.col("v2").sum(), pl.col("v3").sum()))
    res["q7"] = run("q7 — 6-key, SUM+COUNT",
        lambda d: d.group_by(["id1","id2","id3","id4","id5","id6"]).agg(pl.col("v3").sum(), pl.len()))

    # Sort
    res["s1"] = run("sort s1 — id1 ASC",
        lambda d: d.sort("id1"))
    res["s6"] = run("sort s6 — 3-key ASC",
        lambda d: d.sort(["id1", "id2", "id3"]))

    # Join
    print(f"  Loading join CSVs...")
    x = pl.read_csv(CSV_JOIN_X)
    small = pl.read_csv(CSV_JOIN_Y)

    def join_fn(_):
        return x.join(small, on=["id1", "id2", "id3"], how="inner")

    for _ in range(N_WARMUP):
        join_fn(None)
    times = []
    for _ in range(N_ITER_JOIN):
        t0 = time.perf_counter()
        join_fn(None)
        times.append((time.perf_counter() - t0) * 1000)
    ms = median(times)
    res["j1"] = ms
    print(f"  {'join j1 — inner, 3-key':24s} {fmt(ms):>10s}")

    results["polars"] = res
    results["polars_version"] = pl.__version__
    return res


# ─────────────────── Teide ───────────────────

def bench_teide():
    sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "teide-py"))
    os.environ["TEIDE_LIB"] = os.path.join(SCRIPT_DIR, "..", "teide", "build", "libteide.so")

    from teide import TeideLib, OP_SUM, OP_AVG, OP_COUNT
    import ctypes

    lib = TeideLib()
    lib.arena_init()
    lib.sym_init()

    print(f"\n=== Teide (C17 engine) ===")

    print(f"Loading groupby CSV...")
    t0 = time.perf_counter()
    tbl = lib.read_csv(CSV_GROUPBY)
    load_ms = (time.perf_counter() - t0) * 1000
    nrows = lib.table_nrows(tbl)
    print(f"  {nrows:,} rows in {load_ms:.0f}ms")

    def run_groupby(label, key_names, agg_ops, agg_col_names, n_iter=N_ITER):
        g = lib.graph_new(tbl)
        try:
            keys = [lib.scan(g, k) for k in key_names]
            agg_ins = [lib.scan(g, c) for c in agg_col_names]
            nk = len(keys)
            na = len(agg_ops)
            keys_arr = (ctypes.c_void_p * nk)(*keys)
            ops_arr = (ctypes.c_uint16 * na)(*agg_ops)
            ins_arr = (ctypes.c_void_p * na)(*agg_ins)
            root = lib._lib.td_group(g, keys_arr, nk, ops_arr, ins_arr, na)
            root = lib.optimize(g, root)

            for _ in range(N_WARMUP):
                r = lib.execute(g, root)
                if r and r >= 32:
                    lib.release(r)

            times = []
            for _ in range(n_iter):
                t0 = time.perf_counter()
                r = lib.execute(g, root)
                times.append((time.perf_counter() - t0) * 1000)
                if r and r >= 32:
                    lib.release(r)

            ms = median(times)
            print(f"  {label:24s} {fmt(ms):>10s}")
            return ms
        finally:
            lib.graph_free(g)

    def run_sort(label, col_names, descs, n_iter=N_ITER):
        g = lib.graph_new(tbl)
        try:
            table_node = lib.const_table(g, tbl)
            keys = [lib.scan(g, c) for c in col_names]
            root = lib.sort_op(g, table_node, keys, descs)
            root = lib.optimize(g, root)

            for _ in range(N_WARMUP):
                r = lib.execute(g, root)
                if r and r >= 32:
                    lib.release(r)

            times = []
            for _ in range(n_iter):
                t0 = time.perf_counter()
                r = lib.execute(g, root)
                times.append((time.perf_counter() - t0) * 1000)
                if r and r >= 32:
                    lib.release(r)

            ms = median(times)
            print(f"  {label:24s} {fmt(ms):>10s}")
            return ms
        finally:
            lib.graph_free(g)

    res = {}

    res["q1"] = run_groupby("q1 — id1, SUM", ["id1"], [OP_SUM], ["v1"])
    res["q2"] = run_groupby("q2 — id1+id2, SUM", ["id1", "id2"], [OP_SUM], ["v1"])
    res["q3"] = run_groupby("q3 — id3, SUM+AVG", ["id3"], [OP_SUM, OP_AVG], ["v1", "v3"])
    res["q5"] = run_groupby("q5 — id6, 3xSUM", ["id6"], [OP_SUM, OP_SUM, OP_SUM], ["v1", "v2", "v3"])
    res["q7"] = run_groupby("q7 — 6-key, SUM+COUNT",
        ["id1", "id2", "id3", "id4", "id5", "id6"], [OP_SUM, OP_COUNT], ["v3", "v1"])

    res["s1"] = run_sort("sort s1 — id1 ASC", ["id1"], [0])
    res["s6"] = run_sort("sort s6 — 3-key ASC", ["id1", "id2", "id3"], [0, 0, 0])

    # Join
    print(f"  Loading join CSVs...")
    x = lib.read_csv(CSV_JOIN_X)
    y = lib.read_csv(CSV_JOIN_Y)

    g = lib.graph_new(x)
    try:
        left_node = lib.const_table(g, x)
        right_node = lib.const_table(g, y)
        left_keys = [lib.scan(g, k) for k in ["id1", "id2", "id3"]]
        right_keys = []
        for k in ["id1", "id2", "id3"]:
            name_id = lib.sym_intern(k)
            col_vec = lib._lib.td_table_get_col(y, name_id)
            right_keys.append(lib.const_vec(g, col_vec))

        root = lib.join(g, left_node, left_keys, right_node, right_keys, 0)
        root = lib.optimize(g, root)

        for _ in range(N_WARMUP):
            r = lib.execute(g, root)
            if r and r >= 32:
                lib.release(r)

        times = []
        for _ in range(N_ITER_JOIN):
            t0 = time.perf_counter()
            r = lib.execute(g, root)
            times.append((time.perf_counter() - t0) * 1000)
            if r and r >= 32:
                lib.release(r)

        ms = median(times)
        res["j1"] = ms
        print(f"  {'join j1 — inner, 3-key':24s} {fmt(ms):>10s}")
    finally:
        lib.graph_free(g)

    lib.release(y)
    lib.release(x)
    lib.release(tbl)
    lib.sym_destroy()
    lib.arena_destroy_all()

    results["teide"] = res
    return res


# ─────────────────── Main ───────────────────

if __name__ == "__main__":
    for path in [CSV_GROUPBY, CSV_JOIN_X, CSV_JOIN_Y]:
        if not os.path.exists(path):
            print(f"Dataset not found: {path}")
            sys.exit(1)

    bench_duckdb()
    bench_polars()
    bench_teide()

    # Summary table
    print("\n\n" + "="*80)
    print("SUMMARY — H2O.ai 10M rows")
    print("="*80)

    queries = [
        ("q1 — id1, SUM", "q1"),
        ("q2 — id1+id2, SUM", "q2"),
        ("q3 — id3, SUM+AVG", "q3"),
        ("q5 — id6, 3xSUM", "q5"),
        ("q7 — 6-key, SUM+COUNT", "q7"),
        ("sort s1 — id1 ASC", "s1"),
        ("sort s6 — 3-key ASC", "s6"),
        ("join j1 — inner, 3-key", "j1"),
    ]

    print(f"\n  {'Query':<28s} {'Teide':>10s} {'DuckDB':>10s} {'Polars':>10s}")
    print(f"  {'-'*28} {'-'*10} {'-'*10} {'-'*10}")

    for label, key in queries:
        t = results.get("teide", {}).get(key)
        d = results.get("duckdb", {}).get(key)
        p = results.get("polars", {}).get(key)
        print(f"  {label:<28s} {fmt(t) if t else 'N/A':>10s} {fmt(d) if d else 'N/A':>10s} {fmt(p) if p else 'N/A':>10s}")

    # Save JSON
    out_path = os.path.join(SCRIPT_DIR, "results.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to {out_path}")
