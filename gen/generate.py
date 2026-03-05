#!/usr/bin/env python3
"""Generate H2O.ai db-benchmark datasets (groupby + join)."""

import csv
import random
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
DATASETS = SCRIPT_DIR.parent / "datasets"


def parse_sci(s: str) -> int:
    s = s.lower().strip()
    if s.startswith("1e"):
        return int(10 ** int(s[2:]))
    return int(float(s))


def n_label(n: int) -> str:
    """Convert row count to H2O.ai label. Only exact powers of 10 get short form."""
    import math
    if n >= 10 and n == 10 ** int(math.log10(n)):
        return f"1e{int(math.log10(n))}"
    return str(n)


def dataset_prefix(n: int, k: int, seed: int) -> str:
    return f"G1_{n_label(n)}_{n_label(k)}_0_{seed}"


def join_dir_name(n: int) -> str:
    return f"J1_{n_label(n)}"


def generate_groupby(output_dir: Path, n: int, k: int, seed: int):
    random.seed(seed)
    n_high = max(n // k, k)

    id1_vals = [f"id{i:03d}" for i in range(1, k + 1)]
    id2_vals = [f"id{i:03d}" for i in range(1, k + 1)]
    id3_vals = [f"id{i:09d}" for i in range(1, n_high + 1)]

    name = dataset_prefix(n, k, seed)
    d = output_dir / name
    d.mkdir(parents=True, exist_ok=True)
    path = d / f"{name}.csv"

    print(f"Generating groupby: {n:,} rows -> {path}")
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id1", "id2", "id3", "id4", "id5", "id6", "v1", "v2", "v3"])
        for i in range(n):
            w.writerow([
                random.choice(id1_vals),
                random.choice(id2_vals),
                random.choice(id3_vals),
                random.randint(1, k),
                random.randint(1, k),
                random.randint(1, n_high),
                random.randint(1, 5),
                random.randint(1, 15),
                round(random.uniform(0, 100), 6),
            ])
            if (i + 1) % 1_000_000 == 0:
                print(f"  {i+1:,} / {n:,}")
    print(f"  Done: {path}")
    return path


def generate_join(output_dir: Path, n: int, k: int, seed: int):
    n_high = max(n // k, k)
    ns = n_label(n)
    d = output_dir / join_dir_name(n)
    d.mkdir(parents=True, exist_ok=True)

    for label, fname, vcol, s in [
        ("x", f"J1_{ns}_NA_0_0.csv", "v1", seed),
        ("y", f"J1_{ns}_{ns}_0_0.csv", "v2", seed + 1),
    ]:
        random.seed(s)
        path = d / fname
        print(f"Generating join {label}: {n:,} rows -> {path}")
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["id1", "id2", "id3", "id4", "id5", "id6", vcol])
            for i in range(n):
                w.writerow([
                    random.randint(1, k),
                    random.randint(1, k),
                    random.randint(1, n_high),
                    f"id{random.randint(1, k)}",
                    f"id{random.randint(1, k)}",
                    f"id{random.randint(1, n_high)}",
                    round(random.uniform(0, 100), 6),
                ])
                if (i + 1) % 1_000_000 == 0:
                    print(f"  {i+1:,} / {n:,}")
        print(f"  Done: {path}")


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Generate H2O.ai benchmark datasets")
    p.add_argument("--rows", "-n", default="1e7", help="Row count (default: 1e7)")
    p.add_argument("--type", "-t", choices=["groupby", "join", "all"], default="all")
    p.add_argument("--seed", "-s", type=int, default=0)
    p.add_argument("--k", "-K", type=int, default=100, help="Group cardinality (default: 100)")
    p.add_argument("--output", "-o", type=Path, default=None)
    args = p.parse_args()

    n = parse_sci(args.rows)
    out = args.output or DATASETS

    if args.type in ("groupby", "all"):
        generate_groupby(out, n, args.k, args.seed)
    if args.type in ("join", "all"):
        generate_join(out, n, args.k, args.seed)
