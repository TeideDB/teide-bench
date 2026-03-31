#!/usr/bin/env python3
"""Generate single-column CSV files for sort benchmarks."""

import argparse
import os
import sys
import time

import numpy as np

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATASETS = os.path.join(os.path.dirname(SCRIPT_DIR), "datasets", "sort_bench")

PATTERNS = ["random", "few_unique", "nearly_sorted", "rev_nearly_sorted"]
DTYPES = ["u8", "i16", "i32", "i64", "f64", "sym", "str8", "str16"]
def _make_lengths():
    pts = [0]
    for exp in range(7):  # 1..10_000_000
        base = 10 ** exp
        for m in range(1, 10):
            pts.append(base * m)
    pts.append(10_000_000)
    pts.append(100_000_000)
    return sorted(set(pts))

LENGTHS = _make_lengths()

ALPHA = np.frombuffer(b"abcdefghijklmnopqrstuvwxyz0123456789", dtype=np.uint8)
SWAP_FRACTION = 0.01  # 1% swaps for nearly sorted


def _gen_random(rng, dtype, n):
    """Generate random base array for given dtype."""
    if dtype == "u8":
        return rng.integers(0, 256, n, dtype=np.uint8)
    if dtype == "i16":
        return rng.integers(-32768, 32768, n, dtype=np.int16)
    if dtype == "i32":
        return rng.integers(-(1 << 30), 1 << 30, n, dtype=np.int32)
    if dtype == "i64":
        return rng.integers(-(1 << 62), 1 << 62, n, dtype=np.int64)
    if dtype == "f64":
        return rng.uniform(-1e9, 1e9, n)
    if dtype == "sym":
        pool = [f"s{i}" for i in range(1000)]
        return np.array([pool[i] for i in rng.integers(0, len(pool), n)])
    if dtype == "str8":
        raw = ALPHA[rng.integers(0, len(ALPHA), (n, 8), dtype=np.uint8)]
        return raw.view("S8").ravel()
    if dtype == "str16":
        raw = ALPHA[rng.integers(0, len(ALPHA), (n, 16), dtype=np.uint8)]
        return raw.view("S16").ravel()
    raise ValueError(f"Unknown dtype: {dtype}")


def _apply_few_unique(rng, arr, n):
    """Replace values with a small pool of unique values."""
    k = min(max(int(n ** 0.5), 1), 100)
    pool = arr[:k].copy() if len(arr) >= k else arr.copy()
    idx = rng.integers(0, len(pool), n)
    return pool[idx]


def _apply_nearly_sorted(arr, rng, n):
    """Sort then swap ~1% of elements."""
    arr.sort()
    swaps = max(1, int(n * SWAP_FRACTION))
    i = rng.integers(0, n, swaps)
    j = rng.integers(0, n, swaps)
    arr[i], arr[j] = arr[j].copy(), arr[i].copy()
    return arr


def _apply_rev_nearly_sorted(arr, rng, n):
    """Sort descending then swap ~1% of elements."""
    arr[::-1].sort()
    swaps = max(1, int(n * SWAP_FRACTION))
    i = rng.integers(0, n, swaps)
    j = rng.integers(0, n, swaps)
    arr[i], arr[j] = arr[j].copy(), arr[i].copy()
    return arr


def _is_string_type(dtype):
    return dtype in ("sym", "str8", "str16")


def _format_val(v, dtype):
    if _is_string_type(dtype):
        s = v.decode() if isinstance(v, bytes) else str(v)
        return s
    if dtype == "f64":
        return f"{v:.6f}"
    return str(v)


def gen_file(pattern, dtype, n, seed=42):
    """Generate one CSV file."""
    path = os.path.join(DATASETS, pattern, dtype, f"{n}.csv")

    # Check if exists with correct line count
    if os.path.exists(path):
        try:
            with open(path) as f:
                header = f.readline().strip()
                if header == "v":
                    # Count lines efficiently
                    count = sum(1 for _ in f)
                    if count == n:
                        return path
        except OSError:
            pass

    os.makedirs(os.path.dirname(path), exist_ok=True)
    rng = np.random.default_rng(seed)

    if n == 0:
        with open(path, "w") as f:
            f.write("v\n")
        return path

    # Generate base random data
    arr = _gen_random(rng, dtype, n)

    # Apply pattern
    if pattern == "few_unique":
        arr = _apply_few_unique(rng, arr, n)
    elif pattern == "nearly_sorted":
        if _is_string_type(dtype):
            idx = np.argsort(arr)
            arr = arr[idx]
            swaps = max(1, int(n * SWAP_FRACTION))
            i = rng.integers(0, n, swaps)
            j = rng.integers(0, n, swaps)
            arr[i], arr[j] = arr[j].copy(), arr[i].copy()
        else:
            arr = _apply_nearly_sorted(arr, rng, n)
    elif pattern == "rev_nearly_sorted":
        if _is_string_type(dtype):
            idx = np.argsort(arr)[::-1]
            arr = arr[idx]
            swaps = max(1, int(n * SWAP_FRACTION))
            i = rng.integers(0, n, swaps)
            j = rng.integers(0, n, swaps)
            arr[i], arr[j] = arr[j].copy(), arr[i].copy()
        else:
            arr = _apply_rev_nearly_sorted(arr, rng, n)
    # pattern == "random" — use as-is

    # Write CSV in chunks
    chunk = min(1_000_000, n)
    t0 = time.perf_counter()
    with open(path, "w", buffering=16 * 1024 * 1024) as f:
        f.write("v\n")
        for start in range(0, n, chunk):
            end = min(start + chunk, n)
            lines = "\n".join(_format_val(arr[i], dtype) for i in range(start, end))
            f.write(lines + "\n")
    elapsed = time.perf_counter() - t0
    size_mb = os.path.getsize(path) / 1024 ** 2
    print(f"  {pattern}/{dtype}/{n}: {size_mb:.0f} MB in {elapsed:.1f}s")
    return path


def main():
    ap = argparse.ArgumentParser(description="Generate sort benchmark data")
    ap.add_argument("--patterns", default=",".join(PATTERNS),
                    help=f"Comma-separated patterns (default: {','.join(PATTERNS)})")
    ap.add_argument("--types", default=",".join(DTYPES),
                    help=f"Comma-separated types (default: {','.join(DTYPES)})")
    ap.add_argument("--lengths", default=",".join(str(x) for x in LENGTHS),
                    help=f"Comma-separated lengths")
    ap.add_argument("--max-length", type=int, default=None,
                    help="Cap maximum length (e.g. 10000000)")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    patterns = [p.strip() for p in args.patterns.split(",")]
    dtypes = [d.strip() for d in args.types.split(",")]
    lengths = [int(x.strip()) for x in args.lengths.split(",")]

    if args.max_length:
        lengths = [l for l in lengths if l <= args.max_length]

    total = len(patterns) * len(dtypes) * len(lengths)
    print(f"Generating {total} CSV files ({len(patterns)} patterns x {len(dtypes)} types x {len(lengths)} lengths)")

    done = 0
    for pattern in patterns:
        for dtype in dtypes:
            for n in lengths:
                gen_file(pattern, dtype, n, seed=args.seed)
                done += 1
                if done % 50 == 0:
                    print(f"  [{done}/{total}]")

    print(f"Done. {done} files in {DATASETS}")


if __name__ == "__main__":
    main()
