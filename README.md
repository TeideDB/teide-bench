# Teide Benchmarks

H2O.ai db-benchmark: [Teide](https://github.com/TeideDB/teide) vs DuckDB vs Polars vs GlareDB vs [RayForce](https://github.com/RayforceDB/rayforce).

## Quick Start

```bash
git clone https://github.com/TeideDB/teide-bench.git
cd teide-bench
make bench
```

This will:
1. Create a Python venv and install DuckDB, Polars, GlareDB, Teide, and RayForce
2. Generate 10M-row H2O.ai datasets
3. Run all benchmarks and print a comparison table
4. Generate an interactive histogram (`results/bench.html`)

## Custom Size

```bash
make bench ROWS=1e8
```

## Run Specific Engines

```bash
make bench ENGINES=teide,duckdb
```

## Build from Custom Source

Test a local development branch of RayForce or Teide:

```bash
# RayForce from local directory
.venv/bin/python bench.py --engines teide,rayforce --rayforce-dir /path/to/rayforce

# RayForce from git branch (clones from GitHub)
.venv/bin/python bench.py --engines teide,rayforce --rayforce-branch feature/new-sort

# Teide from local directory
.venv/bin/python bench.py --engines teide,duckdb --teide-dir /path/to/teide

# Teide from git branch
.venv/bin/python bench.py --engines teide,duckdb --teide-branch feature/new-join
```

The engine label in results includes branch and commit, e.g. `rayforce@serhii/sort (0d88dc46)`.
Run the benchmark twice with different branches — results merge automatically for comparison.

## Sort Benchmark

Comprehensive single-column sort benchmark across data patterns, types, and vector sizes.

### Generate Data

```bash
.venv/bin/python gen/gen_sort.py --max-length 10000000
```

### Run Sort Benchmark

```bash
# All engines
.venv/bin/python sort_bench_multi.py --max-length 10000000

# Specific engines
.venv/bin/python sort_bench_multi.py --engines duckdb,teide --max-length 1000000

# With custom RayForce build
.venv/bin/python sort_bench_multi.py --engines rayforce --rayforce-dir /path/to/rayforce --max-length 10000000

# Specific patterns/types
.venv/bin/python sort_bench_multi.py --engines duckdb --patterns random,nearly_sorted --types i64,f64
```

### Generate Plots

```bash
.venv/bin/python sort_bench_plot.py
```

### Sort Benchmark Parameters

**Data patterns:** random, few_unique, nearly_sorted, rev_nearly_sorted

**Data types:** u8, i16, i32, i64, f64, sym, str8, str16

**Vector lengths:** 1 to 100M with intermediate points (1,2,3,...,9,10,20,...,90,100,...)

## Results

All output goes to `results/`:

| File | Description |
|------|-------------|
| `results/bench_results.json` | H2O benchmark raw data |
| `results/bench.html` | H2O interactive histogram |
| `results/sort_results.json` | Sort benchmark raw data |
| `results/sort_bench.html` | Sort interactive chart (log-log, filterable) |
| `results/sort_data_viz.html` | Input data pattern visualization |

Results are merged across runs — run a new engine or source directory without losing previous measurements.

## Methodology

- **H2O benchmark:** groupby (q1-q7), sort (s1, s6), join (j1), read_csv (s8, s16)
- **Each operation runs in a separate subprocess** — memory freed between measurements, no swap interference
- **Warmup:** 3 iterations (groupby/sort/join), 1 (csv)
- **Timed:** 7 iterations (groupby/sort), 5 (join), 3 (csv)
- **Report:** median

## Prerequisites

- Python 3.9+
- C compiler (clang or gcc)
- make

## License

MIT
