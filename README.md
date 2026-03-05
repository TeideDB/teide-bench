# Teide Benchmarks

H2O.ai db-benchmark: [Teide](https://github.com/TeideDB/teide) vs DuckDB vs Polars.

## Quick Start

```bash
git clone https://github.com/TeideDB/teide-bench.git
cd teide-bench
make bench
```

This will:
1. Create a Python venv and install DuckDB, Polars, and Teide
2. Generate 10M-row H2O.ai datasets
3. Run all benchmarks and print a comparison table

## Custom Size

```bash
make bench ROWS=1e8
```

## Run Specific Engines

```bash
make bench ENGINES=teide,duckdb
```

## Prerequisites

- Python 3.9+
- C compiler + CMake (only if `teide` is not yet on PyPI)

## Methodology

- Dataset: H2O.ai db-benchmark format (groupby, sort, join)
- Warmup: 3 iterations, Timed: 7 iterations (5 for joins), Report: median
- All engines use default thread counts (all available cores)

## Results

*Apple M3 Pro, 14 cores, 36 GB RAM, macOS 15.4*

| Query | Teide | DuckDB | Polars |
|-------|------:|-------:|-------:|
| q1 - id1, SUM v1 | 5.5ms | 8.4ms | 9.8ms |
| q2 - id1+id2, SUM v1 | 5.7ms | 15.9ms | 109.7ms |
| q3 - id3, SUM+AVG | 7.4ms | 59.8ms | 112.5ms |
| q5 - id6, 3xSUM | 12.3ms | 42.4ms | 93.5ms |
| q7 - 6-key, SUM+COUNT | 63.4ms | 189.1ms | 640.3ms |
| sort s1 - id1 ASC | 108.1ms | 174.9ms | 324.8ms |
| sort s6 - 3-key ASC | 136.0ms | 498.1ms | 874.6ms |
| join j1 - inner, 3-key | 36.5ms | 52.5ms | 216.5ms |

## License

MIT
