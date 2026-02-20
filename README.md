# Teide Benchmarks

Benchmark suite for the [Teide](https://github.com/TeideDB/teide) columnar dataframe engine, comparing against DuckDB on H2O.ai benchmark queries.

## Setup

```bash
pip install -e .
```

Requires benchmark datasets at `../rayforce-bench/datasets/` (H2O.ai 10M row CSVs).

## Benchmark Results (10M rows, Teide single-process vs DuckDB 28 threads)

### Groupby

| Query | Teide | DuckDB | Speedup |
|-------|-------|--------|---------|
| q1 (id1, SUM) | 2.2ms | 8.1ms | 3.7x |
| q2 (id1+id2, SUM) | 6.8ms | 21.5ms | 3.2x |
| q3 (id3, SUM+AVG) | 18ms | 65ms | 3.6x |
| q4 (id4, 3xAVG) | 5.0ms | 5.0ms | TIED |
| q5 (id6, 3xSUM) | 32ms | 52ms | 1.6x |
| q6 (id3, MAX+MIN) | 24ms | 66.5ms | 2.8x |
| q7 (6-key, SUM+COUNT) | 82ms | 177ms | 2.2x |

### Sort

| Query | Teide | DuckDB | Speedup |
|-------|-------|--------|---------|
| s1 (id1 SYM ASC) | 101ms | 181ms | 1.8x |
| s2 (id3 SYM ASC) | 143ms | 257ms | 1.8x |
| s3 (id4 I64 ASC) | 103ms | 140ms | 1.4x |
| s4 (v3 F64 DESC) | 163ms | 203ms | 1.2x |
| s5 (id1,id2 ASC) | 122ms | 227ms | 1.9x |
| s6 (id1,id2,id3 ASC) | 155ms | 434ms | 2.8x |

### Join

| Query | Teide | DuckDB | Speedup |
|-------|-------|--------|---------|
| j1-inner (id1,id2,id3) | 87ms | 93ms | 1.1x |
| j2-left (id1,id2,id3) | 126ms | 137ms | 1.1x |

## Running Benchmarks

```bash
# Groupby benchmarks (Teide)
python python/groupby.py

# Groupby benchmarks (DuckDB reference)
python python/duckdb/groupby.py

# Sort benchmarks
python python/sort.py

# Join benchmarks
python python/join.py
```

## License

MIT
