# Storage Benchmark Report (Goal 3)

## 1. Introduction

This report compares CSV, JSON Lines, Parquet and PostgreSQL as stores for the curated `sales_order_lines` dataset (49,897 rows, 20 columns, curated run `cleanroom_1`). Every figure is a measurement from one machine on this one dataset, not a claim about the formats in general.

## 2. Methodology

- **Machine.** Intel Core Ultra 7 155H with 4 logical CPUs and 7.8 GiB RAM visible to WSL 2 (Ubuntu 26.04); Python 3.11.16, pandas 2.2.3, pyarrow 17.0.0, psycopg 3.2.3; PostgreSQL 16.15 in Docker Desktop with `shared_buffers` of 128 MB (`docs/evidence/goal3_machine_context.txt`).
- **Same logical rows.** Each file and the PostgreSQL table was read back, converted to the pipeline's types, and every `record_hash` was recomputed. All 49,897 hashes matched (0 mismatches) with the same `order_id` set, so all systems hold the same data. CSV and JSON Lines carry money as exact decimal strings.
- **Timing.** One discarded warm-up, then 5 timed runs; tables show medians, and all runs are in `data/benchmarks/benchmark_details.json`. Reads are warm because the data was written moments earlier; the OS cache could not be flushed without root.
- **Endpoint.** Every read returns a pandas DataFrame through the default reader (`read_csv` with type inference, `read_json(lines=True)`, `read_parquet`, a `SELECT` fetched into a DataFrame). CSV and JSON Lines return untyped values; restoring types is not timed.
- **Filter and size.** `status = 'DELIVERED'` matches 8,355 rows (16.7%). CSV and JSON Lines read everything and then filter; Parquet filters inside the reader; PostgreSQL runs a `WHERE` query without and then with an index on `status`. Sizes are file bytes, or `pg_total_relation_size` including the primary key index for PostgreSQL.

## 3. Results

| Storage | Size (bytes) | Size vs CSV | Write (s) | Full read (s) | Filtered read (s) |
|---|---|---|---|---|---|
| CSV | 14,181,711 | 1.00 | 0.975 | 0.251 | 0.243 |
| JSON Lines | 30,467,326 | 2.15 | 0.998 | 0.656 | 0.713 |
| Parquet, snappy | 5,396,477 | 0.38 | 0.179 | 0.113 | 0.036 |
| Parquet, zstd | 3,421,025 | 0.24 | 0.183 | 0.108 | 0.033 |
| PostgreSQL | 15,261,696 | 1.08 | 1.037 | 1.158 | 0.186 |
| PostgreSQL with `status` index | 15,605,760 | 1.10 | 0.044 (index build) | 1.183 | 0.190 |

`EXPLAIN ANALYZE` put PostgreSQL's own execution of the filtered query at 7.7 ms as a sequential scan and 3.4 ms as a bitmap heap scan with the index.

Partitioning by `order_year` and `order_month` (from `order_timestamp` in UTC) gave 21 monthly partitions of 951 to 2,540 rows. The selected partition, January 2026, returned 2,506 rows, all inside that month:

| Read | Files | Bytes | Median (s) |
|---|---|---|---|
| Whole partitioned dataset | 21 | 6,740,181 | 0.1218 |
| Selected partition | 1 | 335,453 (5.0%) | 0.0153 |

The speedup is $S = t_{\text{whole}} / t_{\text{partition}} = 0.1218 / 0.0153 \approx 8.0$. Together the 21 files are $6{,}740{,}181 / 5{,}396{,}477 \approx 1.25$ times the size of the single snappy file.

## 4. Discussion

**Smallest format.** Parquet with zstd, at 24% of the CSV size (snappy 38%). Parquet stores each column contiguously as typed binary values with dictionary encoding enabled, so low-cardinality columns such as `status`, `brand`, `category` and `customer_tier` take 11 to 19 KB each, under half a byte per row. The codec matters mostly for `record_hash` (64 hexadecimal characters per row): it is 3,344,082 bytes (62%) of the snappy file and 1,723,439 bytes (50%) of the zstd file (`docs/evidence/goal3_parquet_columns.txt`). Snappy barely shrinks it, while zstd's entropy coding roughly halves it, since a hexadecimal character carries only 4 bits. JSON Lines is largest because every row repeats all 20 key names.

**Fastest full read.** Parquet, at about 0.11 s. The snappy and zstd medians differ by 0.005 s while their runs overlap, so I treat them as equal. Relative to snappy, CSV was about 2.2 times slower, JSON Lines 5.8 times and PostgreSQL 10.2 times. That does not make Parquet best for every workload: its files are immutable, whereas PostgreSQL provides transactions, constraints, concurrent writers and key lookups. PostgreSQL's engine also finished the filtered scan in milliseconds; most of its measured time is moving rows over the connection and building Python objects.

**Filtered retrieval.** The Parquet filtered read took 31% of its full read (snappy), because matching happens inside the reader and only the 8,355 matching rows become pandas objects. CSV and JSON Lines gained nothing (97% and 109%). PostgreSQL took 0.186 s, about 5.2 times the Parquet time. The index changed the plan and halved the engine time, but the measured time did not move (0.186 s against 0.190 s), because the engine is at most about 4% of the elapsed time and 16.7% selectivity limits an index. Returning only the needed columns, aggregating in SQL, a covering index or partitioning the table by month are options that could change the result more.

**JSON Lines versus one JSON array.** Each line is a complete record, so a producer can append without rewriting the file, a consumer can stream line by line, a truncated write damages only the last line, and files can be split or joined on line boundaries. An array must be parsed as one document. Here JSON Lines was still the largest and slowest to read, so its advantage is in handling, not size or speed.

**High-cardinality or low-locality partition keys.** A high-cardinality key creates many tiny files whose listing, opening and planning cost outweighs the read savings; partitioning by `order_id` would create 49,897 files. Even 21 monthly partitions cost 25% more bytes than one file. If queries filter on a column other than the key, nothing is pruned and every partition is read, and a skewed key leaves one partition holding most of the data.

## 5. Limitations

One machine, one dataset of about 14 MB as CSV, and 5 repeats, so differences of a few percent (such as snappy against zstd on reads) are within run-to-run variation. Caches were warm. PostgreSQL ran in a container reached over the local network, and its timings include converting rows to Python objects. The `benchmark` schema and its table remain in the database after the run so that plans and sizes can be inspected.
