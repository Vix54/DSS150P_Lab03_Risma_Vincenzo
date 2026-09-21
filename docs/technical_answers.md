# Technical Questions

## 1. Why is record_hash useful for rerun-safe loading, and which columns should not be included in it?

`record_hash` is a SHA-256 fingerprint of the business content of one curated row. When the load meets an `order_id` that already exists in `curated.sales_order_lines`, it compares the incoming hash with the stored one. Equal hashes mean the content is unchanged, so the row is left alone; different hashes mean the content changed, so the row is updated. A rerun on unchanged input therefore writes nothing, and it can never create a second row for the same `order_id`.

The hash must exclude anything that changes without the business content changing. `pipeline_run_id` and `processed_at_utc` differ on every run, so including them would give every row a new hash and force every row to be rewritten. `source_updated_at` is excluded as well: a new source version that changes only `updated_at`, as `O0010000` does, leaves the content identical and should cause no update. Two curated runs with different run IDs produced identical hashes for every row (`docs/evidence/goal2_curated.txt`), which is the property that makes a second load a no-op.

## 2. Why should raw data usually be preserved even when staging and curated outputs are sufficient for analytics?

Raw is the only record of what the source actually delivered on a given run. Staging and curated outputs depend on the rules in force at the time, so when a rule changes or a bug is found, the data can be rebuilt from raw without asking the source again, and the source may have changed or overwritten its files by then. Raw also makes a run auditable: each snapshot has a manifest with the SHA-256 of every file, which shows exactly which input produced which output. In this pipeline the raw files also hold the superseded versions of records, such as the five repeated order keys, which the staging rules count and discard but which a later rule might need.

## 3. What is the difference between a data-quality rejection and a system exception?

A data-quality rejection concerns one record that breaks a rule while the pipeline itself works correctly, such as the order with quantity 0, the order with status `UNKNOWN` or the order that references a customer that does not exist. The record is quarantined with a reason code and the run continues, and every quarantined record is counted so that raw rows equal staged plus superseded plus quarantined rows. A system exception means the pipeline cannot do its job, for example a missing source file, an unreachable database or row counts that do not balance. It raises a `PipelineError` tagged with the stage, stops the run and exits with a non-zero code, and a retry after the cause is fixed must be safe.

## 4. Why might Parquet outperform CSV for selected analytical workloads even if both contain the same rows?

Parquet is columnar, typed and compressed, whereas CSV is row-oriented text. A query that needs only some columns can read just those column chunks, while CSV must scan and parse every field of every row. Parquet stores numbers and timestamps in binary and uses dictionary encoding and compression, so there are fewer bytes to read and no text-to-number parsing, and a reader can apply a filter while decoding. In this project the Parquet file was 38% (snappy) or 24% (zstd) of the CSV size, a full read took 0.113 s against 0.251 s, and the filtered read fell to 31% of the full read. The CSV filtered read (0.243 s) took as long as its full read, because the filter runs only after every row has been parsed (`data/benchmarks/benchmark_results.csv`).

## 7. What trade-off is introduced by partitioning too aggressively?

Partitioning lets a query read only the folders it needs, but every partition adds files, folders and metadata. Too many small partitions slow down listing, opening and planning, compress worse, and make writes and maintenance more work. Here the selected monthly partition was read in 0.015 s against 0.122 s for the whole dataset, but the 21 partition files together take 6.74 MB compared with 5.40 MB for a single file, which is 25% more. Partitioning by a high-cardinality column such as `order_id` would create one file per order, and queries that do not filter on the partition key would still read every partition.
