# Technical Questions

## 1. Why is record_hash useful for rerun-safe loading, and which columns should not be included in it?

`record_hash` is a SHA-256 fingerprint of the business content of one curated row. When the load meets an `order_id` that already exists in `curated.sales_order_lines`, it compares the incoming hash with the stored one. Equal hashes mean the content is unchanged, so the row is left alone; different hashes mean the content changed, so the row is updated. A rerun on unchanged input therefore writes nothing, and it can never create a second row for the same `order_id`.

The hash must exclude anything that changes without the business content changing. `pipeline_run_id` and `processed_at_utc` differ on every run, so including them would give every row a new hash and force every row to be rewritten. `source_updated_at` is excluded as well: a new source version that changes only `updated_at`, as `O0010000` does, leaves the content identical and should cause no update. Two curated runs with different run IDs produced identical hashes for every row (`docs/evidence/goal2_curated.txt`), which is the property that makes a second load a no-op.

## 2. Why should raw data usually be preserved even when staging and curated outputs are sufficient for analytics?

Raw is the only record of what the source actually delivered on a given run. Staging and curated outputs depend on the rules in force at the time, so when a rule changes or a bug is found, the data can be rebuilt from raw without asking the source again, and the source may have changed or overwritten its files by then. Raw also makes a run auditable: each snapshot has a manifest with the SHA-256 of every file, which shows exactly which input produced which output. In this pipeline the raw files also hold the superseded versions of records, such as the five repeated order keys, which the staging rules count and discard but which a later rule might need.

## 3. What is the difference between a data-quality rejection and a system exception?

A data-quality rejection concerns one record that breaks a rule while the pipeline itself works correctly, such as the order with quantity 0, the order with status `UNKNOWN` or the order that references a customer that does not exist. The record is quarantined with a reason code and the run continues, and every quarantined record is counted so that raw rows equal staged plus superseded plus quarantined rows. A system exception means the pipeline cannot do its job, for example a missing source file, an unreachable database or row counts that do not balance. It raises a `PipelineError` tagged with the stage, stops the run and exits with a non-zero code, and a retry after the cause is fixed must be safe.
