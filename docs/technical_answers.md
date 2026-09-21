# Technical Questions

## 1. Why is record_hash useful for rerun-safe loading, and which columns should not be included in it?

`record_hash` is a SHA-256 fingerprint of the business content of one curated row. When the load meets an `order_id` that already exists in `curated.sales_order_lines`, it compares the incoming hash with the stored one. Equal hashes mean the content is unchanged, so the row is left alone; different hashes mean the content changed, so the row is updated. A rerun on unchanged input therefore writes nothing, and it can never create a second row for the same `order_id`.

The hash must exclude anything that changes without the business content changing. `pipeline_run_id` and `processed_at_utc` differ on every run, so including them would give every row a new hash and force every row to be rewritten. `source_updated_at` is excluded as well: a new source version that changes only `updated_at`, as `O0010000` does, leaves the content identical and should cause no update. Two curated runs with different run IDs produced identical hashes for every row (`docs/evidence/goal2_curated.txt`), which is the property that makes a second load a no-op.
