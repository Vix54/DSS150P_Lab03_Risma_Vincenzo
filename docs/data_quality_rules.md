# Data Quality Rules (Goal 2)

## 1. Principles

1. Every rule is tied to an observation in `docs/evidence/goal2_profiling.txt` or to a bound given by the lab and stored in `config/settings.yml` under `quality`. A rule with neither is not enforced.
2. A record that breaks a rule is a data-quality failure: it is quarantined with a reason code. A missing file, an unreachable database or an unexpected schema is a system failure: it raises an exception and stops the run.
3. Source files are never edited. No invalid record is dropped without a quarantine row, and superseded versions are counted, so for every source the raw row count equals staged rows plus superseded rows plus quarantined rows.
4. Every timestamp is parsed and stored in UTC.

## 2. Duplicate versions

| Source | Physical rows | Distinct keys | Superseded versions | Exact copies |
|---|---|---|---|---|
| customers (`customer_id`) | 3003 | 3000 | 3 | 0 |
| products (`product_id`) | 601 | 600 | 1 | 0 |
| orders (`order_id`) | 50005 | 50000 | 5 | 0 |

No repeated key is an exact copy. Each pair holds an older and a newer version of the same business record, and the versions differ in `updated_at` plus, depending on the record, `email`, `name` or `status`. Every pair has exactly one row at its latest `updated_at`, so no ties were observed.

## 3. Staging rules

### 3.1 customers

| ID | Rule | Action | Evidence |
|---|---|---|---|
| C-01 | Keep the version with the greatest parsed `updated_at` per `customer_id` | Fix | 3 keys with 2 versions each; versions differ in `email` and `updated_at` |
| C-02 | Trim and lowercase `email` | Fix | 3 emails differ from their lowercase form; none padded, so trimming follows the lab rule |
| C-03 | A blank `email` becomes null and `email_missing` is set to true; the row is kept | Keep and flag | 4 blank emails in the raw file; the count after deduplication is recorded when staging runs |
| C-04 | Trim and title-case `city` | Fix | 2 padded values; 11 distinct values as written, 10 after trimming and title-casing |
| C-05 | Parse `created_at` and `updated_at` as UTC | Fix | 3003 of 3003 are ISO 8601 with `+00:00`; 0 unparseable; 0 rows with `updated_at` before `created_at` |
| C-06 | Add `pipeline_run_id` and `staged_at_utc` | Audit | Lab requirement |

`first_name`, `last_name` and `customer_tier` are carried unchanged: no blanks, no padding, and `customer_tier` has 4 values with no variants.

### 3.2 products

| ID | Rule | Action | Evidence |
|---|---|---|---|
| P-01 | Keep the version with the greatest parsed `updated_at` per `product_id` | Fix | `P0300` has 2 versions differing in `name` and `updated_at` |
| P-02 | Flatten `category` into `category_name` and `category_department` | Fix | 601 of 601 records have `category` as an object with exactly the keys `department` and `name` |
| P-03 | Quarantine when `unit_price` is null, non-numeric or negative | Quarantine `product_unit_price_invalid` | `P0078` has `unit_price` -199.0; 0 non-numeric values; no zero prices, so zero is an untested case with no rule |
| P-04 | Parse `updated_at` as UTC | Fix | 601 of 601 are ISO 8601 with `+00:00` |
| P-05 | Add `pipeline_run_id` and `staged_at_utc` | Audit | Lab requirement |

`name`, `brand` and `active` are carried unchanged: no blanks or padding, and each has a single type across all records. No rule uses `active` because the lab defines none and the data gives no evidence for one.

### 3.3 orders

| ID | Rule | Action | Evidence |
|---|---|---|---|
| O-01 | Keep the version with the greatest parsed `updated_at` per `order_id` | Fix | 5 keys with 2 versions each; 4 differ in `status` and `updated_at`, and `O0010000` differs only in `updated_at` |
| O-02 | Parse `order_timestamp` and `updated_at` as UTC | Fix | 50005 of 50005 are ISO 8601 with `+00:00`; 0 unparseable; 0 rows with `updated_at` before `order_timestamp` |
| O-03 | Quarantine when `quantity` is not an integer from 1 to 20 (`quality.min_quantity`, `quality.max_quantity`) | Quarantine `order_quantity_invalid` | `O0000112` has quantity 0; observed maximum is 8 |
| O-04 | Quarantine when `status` is not exactly one of `quality.allowed_order_statuses`; no normalization | Quarantine `order_status_not_allowed` | `O0004445` has status `UNKNOWN`; trimming and upper-casing repairs 0 rows |
| O-05 | Add `pipeline_run_id` and `staged_at_utc` | Audit | Lab requirement |

`customer_id`, `product_id`, `unit_price` and `discount_pct` are carried unchanged. Observed `unit_price` minimum is 160.67, and `discount_pct` takes only the values 0.0, 0.05, 0.1 and 0.15.

### 3.4 Technical failures

The lab asks for invalid technical records to be quarantined. Three generic checks apply to every source; none of them fired on the current data (0 unparseable timestamps, 0 blank keys, 0 non-numeric amounts).

| ID | Rule | Action |
|---|---|---|
| T-01 | A blank business key | Quarantine `business_key_missing` |
| T-02 | A timestamp that is not ISO 8601 | Quarantine `timestamp_invalid` |
| T-03 | An order whose `unit_price` or `discount_pct` is not numeric | Quarantine `order_amount_field_invalid` |

Versions are resolved before value rules are applied, so an older version that would have failed a rule but was superseded is counted as superseded, not quarantined.

## 4. Curated rules

| ID | Rule | Action | Evidence |
|---|---|---|---|
| X-01 | The order's `customer_id` must exist among valid staged customers | Quarantine `order_customer_not_found` | `O0002223` references `C99999`; no match after trimming and upper-casing |
| X-02 | The order's `product_id` must exist in the products source | Quarantine `order_product_not_found` | `O0003334` references `P9999`; no match after trimming and upper-casing |
| X-03 | The order's `product_id` must not belong to a quarantined product | Quarantine `order_product_quarantined` | 99 orders reference `P0078`, which fails P-03 |
| X-04 | `gross_amount = quantity * unit_price`, `discount_amount = gross_amount * discount_pct`, `net_amount = gross_amount - discount_amount`, using the order's own `unit_price`, decimal arithmetic and half-up rounding to 2 places | Calculate | Of 50004 orders with a matching product, 49905 have the same price as their product's latest version; all 99 that differ belong to `P0078`, so the price source changes no curated row |
| X-05 | Add `source_updated_at`, `pipeline_run_id`, `processed_at_utc` and `record_hash` | Audit | Lab requirement |

`record_hash` covers business columns only and excludes `pipeline_run_id` and `processed_at_utc`. The exact column list is recorded when the curated transformation is implemented.

## 5. Quarantine records

A record that fails several rules gets one quarantine row listing every reason code. Each row carries the source name, the business key, the reason codes, the stage that rejected it, the original record as JSON, `pipeline_run_id` and `quarantined_at_utc`. Rows are stored under `data/quarantine/run_id=<run id>/`.

## 6. Candidate validation rules

These checks run on the curated output and are repeated after the PostgreSQL load.

| ID | Check |
|---|---|
| V-01 | `order_id` is non-null and unique |
| V-02 | `customer_id` and `product_id` are non-null |
| V-03 | `quantity` is an integer from 1 to 20 |
| V-04 | `unit_price` is greater than or equal to 0 |
| V-05 | `discount_pct` is between 0 and 1 |
| V-06 | `gross_amount`, `discount_amount` and `net_amount` are greater than or equal to 0 |
| V-07 | `net_amount` equals `gross_amount - discount_amount` |
| V-08 | `status` is one of `quality.allowed_order_statuses` |
| V-09 | `order_timestamp`, `source_updated_at` and `processed_at_utc` are timezone-aware with a zero UTC offset |
| V-10 | `pipeline_run_id`, `processed_at_utc` and `record_hash` are non-null, and `record_hash` is 64 hexadecimal characters |
| V-11 | In PostgreSQL, `COUNT(*)` equals `COUNT(DISTINCT order_id)` in `curated.sales_order_lines` |

## 7. Nested structure

`products.json` holds `category` as an object with `name` and `department`. Staging represents it as two flat columns, `category_name` and `category_department`. Curated keeps `category` as `category_name`, because the provided `curated.sales_order_lines` has no department column. Evidence: all 601 records have `category` as an object with exactly those two keys.

## 8. Keys

| Layer | Key constraint | Basis |
|---|---|---|
| Source files | None | `order_id`, `customer_id` and `product_id` each repeat in the raw data |
| Staging | Unique key enforced by deduplication rules C-01, P-01 and O-01 | 3000, 600 and 50000 distinct keys |
| Curated table | `PRIMARY KEY (order_id)` | `order_id` is unique after deduplication |
