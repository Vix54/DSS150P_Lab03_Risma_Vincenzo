# Run Evidence

## Week 4
- Python version: 3.11.16 in `.venv` (uv-managed CPython), the same minor version as the `python:3.11-slim` image and the Airflow image. The host's system Python is 3.14.4, and the pinned `pandas==2.2.3`, `pyarrow==17.0.0` and `PyYAML==6.0.2` have no prebuilt wheels for 3.14; only newer releases do (`docs/evidence/goal1_python_choice.txt`). Package versions and both `validate-env` runs are in `docs/evidence/goal1_environment.txt`.
- Why `.venv` is not committed: the virtual environment is a generated, machine-specific directory. It holds absolute paths and interpreter links for this laptop and compiled wheels for this OS and CPU, so it does not work when copied elsewhere. It is also large and fully reproducible from `requirements.txt`, so committing it would add thousands of files to the history without adding any information. `.venv/` is listed in `.gitignore` (confirmed with `git check-ignore`) and in `.dockerignore`, so it is not baked into the image either.
- Git status/log evidence: `docs/evidence/goal1_git_log.txt`
- Docker image/container evidence: `docs/evidence/goal1_docker_build.txt` (clean no-cache build of the pipeline image) and `docs/evidence/goal1_docker.txt` (healthy PostgreSQL container, `validate-env` inside the container, expected schemas and tables).
- External configuration evidence: non-secret defaults live in `config/settings.yml` (paths for each data layer, allowed order statuses, quantity bounds, benchmark settings, the source file list). Environment-specific and secret values (PostgreSQL host, port, database, user and password) live in `.env`, which is git-ignored; only `.env.example` with a placeholder is committed. `src/config.py` is the single place that reads both and turns them into usable settings, and it raises an error when a required variable is missing instead of falling back to a default password. `docker-compose.yml` likewise fails fast if `POSTGRES_PASSWORD` is unset. The separation is visible in the evidence: the same code and the same `.env` report `db_target=localhost:5432` on the host and `db_target=postgres:5432` inside the container, because Compose sets `POSTGRES_HOST=postgres` for the container and `python-dotenv` does not override variables that are already set.
- Source integrity baseline: `docs/evidence/source_sha256.txt` (SHA-256 of the three source files before any pipeline run).

## Week 5
- Raw row counts: customers 3003, products 601, orders 50005 physical rows, recounted from the raw files by `validate` (`docs/evidence/goal2_staging.txt`, `docs/evidence/goal2_validate_runall.txt`).
- Staging row counts: customers 3000, products 599, orders 49998. Superseded versions were 3, 1 and 5, and the staging quarantine held 0, 1 and 2 records, so raw rows equal staged plus superseded plus quarantined for every source (`docs/evidence/goal2_staging.txt`).
- Curated row counts: 49897 rows in `sales_order_lines` from 49998 staged orders (`docs/evidence/goal2_curated.txt`).
- Quarantine row counts: 104 records in total. Staging rejected 3 (`P0078` for a negative price, `O0000112` for quantity 0, `O0004445` for status `UNKNOWN`). The curated stage rejected 101 (99 `order_product_quarantined`, 1 `order_customer_not_found`, 1 `order_product_not_found`). Reasons are defined in `docs/data_quality_rules.md`.
- First load affected rows: 49897 inserted, 0 updated (`docs/evidence/goal2_load.txt`).
- Second rerun affected rows / evidence of idempotency: 0 inserted, 0 updated, 49897 unchanged. A load of a second run ID with identical content also changed nothing, and `COUNT(*)` equals `COUNT(DISTINCT order_id)` in `curated.sales_order_lines` (`docs/evidence/goal2_load.txt`). Editing one stored row and reloading updated exactly that row (`docs/evidence/goal2_load_repair.txt`).
- Sample audit columns: `docs/evidence/goal2_audit_sample.txt`.
- Error-handling evidence: an unreachable database and a missing curated run each stop the stage with a message naming the stage and exit code 1 (`docs/evidence/goal2_load.txt`, `docs/evidence/goal2_validate_runall.txt`); the unit tests in `tests/` cover missing sources, tampered snapshots, tied versions and unexpected exceptions.

## Week 6
- Benchmark table attached: yes (`data/benchmarks/benchmark_results.csv`, `data/benchmarks/benchmark_details.json`)
- Partition selected: `order_year=2026, order_month=1`
- Partition row count: 2506
- PostgreSQL verification query: `SELECT COUNT(*) FROM curated.sales_order_lines WHERE order_timestamp >= '2026-01-01' AND order_timestamp < '2026-02-01';` → 2506, matching the partition read (`docs/evidence/goal3_benchmark.txt`)

## Week 7
- DAG ID: `dss150p_sales_pipeline` (`dags/dss150p_pipeline.py`)
- Schedule: `0 2 * * *` (daily, 02:00 UTC), `catchup=False`, `max_active_runs=1`
- Parameters used: `run_mode` (`full`/`partition`), `year`, `month`; full run `manual_full_1`, partition run `manual_partition_1` with `{"run_mode":"partition","year":2026,"month":1}` (`docs/evidence/goal4_full_run.txt`, `docs/evidence/goal4_partition_run.txt`)
- Successful run ID: `manual_full_1` (all 4 tasks succeeded; `audit.pipeline_runs` shows `inserted=0 updated=0 unchanged=49897`) and `manual_partition_1` (`audit.partition_loads` shows `2026-01`, 2506 rows)
- Deliberate failure run ID: `manual_failure_1`, triggered with `data/source/orders.csv` renamed away
- Retry/failure-handling evidence: `extract` retried twice then failed on try 3; `transform`, `load`, `validate` correctly marked `upstream_failed`; the failure callback wrote 3 entries (2 `retry`, 1 `failure`) to `logs/dss150p_failures.jsonl` (`docs/evidence/goal4_failure.txt`)
- Final recovery run ID: `manual_failure_1` (source restored, SHA-256 verified against `docs/evidence/source_sha256.txt`, failed tasks cleared, all 4 tasks completed `success`, `total = distinct_orders = 49897`) (`docs/evidence/goal4_recovery.txt`)
