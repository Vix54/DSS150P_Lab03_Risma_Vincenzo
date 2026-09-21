# DSS150P Laboratory Activity 3: Modular, Rerun-Safe Data Pipeline

Course: DSS150P, Fundamentals of Data Engineering
Author: Vincenzo Risma

## 1. Overview

This repository implements an e-commerce sales pipeline that converts three source files (`customers.csv`, `products.json`, `orders.csv`) into a curated sales-order-line dataset in PostgreSQL. The design separates raw snapshots, staging, curated output and quarantined records. It is reproducible, safe to rerun and validated after every run. It also benchmarks CSV, JSON Lines, Parquet and PostgreSQL on the curated data, writes a partitioned Parquet dataset and can load a single monthly partition. Apache Airflow orchestration is planned for the next goal.

## 2. Current Status

| Goal | Scope | Status |
|---|---|---|
| 1 | Reproducible environment, externalized configuration, Docker Compose | Complete |
| 2 | Raw, staging, curated and quarantine layers; audit; rerun-safe PostgreSQL load; validation | Complete |
| 3 | Storage format benchmark, partitioned Parquet, selected-partition load | Complete |
| 4 | Airflow DAG with schedule, parameters, retries and failure recovery | Not started |

## 3. Tested Environment

| Component | Version |
|---|---|
| Operating system | Ubuntu 26.04 LTS on WSL 2 |
| Python (virtual environment) | 3.11.16 |
| Docker Engine / Compose | 29.7.2 / v5.4.0 (Docker Desktop with WSL integration) |
| PostgreSQL image | postgres:16 |
| Pipeline image base | python:3.11-slim |
| Pinned packages | pandas 2.2.3, pyarrow 17.0.0, psycopg 3.2.3, python-dotenv 1.0.1, PyYAML 6.0.2 |
| Test dependency | pytest 9.1.1 (`requirements-dev.txt`) |

Python 3.11 is required. The pinned versions of pandas, pyarrow and PyYAML publish no prebuilt wheels for Python 3.14, so the system interpreter on newer distributions cannot install them.

## 4. Local Setup

Prerequisites: Git, Python 3.11, Docker with Compose v2 or later, and at least 6 GB of free disk space.

### 4.1 Clone the repository

```
git clone https://github.com/Vix54/DSS150P_Lab03_Risma_Vincenzo.git
cd DSS150P_Lab03_Risma_Vincenzo
```

### 4.2 Create the virtual environment

Option A, using uv (https://docs.astral.sh/uv/), which installs a managed Python 3.11 without changing the system Python:

```
uv venv --seed --python 3.11 .venv
source .venv/bin/activate
```

Option B, when Python 3.11 is already installed (not exercised on the tested machine, which has no system Python 3.11):

```
python3.11 -m venv .venv
source .venv/bin/activate
```

The `--seed` flag installs pip inside the environment. Then install the pinned dependencies, including pytest:

```
python -m pip install --upgrade pip
pip install -r requirements-dev.txt
```

The `.venv` directory is machine-specific and reproducible from the requirements files, so it is excluded from Git and from the Docker build context.

### 4.3 Configure environment variables

```
cp .env.example .env
```

Edit `.env` and replace the placeholder password. The file is excluded from Git and must never be committed.

| Variable | Value in `.env.example` | Purpose |
|---|---|---|
| `POSTGRES_HOST` | `localhost` | Host used by commands run on the host machine. Compose overrides it to `postgres` inside containers |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |
| `POSTGRES_DB` | `dss150p` | Warehouse database |
| `POSTGRES_USER` | `dss150p` | Database user |
| `POSTGRES_PASSWORD` | `change_me` | Required. There is no default in code or Compose files |

### 4.4 Verify the local environment

```
python -m src.cli validate-env
```

Expected output, with the exit code 0:

```
python_version=3.11.16
pandas==2.2.3
pyarrow==17.0.0
psycopg==3.2.3
python-dotenv==1.0.1
PyYAML==6.0.2
project_root=<path to this repository>
db_target=localhost:5432/dss150p user=dss150p
environment_ok=True
```

If a required variable is missing, the command prints a `PROBLEM:` line and exits with code 1.

## 5. Containers

### 5.1 Build the image and start PostgreSQL

```
docker compose build pipeline
docker compose up -d postgres
docker compose ps
```

Wait until `dss150p-postgres` reports `healthy`.

### 5.2 Run the environment check inside the pipeline container

```
docker compose run --rm pipeline python -m src.cli validate-env
```

The output matches section 4.4 except for `project_root=/app` and `db_target=postgres:5432/dss150p`. The same code and the same `.env` resolve to different hosts because Compose sets `POSTGRES_HOST=postgres` for the container. Only `validate-env` has been verified inside the container; run the pipeline stages from the host virtual environment (see section 6).

### 5.3 Inspect the database

```
docker exec dss150p-postgres psql -U dss150p -d dss150p -c "\dn"
docker exec dss150p-postgres psql -U dss150p -d dss150p -c "\dt curated.*"
docker exec dss150p-postgres psql -U dss150p -d dss150p -c "\dt audit.*"
```

Expected: the schemas `audit`, `curated`, `public` and `staging` (plus `benchmark` once the benchmark has run); the table `curated.sales_order_lines`; and the tables `audit.partition_loads` and `audit.pipeline_runs`. The commands assume the default user and database from `.env.example`.

### 5.4 Apply the table constraints to an existing database

`sql/init/02_curated_constraints.sql` adds the `CHECK` constraints to `curated.sales_order_lines` (quantity range, non-negative prices and amounts, discount range, net amount formula, allowed statuses, hash length), and `sql/init/03_audit_events.sql` creates the append-only table `audit.pipeline_run_events`, where update, delete and truncate are rejected by triggers. A new volume runs both automatically. A volume created earlier needs them applied once; both scripts are idempotent:

```
docker exec -i dss150p-postgres psql -U dss150p -d dss150p -v ON_ERROR_STOP=1 < sql/init/02_curated_constraints.sql
docker exec -i dss150p-postgres psql -U dss150p -d dss150p -v ON_ERROR_STOP=1 < sql/init/03_audit_events.sql
```

### 5.5 Container names

| Name | Source | Notes |
|---|---|---|
| `dss150p-postgres` | `postgres` service | Publishes port 5432. Data is stored in a named Docker volume |
| `dss150p-pipeline` | `pipeline` service | Configured name. One-off runs with `docker compose run --rm` create a temporary container with a generated name and remove it on exit |

The SQL files in `sql/init/` run only when the data volume is first created. `docker compose stop` and `docker compose down` keep the volume; `docker compose down -v` deletes the database.

## 6. Running the Pipeline

Start PostgreSQL first (section 5.1) and activate the virtual environment.

### 6.1 One command

```
python -m src.cli run-all
```

`run-all` runs extract, transform, load and validate under one run ID and prints it. Use `--run-id NAME` to choose the ID; the `PIPELINE_RUN_ID` environment variable also works, which is how an orchestrator passes one ID through every stage. Characters other than letters, digits, `_`, `.` and `-` are replaced by `_`, so IDs are safe in folder names.

### 6.2 Stage commands

| Command | What it does | Run ID when `--run-id` is omitted |
|---|---|---|
| `extract` | Copies the three source files byte for byte into `data/raw/run_id=<id>/` and writes a manifest with the SHA-256 of each file and an `_ingested_at_utc` timestamp | A new ID is generated |
| `transform` | Builds the staging layer, then the curated layer, and writes quarantine records | Newest complete raw run |
| `load` | Upserts the curated rows into `curated.sales_order_lines` on `order_id`, updating a row only when its `record_hash` differs, and records the run in `audit.pipeline_runs` | Newest complete curated run |
| `validate` | Read-only checks: source and raw hashes, row counts across layers, curated rules V-01 to V-10, and V-11 plus hash match, audit row and load event in PostgreSQL. `--year Y --month M` limits the database checks to one month and checks its `audit.partition_loads` row and partition-load event | Newest complete curated run |
| `run-all` | Runs the four stages above in order | A new ID is generated |
| `benchmark` | Compares storage formats and PostgreSQL on a curated run and writes the partitioned dataset (section 6.6). `--repeats` must be at least 5 | Newest complete curated run |
| `load-partition --year Y --month M` | Loads one monthly partition through the same hash-guarded upsert and records it in `audit.partition_loads` and `audit.pipeline_run_events` (section 6.6) | Newest complete curated run |

### 6.3 Output layout

```
data/
├── raw/run_id=<id>/          three source copies, manifest.json, _SUCCESS.json
├── staging/run_id=<id>/      customers.parquet, products.parquet, orders.parquet, _SUCCESS.json
├── curated/run_id=<id>/      sales_order_lines.parquet, _SUCCESS.json
├── quarantine/run_id=<id>/   staging.parquet, curated.parquet
├── benchmarks/               benchmark_results.csv and benchmark_details.json (tracked), files/ (generated)
└── partitioned/              order_year=<year>/order_month=<month>/part-0.parquet, _SUCCESS.json
```

The raw, staging, curated, quarantine and partitioned folders and `data/benchmarks/files/` are generated and excluded from Git. `data/source/` holds the source files and is never modified.

### 6.4 Rerun behaviour

- `_SUCCESS.json` is written last in each layer folder. A stage that finds it skips its work; for the raw layer it first re-verifies the copies against the manifest hashes.
- A folder without `_SUCCESS.json` is a half-written output from a failed attempt. It is discarded and rebuilt.
- A load compares each incoming `record_hash` with the stored one and writes only rows that are new or changed. `record_hash` covers the business columns and `source_updated_at` but not the run ID or processing time, so a new run over unchanged source data writes nothing, while a new source version updates its row.
- Before it writes anything, a load checks the curated output against rules V-01 to V-10, including a recomputed hash. A run built under an older hash definition is refused and must be rebuilt with a new run ID.
- Every load and partition load also appends a row to `audit.pipeline_run_events`, which is append-only. `audit.pipeline_runs` and `audit.partition_loads` keep the latest outcome per run and partition.
- A failed stage exits with code 1 and prints `ERROR cli [stage] message`. A database error rolls the load back completely.

### 6.5 Clean-room rebuild

The generated layers can be deleted and rebuilt at any time:

```
rm -r data/raw data/staging data/curated data/quarantine
python -m src.cli run-all
```

On the tested machine the rebuild reported `inserted=0 updated=0 unchanged=49897`, because the rebuilt rows hash identically to the rows already loaded (`docs/evidence/goal2_cleanroom.txt`).

### 6.6 Benchmark and partitions

```
python -m src.cli benchmark --repeats 5
python -m src.cli load-partition --year 2026 --month 1
```

`benchmark` needs the PostgreSQL container running. For each of CSV, JSON Lines, Parquet with snappy and Parquet with zstd it writes the curated rows and times the write, a full read and a filtered read (`status = 'DELIVERED'`) as the median of 5 runs after a discarded warm-up. It reads each file back and recomputes every `record_hash` to prove the formats hold the same rows. For PostgreSQL it uses a scratch table, `benchmark.sales_order_lines_bench`, without and with an index on `status`; that table is dropped and recreated on every run and stays afterwards (`DROP SCHEMA benchmark CASCADE` removes it). The results go to `data/benchmarks/benchmark_results.csv`, with all runs, query plans and checks in `data/benchmarks/benchmark_details.json`.

The same command deletes and rewrites `data/partitioned/` as Parquet partitioned by `order_year` and `order_month`, derived from `order_timestamp` in UTC, and times a one-partition read against a whole-dataset read. The partition compared is set in `config/settings.yml` under `storage_benchmark.selected_partition`.

`load-partition` uses the newest complete curated run unless `--run-id` is given. It rebuilds `data/partitioned/` from that run if the dataset came from another run, reads one partition, checks that every row belongs to that month and passes rules V-01 to V-10, upserts the rows, and refreshes the `audit.partition_loads` row for the key `YYYY-MM` and appends an event in the same transaction. It stops with an error if the partition does not exist. Running it again changes nothing. `python -m src.cli validate --year 2026 --month 1` then checks that month against the database.

## 7. Data Rules and Results

The rules are listed with their evidence in `docs/data_quality_rules.md`: latest version per business key, text normalization, UTC timestamps, quarantine reasons, exact decimal amounts with half-up rounding, and the columns covered by `record_hash`. Counts from the verified run:

| Layer | customers | products | orders |
|---|---|---|---|
| Raw rows | 3003 | 601 | 50005 |
| Staged rows | 3000 | 599 | 49998 |
| Superseded versions | 3 | 1 | 5 |
| Quarantined at staging | 0 | 1 | 2 |

The curated layer holds 49897 rows, and the curated stage quarantined 101 more orders (99 whose product was rejected, 1 with an unknown customer and 1 with an unknown product). The quarantine total is 104 records.

### 7.1 Storage benchmark summary

Medians of 5 runs on the tested machine, for the 49,897 curated rows (the full method, results and interpretation are in `docs/benchmark_report.md`). These are measurements from one machine, not general claims.

| Storage | Size (bytes) | Full read (s) | Filtered read (s) |
|---|---|---|---|
| CSV | 14,181,711 | 0.251 | 0.243 |
| JSON Lines | 30,467,326 | 0.656 | 0.713 |
| Parquet, snappy | 5,396,477 | 0.113 | 0.036 |
| Parquet, zstd | 3,421,025 | 0.108 | 0.033 |
| PostgreSQL | 15,261,696 | 1.158 | 0.186 |
| PostgreSQL with an index on `status` | 15,605,760 | 1.183 | 0.190 |

Reading one of the 21 monthly partitions (2,506 rows) took 0.0153 s against 0.1218 s for the whole partitioned dataset.

## 8. Testing

```
python -m pytest -q
```

The 50 unit tests need no database. They cover run IDs, raw extraction, staging rules, the curated join and amounts, `record_hash`, the validation rules and integrity checks, error wrapping, the benchmark timing helper, format round trips and partition handling. `load`, `validate`, `benchmark` and `load-partition` need the PostgreSQL container running.

## 9. Configuration Model

- `config/settings.yml` holds non-secret defaults: the directory for each data layer, the source file list, allowed order statuses, quantity bounds, benchmark settings and the selected partition.
- `.env` holds environment-specific and secret values. Only `.env.example`, with a placeholder, is committed.
- `src/config.py` is the single place that reads both sources. It raises an error when a required database variable is missing instead of falling back to a default.
- `docker-compose.yml` fails at startup when `POSTGRES_PASSWORD` is unset.

## 10. Repository Layout

```
.
├── config/settings.yml
├── dags/dss150p_pipeline.py
├── data/
│   ├── source/                  source files (tracked, never modified)
│   └── benchmarks/              benchmark results (tracked); files/ is generated
├── docs/
│   ├── evidence/                command output recorded for each goal
│   ├── benchmark_report.md
│   ├── data_quality_rules.md
│   ├── technical_answers.md
│   └── run_evidence.md
├── scripts/profile_sources.py   read-only source profiling
├── sql/init/                    database bootstrap and constraints
├── src/
│   ├── common/                  run IDs, hashing, errors, logging, environment check
│   ├── extract/  transform/  load/  validate/
│   ├── benchmark/               timing, formats, partitions, PostgreSQL benchmark
│   ├── cli.py
│   └── config.py
├── templates/
├── tests/
├── Dockerfile  Dockerfile.airflow
├── docker-compose.yml  docker-compose.airflow.yml
├── requirements.txt  requirements-dev.txt  requirements-airflow.txt
└── .env.example  .gitignore  .dockerignore
```

## 11. Evidence

| File | Content |
|---|---|
| `docs/run_evidence.md` | Run-evidence summary, filled in per goal |
| `docs/evidence/goal1_environment.txt` | Python and package versions, both `validate-env` outcomes, `pip freeze` |
| `docs/evidence/goal1_docker_build.txt` | Clean, uncached build of the pipeline image |
| `docs/evidence/goal1_docker.txt` | Healthy PostgreSQL container, `validate-env` inside the container, schemas and tables |
| `docs/evidence/goal1_python_choice.txt` | Evidence that the pinned packages have no wheels for Python 3.14 |
| `docs/evidence/goal1_git_log.txt` | Git history at the Goal 1 checkpoint |
| `docs/evidence/source_sha256.txt` | SHA-256 baseline of the source files |
| `docs/evidence/goal2_profiling.txt` | Output of `python -m scripts.profile_sources` |
| `docs/evidence/goal2_extract.txt` | Raw extraction, rerun, manifest and hash comparison |
| `docs/evidence/goal2_staging.txt` | Staging counts, quarantine records and layer files |
| `docs/evidence/goal2_curated.txt` | Curated counts, hash stability across runs, independent recheck of every amount |
| `docs/evidence/goal2_load.txt` | Constraints, first load, reruns, SQL duplicate check, audit rows, failure cases |
| `docs/evidence/goal2_load_repair.txt` | Update path: one edited row repaired by reloading |
| `docs/evidence/goal2_audit_sample.txt` | Sample of the curated audit columns |
| `docs/evidence/goal2_validate_runall.txt` | `validate`, `run-all`, the lab's `run-all` then `load` twice, and a database failure |
| `docs/evidence/goal2_cleanroom.txt` | Deletion of the generated layers and reproducible rebuild |
| `docs/evidence/goal3_machine_context.txt` | Hardware, operating system and software context for the benchmark |
| `docs/evidence/goal3_benchmark.txt` | Benchmark run, verification results, plans, partition tree and SQL cross-checks |
| `docs/evidence/goal3_parquet_columns.txt` | Compressed size of every column in the two Parquet files |
| `docs/evidence/goal3_load_partition.txt` | Partition load, rerun, audit row and a missing partition |
| `docs/evidence/goal3_load_partition_restore.txt` | Month deleted from the table, restored by the partition load, then validated |
| `data/benchmarks/benchmark_results.csv` | Benchmark medians per storage system |
| `data/benchmarks/benchmark_details.json` | All runs, plans, sizes and partition figures |
| `docs/benchmark_report.md` | Benchmark methodology, results and interpretation |

To confirm the source files are unchanged:

```
sha256sum -c docs/evidence/source_sha256.txt
```

## 12. Command-Line Interface

| Command | Status |
|---|---|
| `validate-env`, `extract`, `transform`, `load`, `validate`, `run-all`, `benchmark`, `load-partition` | Implemented |

## 13. Known Limitations

- The load decides whether a row changed by comparing `record_hash` values. A row edited directly in PostgreSQL without updating its hash looks unchanged to the load; `validate` detects it by recomputing the hash from the stored values.
- Only `validate-env` has been verified inside the pipeline container. Files written by a container into the bind-mounted `data/` folder would be owned by root.
- Benchmark timings are warm-cache measurements from one machine and one dataset of about 14 MB, so small differences fall within run-to-run variation.

## 14. Troubleshooting

| Symptom | Likely cause | Resolution |
|---|---|---|
| `ModuleNotFoundError` or `PROBLEM: package not installed` | The virtual environment is not active or dependencies are not installed | Activate `.venv` and run `pip install -r requirements-dev.txt` |
| pip tries to compile pandas or pyarrow and fails | Python 3.14 is in use | Recreate `.venv` with Python 3.11 |
| `PROBLEM: Missing required environment variables` | `.env` is missing or incomplete | Run `cp .env.example .env` and fill in the values |
| Compose reports that `POSTGRES_PASSWORD` is missing | `.env` is missing or has no password | Set `POSTGRES_PASSWORD` in `.env` |
| `docker` is not found inside WSL | Docker Desktop WSL integration is disabled for the distribution | Enable it in Docker Desktop under Settings, Resources, WSL integration |
| Container name `dss150p-postgres` is already in use | A container from another project uses the name | Rename it with `docker rename`; remove it only if it is no longer needed |
| Port 5432 is already allocated | Another PostgreSQL instance publishes the same port | Stop that instance or change the published port in `docker-compose.yml` |
| Login fails after changing the password in `.env` | The database was initialized with the earlier password | A new password takes effect only when the volume is recreated, which deletes the data |
| `ERROR cli [load] cannot connect to localhost:5432/…` | PostgreSQL is not running, or the host or port in `.env` is wrong | Run `docker compose up -d postgres` and wait for `healthy` |
| `ERROR cli [run-id] no complete run found in raw_dir` | `transform` was run before any `extract` | Run `extract` first, or use `run-all` |
| `ERROR cli [transform] several versions share the greatest updated_at` | Two versions of one record have the same `updated_at`, and the pipeline refuses to guess | Resolve the tie in the source data or amend the rule in `docs/data_quality_rules.md` |
| `ERROR cli [validate] … no longer match their own record_hash` | A stored row was edited outside the pipeline | Inspect the row; to let the load repair it, set its `record_hash` to a different value and run `load` again |
| `ERROR cli [benchmark] --repeats must be at least 5` | `--repeats` was set below the minimum needed for a meaningful median | Use `--repeats 5` or more |
| `ERROR cli [load] … fails the pre-load checks and was not loaded` | The curated output is stale, for example built under an older hash definition, or was altered | Rebuild it with `python -m src.cli run-all --run-id NEW_ID` |
| `ERROR cli [validate] … does not exist; apply sql/init/03_audit_events.sql` | The audit events table is missing from an older volume | Apply the script as shown in section 5.4 |
| `ERROR cli [load-partition] partition … does not exist or has no rows` | No orders fall in that year and month | Choose a month listed under `data/partitioned/` |
