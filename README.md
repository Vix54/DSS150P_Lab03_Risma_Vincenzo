# DSS150P Laboratory Activity 3: Modular, Rerun-Safe Data Pipeline

Course: DSS150P, Fundamentals of Data Engineering
Author: Vincenzo Risma

## 1. Overview

This repository implements an e-commerce sales pipeline that converts three source files (`customers.csv`, `products.json`, `orders.csv`) into a curated sales-order-line dataset in PostgreSQL. The design separates raw snapshots, staging, curated output and quarantined records, and it is intended to be reproducible, safe to rerun, benchmarked across storage formats, partitioned by year and month, and orchestrated with Apache Airflow.

## 2. Current Status

| Goal | Scope | Status |
|---|---|---|
| 1 | Reproducible environment, externalized configuration, Docker Compose | Complete |
| 2 | Raw, staging, curated and quarantine layers; audit; rerun-safe PostgreSQL load | In progress. Source profiling is complete; transformations are not yet implemented |
| 3 | Storage format benchmark, partitioned Parquet, selected-partition load | Not started |
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

Python 3.11 is required. The pinned versions of pandas, pyarrow and PyYAML publish no prebuilt wheels for Python 3.14, so the system interpreter on newer distributions cannot install them.

## 4. Local Setup

Prerequisites: Git, Python 3.11, Docker with Compose v2 or later, and at least 6 GB of free disk space.

### 4.1 Clone the repository

```
git clone https://github.com/Vix54/DSS150P_Lab03_Risma_Vincenzo.git
cd DSS150P_Lab03_Risma_Vincenzo
```

### 4.2 Create the virtual environment

Option A, using uv (installs a managed Python 3.11 without changing the system Python):

```
uv venv --seed --python 3.11 .venv
source .venv/bin/activate
```

Option B, when Python 3.11 is already installed:

```
python3.11 -m venv .venv
source .venv/bin/activate
```

The `--seed` flag installs pip inside the environment. Then install the pinned dependencies:

```
python -m pip install --upgrade pip
pip install -r requirements.txt
```

The `.venv` directory is machine-specific and reproducible from `requirements.txt`, so it is excluded from Git and from the Docker build context.

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

### 5.2 Run the same command inside the pipeline container

```
docker compose run --rm pipeline python -m src.cli validate-env
```

The output matches section 4.4 except for `project_root=/app` and `db_target=postgres:5432/dss150p`. The same code and the same `.env` resolve to different hosts because Compose sets `POSTGRES_HOST=postgres` for the container.

### 5.3 Inspect the database

```
docker exec dss150p-postgres psql -U dss150p -d dss150p -c "\dn"
docker exec dss150p-postgres psql -U dss150p -d dss150p -c "\dt curated.*"
docker exec dss150p-postgres psql -U dss150p -d dss150p -c "\dt audit.*"
```

Expected: the schemas `audit`, `curated`, `public` and `staging`; the table `curated.sales_order_lines`; and the tables `audit.partition_loads` and `audit.pipeline_runs`. The commands assume the default user and database from `.env.example`.

### 5.4 Container names

| Name | Source | Notes |
|---|---|---|
| `dss150p-postgres` | `postgres` service | Publishes port 5432. Data is stored in a named Docker volume |
| `dss150p-pipeline` | `pipeline` service | Configured name. One-off runs with `docker compose run --rm` create a temporary container with a generated name and remove it on exit |

The SQL files in `sql/init/` run only when the data volume is first created. `docker compose stop` and `docker compose down` keep the volume; `docker compose down -v` deletes the database.

## 6. Configuration Model

- `config/settings.yml` holds non-secret defaults: the directory for each data layer, the source file list, allowed order statuses, quantity bounds and benchmark settings.
- `.env` holds environment-specific and secret values. Only `.env.example`, with a placeholder, is committed.
- `src/config.py` is the single place that reads both sources. It raises an error when a required database variable is missing instead of falling back to a default.
- `docker-compose.yml` fails at startup when `POSTGRES_PASSWORD` is unset.

## 7. Repository Layout

```
.
├── config/settings.yml
├── dags/dss150p_pipeline.py
├── data/source/                 source files (tracked, never modified)
├── docs/
│   ├── evidence/                command output recorded for each goal
│   └── run_evidence.md
├── scripts/profile_sources.py   read-only source profiling
├── sql/init/                    database bootstrap scripts
├── src/
│   ├── extract/  transform/  load/  validate/  benchmark/  common/
│   ├── cli.py
│   └── config.py
├── templates/
├── tests/
├── Dockerfile  Dockerfile.airflow
├── docker-compose.yml  docker-compose.airflow.yml
├── requirements.txt  requirements-airflow.txt
└── .env.example  .gitignore  .dockerignore
```

Generated data layers (`data/raw`, `data/staging`, `data/curated`, `data/quarantine`, `data/partitioned`) are excluded from Git.

## 8. Evidence

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

To confirm the source files are unchanged:

```
sha256sum -c docs/evidence/source_sha256.txt
```

## 9. Command-Line Interface

| Command | Status |
|---|---|
| `validate-env` | Implemented |
| `extract`, `transform`, `load`, `validate`, `benchmark`, `load-partition`, `run-all` | Planned. They currently raise `NotImplementedError` |

## 10. Troubleshooting

| Symptom | Likely cause | Resolution |
|---|---|---|
| `ModuleNotFoundError` or `PROBLEM: package not installed` | The virtual environment is not active or dependencies are not installed | Activate `.venv` and run `pip install -r requirements.txt` |
| pip tries to compile pandas or pyarrow and fails | Python 3.14 is in use | Recreate `.venv` with Python 3.11 |
| `PROBLEM: Missing required environment variables` | `.env` is missing or incomplete | Run `cp .env.example .env` and fill in the values |
| Compose reports that `POSTGRES_PASSWORD` is missing | `.env` is missing or has no password | Set `POSTGRES_PASSWORD` in `.env` |
| `docker` is not found inside WSL | Docker Desktop WSL integration is disabled for the distribution | Enable it in Docker Desktop under Settings, Resources, WSL integration |
| Container name `dss150p-postgres` is already in use | A container from another project uses the name | Rename it with `docker rename`; remove it only if it is no longer needed |
| Port 5432 is already allocated | Another PostgreSQL instance publishes the same port | Stop that instance or change the published port in `docker-compose.yml` |
| Login fails after changing the password in `.env` | The database was initialized with the earlier password | A new password takes effect only when the volume is recreated, which deletes the data |
