# Snowflake Data Orchestration Pipeline

An end-to-end data orchestration pipeline that ingests NYC yellow taxi trip data into Snowflake using Dagster for orchestration and dbt for transformations.

## Architecture

```
S3 (parquet) ──▶ Dagster (Python) ──▶ Snowflake RAW ──▶ dbt staging ──▶ dbt marts
                  taxi_trips asset      TAXI_TRIPS       stg_taxi_trips   trip_metrics
```

**Dagster** orchestrates the full pipeline — ingestion runs first, then dbt models build in dependency order.

## Project Structure

```
.
├── .github/workflows/
│   ├── dev-ci.yml              # CI: runs on push/PR to dev branch
│   └── prod-cd.yml             # CD: validates and deploys on push to main
├── analytics/                  # dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── sources/
│   │   │   └── raw_taxi.yml    # Source definition (TAXI_DATA.RAW.TAXI_TRIPS)
│   │   ├── staging/
│   │   │   ├── stg_taxi_trips.sql   # Cleans/renames raw columns, filters bad data
│   │   │   └── staging.yml
│   │   └── marts/
│   │       ├── trip_metrics.sql     # Hourly aggregations by pickup location
│   │       └── marts.yml
│   └── tests/
├── src/dagster_snowflake_tutorial/
│   ├── definitions.py          # Dagster entry point with resource config
│   └── defs/
│       ├── assets/
│       │   └── ingestion.py    # taxi_trips asset: S3 → Snowflake
│       ├── dbt_transforms/
│       │   └── defs.yaml       # DbtProjectComponent config
│       ├── resources.py        # Job and schedule definitions
│       └── schedules.py
├── tests/                      # Python test suite
│   ├── test_definitions.py     # Dagster definition validation
│   ├── test_ingestion.py       # Ingestion asset unit tests (mocked)
│   └── test_dbt_models.py      # dbt parse/compile validation
└── pyproject.toml
```

## Snowflake Objects

| Object | Location | Description |
|--------|----------|-------------|
| Source table | `TAXI_DATA.RAW.TAXI_TRIPS` | Raw NYC yellow taxi trip data (Jan 2023) |
| Staging view | `TAXI_DATA.DBT.STG_TAXI_TRIPS` | Cleaned columns, filtered bad data |
| Marts table | `TAXI_DATA.DBT.TRIP_METRICS` | Hourly trip metrics by pickup location |
| Warehouse | `TAXI_WH` | Compute warehouse for dbt runs |

## Data Pipeline

### Ingestion (`taxi_trips` asset)
- Loads January 2023 NYC yellow taxi trip data from CloudFront (parquet)
- Creates `TAXI_DATA.RAW.TAXI_TRIPS` with 19 columns
- Truncates and reloads on each run (full refresh)

### Staging (`stg_taxi_trips`)
- Renames columns to snake_case
- Calculates `trip_duration_minutes`
- Filters: `trip_distance > 0`, `total_amount > 0`, date range `2023-01-01` to `2023-02-01`
- Materialized as a **view**

### Marts (`trip_metrics`)
- Aggregates by hour and pickup location
- Metrics: `trip_count`, `avg_trip_distance`, `avg_trip_duration_minutes`, `avg_total_amount`, `total_revenue`, `avg_tip_amount`
- Materialized as a **table**

## Environments

| Target | Schema | Triggered by |
|--------|--------|-------------|
| `dev` | `TAXI_DATA.DBT` | `dbt run --target dev` or workspace Run File |
| `prod` | `TAXI_DATA.PROD` | PR merge to `main` via CD pipeline |

## Getting Started

### Prerequisites

- Snowflake account with `ACCOUNTADMIN` role
- Python 3.10+
- [uv](https://docs.astral.sh/uv/) package manager

### 1. Snowflake Setup

Run in a Snowflake worksheet:

```sql
CREATE WAREHOUSE IF NOT EXISTS TAXI_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

CREATE DATABASE IF NOT EXISTS TAXI_DATA;
CREATE SCHEMA IF NOT EXISTS TAXI_DATA.RAW;
CREATE SCHEMA IF NOT EXISTS TAXI_DATA.DBT;
CREATE SCHEMA IF NOT EXISTS TAXI_DATA.PROD;
CREATE SCHEMA IF NOT EXISTS TAXI_DATA.DEV;
```

### 2. Install Dependencies

```bash
uv sync --all-extras
```

### 3. Environment Variables

Create a `.env` file (do not commit):

```
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
```

Load in PowerShell:
```powershell
Get-Content .env | ForEach-Object {
    if ($_ -match '^\s*([^#][^=]+)=(.*)$') {
        [System.Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim(), 'Process')
    }
}
```

Load in Bash:
```bash
export $(grep -v '^#' .env | xargs)
```

### 4. Run Dagster Locally

```bash
dg dev
```

Open http://localhost:3000 to see the pipeline graph and trigger runs.

### 5. Run dbt Standalone (Snowsight Workspace)

```bash
dbt deps
dbt run --target dev
dbt test
```

## CI/CD

### Dev Pipeline (`dev-ci.yml`)
Triggers on push/PR to `dev` branch:
1. `dbt parse` — validates model SQL
2. `dg check defs` — validates Dagster definitions
3. `pytest tests/ -v` — runs Python test suite

### Prod Pipeline (`prod-cd.yml`)
Triggers on push to `main` (PR merge):
1. **Validate** — parse, check defs, run tests
2. **Deploy** — `dbt build --target prod`, then `snow dbt deploy` to update the Snowflake DBT PROJECT object

### GitHub Setup Required

1. Create a `prod` environment (Settings → Environments)
2. Add secrets: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`

## Testing

```bash
uv run pytest tests/ -v
```

| Test file | Coverage |
|-----------|----------|
| `test_definitions.py` | Dagster defs load, assets present, resources configured |
| `test_ingestion.py` | SQL sequence, metadata output, table schema (mocked) |
| `test_dbt_models.py` | dbt parse/compile, staging filters, marts aggregation |

## dbt Deployed Projects

| Project | Location | Target |
|---------|----------|--------|
| Dev | `TAXI_DATA.DEV.DAGSTER_V1` | `dev` |
| Prod | `TAXI_DATA.PROD.DAGSTER_V1` | `prod` |

Execute manually:
```sql
EXECUTE DBT PROJECT TAXI_DATA.DEV.DAGSTER_V1 ARGS = 'build';
EXECUTE DBT PROJECT TAXI_DATA.PROD.DAGSTER_V1 ARGS = 'build';
```

## Factors Impacting Data Pipeline Complexity

The following dimensions contribute to a more complex production data pipeline. They represent potential areas of growth beyond this project's current scope.

### Transformation Complexity

- Multiple source systems (APIs, databases, files, streams)
- Incremental models with merge logic rather than full refreshes
- Snapshots (SCD Type 2) for tracking historical changes
- Intermediate models between staging and marts
- Complex joins across many fact/dimension tables
- Window functions, recursive CTEs, or multi-step business logic
- dbt macros with conditional logic and dynamic SQL generation
- Custom materializations or hooks

### Ingestion Complexity

- Multiple assets with varied schedules and dependencies
- Partitioned ingestion (date-based, incremental loads)
- Schema evolution handling
- Data quality checks/sensors that gate downstream processing
- Retry logic, backfills, and failure handling
- Multiple file formats and source types in one pipeline

### Dagster Orchestration Complexity

- Asset partitions (daily/hourly)
- Sensors triggering on external events (new files, webhooks)
- Conditional branching in the DAG
- Cross-job dependencies
- Resource-level configuration per environment
- Observable source assets with freshness policies
- Multi-code-location deployments

### Data Modeling Complexity

- Star/snowflake schema with multiple facts and dimensions
- Mart-level tests with `dbt_expectations` or `dbt_utils`
- Exposures tied to dashboards
- Semantic layer definitions
- Multiple downstream marts serving different teams

## Resources

- [Dagster Documentation](https://docs.dagster.io/)
- [dbt Projects on Snowflake](https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake)
- [Dagster + dbt Integration](https://docs.dagster.io/integrations/libraries/dbt/)
- [Snowflake CI/CD for dbt](https://docs.snowflake.com/en/user-guide/tutorials/dbt-projects-on-snowflake-ci-cd-tutorial)
