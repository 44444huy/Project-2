# Product Review Lakehouse

Batch lakehouse for the Electronics subset of Amazon Reviews 2023. It supports
a verified one-million-review profile and the complete source snapshot through
the same finite batch pipeline.

## Current flow

```text
Amazon Reviews JSONL source objects
    -> MinIO Client copies files to lakehouse/raw/
    -> Airflow runs one finite batch at a time
    -> Spark reads Raw
    -> Spark SQL creates and merges Iceberg Bronze tables
    -> Spark parses, validates and normalizes Silver tables
    -> Silver left-joins reviews to product metadata
    -> Spark rebuilds question-driven Gold aggregates
    -> Gold Iceberg tables registered in Polaris
       -> Iceberg files in lakehouse/warehouse/
       -> Spark JDBC -> PostgreSQL serving -> Streamlit dashboard
    -> Airflow records run configuration, counts, snapshots and status
```

MinIO is one object-storage service with one bucket:

```text
lakehouse/
├── raw/
│   └── amazon_reviews_2023_electronics/...
└── warehouse/
    └── Iceberg data and metadata files
```

## Code layout

```text
docker-compose.yaml
app/dashboard.py
airflow/
└── dags/product_review_lakehouse.py
postgres/init/00_create_polaris_database.sql
scripts/
├── download_full_dataset.py
├── render_spark_config.sh
└── setup_polaris_catalog.py
spark/
├── Dockerfile
└── conf/spark-defaults.conf.template
src/
├── bronze/load_product_reviews_bronze.py
├── silver/build_product_reviews_silver.py
├── gold/build_product_review_gold.py
├── serving/publish_gold_to_postgres.py
└── operations/finalize_pipeline_run.py
tests/
```

The active Bronze job keeps its processing steps explicit:

1. Load the JSON dataset config and selected processing scope.
2. Read each Raw JSONL object with Spark.
3. Add lineage columns and a deterministic `record_id`.
4. Run explicit `CREATE TABLE IF NOT EXISTS` SQL.
5. Run an insert-only Iceberg `MERGE`.
6. Print reconciliation counts.

The Silver job is also direct: schemas and business rules are visible in one
file, without a registry, plugin framework, or extra Python dependency. It
normalizes whitespace and builds compact issue arrays with `F.filter`.

Raw landing uses the official MinIO Client image in the one-shot `raw-setup`
service. The complete source is downloaded in resumable gzip parts with
SHA-256 manifests before those parts are landed. Smaller profiles read a
bounded number of rows from that same immutable Raw snapshot.

## Current milestone

The current verified profile processes exactly 1,000,000 reviews from the first
full-source parts together with all 1,610,012 product metadata rows. The full
Raw source remains in MinIO, so the same pipeline can later be run in `full`
mode without downloading the dataset again.

It writes:

- `polaris.bronze.reviews_raw`;
- `polaris.bronze.products_raw`.
- `polaris.silver.reviews`;
- `polaris.silver.products`;
- `polaris.silver.rejected_rows`;
- `polaris.silver.review_product`.
- `polaris.gold.monthly_review_metrics`;
- `polaris.gold.product_review_metrics`;
- `polaris.gold.product_monthly_review_metrics`;
- `polaris.gold.category_rating_metrics`;
- `polaris.gold.data_quality_summary`.
- `serving.monthly_review_metrics` in PostgreSQL;
- `serving.product_review_metrics` in PostgreSQL;
- `serving.product_monthly_review_metrics` in PostgreSQL;
- `serving.category_rating_metrics` in PostgreSQL;
- `serving.data_quality_summary` in PostgreSQL.

The one-million-review run completed all stages and published the five Gold
marts to PostgreSQL. See `docs/benchmark_1000000.md` for the current verified
results. `docs/benchmark_50000.md` is retained as an earlier scale-test record.

## Run

Create a local environment file before starting the stack. This file is ignored
by Git and must never be committed.

```powershell
Copy-Item .env.example .env
```

Replace the placeholder credentials in .env with local development values.
All PostgreSQL, Polaris, MinIO and Spark credentials are loaded from this file
at runtime; .env.example intentionally contains no usable secrets.

`DATA_MODE` selects the input scope:

- `million`: use exactly 1,000,000 reviews from the first two full-source parts
  and all product metadata;
- `full`: process all downloaded source parts.

To prepare the complete source for the `million` or `full` profile:

```powershell
py -3 scripts/download_full_dataset.py
```

Downloaded data remains outside Git. Local `data/full` parts may be removed
after successful Raw landing because the retained copy is in MinIO.

Build the images and start the operational stack:

```powershell
docker compose up -d --build airflow-webserver airflow-scheduler dashboard
docker compose ps -a
```

This starts PostgreSQL, bootstraps Polaris, starts MinIO, creates the
`lakehouse` bucket, lands the configured Raw scope, creates
`review_catalog`, initializes Airflow, and starts its scheduler, web UI and the
dashboard. One-shot setup services should finish with `Exited (0)`.

Open Airflow at `http://localhost:8080`, sign in with the local credentials in
`.env`, and trigger `product_review_lakehouse`. Its task order is
`record_run_start -> bronze -> silver -> gold -> publish -> finalize_run`.
Only one DAG run and one stage are active at a time. Airflow owns task retries,
while each data task calls its Spark application directly with `spark-submit`.

Each run is written to `serving.pipeline_runs` with its source and runtime
configuration, code version, status, duration, reconciled table counts and
current Iceberg snapshot IDs. Publish marks the displayed Serving version in
the same PostgreSQL transaction that swaps all five live tables; finalization
then marks the run successful.

Airflow is the only supported pipeline entrypoint. Do not run individual data
stages manually: trigger a new DAG run so task order, retries, failure status,
logs and final reconciliation remain under one orchestrator. Rerunning the DAG
is safe because Bronze uses an insert-only idempotent `MERGE`, Silver and Gold
rebuild deterministically, and Publish replaces the complete Serving version.

The publisher writes and reconciles all five staging tables before replacing
the live Serving set in one PostgreSQL transaction. A rerun replaces rows; it
does not append duplicates. See `docs/serving_contract.md` for the exact
Serving and Iceberg transaction boundaries.

Start the dashboard independently after publication if it is not already up:

```powershell
docker compose up -d --build dashboard
```

The UI queries only PostgreSQL and labels the active million or full scope from
configuration. See `docs/dashboard.md` for its table mapping and verification
boundary.

Useful endpoints:

- MinIO API: `http://localhost:9000`
- MinIO console: `http://localhost:9001`
- Polaris API: `http://localhost:8181/api/catalog`
- Polaris health: `http://localhost:8182/q/health`
- Airflow: `http://localhost:8080`
- Active Spark application UI: `http://localhost:4041`
- Dashboard: `http://localhost:8501`

Stop services while retaining named volumes:

```powershell
docker compose down
```

Do not add `-v` unless a clean reset is explicitly required.

## Tests

```powershell
docker compose --profile tools run --rm --build tests
```

## Interpretation limits

- Review count is not sales volume.
- Ratings of 1–2 are low ratings, not text-sentiment predictions.
- Product `price` is a metadata-crawl snapshot, not the price at review time.
- A one-million-row source slice is not representative of the full dataset.
