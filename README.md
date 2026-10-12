# Product Review Lakehouse

Batch lakehouse for the Electronics subset of Amazon Reviews 2023. It
processes a fixed pair of JSONL files as a finite local engineering batch.

## Current flow

```text
JSONL samples
    -> MinIO Client copies files to lakehouse/raw/
    -> Spark reads Raw
    -> Spark SQL creates and merges Iceberg Bronze tables
    -> Spark parses, validates and normalizes Silver tables
    -> Silver left-joins reviews to product metadata
    -> Spark rebuilds question-driven Gold aggregates
    -> Gold Iceberg tables registered in Polaris
       -> Iceberg files in lakehouse/warehouse/
       -> Spark JDBC -> PostgreSQL serving -> Streamlit dashboard
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
postgres/init/00_create_polaris_database.sql
scripts/
├── run_batch_pipeline.sh
└── setup_polaris_catalog.py
spark/
├── Dockerfile
└── conf/spark-defaults.conf
src/
├── bronze/load_product_reviews_bronze.py
├── silver/build_product_reviews_silver.py
├── gold/build_product_review_gold.py
└── serving/publish_gold_to_postgres.py
tests/
```

The active Bronze job keeps its processing steps explicit:

1. Load the small JSON dataset config.
2. Read each Raw JSONL object with Spark.
3. Add lineage columns and a deterministic `record_id`.
4. Run explicit `CREATE TABLE IF NOT EXISTS` SQL.
5. Run an insert-only Iceberg `MERGE`.
6. Print reconciliation counts.

The Silver job is also direct: schemas and business rules are visible in one
file, without a registry, plugin framework, or extra Python dependency. It
normalizes whitespace and builds compact issue arrays with `F.filter`.

Raw landing uses the official MinIO Client image in the one-shot `raw-setup`
service. The active runtime therefore does not require a custom S3 client,
checksum manifest, landing receipt or JSON run registry.

## Current milestone

The first milestone processes:

- 5,000 review rows;
- 5,000 product metadata rows.

It writes:

- `polaris.bronze.reviews_raw`;
- `polaris.bronze.products_raw`.
- `polaris.silver.reviews`;
- `polaris.silver.products`;
- `polaris.silver.rejected_rows`;
- `polaris.silver.review_product`.
- `polaris.gold.monthly_review_metrics`;
- `polaris.gold.product_review_metrics`;
- `polaris.gold.category_rating_metrics`;
- `polaris.gold.data_quality_summary`.
- `serving.monthly_review_metrics` in PostgreSQL;
- `serving.product_review_metrics` in PostgreSQL;
- `serving.category_rating_metrics` in PostgreSQL;
- `serving.data_quality_summary` in PostgreSQL.

The Gold schemas are provisional 5k analytical models. Their counts and group
coverage must be revalidated when the complete inputs replace the engineering
samples.

## Run

Create a local environment file before starting the stack. This file is ignored
by Git and must never be committed.

Run: Copy-Item .env.example .env

Replace the placeholder credentials in .env with local development values.
All PostgreSQL, Polaris, MinIO and Spark credentials are loaded from this file
at runtime; .env.example intentionally contains no usable secrets.

Build the Spark image and start the shared services:

```powershell
docker compose up -d --build spark
docker compose ps -a
```

This starts PostgreSQL, bootstraps Polaris, starts MinIO, creates the
`lakehouse` bucket, copies the 5k files to `raw/`, creates `review_catalog`, and
starts the utility Spark container. One-shot setup services should finish with
`Exited (0)`.

Run the complete finite batch pipeline:

```powershell
docker compose run --rm spark-pipeline
```

The runner executes `bronze -> silver -> gold -> publish` in dependency order,
starts a fresh Spark application for every stage, and stops immediately if a
stage fails.

Running the same command again is safe: Bronze inserts only unseen `record_id`
values, Silver and Gold rebuild deterministically, and Publish replaces the
serving tables.

To rerun only selected stages, list them in dependency order:

```powershell
docker compose run --rm spark-pipeline silver gold publish
```

Valid stage names are `bronze`, `silver`, `gold`, and `publish`. The
utility `spark` container remains available for troubleshooting:

```powershell
docker compose exec spark /opt/spark/bin/spark-submit /opt/project/src/silver/build_product_reviews_silver.py
```

The publisher replaces each serving table and reads it back to verify that its
row count matches the corresponding Iceberg Gold table. A rerun replaces rows;
it does not append duplicates. See `docs/serving_contract.md` for the serving
boundary and its transaction limitation.

Start the dashboard after publication:

```powershell
docker compose up -d --build dashboard
```

The UI queries only PostgreSQL and keeps the sample limitations visible. See
`docs/dashboard.md` for its table mapping and verification boundary.

Useful endpoints:

- MinIO API: `http://localhost:9000`
- MinIO console: `http://localhost:9001`
- Polaris API: `http://localhost:8181/api/catalog`
- Polaris health: `http://localhost:8182/q/health`
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
- The first 5,000 lines are an engineering sample, not a representative sample.
# Project-2
