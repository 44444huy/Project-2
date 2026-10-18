# Phase 1 (5k) status

> Historical checkpoint only. For the current implementation, see the root
> README, `docs/benchmark_1000000.md`, and `docs/orchestration.md`.

Phase 1 covers Raw, Bronze, Silver and provisional Gold models for the first
5,000 review lines and first 5,000 product lines. Docker Compose is the
official runtime.

## Implemented

- PostgreSQL persists Polaris metadata.
- Polaris is the Iceberg REST catalog.
- MinIO bucket `lakehouse` contains both `raw/` and `warehouse/`.
- Official MinIO Client copies the two fixed samples into Raw.
- One direct Spark job handles both JSONL sources.
- Spark creates and merges `polaris.bronze.reviews_raw` and
  `polaris.bronze.products_raw`.
- Every physical line is retained with source lineage and parse status.
- Stable `record_id` values make reruns idempotent.
- One direct Silver job parses typed fields, normalizes text, validates core
  values, separates rejected rows, and left-joins reviews to products.
- Silver reruns use a deterministic full-batch overwrite and do not append.
- Gold tables answer only the stated monthly, product, category/rating and
  data-quality questions.
- A direct Spark JDBC job publishes the four Gold tables to PostgreSQL schema
  `serving` and verifies every source/target row count.
- A one-file Streamlit dashboard queries only PostgreSQL `serving` tables with
  parameterized SQL; it does not query Iceberg directly.

The four data jobs use only the standard library and PySpark. The Spark image
does not install `boto3` or a project-specific utility package. The separate
dashboard image installs Streamlit and psycopg only as direct dependencies.

## Runtime components

- Apache Spark 3.5.9, Scala 2.12, Java 17, Python 3;
- Apache Iceberg Spark runtime 1.12.0;
- Apache Polaris 1.7.0;
- PostgreSQL 16;
- MinIO and MinIO Client;
- PostgreSQL JDBC 42.7.13;
- Streamlit 1.65.0 and psycopg 3.3.6.

The Iceberg, AWS bundle, and Hadoop AWS JARs in `spark-defaults.conf` are the
connectors Spark needs for Iceberg and S3-compatible MinIO. They are not
additional business-code frameworks.

## Verified 5k results

- `polaris.silver.reviews`: 5,000 rows and 5,000 distinct IDs;
- `polaris.silver.products`: 5,000 rows and 5,000 distinct IDs;
- `polaris.silver.rejected_rows`: 0 rows;
- `polaris.silver.review_product`: 5,000 rows;
- 40 reviews match the independent product sample by `parent_asin`;
- 1,792 reviews have warnings, mostly normalized whitespace;
- 2,939 products have warnings, mostly missing price;
- all focused Bronze/Silver tests pass in the Spark container;
- second Bronze and Silver runs retain the same row counts.

Gold verification:

- 12 focused Bronze/Silver/Gold/Serving tests pass;
- monthly metrics contain 182 observed months and sum to 5,000 reviews;
- product metrics contain 9,474 distinct `parent_asin` values and sum to 5,000
  reviews;
- category-rating metrics contain 22 rows and sum to 5,000 reviews;
- monthly low-rating count is 591, matching ratings 1 and 2;
- quality summary contains 12 status/issue rows;
- a second Gold run retains the same table counts.

PostgreSQL serving verification:

- `serving.monthly_review_metrics`: 182 rows;
- `serving.product_review_metrics`: 9,474 rows;
- `serving.category_rating_metrics`: 22 rows;
- `serving.data_quality_summary`: 12 rows;
- a second publication retains all four counts.
- the dashboard container is healthy and Streamlit AppTest renders all four
  tabs with no exception while connected to PostgreSQL.

Warnings remain in clean tables. They are not rejected rows.

## Not implemented yet

- full-dataset processing;
- complete-data quality and profiling reports;
- Airflow orchestration;
- full-dataset dashboard validation.
