# PostgreSQL serving contract

`src/serving/publish_gold_to_postgres.py` is the boundary between analytical
Iceberg tables and the application database. Spark reads the five verified
Gold tables and writes query-ready copies to PostgreSQL schema `serving`.
Trino and dbt are not part of this path.

## Table mapping

| Iceberg source | PostgreSQL target | Grain |
| --- | --- | --- |
| `polaris.gold.monthly_review_metrics` | `serving.monthly_review_metrics` | one observed month |
| `polaris.gold.product_review_metrics` | `serving.product_review_metrics` | one `parent_asin` |
| `polaris.gold.product_monthly_review_metrics` | `serving.product_monthly_review_metrics` | one `parent_asin` and observed month |
| `polaris.gold.category_rating_metrics` | `serving.category_rating_metrics` | one product group and rating |
| `polaris.gold.data_quality_summary` | `serving.data_quality_summary` | one source, metric type and metric name |

PostgreSQL does not have the same Spark array mapping used by this simple JDBC
path. Product `categories` is therefore published as the JSON text column
`categories_json`. All other columns keep their Gold names.

## Publication behavior

The publisher takes a PostgreSQL advisory lock so two publish runs cannot
modify the deterministic staging names at the same time. Spark writes all five
Gold outputs to `serving.<table>__staging`; the dashboard continues reading the
existing target tables during this phase.

Every staging table is compared with its Gold source at three levels:

- total row count;
- non-null and unique grain keys;
- aggregate totals such as review, low-rating, verified-purchase, helpful-vote,
  and quality row counts.

Any mismatch stops publication before the live tables are changed. The
validated one-million publication produced 280 monthly rows, 1,609,933
product rows, 739,414 product-month rows, 177 category/rating rows, and 14
data-quality rows. Every review-bearing mart reconciled to 1,000,000 input
reviews.

Before the atomic swap, the publisher creates unique grain indexes, product
review-count and trigram search indexes, then analyzes all staging tables. The
verified live schema has one index on each small table and four indexes on the
product table.

## Transaction boundary

After all five staging tables pass reconciliation, PostgreSQL renames the
current targets to temporary previous names, renames all staging tables to the
live names, and drops the previous tables in one transaction. Readers therefore
see either the complete old table set or the complete new table set. A swap
error is rolled back, leaving the old set intact; leftover staging tables are
cleaned before the job exits.

When Airflow supplies an orchestration run ID, the same transaction also marks
that row in `serving.pipeline_runs` as `PUBLISHED` and records `published_at`.
The dashboard can therefore identify exactly which completed swap it is showing.
The later `finalize_run` task adds reconciled table counts and current Iceberg
snapshot IDs before changing the status to `SUCCESS`.

This guarantee covers the five PostgreSQL Serving tables only. It is not a
distributed transaction with the preceding Iceberg Gold reads. The Airflow DAG
limits the pipeline to one active run and the publisher lock prevents overlapping
manual publication, while idempotent replacement allows a failed batch to be
rerun safely.
