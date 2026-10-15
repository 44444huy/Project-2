# PostgreSQL serving contract

`src/serving/publish_gold_to_postgres.py` is the boundary between analytical
Iceberg tables and the application database. Spark reads the four verified
Gold tables and writes query-ready copies to PostgreSQL schema `serving`.
Trino and dbt are not part of this path.

## Table mapping

| Iceberg source | PostgreSQL target | Grain |
| --- | --- | --- |
| `polaris.gold.monthly_review_metrics` | `serving.monthly_review_metrics` | one observed month |
| `polaris.gold.product_review_metrics` | `serving.product_review_metrics` | one `parent_asin` |
| `polaris.gold.category_rating_metrics` | `serving.category_rating_metrics` | one product group and rating |
| `polaris.gold.data_quality_summary` | `serving.data_quality_summary` | one source, metric type and metric name |

PostgreSQL does not have the same Spark array mapping used by this simple JDBC
path. Product `categories` is therefore published as the JSON text column
`categories_json`. All other columns keep their Gold names.

## Publication behavior

The publisher takes a PostgreSQL advisory lock so two publish runs cannot
modify the deterministic staging names at the same time. Spark writes all four
Gold outputs to `serving.<table>__staging`; the dashboard continues reading the
existing target tables during this phase.

Every staging table is compared with its Gold source at three levels:

- total row count;
- non-null and unique grain keys;
- aggregate totals such as review, low-rating, verified-purchase, helpful-vote,
  and quality row counts.

Any mismatch stops publication before the live tables are changed. The
validated 50k publication produced 236 monthly rows, 81,998 product rows, 97
category/rating rows, and 12 data-quality rows. The review-bearing marts each
reconciled back to 50,000 input reviews.

## Transaction boundary

After all four staging tables pass reconciliation, PostgreSQL renames the
current targets to temporary previous names, renames all staging tables to the
live names, and drops the previous tables in one transaction. Readers therefore
see either the complete old table set or the complete new table set. A swap
error is rolled back, leaving the old set intact; leftover staging tables are
cleaned before the job exits.

This guarantee covers the four PostgreSQL Serving tables only. It is not a
distributed transaction with the preceding Iceberg Gold reads. The normal batch
order and publisher lock prevent another project publish run from overlapping,
while idempotent replacement allows a failed batch to be rerun safely.
