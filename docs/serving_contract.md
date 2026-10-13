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

Each target table is replaced rather than appended. After every write, Spark
reads the target table and compares it with the source Gold table at three
levels:

- total row count;
- non-null and unique grain keys;
- aggregate totals such as review, low-rating, verified-purchase, helpful-vote,
  and quality row counts.

Any mismatch fails the job. The validated 50k publication produced 236 monthly
rows, 81,998 product rows, 97 category/rating rows, and 12 data-quality rows.
The review-bearing marts each reconciled back to 50,000 input reviews.

## Transaction boundary

The four JDBC overwrites are four separate table operations. They are not one
distributed transaction with the Iceberg reads and are not one atomic
PostgreSQL transaction covering all four tables. If publication fails midway,
the already-written tables may be newer than the remaining tables. The job
must be rerun after the cause is fixed; its replace behavior restores a
consistent set from the same Gold state.

For the finite local batch this direct implementation is intentionally kept
simple. If concurrent dashboard availability becomes a requirement, the next
design step is staging tables plus one PostgreSQL transaction that swaps or
renames the complete table set.
