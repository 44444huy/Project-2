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
reads the target table and compares its row count with the source Gold table.
A mismatch fails the job. On the 5k milestone both the first and second runs
produced the same counts:

- monthly: 182;
- product: 9,474;
- category/rating: 22;
- data quality: 12.

## Transaction boundary

The four JDBC overwrites are four separate table operations. They are not one
distributed transaction with the Iceberg reads and are not one atomic
PostgreSQL transaction covering all four tables. If publication fails midway,
the already-written tables may be newer than the remaining tables. The job
must be rerun after the cause is fixed; its replace behavior restores a
consistent set from the same Gold state.

For the fixed 5k milestone this direct implementation is intentionally kept
simple. If concurrent dashboard availability becomes a requirement, the next
design step is staging tables plus one PostgreSQL transaction that swaps or
renames the complete table set.
