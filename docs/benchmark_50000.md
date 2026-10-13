# 50,000-row scale validation

This validation increases the finite engineering batch from 5,000 to 50,000
rows for each source. It uses the first 50,000 lines, so it validates pipeline
scale and consistency rather than statistical representativeness.

## Result

The complete Raw-to-Serving run finished successfully in approximately 195
seconds on the local Docker environment.

| Check | Result |
| --- | ---: |
| Bronze reviews | 50,000 |
| Bronze products | 50,000 |
| Silver reviews | 50,000 |
| Silver products | 50,000 |
| Silver rejected rows | 0 |
| Reviews matched to sampled products | 4,609 |
| Gold monthly rows | 236 |
| Gold product rows | 81,998 |
| Gold category-rating rows | 97 |
| Gold quality rows | 12 |

PostgreSQL Serving matched all four Gold table row counts. The monthly, product,
and category-rating marts each reconciled to 50,000 source reviews through their
aggregate review counts.

## File-layout evaluation

Spark's default 200 shuffle partitions produced many small files for this local
batch. The project now sets `spark.sql.shuffle.partitions` to 8. The derived
stages were rebuilt and republished with unchanged business results.

| Iceberg table | Files before | Files after |
| --- | ---: | ---: |
| Silver products | 200 | 8 |
| Gold monthly | 140 | 8 |
| Gold product | 200 | 8 |
| Gold category-rating | 27 | 8 |
| Gold quality | 12 | 9 |

Silver reviews and Silver review-product remain at 236 files because both are
partitioned by review month. The 50k first-N sample is spread across 236 months,
so these partitions are small at sample scale. The monthly layout is retained
because the full dataset and time-based analytical queries can make use of it;
it should be reassessed using full-data file sizes and actual query patterns.

Resource observations during the full run reached about 1.23 GiB for the Spark
container and about 690 MiB for MinIO at sampled moments. These are observations,
not measured peak values.