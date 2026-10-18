# 1,000,000-review scale validation

This validation uses exactly 1,000,000 reviews from the first two full-source
parts and the complete 1,610,012-row Electronics product metadata source. It
demonstrates local Spark processing at a meaningful scale; the first-source
slice is not claimed to be statistically representative.

## Airflow-orchestrated end-to-end result

The complete Airflow DAG completed successfully on the local Docker profile with
`local[4]`, 4 GiB Spark driver memory, 128 shuffle partitions, and adaptive
query execution. Airflow executed one stage at a time and recorded the run as
`SUCCESS` after final reconciliation.

| Stage | Time |
| --- | ---: |
| Record run start | 0.3 s |
| Bronze | 223.1 s |
| Silver | 953.4 s |
| Gold | 303.0 s |
| Publish | 192.9 s |
| Final reconciliation | 78.2 s |
| Recorded pipeline duration | 1,746.9 s (29 min 7 s) |

## Reconciled counts

| Check | Result |
| --- | ---: |
| Bronze reviews | 1,000,000 |
| Bronze products | 1,610,012 |
| Silver reviews | 1,000,000 |
| Silver products | 1,609,918 |
| Silver rejected products | 94 |
| Silver review-product rows | 1,000,000 |
| Reviews matched to metadata | 999,977 |
| Gold monthly rows | 280 |
| Gold product rows | 1,609,933 |
| Gold product-month rows | 739,414 |
| Gold category-rating rows | 177 |
| Gold quality rows | 14 |

PostgreSQL Serving matched all five Gold row counts. Monthly, product,
product-month, and category-rating measures each reconciled to 1,000,000
reviews. The sum of the five rating buckets in the product-month mart also
equaled 1,000,000. Final run metadata contains counts for 16 tables and current
snapshot IDs for all 11 Iceberg tables. No duplicate grain keys or rates outside
0–1 were found. Airflow and dashboard health endpoints returned HTTP 200, and
the active Spark UI returned HTTP 200 while a Spark application was running.

## Quality summary

- Review PASS: 732,500.
- Review WARNING: 267,500.
- Product PASS: 481,212.
- Product WARNING: 1,128,706.
- Product REJECTED: 94.
- Low-rating reviews: 136,736.
- Verified-purchase reviews: 903,914.

## Runtime observations

The earlier full-review attempt reached MinIO's free-drive safety threshold.
The complete Raw snapshot was retained, while the failed warehouse was purged
through Polaris and unreferenced Silver files were removed. The one-million
run completed with more than 50 GiB free on drive E. A sampled Silver check
used about 3.9 GiB of the container's 7.4 GiB memory limit; this is an
observation, not a measured peak.
