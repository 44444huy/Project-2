# Bronze data contract

Bronze is implemented by one direct Spark job:
`src/bronze/load_product_reviews_bronze.py`.

The job creates a Spark session, reads each source, adds technical columns,
creates Iceberg tables with SQL, and runs an insert-only SQL `MERGE`.

## Inputs

`config/dataset.json` contains the stable dataset ID, pinned source reference,
and the two source descriptions. Spark reads these Raw objects:

```text
s3a://lakehouse/raw/amazon_reviews_2023_electronics/samples/first_5000/reviews/Electronics.jsonl
s3a://lakehouse/raw/amazon_reviews_2023_electronics/samples/first_5000/products/meta_Electronics.jsonl
```

## Tables

- `polaris.bronze.reviews_raw`
- `polaris.bronze.products_raw`

Every physical JSONL line becomes one Bronze row. Duplicate JSON content is
not removed, and a malformed line is retained for later quality analysis.
Business typing, cleaning, filtering, and the review-product join belong to
Silver.

## Columns

| Column | Meaning |
| --- | --- |
| `record_id` | Stable SHA-256 key from dataset, source, file name, and one-based line number. It is the batch idempotency key used by the insert-only `MERGE`. |
| `dataset_id` | Stable dataset identifier. |
| `source_name` | `reviews` or `products`. |
| `source_file_name` | File name defined in the dataset config. |
| `source_line_number` | One-based physical JSONL line number. |
| `source_url` | Pinned upstream file URL. |
| `source_reference` | Pinned upstream Git commit. |
| `first_pipeline_run_id` | Label for the initial 5k batch. Kept on the existing Bronze schema. |
| `first_ingested_at_utc` | Timestamp when the row was first inserted. |
| `raw_json` | Original JSONL line. |
| `parse_status` | `parseable` or `malformed`. |
| `parse_error` | Null or `not_valid_json_object`. |

## Rerun behavior

The `record_id` does not depend on execution time. The Iceberg `MERGE` inserts
only IDs that are not already present, so running the same 5k batch again does
not duplicate rows.

For each source the job checks that it read exactly 5,000 rows and prints the
input, parseable, malformed, and final table counts. There is no custom run
registry or checksum subsystem in the active path.
