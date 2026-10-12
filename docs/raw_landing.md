# Raw landing

MinIO is the Raw store for the project. The one-shot `raw-setup` service in
`docker-compose.yaml` uses the official MinIO Client (`mc`) image to copy the
two fixed 5,000-line JSONL samples into bucket `lakehouse`.

The local engineering sample has one stable location:
`data_survey/first_5000/`. It is not a dataset-version directory.

## Object layout

```text
s3://lakehouse/raw/<dataset_id>/samples/first_<N>/<source>/<file_name>
```

Current objects:

```text
s3://lakehouse/raw/amazon_reviews_2023_electronics/samples/first_5000/reviews/Electronics.jsonl
s3://lakehouse/raw/amazon_reviews_2023_electronics/samples/first_5000/products/meta_Electronics.jsonl
```

The sample scope is part of the path so it cannot be confused with a future
full-data load. Raw files are copied unchanged. This milestone does not create
checksums, manifests, landing receipts, dataset versions, or JSON run files.

## Idempotence

Before copying, `raw-setup` runs `mc stat` on the destination. If the object is
already present it is left unchanged, making the fixed-sample landing step
idempotent.

## Storage zones

One MinIO instance and one bucket are sufficient:

```text
lakehouse/
|-- raw/        # source JSONL objects
`-- warehouse/  # Iceberg data and metadata files
```

MinIO stores the bytes. Polaris catalogs the Iceberg tables. Spark reads Raw
through `s3a://` and writes Bronze through the Polaris Iceberg catalog.
