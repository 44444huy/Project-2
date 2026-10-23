# Raw landing

MinIO is the Raw store for the project. The one-shot `raw-setup` service uses
the official MinIO Client image to land one immutable full source snapshot.

## Processing scopes

- `million` reads exactly 1,000,000 reviews from the full-source parts.
- `full` reads every source part.
- These scopes are applied inside Bronze; they do not create duplicate Raw
  snapshots.

## Object layout

```text
s3://lakehouse/raw/<dataset_id>/full/reviews/part-*.jsonl.gz
s3://lakehouse/raw/<dataset_id>/full/products/part-*.jsonl.gz
s3://lakehouse/raw/<dataset_id>/full/manifest.json
s3://lakehouse/raw/<dataset_id>/full/_COMPLETE
```

`scripts/download_full_dataset.py` streams the pinned source URLs into
resumable, parallel-readable gzip parts. Per-part line counts, byte sizes and
SHA-256 hashes are written to manifests. The verified Raw snapshot contains
43,886,944 reviews and 1,610,012 product rows.

## Idempotence

Raw landing treats `_COMPLETE` as the snapshot completion marker and does not
upload the same snapshot again. Raw objects remain unchanged after landing.
The one approved exception is the 2026-10-23 split-line repair of reviews
parts 00032/00033; see `docs/raw_repair_split_line.md`.

## Storage zones

```text
lakehouse/
|-- raw/        # source JSONL/gzip objects
`-- warehouse/  # Iceberg data and metadata files
```

MinIO stores the bytes. Polaris catalogs the Iceberg tables. Spark reads Raw
through `s3a://` and writes Bronze through the Polaris Iceberg catalog.
