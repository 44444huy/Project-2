# Raw repair: review split across reviews parts 00032/00033

**Status: applied 2026-10-23 with the user's approval.** Originals are kept in
`lakehouse/raw_backup/amazon_reviews_2023_electronics/full_pre_split_line_repair_20261023/`.
After upload, the three live objects matched the repaired manifest SHA-256
values, the backups matched the original manifest, and the two live parts
together hold exactly the original bytes. A boundary scan of all 54 Raw parts
before the repair found no other split line.

## Problem

An interrupted run of `scripts/download_full_dataset.py` finalized
`reviews/part-00032.jsonl.gz` with a partial last line (451 bytes, no trailing
newline). The resumed `Range` request started mid-line, so
`reviews/part-00033.jsonl.gz` line 1 is the rest of that review. Together they
are one valid review (`user_id=AFT7Q36UOUU4OF2J6RSDCAKJCR2Q`,
`parent_asin=B07BF93FJY`, `timestamp=1502167460145`).

Effect: `DATA_MODE=full` Bronze would read two malformed rows and lose one
review, and the manifest counts 43,886,945 lines for 43,886,944 reviews.
`DATA_MODE=million` reads only parts 00000-00001 and is unaffected. The
downloader no longer finalizes a partial line (see
`tests/test_full_dataset_download.py`).

## Repair

The repair moves the 451-byte tail of part 00032 to the start of part 00033.
The concatenated source bytes stay identical; only the part boundary moves.

| Part | Lines | SHA-256 |
| --- | --- | --- |
| 00032 before | 85,086 | `dc05b06f…410319b` |
| 00032 after | 85,085 | `28f3250e…184deb7` |
| 00033 before | 1,103,668 | `df591e57…f71765e7e` |
| 00033 after | 1,103,668 | `042e052f…aec9053a` |

Reviews `line_count`: 43,886,945 -> 43,886,944. `source_bytes` is unchanged.

### 1. Build the corrected files locally (read-only against MinIO)

```bash
W=/path/to/raw_repair   # empty work directory
MSYS_NO_PATHCONV=1 docker compose run --rm --no-deps -v "$W:/work" --entrypoint /bin/sh raw-setup -c '
  mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
  t=local/lakehouse/raw/amazon_reviews_2023_electronics/full
  mc cp $t/manifest.json $t/reviews/part-00032.jsonl.gz $t/reviews/part-00033.jsonl.gz /work/'
python scripts/repair_raw_split_line.py --workdir "$W"
```

The script checks the downloaded parts against the manifest SHA-256 values,
verifies that the two halves form one JSON record (and neither parses alone),
verifies that the repaired pair restores the original bytes exactly, and writes
`$W/repaired/{part-00032.jsonl.gz,part-00033.jsonl.gz,manifest.json}`.

### 2. Apply (only with explicit approval; Raw is otherwise immutable)

Pause the Airflow DAG first. Keep the originals outside `raw/` so no Raw glob
can read them, then replace the three objects:

```bash
MSYS_NO_PATHCONV=1 docker compose run --rm --no-deps -v "$W:/work" --entrypoint /bin/sh raw-setup -c '
  set -eu
  mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
  t=local/lakehouse/raw/amazon_reviews_2023_electronics/full
  b=local/lakehouse/raw_backup/amazon_reviews_2023_electronics/full_pre_split_line_repair_20261023
  mc cp $t/manifest.json $t/reviews/part-00032.jsonl.gz $t/reviews/part-00033.jsonl.gz $b/
  mc cp /work/repaired/part-00033.jsonl.gz $t/reviews/part-00033.jsonl.gz
  mc cp /work/repaired/part-00032.jsonl.gz $t/reviews/part-00032.jsonl.gz
  mc cp /work/repaired/manifest.json $t/manifest.json'
```

`_COMPLETE` is unchanged.

### 3. Verify

Download the three objects again and compare their SHA-256 values with
`$W/repaired/manifest.json`. Then check that part 00032 ends with a newline and
part 00033 starts with `{`.

### Rollback

Copy the three objects from the `raw_backup/...` prefix back to their original
keys.

## Documentation updated

- `docs/raw_landing.md`: 43,886,945 -> 43,886,944 reviews.
- `docs/Báo cáo tiến độ giai đoạn 2 ... .docx`: two occurrences of
  `43.886.945` -> `43.886.944`.
