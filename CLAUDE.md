# Product Review Lakehouse

Batch lakehouse for the Electronics part of Amazon Reviews 2023. Raw JSONL in
MinIO -> Spark -> Iceberg Bronze/Silver/Gold (Polaris catalog) -> PostgreSQL
`serving` schema -> Streamlit dashboard, orchestrated by one Airflow DAG,
`product_review_lakehouse`. `README.md` has the flow and the run steps; the
contracts live in `docs/`.

DAG order: `record_run_start -> schema_check -> bronze -> silver -> gold ->
quality_guard -> publish -> finalize_run`. One run and one task at a time.

## Run and test

- Everything runs in Docker (`docker compose`). Docker Desktop may be stopped:
  check with `docker info` and start it before anything else.
- Tests only work inside the Spark container, where `src/` is on `PYTHONPATH`
  and the paths are `/opt/project/...`:

  ```bash
  docker compose --profile tools run --rm tests
  ```

  Pure-Python tests can also run on the host with `PYTHONPATH=src python -m
  unittest tests.<module>`, but the Spark-based ones cannot.
- Airflow is the only supported pipeline entrypoint. Trigger the DAG
  (`airflow dags trigger product_review_lakehouse -r <run_id>`); do not run the
  stages by hand. A full one-million run takes about 14 minutes.
- Never use `docker compose down -v`: it deletes the named volumes (Iceberg
  data, PostgreSQL, Raw copy in MinIO).
- After changing `docker-compose.yaml` mounts or env, recreate the service
  (`docker compose up -d airflow-scheduler`); after changing the serving DDL,
  rerun `docker compose up postgres-serving-setup`.
- Credentials come from `.env` (ignored by Git, never commit it). Do not print
  its values.

## Data rules

- **Raw in MinIO is immutable.** Do not overwrite or delete Raw objects without
  the user's explicit approval. `DATA_MODE=million` reads only reviews
  `part-00000` and `part-00001` plus all products; `full` reads everything.
- Bronze keeps the original JSON in `raw_json`; Silver, Gold and Serving can be
  rebuilt from it.
- Baselines in `schemas/<source>/vN.json` are reviewed decisions. A version file
  is never overwritten; change a baseline only with
  `python -m schema_evolution.baseline --root schemas approve ...` and a reason
  (see `docs/schema_gate.md`). `schemas/proposals/` is the field-by-field review
  that produced v1.
- Test runs create rows in `serving.pipeline_runs`, `schema_drift_events` and
  `quality_guard_checks`. Delete test runs by exact `run_id` in one transaction;
  never touch the real runs.
- Row counts, rejects and quality numbers used in documents must come from
  `serving.*` or `docs/benchmark_1000000.md`, not from memory.

## The two gates

Both default to `report` (record only, never stop the run). `block` stops the
run and Airflow skips the later tasks (exit code 99).

- `schema_check` (`src/schema_evolution/check_raw_schema.py`): samples Raw and
  compares it with the reviewed baseline before Bronze. Sets
  `SCHEMA_DRIFT_BLOCKED`. Env: `SCHEMA_CHECK_MODE`, `SCHEMA_CHECK_LINES_PER_PART`.
  Docs: `docs/schema_gate.md`.
- `quality_guard` (`src/operations/quality_guard.py`): compares this run's Gold
  quality summary with the published one before Publish. Sets
  `QUALITY_GUARD_BLOCKED`; conf `{"allow_quality_change": true}` overrides.
  Env: `QUALITY_GUARD_MODE` and three `QUALITY_GUARD_MAX_*` thresholds. Docs:
  `docs/quality_guard.md`.

The gate treats a JSON source differently from the Debezium source it was ported
from: nothing is declared, so types, nullability and presence are observed from
a sample. New fields are optional until reviewed, and a few dirty values (for
example `price = "—"`) are only recorded. Fields no layer uses are skipped
(`SOURCE_IGNORED_FIELDS` in `jsonl_extractor.py`).

## Conventions

- Documents in `docs/`, `README.md`, code identifiers and Silver/Gold/Serving
  code comments are in English. Comments in `src/schema_evolution/` and the
  `docs/Báo cáo ... .docx` reports are in Vietnamese; follow the file you edit.
- Keep Spark jobs direct and explicit (no registry or plugin frameworks); rules
  stay visible in one file.
- Commits: English imperative subject, body explains why. Stage the exact files
  of the change; the working tree may hold unrelated changes (use
  `git add <file>`, never `git add -A`).
- Dashboard queries use SQL parameters; never concatenate user input.

## Windows and Git Bash

- Git Bash rewrites Unix-style paths in arguments. Prefix `docker exec` and
  `docker compose run` calls that pass `/opt/...` paths with
  `MSYS_NO_PATHCONV=1`.
- Print Vietnamese with `PYTHONIOENCODING=utf-8`.
- `gh` is not installed; pull requests are opened on the GitHub web UI.
- Line endings: Git warns that LF becomes CRLF; this is expected here.
