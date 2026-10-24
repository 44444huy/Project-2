# Raw schema gate

The `schema_check` task runs after `record_run_start` and before `bronze`. It
checks that the Raw JSONL about to be processed still has the structure that
was reviewed and approved, so a renamed key or a changed type cannot silently
empty Silver, Gold and the dashboard.

```text
record_run_start -> schema_check -> bronze -> silver -> sentiment -> gold -> quality_guard -> publish -> finalize_run
```

## How it works

1. `src/schema_evolution/check_raw_schema.py` reads the same Raw scope that
   Bronze will read (`DATA_MODE`): the first `SCHEMA_CHECK_LINES_PER_PART`
   lines (default 20,000) of every part. In `million` mode that is 40,000
   reviews from 2 parts and 200,000 products from 10 parts.
2. `jsonl_extractor.py` infers each field's type (majority type over non-null
   values), presence rate, null rate and the share of other types.
3. The inferred schema is compared with the latest reviewed baseline,
   `schemas/<source>/vN.json`. Nothing is written to `schemas/` during a run.
4. Each source gets one action, stored in `serving.schema_drift_events`:
   `ACCEPT`, `REVIEW`, `BLOCK`, or `ERROR` (no baseline or no readable line).

A JSON source declares nothing, so several observations are deliberately not
treated as changes:

- a field seen without nulls in the sample when the baseline says it is
  optional;
- a field whose sampled values are all null;
- a few values of another type, such as `price = "—"`; their share is only
  recorded in `other_types`;
- a new field, even if present in every row; it is accepted as optional and
  recorded until someone reviews it.

Fields that no layer uses are skipped entirely (`SOURCE_IGNORED_FIELDS` in
`jsonl_extractor.py`): `images` for reviews, and `images`, `videos`,
`features`, `description`, `subtitle`, `author`, `bought_together` for
products. `details` is checked only as an object (`SOURCE_OPAQUE_FIELDS`).

## Decisions by change type

| Change | Example | Action |
| --- | --- | --- |
| New field | `language` added to every review | ACCEPT |
| Type changed | `rating` number to string | BLOCK |
| Field removed, used by Gold | `parent_asin`, `price` | BLOCK (CRITICAL) |
| Field removed, used only by Silver | `asin`, review `title`, `details` | BLOCK (BREAKING) |
| Field renamed | `parent_asin` to `parent_id` | BLOCK, with a rename hint |
| Required field gains nulls | `parent_asin` empty in some rows | REVIEW |

Which layer uses a field comes from `schemas/<source>/dependencies.json`.
Update those files whenever Silver or Gold starts or stops reading a field.

## Modes

`SCHEMA_CHECK_MODE` is set in `.env` and passed to the Airflow containers.

- `report` (default): every action is recorded and the pipeline always
  continues. Errors of the check itself are recorded as `ERROR`.
- `block`: if any source is `REVIEW`, `BLOCK` or `ERROR`, the run is marked
  `SCHEMA_DRIFT_BLOCKED` in `serving.pipeline_runs`, the task exits with code
  99, and Airflow skips every later task. Serving keeps the last published
  version. An unexpected failure of the check fails the task instead, so
  Airflow retries it.

Change the mode, then recreate the scheduler:

```powershell
docker compose up -d airflow-scheduler
```

Run a few report-mode batches without false alarms before switching to block.

## Reading the result

```sql
SELECT run_id, source_name, action, baseline_version, records,
       jsonb_pretty(changes) AS changes, error_message
FROM serving.schema_drift_events
ORDER BY checked_at DESC
LIMIT 4;
```

Each entry in `changes` names the field, the change type, its severity, the
action and a reason. The `schema_check` task log in Airflow prints the same
summary, and a blocked run's `error_message` in `serving.pipeline_runs` lists
the held changes.

## When a run is held

Decide whether the source really changed:

1. **The change is correct and should be kept.** Update Silver first if it
   reads the field (for example the type in `REVIEW_SCHEMA`), and update
   `dependencies.json` if needed. Then write the next baseline from the
   current one, edit only the changed fields, and approve it:

   ```powershell
   py -3 -c "import json; d = json.load(open('schemas/reviews/v1.json', encoding='utf-8')); json.dump(d['schema'], open('reviews_next.json', 'w', encoding='utf-8'), indent=2, ensure_ascii=False)"
   ```

   Edit `reviews_next.json`, then:

   ```powershell
   $env:PYTHONPATH = "src"
   py -3 -m schema_evolution.baseline --root schemas approve reviews reviews_next.json --reason "rating is now a string; Silver parses it"
   ```

   This writes `schemas/reviews/v2.json`. Commit it and trigger a new DAG run.
   A version file is never overwritten.
2. **The field is not needed.** Add it to `SOURCE_IGNORED_FIELDS` and trigger a
   new run.
3. **The source is wrong.** Do not approve. Keep serving the current version
   and fix or re-download the source.

## Limits

- The gate samples the start of each part. A format change that appears only
  later inside a part is caught after Silver, by its reject counts, not here.
- Value-level problems with a valid type (a timestamp switching from
  milliseconds to seconds, a sudden rise in null `main_category`) are data
  quality issues and are not detected by this gate.
- `schemas/proposals/` keeps the field-by-field review that produced v1.
