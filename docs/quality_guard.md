# Quality guard

The `quality_guard` task runs after `gold` and before `publish`. It compares
the quality summary of the run, `polaris.gold.data_quality_summary`, with the
version that is currently published, `serving.data_quality_summary`. It
catches problems that the Raw schema gate cannot see because every value still
has the right type: a source that starts sending many empty categories, rows
rejected by Silver for a format change deep inside a part, or a much smaller
batch.

```text
record_run_start -> schema_check -> bronze -> silver -> gold -> quality_guard -> publish -> finalize_run
```

## Checks

For each source (reviews, products), using the STATUS and ISSUE rows of the
quality summary:

| Check | Compares | Blocks when |
| --- | --- | --- |
| `REJECT_RATE` | rejected rows / all rows | the rate rises by more than `QUALITY_GUARD_MAX_REJECT_RATE_INCREASE` (default 0.01, one percentage point) |
| `ISSUE_RATE` | rows with each named issue / all rows | the rate rises by more than `QUALITY_GUARD_MAX_ISSUE_RATE_INCREASE` (default 0.05) |
| `ACCEPTED_ROWS` | PASS + WARNING rows | they fall by more than `QUALITY_GUARD_MAX_ACCEPTED_DROP` (default 0.05, five percent) |
| `SOURCE_PRESENT` | sources in the summary | a published source has no rows in this run |

Limits are absolute rate differences, so a source whose reject rate was zero
still has a usable limit. `ACCEPTED_ROWS` is skipped when `DATA_MODE` differs
from the published run, because `million` and `full` are different scopes.
When nothing is published yet, the run passes and becomes the reference.

Every check is stored in `serving.quality_guard_checks`.

## Modes

`QUALITY_GUARD_MODE` in `.env`:

- `report` (default): checks are recorded and the run always continues, even if
  the guard itself fails.
- `block`: any `BLOCK` marks the run `QUALITY_GUARD_BLOCKED` in
  `serving.pipeline_runs`; the task exits with code 99 and Airflow skips
  `publish` and `finalize_run`. Serving keeps the last published version.

## Publishing an intended change

When the change is expected (a new dataset version, a Silver rule change),
trigger the DAG with this run configuration:

```json
{"allow_quality_change": true}
```

The checks are still recorded, with `OVERRIDDEN` instead of `BLOCK`, and the
run continues to Publish.

## Reading the result

```sql
SELECT check_name, source_name, metric_name, action,
       previous_value, current_value, limit_value, message
FROM serving.quality_guard_checks
WHERE run_id = '<run_id>'
  AND action <> 'PASS';
```
