# Airflow orchestration and run history

`product_review_lakehouse` is a manually triggered finite-batch DAG. Its tasks
run in dependency order:

```text
record_run_start -> bronze -> silver -> gold -> publish -> finalize_run
```

The DAG allows one active run and one active task, so two Spark stages cannot
write the lakehouse concurrently. Airflow is the only pipeline entrypoint and
owns task retries. Each data task invokes its Spark application directly with
`spark-submit`; a nonzero exit code is returned directly to Airflow.

`record_run_start` creates a `RUNNING` row in `serving.pipeline_runs` with the
source configuration, runtime configuration, code version and Airflow log URL.
Any failed task changes that row to `FAILED` and records its error. Publish
marks the row `PUBLISHED` inside the same PostgreSQL transaction that swaps the
five live Serving tables. `finalize_run` then reconciles all eleven Iceberg tables
and five Serving tables, records their row counts and current Iceberg snapshot
IDs, and changes the status to `SUCCESS`.

The verified one-million-review run completed successfully in 1,746.9 seconds.
Every task finished with `success`; the five published tables were swapped as
one Serving version and the finalizer recorded counts for 16 tables together
with 11 current Iceberg snapshot IDs.

Airflow is available at `http://localhost:8080`. The active Spark application
UI is proxied at `http://localhost:4041` while a stage is running. The dashboard
Operations tab reads the same run-history table and distinguishes the latest
attempt from the dataset version currently published to Serving.
