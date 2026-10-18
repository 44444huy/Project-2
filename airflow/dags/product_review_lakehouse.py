import json
import os
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


DAG_ID = "product_review_lakehouse"
DATASET_CONFIG_PATH = Path("/opt/project/config/dataset.json")
SPARK_SUBMIT_BIN = "/opt/spark/bin/spark-submit"
SPARK_APPLICATIONS = {
    "bronze": "/opt/project/src/bronze/load_product_reviews_bronze.py",
    "silver": "/opt/project/src/silver/build_product_reviews_silver.py",
    "gold": "/opt/project/src/gold/build_product_review_gold.py",
    "publish": "/opt/project/src/serving/publish_gold_to_postgres.py",
}


def postgres_connection():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def current_configuration():
    dataset = json.loads(DATASET_CONFIG_PATH.read_text(encoding="utf-8"))
    source_config = {
        "dataset_id": dataset["dataset_id"],
        "source_reference": dataset["source_reference"],
        "sources": [
            {
                "name": source["name"],
                "url": source["url"],
                "file_name": source["file_name"],
            }
            for source in dataset["sources"]
        ],
    }
    run_config = {
        "data_mode": os.environ.get("DATA_MODE", "sample"),
        "sample_size": int(os.environ.get("SAMPLE_SIZE", "5000")),
        "spark_master": os.environ.get("SPARK_MASTER", "local[2]"),
        "spark_driver_memory": os.environ.get("SPARK_DRIVER_MEMORY", "2g"),
        "spark_shuffle_partitions": int(
            os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8")
        ),
        "spark_default_parallelism": int(
            os.environ.get("SPARK_DEFAULT_PARALLELISM", "8")
        ),
    }
    return source_config, run_config


def record_run_start(**context):
    source_config, run_config = current_configuration()
    task_instance = context["task_instance"]
    with postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO serving.pipeline_runs (
                    run_id,
                    dag_id,
                    status,
                    data_mode,
                    sample_size,
                    code_version,
                    source_config,
                    run_config,
                    airflow_log_url,
                    started_at,
                    updated_at
                )
                VALUES (%s, %s, 'RUNNING', %s, %s, %s, %s::jsonb, %s::jsonb,
                        %s, NOW(), NOW())
                ON CONFLICT (run_id) DO UPDATE SET
                    status = 'RUNNING',
                    data_mode = EXCLUDED.data_mode,
                    sample_size = EXCLUDED.sample_size,
                    code_version = EXCLUDED.code_version,
                    source_config = EXCLUDED.source_config,
                    run_config = EXCLUDED.run_config,
                    airflow_log_url = EXCLUDED.airflow_log_url,
                    started_at = NOW(),
                    finished_at = NULL,
                    duration_seconds = NULL,
                    table_counts = NULL,
                    iceberg_snapshots = NULL,
                    error_message = NULL,
                    updated_at = NOW()
                """,
                (
                    context["run_id"],
                    DAG_ID,
                    run_config["data_mode"],
                    run_config["sample_size"],
                    os.environ.get("CODE_VERSION", "local-working-tree"),
                    json.dumps(source_config),
                    json.dumps(run_config),
                    task_instance.log_url,
                ),
            )


def record_run_failure(context):
    error = str(context.get("exception") or "Airflow task failed")[:4000]
    task_instance = context.get("task_instance")
    log_url = task_instance.log_url if task_instance else None
    with postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO serving.pipeline_runs (
                    run_id, dag_id, status, data_mode, sample_size,
                    code_version, airflow_log_url, started_at, finished_at,
                    duration_seconds, error_message, updated_at
                )
                VALUES (%s, %s, 'FAILED', %s, %s, %s, %s, NOW(), NOW(), 0,
                        %s, NOW())
                ON CONFLICT (run_id) DO UPDATE SET
                    status = 'FAILED',
                    finished_at = NOW(),
                    duration_seconds = EXTRACT(
                        EPOCH FROM (NOW() - serving.pipeline_runs.started_at)
                    ),
                    error_message = EXCLUDED.error_message,
                    airflow_log_url = COALESCE(
                        EXCLUDED.airflow_log_url,
                        serving.pipeline_runs.airflow_log_url
                    ),
                    updated_at = NOW()
                """,
                (
                    context["run_id"],
                    DAG_ID,
                    os.environ.get("DATA_MODE", "sample"),
                    int(os.environ.get("SAMPLE_SIZE", "5000")),
                    os.environ.get("CODE_VERSION", "local-working-tree"),
                    log_url,
                    error,
                ),
            )


task_environment = {
    "ORCHESTRATION_RUN_ID": "{{ run_id }}",
}


def spark_task(task_id, application):
    return BashOperator(
        task_id=task_id,
        bash_command=f"{SPARK_SUBMIT_BIN} {application}",
        env=task_environment,
        append_env=True,
    )


default_args = {
    "owner": "product-review-lakehouse",
    "retries": int(os.environ.get("AIRFLOW_TASK_RETRIES", "1")),
    "retry_delay": timedelta(
        seconds=int(os.environ.get("AIRFLOW_RETRY_DELAY_SECONDS", "60"))
    ),
    "on_failure_callback": record_run_failure,
}

with DAG(
    dag_id=DAG_ID,
    description="Finite Amazon Reviews lakehouse batch",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args=default_args,
    tags=["lakehouse", "spark", "iceberg"],
) as dag:
    start = PythonOperator(
        task_id="record_run_start",
        python_callable=record_run_start,
    )

    bronze = spark_task("bronze", SPARK_APPLICATIONS["bronze"])
    silver = spark_task("silver", SPARK_APPLICATIONS["silver"])
    gold = spark_task("gold", SPARK_APPLICATIONS["gold"])
    publish = spark_task("publish", SPARK_APPLICATIONS["publish"])
    finalize = BashOperator(
        task_id="finalize_run",
        bash_command=(
            "/opt/spark/bin/spark-submit "
            "/opt/project/src/operations/finalize_pipeline_run.py "
            "--run-id \"$ORCHESTRATION_RUN_ID\""
        ),
        env=task_environment,
        append_env=True,
    )

    start >> bronze >> silver >> gold >> publish >> finalize
