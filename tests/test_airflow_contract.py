import ast
import unittest
from pathlib import Path

from operations.finalize_pipeline_run import ICEBERG_TABLES, SERVING_TABLES


PROJECT = Path("/opt/project")
DAG_PATH = PROJECT / "airflow" / "dags" / "product_review_lakehouse.py"
COMPOSE_PATH = PROJECT / "docker-compose.yaml"


class AirflowContractTests(unittest.TestCase):
    def test_dag_is_valid_python_and_contains_required_stage_order(self):
        source = DAG_PATH.read_text(encoding="utf-8")
        ast.parse(source)

        self.assertIn('dag_id=DAG_ID', source)
        self.assertIn('max_active_runs=1', source)
        self.assertIn('max_active_tasks=1', source)
        self.assertIn(
            'start >> schema_check >> bronze >> silver >> sentiment >> gold',
            source,
        )
        self.assertIn('>> quality_guard >> publish >> finalize', source)
        self.assertIn(
            '"quality_guard": "/opt/project/src/operations/quality_guard.py"',
            source,
        )
        self.assertIn("allow_quality_change", source)
        self.assertIn("def spark_task(", source)
        self.assertIn(
            'bash_command=f"{SPARK_SUBMIT_BIN} {application}"',
            source,
        )
        self.assertIn(
            '"schema_check": '
            '"/opt/project/src/schema_evolution/check_raw_schema.py"',
            source,
        )
        # Cổng schema thoát mã 99 khi chặn: các bước sau bị skip, không retry.
        self.assertIn("skip_on_exit_code=BLOCKED_EXIT_CODE", source)
        self.assertIn("BLOCKED_EXIT_CODE = 99", source)
        expected_applications = {
            "sentiment": "src/sentiment/score_reviews.py",
            "bronze": "src/bronze/load_product_reviews_bronze.py",
            "silver": "src/silver/build_product_reviews_silver.py",
            "gold": "src/gold/build_product_review_gold.py",
            "publish": "src/serving/publish_gold_to_postgres.py",
        }
        for task_id, application in expected_applications.items():
            self.assertIn(
                f'"{task_id}": "/opt/project/{application}"',
                source,
            )
            self.assertIn(
                f'{task_id} = spark_task('
                f'"{task_id}", SPARK_APPLICATIONS["{task_id}"])',
                source,
            )
        self.assertNotIn("run_spark_stage.sh", source)
        self.assertNotIn("PIPELINE_MAX_ATTEMPTS", source)
        self.assertNotIn("sample_size", source)
        self.assertIn("review_limit", source)

    def test_compose_declares_airflow_runtime_services(self):
        compose = COMPOSE_PATH.read_text(encoding="utf-8")
        for service in (
            "airflow-init:",
            "airflow-webserver:",
            "airflow-scheduler:",
        ):
            self.assertIn(service, compose)
        self.assertIn('127.0.0.1:8080:8080', compose)
        self.assertIn('127.0.0.1:4041:4040', compose)
        self.assertNotIn("spark-pipeline:", compose)
        self.assertNotIn("\n  spark:\n", compose)
        self.assertNotIn("PIPELINE_MAX_ATTEMPTS", compose)
        self.assertNotIn("PIPELINE_RETRY_DELAY_SECONDS", compose)
        self.assertIn('DATA_MODE: "${DATA_MODE:-million}"', compose)
        self.assertNotIn("SAMPLE_SIZE:", compose)
        self.assertIn("- minio_data:/data", compose)
        self.assertIn("- ./airflow/logs:/opt/airflow/logs", compose)
        self.assertIn("SPARK_LOCAL_DIR: /tmp/spark-local", compose)
        self.assertIn('SCHEMA_CHECK_MODE: "${SCHEMA_CHECK_MODE:-report}"', compose)
        self.assertIn("- ./schemas:/opt/project/schemas:ro", compose)
        self.assertIn("CREATE TABLE IF NOT EXISTS serving.schema_drift_events", compose)
        self.assertIn("'SCHEMA_DRIFT_BLOCKED'", compose)
        self.assertIn("'QUALITY_GUARD_BLOCKED'", compose)
        self.assertIn('QUALITY_GUARD_MODE: "${QUALITY_GUARD_MODE:-report}"', compose)
        self.assertIn("CREATE TABLE IF NOT EXISTS serving.quality_guard_checks", compose)
        self.assertNotIn("./runtime", compose)
        self.assertNotIn("data_survey", compose)

    def test_finalizer_covers_all_lakehouse_and_serving_tables(self):
        self.assertEqual(len(ICEBERG_TABLES), 12)
        self.assertIn("polaris.silver.review_sentiment", ICEBERG_TABLES)
        self.assertEqual(len(SERVING_TABLES), 5)
        self.assertIn("polaris.bronze.reviews_raw", ICEBERG_TABLES)
        self.assertIn("polaris.gold.data_quality_summary", ICEBERG_TABLES)
        self.assertIn("serving.product_review_metrics", SERVING_TABLES)
        self.assertIn(
            "serving.product_monthly_review_metrics",
            SERVING_TABLES,
        )


if __name__ == "__main__":
    unittest.main()
