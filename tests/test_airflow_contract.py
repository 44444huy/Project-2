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
            'start >> bronze >> silver >> gold >> publish >> finalize',
            source,
        )
        self.assertIn("def spark_task(", source)
        self.assertIn(
            'bash_command=f"{SPARK_SUBMIT_BIN} {application}"',
            source,
        )
        expected_applications = {
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
        self.assertNotIn("./runtime", compose)
        self.assertNotIn("data_survey", compose)

    def test_finalizer_covers_all_lakehouse_and_serving_tables(self):
        self.assertEqual(len(ICEBERG_TABLES), 11)
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
