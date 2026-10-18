import unittest

from pyspark.sql import SparkSession

from serving.publish_gold_to_postgres import (
    assert_reconciled,
    atomic_swap_statements,
    execute_publish_transaction,
    execute_transaction,
    prepare_for_postgres,
    reconciliation_metrics,
    staging_optimization_statements,
    staging_table_name,
)


class FakeStatement:
    def __init__(self, connection):
        self.connection = connection
        self.closed = False

    def execute(self, sql):
        self.connection.executed.append(sql)
        if sql == self.connection.fail_on:
            raise RuntimeError("simulated transaction failure")

    def close(self):
        self.closed = True


class FakePreparedStatement:
    def __init__(self, connection):
        self.connection = connection
        self.parameters = {}
        self.closed = False

    def setString(self, position, value):
        self.parameters[position] = value

    def executeUpdate(self):
        return self.connection.prepared_update_count

    def close(self):
        self.closed = True

class FakeConnection:
    def __init__(self, fail_on=None, prepared_update_count=1):
        self.fail_on = fail_on
        self.executed = []
        self.auto_commit_values = []
        self.committed = False
        self.rolled_back = False
        self.statement = FakeStatement(self)
        self.prepared_update_count = prepared_update_count
        self.prepared_sql = None
        self.prepared = None

    def setAutoCommit(self, value):
        self.auto_commit_values.append(value)

    def createStatement(self):
        return self.statement

    def prepareStatement(self, sql):
        self.prepared_sql = sql
        self.prepared = FakePreparedStatement(self)
        return self.prepared

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


class SimpleServingJobTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("simple-product-review-serving-tests")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_staging_table_name_is_stable_and_schema_scoped(self):
        self.assertEqual(
            staging_table_name("serving.monthly_review_metrics"),
            "serving.monthly_review_metrics__staging",
        )
        with self.assertRaisesRegex(ValueError, "schema.table"):
            staging_table_name("monthly_review_metrics")

    def test_atomic_swap_renames_all_tables_before_dropping_previous(self):
        staged = {
            "serving.monthly": "serving.monthly__staging",
            "serving.product": "serving.product__staging",
        }
        statements = atomic_swap_statements(staged)

        self.assertEqual(
            statements[:3],
            [
                'DROP TABLE IF EXISTS "serving"."monthly__previous"',
                'ALTER TABLE IF EXISTS "serving"."monthly" '
                'RENAME TO "monthly__previous"',
                'ALTER TABLE "serving"."monthly__staging" '
                'RENAME TO "monthly"',
            ],
        )
        self.assertEqual(
            statements[-2:],
            [
                'DROP TABLE IF EXISTS "serving"."monthly__previous"',
                'DROP TABLE IF EXISTS "serving"."product__previous"',
            ],
        )

    def test_staging_indexes_cover_grains_and_product_search(self):
        staged = {
            "serving.monthly_review_metrics": "serving.monthly__staging",
            "serving.product_review_metrics": "serving.product__staging",
            "serving.product_monthly_review_metrics": (
                "serving.product_monthly__staging"
            ),
            "serving.category_rating_metrics": "serving.category__staging",
            "serving.data_quality_summary": "serving.quality__staging",
        }
        statements = staging_optimization_statements(staged)

        self.assertIn("CREATE EXTENSION IF NOT EXISTS pg_trgm", statements)
        self.assertIn(
            'CREATE UNIQUE INDEX ON "serving"."product__staging" '
            '(parent_asin)',
            statements,
        )
        self.assertIn(
            'CREATE INDEX ON "serving"."product__staging" USING GIN '
            '(product_title gin_trgm_ops)',
            statements,
        )
        self.assertIn(
            'CREATE UNIQUE INDEX ON "serving"."product_monthly__staging" '
            '(parent_asin, review_month)',
            statements,
        )
        self.assertIn('ANALYZE "serving"."quality__staging"', statements)

    def test_transaction_rolls_back_on_swap_failure(self):
        connection = FakeConnection(fail_on="second statement")

        with self.assertRaisesRegex(RuntimeError, "simulated"):
            execute_transaction(
                connection,
                ["first statement", "second statement", "third statement"],
            )

        self.assertFalse(connection.committed)
        self.assertTrue(connection.rolled_back)
        self.assertEqual(connection.executed, ["first statement", "second statement"])
        self.assertEqual(connection.auto_commit_values, [False, True])
        self.assertTrue(connection.statement.closed)

    def test_publish_transaction_marks_run_before_commit(self):
        connection = FakeConnection()

        execute_publish_transaction(
            connection,
            ["atomic table swap"],
            "manual__2026-10-16T00:00:00+00:00",
        )

        self.assertTrue(connection.committed)
        self.assertFalse(connection.rolled_back)
        self.assertIn("UPDATE serving.pipeline_runs", connection.prepared_sql)
        self.assertEqual(
            connection.prepared.parameters[1],
            "manual__2026-10-16T00:00:00+00:00",
        )
        self.assertTrue(connection.prepared.closed)

    def test_publish_transaction_rolls_back_when_run_is_missing(self):
        connection = FakeConnection(prepared_update_count=0)

        with self.assertRaisesRegex(RuntimeError, "No pipeline run row"):
            execute_publish_transaction(
                connection,
                ["atomic table swap"],
                "missing",
            )

        self.assertFalse(connection.committed)
        self.assertTrue(connection.rolled_back)
    def test_product_categories_are_serialized_as_json(self):
        source = self.spark.createDataFrame([
            ("P1", ["Electronics", "Computers"], 2),
        ], "parent_asin string, categories array<string>, review_count long")

        row = prepare_for_postgres(
            source,
            "polaris.gold.product_review_metrics",
        ).collect()[0]

        self.assertEqual(
            row.categories_json,
            '["Electronics","Computers"]',
        )
        self.assertNotIn("categories", row.asDict())

    def test_non_product_table_is_unchanged(self):
        source = self.spark.createDataFrame([(1,)], "review_count long")
        result = prepare_for_postgres(
            source,
            "polaris.gold.monthly_review_metrics",
        )
        self.assertEqual(result.columns, ["review_count"])

    def test_reconciliation_metrics_cover_grain_and_totals(self):
        dataframe = self.spark.createDataFrame(
            [("A", 2), ("A", 3), (None, 4)],
            "grain string, review_count long",
        )
        metrics = reconciliation_metrics(
            dataframe,
            key_columns=["grain"],
            sum_columns=["review_count"],
        )
        self.assertEqual(metrics["row_count"], 3)
        self.assertEqual(metrics["distinct_key_count"], 1)
        self.assertEqual(metrics["null_key_rows"], 1)
        self.assertEqual(metrics["review_count_sum"], 9)

    def test_reconciliation_detects_changed_totals(self):
        source = self.spark.createDataFrame(
            [("A", 2), ("B", 3)],
            "grain string, review_count long",
        )
        published = self.spark.createDataFrame(
            [("A", 2), ("B", 4)],
            "grain string, review_count long",
        )
        with self.assertRaisesRegex(RuntimeError, "review_count_sum"):
            assert_reconciled(
                source,
                published,
                "serving.example",
                {
                    "key_columns": ["grain"],
                    "sum_columns": ["review_count"],
                },
            )


if __name__ == "__main__":
    unittest.main()
