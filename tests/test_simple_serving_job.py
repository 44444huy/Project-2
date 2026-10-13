import unittest

from pyspark.sql import SparkSession

from serving.publish_gold_to_postgres import (
    assert_reconciled,
    prepare_for_postgres,
    reconciliation_metrics,
)


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
