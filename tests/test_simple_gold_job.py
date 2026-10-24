import datetime
import unittest

from pyspark.sql import SparkSession

from gold.build_product_review_gold import (
    PRODUCT_GOLD,
    build_category_rating_metrics,
    build_monthly_metrics,
    build_monthly_sentiment_metrics,
    build_product_metrics,
    build_product_monthly_metrics,
    build_product_sentiment_metrics,
    build_quality_summary,
    create_gold_tables,
)


class FakeGoldTable:
    def __init__(self, columns):
        self.columns = columns


class FakeGoldSpark:
    def __init__(self, product_columns):
        self.product_columns = product_columns
        self.statements = []

    def sql(self, statement):
        self.statements.append(statement)

    def table(self, table_name):
        if table_name != PRODUCT_GOLD:
            raise AssertionError(f"Unexpected table lookup: {table_name}")
        return FakeGoldTable(self.product_columns)


class SimpleGoldJobTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("simple-product-review-gold-tests")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_monthly_metrics(self):
        reviews = self.spark.createDataFrame([
            (datetime.datetime(2023, 1, 1), 1.0, True, 2),
            (datetime.datetime(2023, 1, 2), 5.0, False, 0),
            (datetime.datetime(2023, 2, 1), 4.0, True, 1),
        ], "reviewed_at timestamp, rating double, verified_purchase boolean, helpful_vote long")

        rows = build_monthly_metrics(reviews).orderBy("review_month").collect()

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].review_count, 2)
        self.assertEqual(rows[0].low_rating_count, 1)
        self.assertAlmostEqual(rows[0].low_rating_rate, 0.5)
        self.assertAlmostEqual(rows[0].verified_purchase_rate, 0.5)

    def test_product_monthly_metrics_include_rating_buckets(self):
        reviews = self.spark.createDataFrame([
            ("P1", datetime.datetime(2023, 1, 1), 1.0, True),
            ("P1", datetime.datetime(2023, 1, 2), 5.0, False),
            ("P1", datetime.datetime(2023, 2, 1), 4.0, True),
        ], (
            "parent_asin string, reviewed_at timestamp, rating double, "
            "verified_purchase boolean"
        ))

        rows = (
            build_product_monthly_metrics(reviews)
            .orderBy("review_month")
            .collect()
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].review_count, 2)
        self.assertEqual(rows[0].rating_1_count, 1)
        self.assertEqual(rows[0].rating_5_count, 1)
        self.assertAlmostEqual(rows[0].low_rating_rate, 0.5)
        self.assertAlmostEqual(rows[0].verified_purchase_rate, 0.5)

    def test_product_metrics_keep_unmatched_reviews_and_products(self):
        reviews = self.spark.createDataFrame([
            ("P1", 1.0, True, 2),
            ("P1", 5.0, False, 1),
            ("P2", 4.0, True, 0),
        ], "parent_asin string, rating double, verified_purchase boolean, helpful_vote long")
        products = self.spark.createDataFrame([
            ("x1", "P1", "One", "Electronics", "Store", None, 4.5, 10, [], "WARNING"),
            ("x3", "P3", "Three", "Electronics", "Store", None, 4.0, 5, [], "WARNING"),
        ], (
            "record_id string, parent_asin string, title string, main_category string, "
            "store string, price decimal(12,2), average_rating double, rating_number long, "
            "categories array<string>, quality_status string"
        ))

        rows = {
            row.parent_asin: row
            for row in build_product_metrics(reviews, products).collect()
        }

        self.assertEqual(set(rows), {"P1", "P2", "P3"})
        self.assertEqual(rows["P1"].review_count, 2)
        self.assertTrue(rows["P1"].product_found)
        self.assertFalse(rows["P2"].product_found)
        self.assertFalse(rows["P3"].has_reviews)

    def test_existing_gold_column_is_renamed_without_recreating_table(self):
        spark = FakeGoldSpark(["parent_asin", "has_sample_reviews"])

        create_gold_tables(spark)

        self.assertTrue(any(
            f"ALTER TABLE {PRODUCT_GOLD}" in statement
            and "RENAME COLUMN has_sample_reviews TO has_reviews" in statement
            for statement in spark.statements
        ))

    def test_category_metrics_keep_unmatched_group(self):
        joined = self.spark.createDataFrame([
            (False, None, 1.0, True),
            (False, None, 5.0, False),
            (True, "Electronics", 5.0, True),
        ], "product_found boolean, main_category string, rating double, verified_purchase boolean")

        rows = build_category_rating_metrics(joined).collect()
        groups = {row.product_group for row in rows}

        self.assertEqual(groups, {"UNMATCHED_PRODUCT", "Electronics"})
        unmatched = [
            row for row in rows
            if row.product_group == "UNMATCHED_PRODUCT" and row.rating == 1
        ][0]
        self.assertEqual(unmatched.group_review_count, 2)

    def sentiment_inputs(self):
        reviews = self.spark.createDataFrame([
            ("r1", "P1", datetime.datetime(2023, 1, 1), 5.0),
            ("r2", "P1", datetime.datetime(2023, 1, 9), 5.0),
            ("r3", "P1", datetime.datetime(2023, 1, 20), 1.0),
            ("r4", "P2", datetime.datetime(2023, 2, 1), 3.0),
            ("r5", "P2", datetime.datetime(2023, 2, 2), 4.0),
        ], (
            "record_id string, parent_asin string, reviewed_at timestamp, "
            "rating double"
        ))
        sentiment = self.spark.createDataFrame([
            ("r1", "positive"),
            ("r2", "negative"),
            ("r3", "positive"),
            ("r4", "neutral"),
        ], "review_record_id string, sentiment_label string")
        return reviews, sentiment

    def test_monthly_sentiment_counts_labels_and_disagreement(self):
        reviews, sentiment = self.sentiment_inputs()

        rows = (
            build_monthly_sentiment_metrics(reviews, sentiment)
            .orderBy("review_month")
            .collect()
        )

        self.assertEqual(len(rows), 2)
        january, february = rows
        self.assertEqual(january.review_count, 3)
        self.assertEqual(january.scored_review_count, 3)
        self.assertEqual(january.positive_count, 2)
        self.assertEqual(january.negative_count, 1)
        self.assertEqual(january.high_rating_negative_count, 1)
        self.assertEqual(january.low_rating_positive_count, 1)
        self.assertAlmostEqual(january.positive_rate, 2 / 3)
        # r5 has no score yet: counted as a review, left out of the rates.
        self.assertEqual(february.review_count, 2)
        self.assertEqual(february.scored_review_count, 1)
        self.assertAlmostEqual(february.neutral_rate, 1.0)

    def test_product_sentiment_has_one_row_per_product(self):
        reviews, sentiment = self.sentiment_inputs()

        rows = {
            row.parent_asin: row
            for row in build_product_sentiment_metrics(
                reviews, sentiment
            ).collect()
        }

        self.assertEqual(set(rows), {"P1", "P2"})
        self.assertEqual(rows["P1"].review_count, 3)
        self.assertAlmostEqual(rows["P1"].negative_rate, 1 / 3)
        self.assertEqual(rows["P2"].scored_review_count, 1)
        self.assertEqual(rows["P2"].high_rating_negative_count, 0)

    def test_quality_status_counts_are_not_inflated_by_multiple_issues(self):
        reviews = self.spark.createDataFrame([
            ("WARNING", ["A", "B"]),
            ("PASS", []),
        ], "quality_status string, quality_issues array<string>")
        products = self.spark.createDataFrame([
            ("WARNING", ["C"]),
        ], "quality_status string, quality_issues array<string>")
        rejected = self.spark.createDataFrame([], (
            "source_name string, rejection_reasons array<string>"
        ))

        rows = build_quality_summary(reviews, products, rejected).collect()
        lookup = {
            (row.source_name, row.metric_type, row.metric_name): row.row_count
            for row in rows
        }

        self.assertEqual(lookup[("reviews", "STATUS", "WARNING")], 1)
        self.assertEqual(lookup[("reviews", "STATUS", "PASS")], 1)
        self.assertEqual(lookup[("reviews", "ISSUE", "A")], 1)
        self.assertEqual(lookup[("reviews", "ISSUE", "B")], 1)


if __name__ == "__main__":
    unittest.main()
