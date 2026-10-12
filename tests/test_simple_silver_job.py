import json
import unittest

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)

from silver.build_product_reviews_silver import (
    build_products,
    build_review_product,
    build_reviews,
)


BRONZE_SCHEMA = StructType([
    StructField("record_id", StringType(), False),
    StructField("source_file_name", StringType(), False),
    StructField("source_line_number", LongType(), False),
    StructField("raw_json", StringType(), False),
    StructField("parse_status", StringType(), False),
])


class SimpleSilverJobTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("simple-product-review-silver-tests")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def dataframe(self, rows):
        return self.spark.createDataFrame(rows, BRONZE_SCHEMA)

    def test_reviews_keep_blank_text_as_warning_and_reject_bad_rating(self):
        valid = {
            "rating": 5.0,
            "title": "  Good   item ",
            "text": "   ",
            "images": [],
            "asin": "A1",
            "parent_asin": "P1",
            "user_id": "U1",
            "timestamp": 1600000000000,
            "helpful_vote": 0,
            "verified_purchase": True,
        }
        invalid = {**valid, "rating": 6.0, "parent_asin": "P2"}
        bronze = self.dataframe([
            ("r1", "reviews.jsonl", 1, json.dumps(valid), "parseable"),
            ("r2", "reviews.jsonl", 2, json.dumps(invalid), "parseable"),
        ])

        clean, rejected = build_reviews(bronze)
        clean_row = clean.collect()[0]
        rejected_row = rejected.collect()[0]

        self.assertEqual(clean.count(), 1)
        self.assertEqual(clean_row.title, "Good item")
        self.assertEqual(clean_row.text, "")
        self.assertEqual(clean_row.quality_status, "WARNING")
        self.assertIn("TEXT_MISSING_OR_BLANK", clean_row.quality_issues)
        self.assertIn("RATING_OUT_OF_RANGE", rejected_row.rejection_reasons)

    def test_products_keep_missing_price_and_reject_later_duplicate(self):
        product = {
            "main_category": "Electronics",
            "title": " Product ",
            "average_rating": 4.0,
            "rating_number": 10,
            "features": [" Feature   one "],
            "description": [],
            "price": None,
            "images": [],
            "videos": [],
            "store": "Store",
            "categories": ["Electronics"],
            "details": {"Manufacturer": "Example"},
            "parent_asin": "P1",
            "subtitle": None,
        }
        bronze = self.dataframe([
            ("p1", "products.jsonl", 1, json.dumps(product), "parseable"),
            ("p2", "products.jsonl", 2, json.dumps(product), "parseable"),
        ])

        clean, rejected = build_products(bronze)
        clean_row = clean.collect()[0]
        rejected_row = rejected.collect()[0]

        self.assertEqual(clean.count(), 1)
        self.assertEqual(clean_row.record_id, "p1")
        self.assertEqual(clean_row.features, ["Feature one"])
        self.assertIn("PRICE_MISSING", clean_row.quality_issues)
        self.assertIn(
            "DUPLICATE_PARENT_ASIN",
            rejected_row.rejection_reasons,
        )

    def test_review_product_is_left_join(self):
        reviews = self.spark.createDataFrame([
            (
                "r1", "A1", "P1", "U1", 5.0, "Title", "Text", 0,
                True, 1600000000000, None, [], "PASS", [],
                "reviews.jsonl", 1, None,
            ),
            (
                "r2", "A2", "P2", "U2", 4.0, "Title", "Text", 0,
                False, 1600000000000, None, [], "PASS", [],
                "reviews.jsonl", 2, None,
            ),
        ], schema=(
            "record_id string, asin string, parent_asin string, user_id string, "
            "rating double, title string, text string, helpful_vote long, "
            "verified_purchase boolean, review_timestamp_ms long, "
            "reviewed_at timestamp, images array<struct<small_image_url:string," 
            "medium_image_url:string,large_image_url:string,attachment_type:string>>, "
            "quality_status string, quality_issues array<string>, "
            "source_file_name string, source_line_number long, silver_updated_at timestamp"
        ))
        products = self.spark.createDataFrame([
            (
                "p1", "P1", "Electronics", "Product", 4.5, 2, [], [],
                None, [], [], "Store", ["Electronics"], "{}", None, None,
                None, "WARNING", ["PRICE_MISSING"], "products.jsonl", 1, None,
            ),
        ], schema=(
            "record_id string, parent_asin string, main_category string, title string, "
            "average_rating double, rating_number long, features array<string>, "
            "description array<string>, price decimal(12,2), "
            "images array<struct<thumb:string,large:string,variant:string,hi_res:string>>, "
            "videos array<struct<title:string,url:string,user_id:string>>, store string, "
            "categories array<string>, details_json string, author_json string, "
            "bought_together_json string, subtitle string, quality_status string, "
            "quality_issues array<string>, source_file_name string, "
            "source_line_number long, silver_updated_at timestamp"
        ))

        joined = build_review_product(reviews, products).orderBy("review_record_id")
        rows = joined.collect()

        self.assertEqual(len(rows), 2)
        self.assertTrue(rows[0].product_found)
        self.assertFalse(rows[1].product_found)


if __name__ == "__main__":
    unittest.main()
