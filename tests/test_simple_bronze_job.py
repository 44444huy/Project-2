import tempfile
import unittest
from pathlib import Path

from bronze.load_product_reviews_bronze import (
    build_bronze_dataframe,
    load_dataset_config,
    raw_path,
)


class SimpleBronzeJobTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("simple-product-review-bronze-tests")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.source_path = Path(self.temp_dir.name) / "reviews.jsonl"
        self.source_path.write_text(
            '{"rating": 5}\n'
            '{"rating": 5}\n'
            'not-json\n'
            '{"rating": 1}\n',
            encoding="utf-8",
        )
        self.dataset = {
            "dataset_id": "example_reviews",
            "source_reference": "a" * 40,
        }
        self.source = {
            "name": "reviews",
            "file_name": "reviews.jsonl",
            "url": "https://example.test/reviews.jsonl",
        }

    def tearDown(self):
        self.temp_dir.cleanup()

    def _build(self):
        return build_bronze_dataframe(
            self.spark,
            self.dataset,
            self.source,
            self.source_path,
        )

    def test_every_line_is_preserved(self):
        rows = self._build().orderBy("source_line_number").collect()
        self.assertEqual([row.source_line_number for row in rows], [1, 2, 3, 4])
        self.assertEqual(rows[0].raw_json, rows[1].raw_json)
        self.assertNotEqual(rows[0].record_id, rows[1].record_id)
        self.assertEqual(rows[2].parse_status, "malformed")

    def test_record_id_is_stable(self):
        first = [row.record_id for row in self._build().collect()]
        second = [row.record_id for row in self._build().collect()]
        self.assertEqual(first, second)

    def test_project_config_and_raw_path(self):
        dataset, sources = load_dataset_config(
            "/opt/project/config/dataset.json"
        )
        self.assertEqual(set(sources), {"reviews", "products"})
        self.assertEqual(
            raw_path(dataset["dataset_id"], sources["reviews"]),
            "s3a://lakehouse/raw/amazon_reviews_2023_electronics/"
            "samples/first_5000/reviews/Electronics.jsonl",
        )


if __name__ == "__main__":
    unittest.main()
