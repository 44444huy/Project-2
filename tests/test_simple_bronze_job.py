import tempfile
import unittest
from pathlib import Path

from bronze.load_product_reviews_bronze import (
    build_bronze_dataframe,
    configured_data_mode,
    configured_sample_size,
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

    def test_max_rows_limits_input_before_bronze_processing(self):
        rows = build_bronze_dataframe(
            self.spark,
            self.dataset,
            self.source,
            self.source_path,
            max_rows=2,
        ).orderBy("source_line_number").collect()
        self.assertEqual(len(rows), 2)
        self.assertEqual([row.source_line_number for row in rows], [1, 2])

    def test_project_config_and_raw_path(self):
        dataset, sources = load_dataset_config(
            "/opt/project/config/dataset.json"
        )
        self.assertEqual(set(sources), {"reviews", "products"})
        self.assertEqual(
            raw_path(dataset["dataset_id"], sources["reviews"], 5000),
            "s3a://lakehouse/raw/amazon_reviews_2023_electronics/"
            "full/reviews/part-00000.jsonl.gz",
        )
        self.assertEqual(
            raw_path(
                dataset["dataset_id"],
                sources["reviews"],
                data_mode="million",
            ),
            "s3a://lakehouse/raw/amazon_reviews_2023_electronics/"
            "full/reviews/part-0000[01].jsonl.gz",
        )
        self.assertEqual(
            raw_path(
                dataset["dataset_id"],
                sources["products"],
                data_mode="million",
            ),
            "s3a://lakehouse/raw/amazon_reviews_2023_electronics/"
            "full/products/part-*.jsonl.gz",
        )
        self.assertEqual(
            raw_path(
                dataset["dataset_id"],
                sources["reviews"],
                data_mode="full",
            ),
            "s3a://lakehouse/raw/amazon_reviews_2023_electronics/"
            "full/reviews/part-*.jsonl.gz",
        )

    def test_data_mode_accepts_supported_profiles(self):
        self.assertEqual(configured_data_mode("sample"), "sample")
        self.assertEqual(configured_data_mode("MILLION"), "million")
        self.assertEqual(configured_data_mode("FULL"), "full")
        with self.assertRaisesRegex(ValueError, "DATA_MODE"):
            configured_data_mode("everything")

    def test_sample_size_must_be_a_positive_integer(self):
        self.assertEqual(configured_sample_size("50000"), 50000)
        for invalid_value in ("0", "-1", "abc", ""):
            with self.subTest(invalid_value=invalid_value):
                with self.assertRaises(ValueError):
                    configured_sample_size(invalid_value)


if __name__ == "__main__":
    unittest.main()
