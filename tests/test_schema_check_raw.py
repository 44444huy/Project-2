import gzip
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from pyspark.sql import SparkSession

from schema_evolution.check_raw_schema import (
    ERROR,
    UPSERT_EVENT_SQL,
    check_source,
    configured_lines_per_part,
    configured_mode,
    event_parameters,
    format_event,
    profile_partitions,
)
from schema_evolution.policy import ACCEPT, BLOCK


BASELINE_ROOT = Path("/opt/project/schemas")
DATASET = {"dataset_id": "amazon_reviews_2023_electronics"}
REVIEWS_SOURCE = {"name": "reviews", "file_name": "Electronics.jsonl", "url": "u"}

REVIEW = {
    "rating": 5.0,
    "title": "Great",
    "text": "Works well",
    "images": [{"small_image_url": "s", "medium_image_url": "m",
                "large_image_url": "l", "attachment_type": "IMAGE"}],
    "asin": "B001",
    "parent_asin": "B001",
    "user_id": "U1",
    "timestamp": 1588687728923,
    "helpful_vote": 0,
    "verified_purchase": True,
}


class ConfigurationTest(unittest.TestCase):
    def test_report_is_the_default_mode(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            self.assertEqual(configured_mode(), "report")

    def test_unknown_mode_is_rejected(self):
        with self.assertRaises(ValueError):
            configured_mode("block")

    def test_lines_per_part_must_be_positive(self):
        self.assertEqual(configured_lines_per_part("10"), 10)
        with self.assertRaises(ValueError):
            configured_lines_per_part("0")

    def test_parameters_match_sql_placeholders(self):
        event = {
            "source_name": "reviews", "action": ACCEPT, "baseline_version": 1,
            "records": 1, "malformed": 0, "sampled_partitions": 1,
            "lines_per_partition": 1, "raw_path": "p", "changes": [],
            "error_message": None,
        }
        self.assertEqual(
            len(event_parameters("run", "report", event)),
            UPSERT_EVENT_SQL.count("?"),
        )


class CommittedBaselineTest(unittest.TestCase):
    def test_reviewed_baselines_exist_for_both_sources(self):
        for source in ("reviews", "products"):
            document = json.loads(
                (BASELINE_ROOT / source / "v1.json").read_text(encoding="utf-8")
            )
            self.assertEqual(document["version"], 1)
            self.assertIn("parent_asin", document["schema"])
            self.assertFalse(document["schema"]["parent_asin"]["nullable"])


class CheckSourceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("schema-check-tests")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def write_parts(self, *parts):
        for index, records in enumerate(parts):
            path = self.root / f"part-{index:05d}.jsonl.gz"
            with gzip.open(path, "wt", encoding="utf-8") as file:
                for record in records:
                    file.write(json.dumps(record) + "\n")
        return str(self.root / "part-*.jsonl.gz")

    def check(self, path, baseline_root=BASELINE_ROOT, lines=20000):
        with mock.patch(
            "schema_evolution.check_raw_schema.raw_path", return_value=path
        ):
            return check_source(
                self.spark, DATASET, REVIEWS_SOURCE, "million", lines, baseline_root
            )

    def test_reads_only_the_first_lines_of_each_partition(self):
        path = self.write_parts([REVIEW] * 5, [REVIEW] * 5)
        result, partitions = profile_partitions(self.spark, path, 3, (), ("images",))
        self.assertEqual(partitions, 2)
        self.assertEqual(result["records"], 6)
        self.assertNotIn("images", result["schema"])

    def test_matching_data_is_accepted_against_reviewed_baseline(self):
        event = self.check(self.write_parts([REVIEW] * 3, [REVIEW] * 2))
        self.assertEqual(event["action"], ACCEPT)
        self.assertEqual(event["baseline_version"], 1)
        self.assertEqual(event["records"], 5)
        self.assertEqual(event["changes"], [])
        self.assertIn("ACCEPT", format_event(event))

    def test_renamed_key_is_blocked_with_rename_hint(self):
        renamed = dict(REVIEW)
        renamed["parent_id"] = renamed.pop("parent_asin")
        event = self.check(self.write_parts([renamed] * 3))
        self.assertEqual(event["action"], BLOCK)
        removed = next(c for c in event["changes"] if c["field"] == "parent_asin")
        self.assertEqual(removed["rename_candidate"], "parent_id")

    def test_missing_baseline_is_an_error_not_a_silent_accept(self):
        event = self.check(self.write_parts([REVIEW]), baseline_root=self.root / "none")
        self.assertEqual(event["action"], ERROR)
        self.assertIn("Chưa có baseline", event["error_message"])

    def test_no_valid_lines_is_an_error(self):
        path = self.root / "part-00000.jsonl.gz"
        with gzip.open(path, "wt", encoding="utf-8") as file:
            file.write("not json\n")
        event = self.check(str(path))
        self.assertEqual(event["action"], ERROR)


if __name__ == "__main__":
    unittest.main()
