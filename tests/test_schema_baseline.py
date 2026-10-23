import copy
import json
import tempfile
import unittest
from pathlib import Path


FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "schema_evolution"

from schema_evolution.baseline import (
    approve,
    evaluate,
    list_versions,
    load_baseline,
    save_version,
)
from schema_evolution.policy import ACCEPT, BLOCK, REVIEW


TABLE = "schema_lab.customers"


def load_fixture_schema(name):
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


class BaselineTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.v1 = load_fixture_schema("schema_v1.json")
        self.v2 = load_fixture_schema("schema_v2.json")

    def tearDown(self):
        self.tmp.cleanup()


class StorageTest(BaselineTestCase):
    def test_no_baseline_yet(self):
        self.assertEqual(list_versions(TABLE, self.root), [])
        self.assertIsNone(load_baseline(TABLE, self.root))

    def test_save_increments_version(self):
        save_version(TABLE, self.v1, "đầu tiên", self.root)
        save_version(TABLE, self.v2, "thêm email", self.root)

        self.assertEqual(list_versions(TABLE, self.root), [1, 2])
        self.assertEqual(load_baseline(TABLE, self.root)["schema"], self.v2)
        self.assertTrue((self.root / TABLE / "v1.json").exists())

    def test_versions_sort_numerically(self):
        table_dir = self.root / TABLE
        table_dir.mkdir(parents=True)
        for version in (2, 10, 9):
            (table_dir / f"v{version}.json").write_text(
                json.dumps({"version": version, "schema": {}}), encoding="utf-8"
            )
        (table_dir / "notes.txt").write_text("bỏ qua", encoding="utf-8")

        self.assertEqual(list_versions(TABLE, self.root), [2, 9, 10])
        self.assertEqual(load_baseline(TABLE, self.root)["version"], 10)

    def test_saved_document_has_metadata(self):
        document = save_version(TABLE, self.v1, "đầu tiên", self.root)

        self.assertEqual(document["table"], TABLE)
        self.assertEqual(document["version"], 1)
        self.assertEqual(document["reason"], "đầu tiên")
        self.assertTrue(document["created_at"])

    def test_rejects_path_traversal_table_name(self):
        for bad in ("../etc", "a/b", "..", ""):
            with self.assertRaises(ValueError):
                list_versions(bad, self.root)


class EvaluateTest(BaselineTestCase):
    def test_first_schema_becomes_v1(self):
        result = evaluate(TABLE, self.v1, self.root)

        self.assertEqual(result["action"], ACCEPT)
        self.assertEqual(result["new_version"], 1)
        self.assertEqual(list_versions(TABLE, self.root), [1])

    def test_same_schema_does_not_create_version(self):
        evaluate(TABLE, self.v1, self.root)

        result = evaluate(TABLE, copy.deepcopy(self.v1), self.root)

        self.assertEqual(result["action"], ACCEPT)
        self.assertIsNone(result["new_version"])
        self.assertEqual(result["baseline_version"], 1)
        self.assertEqual(list_versions(TABLE, self.root), [1])

    def test_accepted_change_bumps_version(self):
        evaluate(TABLE, self.v1, self.root)

        result = evaluate(TABLE, self.v2, self.root)

        self.assertEqual(result["action"], ACCEPT)
        self.assertEqual(result["new_version"], 2)
        self.assertIn("ADD_FIELD email", load_baseline(TABLE, self.root)["reason"])

    def test_blocked_change_keeps_old_baseline(self):
        evaluate(TABLE, self.v1, self.root)
        broken = copy.deepcopy(self.v1)
        broken["age"]["type"] = "string"

        result = evaluate(TABLE, broken, self.root)

        self.assertEqual(result["action"], BLOCK)
        self.assertIsNone(result["new_version"])
        self.assertEqual(list_versions(TABLE, self.root), [1])

    def test_blocked_change_stays_blocked_on_next_event(self):
        # Lý do cần baseline: event sau mang cùng schema xấu vẫn bị chặn.
        evaluate(TABLE, self.v1, self.root)
        broken = copy.deepcopy(self.v1)
        broken["age"]["type"] = "string"

        evaluate(TABLE, broken, self.root)
        second = evaluate(TABLE, copy.deepcopy(broken), self.root)

        self.assertEqual(second["action"], BLOCK)

    def test_review_change_keeps_old_baseline(self):
        evaluate(TABLE, self.v1, self.root)
        loosened = copy.deepcopy(self.v1)
        loosened["id"]["nullable"] = True

        result = evaluate(TABLE, loosened, self.root)

        self.assertEqual(result["action"], REVIEW)
        self.assertEqual(list_versions(TABLE, self.root), [1])

    def test_accepted_version_equals_new_schema_when_only_field_added(self):
        evaluate(TABLE, self.v1, self.root)
        evaluate(TABLE, self.v2, self.root)

        self.assertEqual(load_baseline(TABLE, self.root)["schema"], self.v2)


class ApprovedDecisionsTest(BaselineTestCase):
    # Nguồn JSON: schema quan sát được không được ghi đè quyết định đã duyệt.

    def approved(self):
        return {
            "title": {"type": "string", "nullable": True, "logical_type": None,
                      "parameters": {}, "source_note": "Chốt nullable = true"},
            "rating": {"type": "number", "nullable": False, "logical_type": None,
                       "parameters": {}},
        }

    def observed(self, **extra_fields):
        schema = {
            "title": {"type": "string", "nullable": False, "logical_type": None,
                      "parameters": {}, "observed_nullability": True},
            "rating": {"type": "number", "nullable": False, "logical_type": None,
                       "parameters": {}, "observed_nullability": True},
        }
        schema.update(extra_fields)
        return schema

    def test_auto_accept_keeps_approved_nullability_and_notes(self):
        approve(TABLE, self.approved(), "Duyệt từng trường", self.root)
        language = {"type": "string", "nullable": True, "logical_type": None,
                    "parameters": {}, "observed_nullability": True}

        result = evaluate(TABLE, self.observed(language=language), self.root)

        self.assertEqual(result["action"], ACCEPT)
        self.assertEqual(result["new_version"], 2)
        schema = load_baseline(TABLE, self.root)["schema"]
        self.assertTrue(schema["title"]["nullable"])
        self.assertEqual(schema["title"]["source_note"], "Chốt nullable = true")
        self.assertEqual(schema["language"], language)

    def test_auto_accepted_new_field_from_json_is_stored_as_optional(self):
        approve(TABLE, self.approved(), "Duyệt từng trường", self.root)
        language = {"type": "string", "nullable": False, "logical_type": None,
                    "parameters": {}, "observed_nullability": True}

        result = evaluate(TABLE, self.observed(language=language), self.root)

        self.assertEqual(result["action"], ACCEPT)
        self.assertTrue(load_baseline(TABLE, self.root)["schema"]["language"]["nullable"])

    def test_observation_without_changes_creates_no_version(self):
        approve(TABLE, self.approved(), "Duyệt từng trường", self.root)

        result = evaluate(TABLE, self.observed(), self.root)

        self.assertEqual(result["action"], ACCEPT)
        self.assertIsNone(result["new_version"])
        self.assertEqual(list_versions(TABLE, self.root), [1])

    def test_accepted_type_widening_updates_only_that_column(self):
        approved = self.approved()
        approved["count"] = {"type": "int32", "nullable": True, "logical_type": None,
                             "parameters": {}, "description": "Số lượng"}
        approve(TABLE, approved, "Duyệt", self.root)
        widened = {"type": "int64", "nullable": True, "logical_type": None,
                   "parameters": {}, "observed_nullability": True}

        evaluate(TABLE, self.observed(count=widened), self.root)

        schema = load_baseline(TABLE, self.root)["schema"]
        self.assertEqual(schema["count"]["type"], "int64")
        self.assertEqual(schema["count"]["description"], "Số lượng")
        self.assertTrue(schema["title"]["nullable"])


class EvaluateDependenciesTest(BaselineTestCase):
    def write_dependencies(self, document):
        path = self.root / "deps" / TABLE
        path.mkdir(parents=True)
        (path / "dependencies.json").write_text(json.dumps(document), encoding="utf-8")

    def evaluate_removal(self):
        # Baseline đã có email (v2); schema mới bỏ email.
        evaluate(TABLE, self.v2, self.root)
        return evaluate(TABLE, self.v1, self.root, dependencies_root=self.root / "deps")

    def test_unused_column_removal_is_review_and_keeps_baseline(self):
        self.write_dependencies({"silver": ["id", "name"]})

        result = self.evaluate_removal()

        self.assertEqual(result["action"], REVIEW)
        self.assertEqual(list_versions(TABLE, self.root), [1])

    def test_used_column_removal_is_blocked(self):
        self.write_dependencies({"silver": ["email"]})

        self.assertEqual(self.evaluate_removal()["action"], BLOCK)

    def test_no_dependencies_file_falls_back_to_block(self):
        self.assertEqual(self.evaluate_removal()["action"], BLOCK)

    def test_dependencies_ignored_when_root_not_given(self):
        self.write_dependencies({"silver": ["id"]})
        evaluate(TABLE, self.v2, self.root)

        self.assertEqual(evaluate(TABLE, self.v1, self.root)["action"], BLOCK)


class ApproveTest(BaselineTestCase):
    def test_approve_unblocks_following_events(self):
        evaluate(TABLE, self.v1, self.root)
        broken = copy.deepcopy(self.v1)
        broken["age"]["type"] = "string"
        evaluate(TABLE, broken, self.root)

        document = approve(TABLE, broken, "Silver đã đọc age dạng string", self.root)
        after = evaluate(TABLE, copy.deepcopy(broken), self.root)

        self.assertEqual(document["version"], 2)
        self.assertIn("Duyệt thủ công", document["reason"])
        self.assertEqual(after["action"], ACCEPT)
        self.assertEqual(after["changes"], [])

    def test_approve_writes_review_copy(self):
        review_root = self.root / "review"
        evaluate(TABLE, self.v1, self.root)

        document = approve(TABLE, self.v2, "thêm email", self.root, review_root)

        copy_path = review_root / TABLE / f"v{document['version']}.json"
        self.assertEqual(json.loads(copy_path.read_text(encoding="utf-8")), document)

    def test_approve_without_review_root_writes_no_copy(self):
        approve(TABLE, self.v1, "đầu tiên", self.root)

        self.assertFalse((self.root / "review").exists())

    def test_automatic_accept_writes_no_review_copy(self):
        review_root = self.root / "review"
        evaluate(TABLE, self.v1, self.root)
        evaluate(TABLE, self.v2, self.root)

        self.assertFalse(review_root.exists())

    def test_approve_requires_reason(self):
        with self.assertRaises(ValueError):
            approve(TABLE, self.v1, "  ", self.root)


if __name__ == "__main__":
    unittest.main()
