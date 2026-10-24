import copy
import json
import unittest
from pathlib import Path


FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "schema_evolution"

from schema_evolution.comparator import compare_schemas
from schema_evolution.policy import ACCEPT, BLOCK, REVIEW, decide, load_policy


def load_fixture_schema(name):
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


class DecideFixtureTest(unittest.TestCase):
    def test_adding_nullable_column_is_accepted(self):
        changes = compare_schemas(
            load_fixture_schema("schema_v1.json"),
            load_fixture_schema("schema_v2.json"),
        )

        decision = decide(changes)

        self.assertEqual(decision["action"], ACCEPT)
        self.assertEqual(decision["changes"][0]["action"], ACCEPT)

    def test_removing_column_is_blocked(self):
        changes = compare_schemas(
            load_fixture_schema("schema_v2.json"),
            load_fixture_schema("schema_v1.json"),
        )

        self.assertEqual(decide(changes)["action"], BLOCK)

    def test_no_changes_is_accepted(self):
        self.assertEqual(decide([])["action"], ACCEPT)


class DecideScenarioTest(unittest.TestCase):
    def setUp(self):
        self.old_schema = load_fixture_schema("schema_v1.json")
        self.new_schema = copy.deepcopy(self.old_schema)

    def test_age_int_to_string_is_blocked(self):
        self.new_schema["age"]["type"] = "string"

        decision = decide(compare_schemas(self.old_schema, self.new_schema))

        self.assertEqual(decision["action"], BLOCK)
        self.assertEqual(decision["changes"][0]["severity"], "BREAKING")

    def test_not_null_to_nullable_needs_review(self):
        self.new_schema["id"]["nullable"] = True

        decision = decide(compare_schemas(self.old_schema, self.new_schema))

        self.assertEqual(decision["action"], REVIEW)

    def test_rename_is_blocked(self):
        # Rename trực tiếp hiện ra là REMOVE + ADD, REMOVE bị chặn.
        self.new_schema["customer_id"] = self.new_schema.pop("age")

        decision = decide(compare_schemas(self.old_schema, self.new_schema))

        self.assertEqual(decision["action"], BLOCK)
        self.assertEqual(
            sorted(change["change"] for change in decision["changes"]),
            ["ADD_FIELD", "REMOVE_FIELD"],
        )

    def test_worst_action_wins(self):
        self.new_schema["email"] = {
            "type": "string", "nullable": True, "logical_type": None, "parameters": {},
        }
        self.new_schema["age"]["type"] = "string"

        decision = decide(compare_schemas(self.old_schema, self.new_schema))

        self.assertEqual(decision["action"], BLOCK)
        self.assertEqual(
            {change["field"]: change["action"] for change in decision["changes"]},
            {"email": ACCEPT, "age": BLOCK},
        )


class DecideRenameCandidateTest(unittest.TestCase):
    OLD = {
        "id": {"type": "int64", "nullable": False, "logical_type": None, "parameters": {}},
        "user_id": {"type": "int32", "nullable": True, "logical_type": None, "parameters": {}},
    }

    def renamed(self):
        new = copy.deepcopy(self.OLD)
        new["customer_id"] = new.pop("user_id")
        return new

    def test_rename_candidate_is_blocked_on_both_sides(self):
        decision = decide(compare_schemas(self.OLD, self.renamed()))

        self.assertEqual(decision["action"], BLOCK)
        by_field = {c["field"]: c for c in decision["changes"]}
        self.assertEqual(by_field["user_id"]["action"], BLOCK)
        self.assertEqual(by_field["customer_id"]["action"], BLOCK)
        self.assertIn("customer_id", by_field["user_id"]["reason"])
        self.assertIn("user_id", by_field["customer_id"]["reason"])

    def test_unused_old_column_still_blocked_when_rename_candidate(self):
        # Không có rename thì cột không ai dùng chỉ REVIEW; có ứng viên đổi tên thì BLOCK.
        dependencies = {"silver": ["id"]}

        plain = decide(compare_schemas(self.OLD, {"id": self.OLD["id"]}),
                       dependencies=dependencies)
        renamed = decide(compare_schemas(self.OLD, self.renamed()),
                         dependencies=dependencies)

        self.assertEqual(plain["action"], REVIEW)
        self.assertEqual(renamed["action"], BLOCK)


class DecideDependencyTest(unittest.TestCase):
    CHANGE = {"field": "fax", "change": "REMOVE_FIELD", "type": "string", "nullable": True}

    def test_unused_column_removal_needs_review_not_block(self):
        decision = decide([self.CHANGE], dependencies={"silver": ["id"]})

        self.assertEqual(decision["action"], REVIEW)

    def test_gold_column_removal_is_blocked(self):
        decision = decide([self.CHANGE], dependencies={"gold": ["fax"]})

        self.assertEqual(decision["action"], BLOCK)
        self.assertEqual(decision["changes"][0]["severity"], "CRITICAL")

    def test_without_dependencies_removal_is_blocked(self):
        self.assertEqual(decide([self.CHANGE])["action"], BLOCK)


class PolicyConfigTest(unittest.TestCase):
    def test_default_policy_maps_all_severities(self):
        self.assertEqual(
            load_policy(),
            {"INFO": "ACCEPT", "WARNING": "REVIEW", "BREAKING": "BLOCK", "CRITICAL": "BLOCK"},
        )

    def test_custom_policy_changes_decision(self):
        changes = [{"field": "x", "change": "REMOVE_FIELD", "type": "string", "nullable": True}]
        lenient = {"INFO": ACCEPT, "WARNING": ACCEPT, "BREAKING": REVIEW, "CRITICAL": BLOCK}

        self.assertEqual(decide(changes, lenient)["action"], REVIEW)

    def test_severity_missing_from_policy_is_blocked(self):
        changes = [{"field": "x", "change": "ADD_FIELD", "type": "string", "nullable": True}]

        self.assertEqual(decide(changes, {})["action"], BLOCK)


if __name__ == "__main__":
    unittest.main()
