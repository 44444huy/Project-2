import copy
import json
import unittest
from pathlib import Path


FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "schema_evolution"

from schema_evolution.comparator import (
    ADD_FIELD,
    NULLABILITY_CHANGE,
    PARAMETER_CHANGE,
    REMOVE_FIELD,
    TYPE_CHANGE,
    compare_schemas,
    describe_type,
)


def load_fixture_schema(name):
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


class CompareFixtureTest(unittest.TestCase):
    def test_v1_to_v2_detects_added_email(self):
        changes = compare_schemas(
            load_fixture_schema("schema_v1.json"),
            load_fixture_schema("schema_v2.json"),
        )

        self.assertEqual(
            changes,
            [
                {
                    "field": "email",
                    "change": ADD_FIELD,
                    "type": "string",
                    "nullable": True,
                }
            ],
        )

    def test_v2_to_v1_detects_removed_email(self):
        changes = compare_schemas(
            load_fixture_schema("schema_v2.json"),
            load_fixture_schema("schema_v1.json"),
        )

        self.assertEqual(
            [(c["field"], c["change"]) for c in changes],
            [("email", REMOVE_FIELD)],
        )

    def test_same_schema_has_no_changes(self):
        schema = load_fixture_schema("schema_v1.json")

        self.assertEqual(compare_schemas(schema, copy.deepcopy(schema)), [])


class CompareSyntheticTest(unittest.TestCase):
    def setUp(self):
        self.old_schema = load_fixture_schema("schema_v1.json")
        self.new_schema = copy.deepcopy(self.old_schema)

    def test_type_change(self):
        self.new_schema["age"]["type"] = "string"

        self.assertEqual(
            compare_schemas(self.old_schema, self.new_schema),
            [
                {
                    "field": "age",
                    "change": TYPE_CHANGE,
                    "from": "int32",
                    "to": "string",
                    "nullable": True,
                }
            ],
        )

    def test_decimal_precision_change_is_type_change(self):
        self.new_schema["balance"]["parameters"][
            "connect.decimal.precision"
        ] = "14"

        self.assertEqual(
            compare_schemas(self.old_schema, self.new_schema),
            [
                {
                    "field": "balance",
                    "change": TYPE_CHANGE,
                    "from": "Decimal(12,2)",
                    "to": "Decimal(14,2)",
                    "nullable": True,
                }
            ],
        )

    def test_nullability_change(self):
        self.new_schema["id"]["nullable"] = True

        self.assertEqual(
            compare_schemas(self.old_schema, self.new_schema),
            [
                {
                    "field": "id",
                    "change": NULLABILITY_CHANGE,
                    "from": False,
                    "to": True,
                }
            ],
        )

    def test_type_and_nullability_change_on_same_field(self):
        self.new_schema["id"]["type"] = "string"
        self.new_schema["id"]["nullable"] = True

        self.assertEqual(
            [(c["field"], c["change"]) for c in compare_schemas(
                self.old_schema, self.new_schema
            )],
            [("id", TYPE_CHANGE), ("id", NULLABILITY_CHANGE)],
        )

    def test_type_change_reports_new_nullability(self):
        self.new_schema["id"]["type"] = "string"
        self.new_schema["id"]["nullable"] = True

        type_change = compare_schemas(self.old_schema, self.new_schema)[0]

        self.assertEqual(type_change["change"], TYPE_CHANGE)
        self.assertTrue(type_change["nullable"])


class CompareParameterTest(unittest.TestCase):
    ENUM = "io.debezium.data.Enum"

    def enum_schema(self, allowed):
        return {
            "status": {
                "type": "string",
                "nullable": False,
                "logical_type": self.ENUM,
                "parameters": {"allowed": allowed},
            }
        }

    def test_enum_allowed_values_change(self):
        self.assertEqual(
            compare_schemas(self.enum_schema("a,b"), self.enum_schema("a,b,c")),
            [
                {
                    "field": "status",
                    "change": PARAMETER_CHANGE,
                    "type": "Enum",
                    "from": {"allowed": "a,b"},
                    "to": {"allowed": "a,b,c"},
                }
            ],
        )

    def test_column_length_change(self):
        old = {"code": {"type": "string", "nullable": True,
                        "logical_type": None, "parameters": {"length": "50"}}}
        new = {"code": {"type": "string", "nullable": True,
                        "logical_type": None, "parameters": {"length": "255"}}}

        changes = compare_schemas(old, new)

        self.assertEqual([(c["field"], c["change"]) for c in changes],
                         [("code", PARAMETER_CHANGE)])
        self.assertEqual(changes[0]["from"], {"length": "50"})
        self.assertEqual(changes[0]["to"], {"length": "255"})

    def test_parameter_added_where_none_existed(self):
        old = {"code": {"type": "string", "nullable": True,
                        "logical_type": None, "parameters": {}}}
        new = {"code": {"type": "string", "nullable": True,
                        "logical_type": None, "parameters": {"length": "50"}}}

        self.assertEqual(compare_schemas(old, new)[0]["change"], PARAMETER_CHANGE)

    def test_identical_parameters_have_no_changes(self):
        self.assertEqual(
            compare_schemas(self.enum_schema("a,b"), self.enum_schema("a,b")), [])

    def test_decimal_change_is_not_reported_twice(self):
        old = {"balance": {"type": "bytes", "nullable": True,
                           "logical_type": "org.apache.kafka.connect.data.Decimal",
                           "parameters": {"scale": "2", "connect.decimal.precision": "12"}}}
        new = {"balance": {**old["balance"],
                           "parameters": {"scale": "2", "connect.decimal.precision": "14"}}}

        changes = compare_schemas(old, new)

        self.assertEqual([c["change"] for c in changes], [TYPE_CHANGE])


class RenameCandidateTest(unittest.TestCase):
    def column(self, type_="string", nullable=True, parameters=None):
        return {"type": type_, "nullable": nullable, "logical_type": None,
                "parameters": parameters or {}}

    def by_field(self, changes):
        return {change["field"]: change for change in changes}

    def test_same_signature_pair_is_marked(self):
        old = {"id": self.column("int64", False), "user_id": self.column("int32")}
        new = {"id": self.column("int64", False), "customer_id": self.column("int32")}

        changes = self.by_field(compare_schemas(old, new))

        self.assertEqual(changes["user_id"]["rename_candidate"], "customer_id")
        self.assertEqual(changes["customer_id"]["renamed_from"], "user_id")

    def test_different_type_is_not_a_candidate(self):
        old = {"user_id": self.column("int32")}
        new = {"customer_id": self.column("string")}

        changes = self.by_field(compare_schemas(old, new))

        self.assertNotIn("rename_candidate", changes["user_id"])
        self.assertNotIn("renamed_from", changes["customer_id"])

    def test_different_nullability_is_not_a_candidate(self):
        old = {"user_id": self.column("int32", True)}
        new = {"customer_id": self.column("int32", False)}

        changes = self.by_field(compare_schemas(old, new))

        self.assertNotIn("rename_candidate", changes["user_id"])

    def test_ambiguous_match_is_not_marked(self):
        old = {"a": self.column("string"), "b": self.column("string")}
        new = {"c": self.column("string")}

        changes = compare_schemas(old, new)

        self.assertTrue(all("rename_candidate" not in c and "renamed_from" not in c
                            for c in changes))

    def test_plain_add_or_remove_is_not_marked(self):
        added = compare_schemas({}, {"email": self.column()})
        removed = compare_schemas({"email": self.column()}, {})

        self.assertNotIn("renamed_from", added[0])
        self.assertNotIn("rename_candidate", removed[0])


class DescribeTypeTest(unittest.TestCase):
    def test_plain_type(self):
        self.assertEqual(describe_type({"type": "int64"}), "int64")

    def test_logical_type_uses_short_name(self):
        column = {
            "type": "int32",
            "logical_type": "io.debezium.time.Date",
            "parameters": {},
        }

        self.assertEqual(describe_type(column), "Date")


if __name__ == "__main__":
    unittest.main()
