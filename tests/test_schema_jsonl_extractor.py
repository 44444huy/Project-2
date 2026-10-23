import gzip
import json
import tempfile
import unittest
from pathlib import Path

from schema_evolution.comparator import REMOVE_FIELD, TYPE_CHANGE, compare_schemas
from schema_evolution.jsonl_extractor import (
    SOURCE_IGNORED_FIELDS,
    SOURCE_OPAQUE_FIELDS,
    build_profile,
    json_type,
    profile_lines,
    read_lines,
    summarize_types,
)
from schema_evolution.policy import ACCEPT, BLOCK, decide


def lines(*records):
    return [json.dumps(record) for record in records]


def schema_of(*records, opaque_fields=()):
    return profile_lines(lines(*records), opaque_fields)["schema"]


REVIEW = {
    "rating": 5.0,
    "title": "Great",
    "text": "Works well",
    "images": [],
    "asin": "B001",
    "parent_asin": "B001",
    "user_id": "U1",
    "timestamp": 1588687728923,
    "helpful_vote": 0,
    "verified_purchase": True,
}


class JsonTypeTest(unittest.TestCase):
    def test_bool_is_not_integer(self):
        self.assertEqual(json_type(True), "boolean")
        self.assertEqual(json_type(1), "integer")

    def test_basic_types(self):
        self.assertEqual(json_type(1.5), "number")
        self.assertEqual(json_type("a"), "string")
        self.assertEqual(json_type([]), "array")
        self.assertEqual(json_type({}), "object")

    def test_integer_and_number_merge_to_number(self):
        self.assertEqual(summarize_types({"integer": 3, "number": 1}), ("number", {}))

    def test_majority_type_wins_and_minority_is_reported(self):
        self.assertEqual(
            summarize_types({"number": 9998, "string": 2}),
            ("number", {"string": 0.0002}),
        )

    def test_tie_is_deterministic(self):
        self.assertEqual(summarize_types({"string": 1, "number": 1})[0], "number")

    def test_no_observed_type_is_null(self):
        self.assertEqual(summarize_types({}), ("null", {}))


class ProfileLinesTest(unittest.TestCase):
    def test_review_fields_and_types(self):
        schema = schema_of(REVIEW, REVIEW)
        self.assertEqual(schema["rating"]["type"], "number")
        self.assertEqual(schema["timestamp"]["type"], "integer")
        self.assertEqual(schema["verified_purchase"]["type"], "boolean")
        self.assertFalse(schema["parent_asin"]["nullable"])
        self.assertEqual(schema["parent_asin"]["presence_rate"], 1.0)

    def test_missing_key_lowers_presence_and_is_nullable(self):
        schema = schema_of({"a": 1, "b": "x"}, {"a": 2}, {"a": 3}, {"a": 4})
        self.assertEqual(schema["b"]["presence_rate"], 0.25)
        self.assertTrue(schema["b"]["nullable"])
        self.assertFalse(schema["a"]["nullable"])

    def test_null_value_counts_as_null_not_as_type(self):
        schema = schema_of({"price": None}, {"price": 9.99})
        self.assertEqual(schema["price"]["type"], "number")
        self.assertEqual(schema["price"]["null_rate"], 0.5)
        self.assertEqual(schema["price"]["presence_rate"], 1.0)
        self.assertTrue(schema["price"]["nullable"])

    def test_field_that_is_always_null_has_null_type(self):
        schema = schema_of({"bought_together": None})
        self.assertEqual(schema["bought_together"]["type"], "null")

    def test_nested_object_is_flattened(self):
        schema = schema_of({"a": {"b": 1, "c": "x"}}, {"a": {"b": 2}})
        self.assertEqual(schema["a"]["type"], "object")
        self.assertEqual(schema["a.b"]["presence_rate"], 1.0)
        self.assertEqual(schema["a.c"]["presence_rate"], 0.5)

    def test_array_of_objects_describes_element_fields(self):
        image = {"thumb": "t", "large": "l", "variant": "MAIN", "hi_res": None}
        schema = schema_of({"images": [image, image]}, {"images": []})
        self.assertEqual(schema["images"]["type"], "array")
        self.assertEqual(schema["images[]"]["type"], "object")
        self.assertEqual(schema["images[].thumb"]["type"], "string")
        self.assertEqual(schema["images[].hi_res"]["type"], "null")

    def test_empty_arrays_do_not_create_element_field(self):
        schema = schema_of({"videos": []})
        self.assertNotIn("videos[]", schema)

    def test_array_of_strings(self):
        schema = schema_of({"categories": ["Electronics", "Cables"]})
        self.assertEqual(schema["categories[]"]["type"], "string")

    def test_opaque_field_is_not_expanded(self):
        records = [
            {"details": {"Brand": "X", "Color": "Red"}},
            {"details": {"Weight": "1 kg"}},
        ]
        schema = schema_of(*records, opaque_fields=SOURCE_OPAQUE_FIELDS["products"])
        self.assertEqual(schema["details"]["type"], "object")
        self.assertTrue(schema["details"]["opaque"])
        self.assertEqual([path for path in schema if path.startswith("details.")], [])

    def test_null_values_do_not_count_toward_majority(self):
        records = [{"price": None}] * 6 + [{"price": 9.99}] * 3 + [{"price": "—"}]
        price = schema_of(*records)["price"]
        self.assertEqual(price["type"], "number")
        self.assertEqual(price["other_types"], {"string": 0.25})
        self.assertEqual(price["null_rate"], 0.6)

    def test_rare_dirty_value_does_not_change_type(self):
        clean = [{"price": 1.0}] * 99
        dirty = clean + [{"price": "—"}]
        self.assertEqual(compare_schemas(schema_of(*clean), schema_of(*dirty)), [])

    def test_ignored_field_and_its_children_are_skipped(self):
        image = {"small_image_url": "s", "large_image_url": "l"}
        result = profile_lines(
            lines({**REVIEW, "images": [image]}),
            ignored_fields=SOURCE_IGNORED_FIELDS["reviews"],
        )
        self.assertEqual([path for path in result["schema"] if path.startswith("images")], [])
        self.assertIn("rating", result["schema"])

    def test_change_in_ignored_field_is_not_reported(self):
        ignored = SOURCE_IGNORED_FIELDS["products"]
        before = profile_lines(lines({"title": "x", "bought_together": None}),
                               ignored_fields=ignored)["schema"]
        after = profile_lines(lines({"title": "x", "bought_together": "B001,B002"}),
                              ignored_fields=ignored)["schema"]
        self.assertEqual(compare_schemas(before, after), [])

    def test_merged_profiles_equal_profile_of_all_lines(self):
        first = [{"price": 1.0}, {"price": None}]
        second = [{"price": "—", "store": "A"}]
        merged = build_profile(lines(*first)).merge(build_profile(lines(*second)))
        self.assertEqual(merged.result(), profile_lines(lines(*first, *second)))

    def test_malformed_and_non_object_lines_are_counted(self):
        result = profile_lines(["{\"a\": 1}", "not json", "[1, 2]", "", "{\"a\": 2}"])
        self.assertEqual(result["records"], 2)
        self.assertEqual(result["malformed"], 2)

    def test_output_keeps_comparator_keys(self):
        column = schema_of({"a": 1})["a"]
        self.assertEqual(
            set(column),
            {"type", "nullable", "logical_type", "parameters",
             "observed_nullability", "presence_rate", "null_rate", "other_types",
             "opaque"},
        )


class ComparatorIntegrationTest(unittest.TestCase):
    def test_same_data_has_no_changes(self):
        self.assertEqual(
            compare_schemas(schema_of(REVIEW), schema_of(REVIEW)),
            [],
        )

    def test_renamed_key_is_flagged_and_blocked(self):
        renamed = {**REVIEW}
        renamed["parent_id"] = renamed.pop("parent_asin")
        changes = compare_schemas(schema_of(REVIEW), schema_of(renamed))
        removed = next(c for c in changes if c["change"] == REMOVE_FIELD)
        self.assertEqual(removed["field"], "parent_asin")
        self.assertEqual(removed["rename_candidate"], "parent_id")
        self.assertEqual(decide(changes)["action"], BLOCK)

    def test_rating_becoming_string_is_blocked(self):
        changed = {**REVIEW, "rating": "5.0"}
        changes = compare_schemas(schema_of(REVIEW), schema_of(changed))
        self.assertEqual(changes[0]["change"], TYPE_CHANGE)
        self.assertEqual(decide(changes)["action"], BLOCK)

    def test_all_null_sample_is_not_a_type_change(self):
        documented = {"bought_together": {
            "type": "array", "nullable": True, "logical_type": None, "parameters": {},
        }}
        observed = schema_of({"bought_together": None})
        self.assertEqual(compare_schemas(documented, observed), [])

    def test_required_field_seen_only_as_null_is_still_reported(self):
        observed = schema_of({**REVIEW, "rating": None})
        changes = compare_schemas(schema_of(REVIEW), observed)
        self.assertEqual([c["change"] for c in changes], ["NULLABILITY_CHANGE"])

    def test_optional_field_seen_without_nulls_is_not_reported(self):
        approved = {"title": {
            "type": "string", "nullable": True, "logical_type": None, "parameters": {},
        }}
        self.assertEqual(compare_schemas(approved, schema_of({"title": "Great"})), [])

    def test_new_key_present_in_every_row_is_accepted_as_optional(self):
        changes = compare_schemas(schema_of(REVIEW), schema_of({**REVIEW, "language": "en"}))
        self.assertEqual(
            [(c["change"], c["field"], c["nullable"]) for c in changes],
            [("ADD_FIELD", "language", True)],
        )
        self.assertEqual(decide(changes)["action"], ACCEPT)

    def test_rename_is_still_detected_when_new_key_is_in_every_row(self):
        renamed = dict(REVIEW)
        renamed["parent_id"] = renamed.pop("parent_asin")
        changes = compare_schemas(schema_of(REVIEW), schema_of(renamed))
        added = next(c for c in changes if c["field"] == "parent_id")
        self.assertEqual(added["renamed_from"], "parent_asin")
        self.assertEqual(decide(changes)["action"], BLOCK)

    def test_new_optional_key_is_accepted(self):
        extended = [REVIEW, {**REVIEW, "language": "en"}]
        changes = compare_schemas(schema_of(REVIEW), schema_of(*extended))
        self.assertEqual(decide(changes)["action"], ACCEPT)


class ReadLinesTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_reads_gzip_with_limit_and_step(self):
        path = self.root / "part-00000.jsonl.gz"
        with gzip.open(path, "wt", encoding="utf-8") as file:
            for index in range(10):
                file.write(json.dumps({"i": index}) + "\n")

        values = [json.loads(line)["i"] for line in read_lines(path, limit=3, step=2)]
        self.assertEqual(values, [0, 2, 4])

    def test_reads_plain_jsonl(self):
        path = self.root / "sample.jsonl"
        path.write_text("{\"i\": 0}\n{\"i\": 1}\n", encoding="utf-8")
        self.assertEqual(len(list(read_lines(path))), 2)

    def test_step_must_be_positive(self):
        with self.assertRaises(ValueError):
            list(read_lines(self.root / "missing.jsonl", step=0))


if __name__ == "__main__":
    unittest.main()
