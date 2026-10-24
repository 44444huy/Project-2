import json
import tempfile
import unittest
from pathlib import Path



from schema_evolution.dependencies import load_dependencies, uses_column


class ArrayElementTest(unittest.TestCase):
    def test_array_column_covers_its_elements(self):
        self.assertTrue(uses_column(["categories"], "categories[]"))
        self.assertTrue(uses_column(["images"], "images[].thumb"))

    def test_element_does_not_match_a_different_column(self):
        self.assertFalse(uses_column(["categories"], "categoriesx"))


class ProjectDependencyFilesTest(unittest.TestCase):
    # Hai file dependency thật của Project2 phải đọc được và khai báo đúng lớp.

    def load(self, source):
        return load_dependencies(Path("/opt/project/schemas") / source / "dependencies.json")

    def test_reviews_gold_columns(self):
        dependencies = self.load("reviews")
        self.assertEqual(dependencies["bronze"], ["*"])
        for column in ("parent_asin", "rating", "timestamp"):
            self.assertTrue(uses_column(dependencies["gold"], column))
        self.assertFalse(uses_column(dependencies["gold"], "asin"))

    def test_products_gold_columns(self):
        dependencies = self.load("products")
        for column in ("parent_asin", "title", "price", "categories[]"):
            self.assertTrue(uses_column(dependencies["gold"], column))
        self.assertFalse(uses_column(dependencies["gold"], "details"))


class LoadDependenciesTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "dependencies.json"

    def tearDown(self):
        self.tmp.cleanup()

    def write(self, document):
        self.path.write_text(json.dumps(document), encoding="utf-8-sig")

    def test_missing_file_returns_none(self):
        self.assertIsNone(load_dependencies(self.path))

    def test_valid_file(self):
        self.write({"bronze": ["*"], "silver": ["id"], "gold": ["age"]})

        self.assertEqual(
            load_dependencies(self.path),
            {"bronze": ["*"], "silver": ["id"], "gold": ["age"]},
        )

    def test_partial_layers_are_allowed(self):
        self.write({"silver": ["id"]})

        self.assertEqual(load_dependencies(self.path), {"silver": ["id"]})

    def test_unknown_layer_is_rejected(self):
        self.write({"platinum": ["id"]})

        with self.assertRaisesRegex(ValueError, "platinum"):
            load_dependencies(self.path)

    def test_columns_must_be_list_of_strings(self):
        for bad in ("id", ["id", 1], {"id": 1}):
            self.write({"silver": bad})
            with self.assertRaisesRegex(ValueError, "silver"):
                load_dependencies(self.path)

    def test_not_an_object_is_rejected(self):
        self.write(["id"])

        with self.assertRaises(ValueError):
            load_dependencies(self.path)

    def test_invalid_json_is_rejected(self):
        self.path.write_text("{not json", encoding="utf-8")

        with self.assertRaisesRegex(ValueError, "JSON"):
            load_dependencies(self.path)


class UsesColumnTest(unittest.TestCase):
    def test_exact_match(self):
        self.assertTrue(uses_column(["id", "name"], "name"))
        self.assertFalse(uses_column(["id", "name"], "age"))

    def test_wildcard_uses_everything(self):
        self.assertTrue(uses_column(["*"], "anything"))

    def test_empty_list_uses_nothing(self):
        self.assertFalse(uses_column([], "id"))

    def test_nested_child_covered_by_parent(self):
        self.assertTrue(uses_column(["address"], "address.city"))

    def test_removing_parent_affects_declared_child(self):
        self.assertTrue(uses_column(["address.city"], "address"))

    def test_similar_prefix_is_not_a_match(self):
        self.assertFalse(uses_column(["address"], "address_line"))
        self.assertFalse(uses_column(["id"], "identity"))


if __name__ == "__main__":
    unittest.main()
