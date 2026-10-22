import unittest



from schema_evolution.classifier import (
    BREAKING,
    CRITICAL,
    INFO,
    WARNING,
    classify_change,
    classify_changes,
    worst_severity,
)
from schema_evolution.comparator import (
    ADD_FIELD,
    NULLABILITY_CHANGE,
    PARAMETER_CHANGE,
    REMOVE_FIELD,
    TYPE_CHANGE,
)


def severity_of(change):
    return classify_change(change)[0]


def type_change(old, new):
    return {"field": "x", "change": TYPE_CHANGE, "from": old, "to": new, "nullable": True}


def parameter_change(old, new):
    return {"field": "x", "change": PARAMETER_CHANGE, "type": "t", "from": old, "to": new}


class AddRemoveTest(unittest.TestCase):
    def test_add_nullable_field_is_info(self):
        change = {"field": "email", "change": ADD_FIELD, "type": "string", "nullable": True}
        self.assertEqual(severity_of(change), INFO)

    def test_add_required_field_is_warning(self):
        change = {"field": "email", "change": ADD_FIELD, "type": "string", "nullable": False}
        self.assertEqual(severity_of(change), WARNING)

    def test_remove_field_is_breaking(self):
        change = {"field": "fax", "change": REMOVE_FIELD, "type": "string", "nullable": True}
        self.assertEqual(severity_of(change), BREAKING)


class NullabilityTest(unittest.TestCase):
    def test_not_null_to_nullable_is_warning(self):
        change = {"field": "id", "change": NULLABILITY_CHANGE, "from": False, "to": True}
        self.assertEqual(severity_of(change), WARNING)

    def test_nullable_to_not_null_is_breaking(self):
        change = {"field": "id", "change": NULLABILITY_CHANGE, "from": True, "to": False}
        self.assertEqual(severity_of(change), BREAKING)


class TypeChangeTest(unittest.TestCase):
    def test_int_widening_is_info(self):
        self.assertEqual(severity_of(type_change("int32", "int64")), INFO)

    def test_float_widening_is_info(self):
        self.assertEqual(severity_of(type_change("float32", "float64")), INFO)

    def test_int_narrowing_is_breaking(self):
        self.assertEqual(severity_of(type_change("int64", "int32")), BREAKING)

    def test_int_to_string_is_breaking(self):
        self.assertEqual(severity_of(type_change("int32", "string")), BREAKING)

    def test_decimal_precision_increase_is_info(self):
        self.assertEqual(severity_of(type_change("Decimal(12,2)", "Decimal(14,2)")), INFO)

    def test_decimal_precision_decrease_is_breaking(self):
        self.assertEqual(severity_of(type_change("Decimal(14,2)", "Decimal(12,2)")), BREAKING)

    def test_decimal_scale_decrease_is_breaking(self):
        self.assertEqual(severity_of(type_change("Decimal(12,4)", "Decimal(12,2)")), BREAKING)

    def test_decimal_scale_increase_without_room_is_breaking(self):
        # Thêm scale mà không thêm precision làm mất chỗ cho phần nguyên.
        self.assertEqual(severity_of(type_change("Decimal(12,2)", "Decimal(12,4)")), BREAKING)


class ParameterChangeTest(unittest.TestCase):
    def test_enum_value_added_is_warning(self):
        change = parameter_change({"allowed": "a,b"}, {"allowed": "a,b,c"})
        self.assertEqual(severity_of(change), WARNING)

    def test_enum_value_removed_is_breaking(self):
        change = parameter_change({"allowed": "a,b,c"}, {"allowed": "a,b"})
        self.assertEqual(severity_of(change), BREAKING)

    def test_length_increase_is_info(self):
        change = parameter_change({"length": "50"}, {"length": "255"})
        self.assertEqual(severity_of(change), INFO)

    def test_length_decrease_is_breaking(self):
        change = parameter_change({"length": "255"}, {"length": "50"})
        self.assertEqual(severity_of(change), BREAKING)

    def test_unknown_parameter_is_warning(self):
        change = parameter_change({}, {"foo": "1"})
        self.assertEqual(severity_of(change), WARNING)

    def test_worst_parameter_wins(self):
        change = parameter_change(
            {"length": "50", "allowed": "a,b"},
            {"length": "255", "allowed": "a"},
        )
        self.assertEqual(severity_of(change), BREAKING)


class RemovalDependencyTest(unittest.TestCase):
    DEPENDENCIES = {"bronze": ["*"], "silver": ["id", "name"], "gold": ["name"]}

    def removal(self, field):
        return {"field": field, "change": REMOVE_FIELD, "type": "string", "nullable": True}

    def severity(self, field, dependencies):
        return classify_change(self.removal(field), dependencies)[0]

    def test_no_dependencies_file_stays_breaking(self):
        self.assertEqual(self.severity("fax", None), BREAKING)

    def test_column_used_by_gold_is_critical(self):
        self.assertEqual(self.severity("name", self.DEPENDENCIES), CRITICAL)

    def test_column_used_only_by_silver_is_breaking(self):
        self.assertEqual(self.severity("id", self.DEPENDENCIES), BREAKING)

    def test_unused_column_is_warning(self):
        self.assertEqual(self.severity("fax", self.DEPENDENCIES), WARNING)

    def test_bronze_wildcard_does_not_count_as_a_dependency(self):
        self.assertEqual(self.severity("fax", {"bronze": ["*"]}), WARNING)

    def test_empty_dependencies_mean_nothing_uses_the_column(self):
        self.assertEqual(self.severity("name", {}), WARNING)

    def test_gold_wins_over_silver(self):
        deps = {"silver": ["x"], "gold": ["x"]}
        self.assertEqual(self.severity("x", deps), CRITICAL)

    def test_classify_changes_passes_dependencies(self):
        result = classify_changes([self.removal("name")], self.DEPENDENCIES)

        self.assertEqual(result[0]["severity"], CRITICAL)
        self.assertIn("Gold", result[0]["reason"])


class SafetyTest(unittest.TestCase):
    def test_unknown_change_kind_is_breaking(self):
        self.assertEqual(severity_of({"field": "x", "change": "SOMETHING_NEW"}), BREAKING)

    def test_classify_changes_adds_severity_and_reason(self):
        change = {"field": "email", "change": ADD_FIELD, "type": "string", "nullable": True}

        result = classify_changes([change])

        self.assertEqual(result[0]["severity"], INFO)
        self.assertTrue(result[0]["reason"])
        self.assertNotIn("severity", change)

    def test_worst_severity(self):
        self.assertEqual(worst_severity([INFO, BREAKING, WARNING]), BREAKING)
        self.assertEqual(worst_severity([]), INFO)


if __name__ == "__main__":
    unittest.main()
