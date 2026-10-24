import unittest

from operations.quality_guard import (
    BLOCK,
    BLOCKED_EXIT_CODE,
    DEFAULT_THRESHOLDS,
    OVERRIDDEN,
    PASS,
    apply_override,
    blocked_summary,
    configured_mode,
    configured_thresholds,
    evaluate_quality,
    exit_code,
    override_requested,
    summarize,
)


def quality_rows(source, passed, warned, rejected, issues=None):
    rows = [
        {"source_name": source, "metric_type": "STATUS", "metric_name": "PASS",
         "row_count": passed},
        {"source_name": source, "metric_type": "STATUS", "metric_name": "WARNING",
         "row_count": warned},
    ]
    if rejected:
        rows.append({"source_name": source, "metric_type": "STATUS",
                     "metric_name": "REJECTED", "row_count": rejected})
    for name, count in (issues or {}).items():
        rows.append({"source_name": source, "metric_type": "ISSUE",
                     "metric_name": name, "row_count": count})
    return rows


# Published one-million run, rounded.
PUBLISHED = (
    quality_rows("reviews", 732500, 267500, 0, {"TEXT_MISSING_OR_BLANK": 220})
    + quality_rows("products", 481212, 1128706, 94,
                   {"MAIN_CATEGORY_MISSING": 106330, "PRICE_MISSING": 1083486})
)


def blocked(checks):
    return [(c["source_name"], c["check_name"], c["metric_name"])
            for c in checks if c["action"] == BLOCK]


class SummarizeTest(unittest.TestCase):
    def test_status_and_issue_counts(self):
        summary = summarize(PUBLISHED)
        self.assertEqual(summary["products"]["total"], 1610012)
        self.assertEqual(summary["products"]["accepted"], 1609918)
        self.assertEqual(summary["products"]["rejected"], 94)
        self.assertEqual(summary["reviews"]["issues"]["TEXT_MISSING_OR_BLANK"], 220)

    def test_counts_given_as_strings_are_accepted(self):
        rows = [{"source_name": "reviews", "metric_type": "STATUS",
                 "metric_name": "PASS", "row_count": "5"}]
        self.assertEqual(summarize(rows)["reviews"]["accepted"], 5)


class EvaluateQualityTest(unittest.TestCase):
    def evaluate(self, current, compare_counts=True):
        return evaluate_quality(PUBLISHED, current, DEFAULT_THRESHOLDS, compare_counts)

    def test_same_quality_passes(self):
        checks = self.evaluate(PUBLISHED)
        self.assertEqual(blocked(checks), [])
        self.assertTrue(all(c["action"] == PASS for c in checks))

    def test_first_publish_has_no_reference(self):
        checks = evaluate_quality([], PUBLISHED, DEFAULT_THRESHOLDS)
        self.assertEqual([c["check_name"] for c in checks], ["BASELINE"])
        self.assertEqual(checks[0]["action"], PASS)

    def test_reject_rate_rise_from_zero_is_blocked(self):
        # Reviews never had rejects; a renamed key would reject most rows.
        current = (
            quality_rows("reviews", 10, 0, 999990, {"PARENT_ASIN_MISSING": 999990})
            + PUBLISHED[3:]
        )
        self.assertIn(("reviews", "REJECT_RATE", "REJECTED"), blocked(self.evaluate(current)))

    def test_missing_category_spike_is_blocked(self):
        current = (
            PUBLISHED[:3]
            + quality_rows("products", 0, 1609918, 94,
                           {"MAIN_CATEGORY_MISSING": 1450000, "PRICE_MISSING": 1083486})
        )
        self.assertEqual(
            blocked(self.evaluate(current)),
            [("products", "ISSUE_RATE", "MAIN_CATEGORY_MISSING")],
        )

    def test_small_issue_change_passes(self):
        current = (
            PUBLISHED[:3]
            + quality_rows("products", 481000, 1128918, 94,
                           {"MAIN_CATEGORY_MISSING": 106330, "PRICE_MISSING": 1083170,
                            "PRICE_INVALID": 316})
        )
        self.assertEqual(blocked(self.evaluate(current)), [])

    def test_accepted_row_drop_is_blocked_only_when_counts_compare(self):
        current = (
            quality_rows("reviews", 500000, 100000, 0, {"TEXT_MISSING_OR_BLANK": 130})
            + PUBLISHED[3:]
        )
        self.assertIn(("reviews", "ACCEPTED_ROWS", "ACCEPTED"), blocked(self.evaluate(current)))
        self.assertEqual(blocked(self.evaluate(current, compare_counts=False)), [])

    def test_missing_source_is_blocked(self):
        checks = self.evaluate(PUBLISHED[:3])
        self.assertIn(("products", "SOURCE_PRESENT", "*"), blocked(checks))


class DecisionTest(unittest.TestCase):
    def setUp(self):
        self.checks = [
            {"check_name": "REJECT_RATE", "source_name": "reviews",
             "metric_name": "REJECTED", "action": BLOCK, "message": "0% -> 99%."},
            {"check_name": "ISSUE_RATE", "source_name": "reviews",
             "metric_name": "TEXT_MISSING_OR_BLANK", "action": PASS, "message": "ok"},
        ]

    def test_report_mode_never_blocks(self):
        self.assertEqual(exit_code("report", self.checks), 0)

    def test_block_mode_blocks(self):
        self.assertEqual(exit_code("block", self.checks), BLOCKED_EXIT_CODE)

    def test_override_lets_the_run_continue(self):
        checks = apply_override(self.checks, True)
        self.assertEqual(checks[0]["action"], OVERRIDDEN)
        self.assertEqual(exit_code("block", checks), 0)

    def test_summary_lists_blocked_checks(self):
        self.assertIn("reviews REJECT_RATE REJECTED", blocked_summary(self.checks))
        self.assertNotIn("TEXT_MISSING_OR_BLANK", blocked_summary(self.checks))


class ConfigurationTest(unittest.TestCase):
    def test_modes(self):
        self.assertEqual(configured_mode("block"), "block")
        with self.assertRaises(ValueError):
            configured_mode("strict")

    def test_thresholds_from_environment(self):
        thresholds = configured_thresholds({"QUALITY_GUARD_MAX_ISSUE_RATE_INCREASE": "0.1"})
        self.assertEqual(thresholds["max_issue_rate_increase"], 0.1)
        self.assertEqual(thresholds["max_reject_rate_increase"], 0.01)
        with self.assertRaises(ValueError):
            configured_thresholds({"QUALITY_GUARD_MAX_ACCEPTED_DROP": "-1"})

    def test_override_flag_parsing(self):
        self.assertTrue(override_requested("True"))
        self.assertFalse(override_requested("False"))
        self.assertFalse(override_requested(""))


if __name__ == "__main__":
    unittest.main()
