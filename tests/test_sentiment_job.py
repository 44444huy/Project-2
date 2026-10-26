import unittest

from sentiment.score_reviews import (
    MAX_TEXT_TOKENS,
    MODEL_REVISION,
    SENTIMENT_SCHEMA,
    chunks,
    configured_batch_size,
    configured_device,
    configured_max_rows,
    length_sorted_batches,
    needs_full_rescore,
    review_text,
)


class ReviewTextTest(unittest.TestCase):
    def test_title_and_text_are_joined(self):
        self.assertEqual(review_text("Great", "Works well"), "Great. Works well")

    def test_missing_title_or_text(self):
        self.assertEqual(review_text("", "Works well"), "Works well")
        self.assertEqual(review_text("Great", None), "Great")
        self.assertEqual(review_text(None, None), "")


class RescoreTest(unittest.TestCase):
    def test_empty_table_needs_no_full_rescore(self):
        self.assertFalse(needs_full_rescore([]))

    def test_same_revision_is_incremental(self):
        self.assertFalse(needs_full_rescore([MODEL_REVISION]))

    def test_other_revision_triggers_full_rescore(self):
        self.assertTrue(needs_full_rescore(["old-revision", MODEL_REVISION]))


class BatchingTest(unittest.TestCase):
    def test_batches_are_sorted_by_length(self):
        rows = [("a", "xxx"), ("b", "x"), ("c", "xx")]
        batches = list(length_sorted_batches(rows, 2, key=lambda row: len(row[1])))
        self.assertEqual(batches, [[("b", "x"), ("c", "xx")], [("a", "xxx")]])

    def test_chunks_keep_the_remainder(self):
        self.assertEqual(list(chunks(iter(range(5)), 2)), [[0, 1], [2, 3], [4]])


class ConfigurationTest(unittest.TestCase):
    def test_device(self):
        self.assertEqual(configured_device("CPU"), "cpu")
        with self.assertRaises(ValueError):
            configured_device("tpu")

    def test_batch_size_and_max_rows(self):
        self.assertEqual(configured_batch_size("32"), 32)
        with self.assertRaises(ValueError):
            configured_batch_size("0")
        self.assertIsNone(configured_max_rows(""))
        self.assertEqual(configured_max_rows("1000"), 1000)


class ContractTest(unittest.TestCase):
    def test_model_limits_and_schema(self):
        self.assertEqual(MAX_TEXT_TOKENS, 254)
        self.assertEqual(
            [field.name for field in SENTIMENT_SCHEMA.fields],
            [
                "review_record_id", "sentiment_label", "negative_prob",
                "neutral_prob", "positive_prob", "text_truncated", "model_name",
                "model_revision", "scored_run_id", "scored_at",
            ],
        )


if __name__ == "__main__":
    unittest.main()
