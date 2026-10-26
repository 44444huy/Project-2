import json
import os
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


REVIEWS_SILVER = "polaris.silver.reviews"
SENTIMENT_SILVER = "polaris.silver.review_sentiment"

# Pretrained model used as-is; the revision is pinned so scores can be repeated.
MODEL_NAME = "cardiffnlp/twitter-roberta-base-sentiment-latest"
MODEL_REVISION = "3216a57f2a0d9c45a2e6c20157c20c49fb4bf9c7"
LABELS = ("negative", "neutral", "positive")
# 256 positions minus the two special tokens. Measured on 500 reviews, keeping
# the end of long reviews instead did not change agreement with reference labels.
MAX_TEXT_TOKENS = 254
# Rows pulled to the driver, sorted by length and committed together.
CHUNK_ROWS = 50_000

SENTIMENT_SCHEMA = StructType([
    StructField("review_record_id", StringType(), False),
    StructField("sentiment_label", StringType(), False),
    StructField("negative_prob", DoubleType(), False),
    StructField("neutral_prob", DoubleType(), False),
    StructField("positive_prob", DoubleType(), False),
    StructField("text_truncated", BooleanType(), False),
    StructField("model_name", StringType(), False),
    StructField("model_revision", StringType(), False),
    StructField("scored_run_id", StringType(), False),
    StructField("scored_at", TimestampType(), False),
])


def configured_device(value=None):
    device = (value or os.environ.get("SENTIMENT_DEVICE", "cuda")).strip().lower()
    if device not in {"cuda", "cpu"}:
        raise ValueError("SENTIMENT_DEVICE must be 'cuda' or 'cpu'")
    return device


def configured_batch_size(value=None):
    size = int(value or os.environ.get("SENTIMENT_BATCH_SIZE", "64"))
    if size <= 0:
        raise ValueError("SENTIMENT_BATCH_SIZE must be positive")
    return size


def configured_max_rows(value=None):
    # Only for trial runs: score at most this many unscored reviews.
    raw = value if value is not None else os.environ.get("SENTIMENT_MAX_ROWS", "")
    return int(raw) if str(raw).strip() else None


def review_text(title, text):
    combined = f"{(title or '').strip()}. {(text or '').strip()}"
    return combined.strip(". ").strip()


def needs_full_rescore(existing_revisions, revision=MODEL_REVISION):
    # Scores from another model revision are not comparable, so a revision
    # change rescores every review instead of mixing two models in one table.
    return any(existing != revision for existing in existing_revisions)


def length_sorted_batches(rows, batch_size, key):
    # Similar lengths in one batch need little padding; this doubled throughput
    # in the GPU benchmark.
    ordered = sorted(rows, key=key)
    for start in range(0, len(ordered), batch_size):
        yield ordered[start:start + batch_size]


def chunks(iterator, size):
    chunk = []
    for item in iterator:
        chunk.append(item)
        if len(chunk) == size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


class SentimentModel:
    def __init__(self, device, batch_size):
        # Imported here so the rest of the module (and its tests) works without
        # PyTorch installed.
        import torch
        from transformers import AutoModelForSequenceClassification, AutoTokenizer

        if device == "cuda" and not torch.cuda.is_available():
            raise RuntimeError(
                "SENTIMENT_DEVICE=cuda but no GPU is visible. Start the scheduler "
                "with docker-compose.gpu.yaml, or set SENTIMENT_DEVICE=cpu to run "
                "slowly on the CPU."
            )
        self.torch = torch
        self.device = device
        self.batch_size = batch_size
        self.tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, revision=MODEL_REVISION)
        dtype = torch.float16 if device == "cuda" else torch.float32
        self.model = AutoModelForSequenceClassification.from_pretrained(
            MODEL_NAME, revision=MODEL_REVISION, torch_dtype=dtype,
        ).to(device).eval()
        self.labels = [self.model.config.id2label[i] for i in range(len(LABELS))]
        if sorted(self.labels) != sorted(LABELS):
            raise RuntimeError(f"Unexpected model labels: {self.labels}")

    def score(self, rows):
        # rows: (record_id, text). Returns (record_id, label, probabilities, truncated).
        token_ids = {
            record_id: self.tokenizer(text, add_special_tokens=False)["input_ids"]
            for record_id, text in rows
        }
        results = []
        for batch in length_sorted_batches(
            list(token_ids.items()), self.batch_size, key=lambda item: len(item[1])
        ):
            inputs = [
                self.tokenizer.build_inputs_with_special_tokens(ids[:MAX_TEXT_TOKENS])
                for _, ids in batch
            ]
            encoded = self.tokenizer.pad(
                {"input_ids": inputs}, return_tensors="pt"
            ).to(self.device)
            with self.torch.inference_mode():
                probabilities = self.model(**encoded).logits.float().softmax(-1).cpu().tolist()
            for (record_id, ids), probs in zip(batch, probabilities):
                by_label = dict(zip(self.labels, probs))
                label = max(by_label, key=by_label.get)
                results.append((record_id, label, by_label, len(ids) > MAX_TEXT_TOKENS))
        return results


def ensure_table(spark):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SENTIMENT_SILVER} (
            review_record_id STRING NOT NULL,
            sentiment_label STRING NOT NULL,
            negative_prob DOUBLE NOT NULL,
            neutral_prob DOUBLE NOT NULL,
            positive_prob DOUBLE NOT NULL,
            text_truncated BOOLEAN NOT NULL,
            model_name STRING NOT NULL,
            model_revision STRING NOT NULL,
            scored_run_id STRING NOT NULL,
            scored_at TIMESTAMP NOT NULL
        )
        USING iceberg
        TBLPROPERTIES ('format-version' = '2')
    """)


def main():
    device = configured_device()
    batch_size = configured_batch_size()
    max_rows = configured_max_rows()
    run_id = os.environ.get("ORCHESTRATION_RUN_ID") or "manual-sentiment"

    spark = SparkSession.builder.appName("score-review-sentiment").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        ensure_table(spark)
        existing = spark.table(SENTIMENT_SILVER)
        revisions = [row.model_revision for row in existing.select("model_revision").distinct().collect()]
        if needs_full_rescore(revisions):
            print(f"Model revision changed from {revisions}; rescoring all reviews", flush=True)
            spark.sql(f"DELETE FROM {SENTIMENT_SILVER}")

        reviews = spark.table(REVIEWS_SILVER).select("record_id", "title", "text")
        # Reruns and retries only score reviews that have no score yet.
        unscored = reviews.join(
            spark.table(SENTIMENT_SILVER).select(F.col("review_record_id").alias("record_id")),
            on="record_id",
            how="left_anti",
        )
        if max_rows is not None:
            unscored = unscored.limit(max_rows)
        # Fix the work list before appending to the table it was computed from.
        unscored = unscored.localCheckpoint()

        model = SentimentModel(device, batch_size)
        scored = 0
        started = time.perf_counter()
        rows = (
            (row.record_id, review_text(row.title, row.text))
            for row in unscored.toLocalIterator(prefetchPartitions=True)
        )
        for chunk in chunks(rows, CHUNK_ROWS):
            scored_at = datetime.now(timezone.utc).replace(tzinfo=None)
            output = [
                (record_id, label, probs["negative"], probs["neutral"], probs["positive"],
                 truncated, MODEL_NAME, MODEL_REVISION, run_id, scored_at)
                for record_id, label, probs, truncated in model.score(chunk)
            ]
            # One Iceberg commit per chunk, so a failed run keeps finished chunks.
            spark.createDataFrame(output, SENTIMENT_SCHEMA).writeTo(SENTIMENT_SILVER).append()
            scored += len(output)
            elapsed = time.perf_counter() - started
            print(f"Scored {scored:,} reviews ({scored / elapsed:.0f}/s)", flush=True)

        table = spark.table(SENTIMENT_SILVER)
        review_count = reviews.count()
        covered = table.join(
            reviews.select(F.col("record_id").alias("review_record_id")),
            on="review_record_id",
        ).count()
        if max_rows is None and covered != review_count:
            raise RuntimeError(
                f"{SENTIMENT_SILVER} covers {covered} of {review_count} Silver reviews"
            )

        labels = {
            row.sentiment_label: row["count"]
            for row in table.groupBy("sentiment_label").count().collect()
        }
        print(json.dumps({
            "run_id": run_id,
            "device": device,
            "scored_this_run": scored,
            "covered_reviews": covered,
            "silver_reviews": review_count,
            "labels": labels,
            "truncated": table.filter("text_truncated").count(),
        }), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
