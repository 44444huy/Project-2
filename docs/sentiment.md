# Review sentiment

The `sentiment` task runs between `silver` and `gold`. It labels every Silver
review as `negative`, `neutral` or `positive` from its title and text, as a
signal next to the star rating (which `low_rating` already covers).

```text
record_run_start -> schema_check -> bronze -> silver -> sentiment -> gold -> quality_guard -> publish -> finalize_run
```

## Model

- `cardiffnlp/twitter-roberta-base-sentiment-latest`, revision
  `3216a57f2a0d9c45a2e6c20157c20c49fb4bf9c7`, used as published. Nothing is
  trained or fine-tuned.
- Input: `title. text`, the first 254 tokens (256 with special tokens).
- On the GPU: fp16, batches of `SENTIMENT_BATCH_SIZE` (default 64) reviews of
  similar length.

## Output

`polaris.silver.review_sentiment`, one row per scored review:
`review_record_id`, `sentiment_label`, `negative_prob`, `neutral_prob`,
`positive_prob`, `text_truncated`, `model_name`, `model_revision`,
`scored_run_id`, `scored_at`.

The job only scores reviews that have no row yet, committing every 50,000
reviews, so a retry or rerun continues where it stopped. When the pinned
revision changes, the table is cleared and every review is rescored. It fails
if the table does not cover every Silver review at the end.

## Running it

The task needs a GPU unless `SENTIMENT_DEVICE=cpu`. Start the scheduler with
the GPU override:

```powershell
docker compose -f docker-compose.yaml -f docker-compose.gpu.yaml up -d airflow-scheduler
```

Without it the task fails with a message instead of running for hours on the
CPU. The model is downloaded on first use into the `hf_cache` volume.

`SENTIMENT_MAX_ROWS` limits a trial run to that many unscored reviews.

## Measured speed and quality

Measured on an RTX 4050 Laptop GPU (6 GiB) on 23/10/2026, outside the pipeline:

- About 860 reviews per second on 10,000 reviews, about 19 minutes per
  million; fp16 gave the same label as fp32 for 9,999 of them.
- On 500 reviews (100 per star, outside the one-million scope) labelled by
  Claude as a reference: 80.6% agreement, Cohen's kappa 0.675. F1 is 0.88 for
  negative and 0.87 for positive but only 0.30 for neutral: mixed reviews are
  often labelled positive.
- Keeping the end of long reviews instead of the start did not change the
  agreement; only 3.6% of those reviews exceed 254 tokens.

## Interpretation limits

- The model was trained on tweets, not product reviews.
- Treat `neutral` counts with caution; positive and negative are reliable
  enough for trends and for finding reviews whose text disagrees with the
  stars.
- The reference labels came from one LLM rater, not from people.
