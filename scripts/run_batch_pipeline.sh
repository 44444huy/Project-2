#!/bin/sh

set -eu

SPARK_SUBMIT_BIN="${SPARK_SUBMIT_BIN:-/opt/spark/bin/spark-submit}"
PIPELINE_MAX_ATTEMPTS="${PIPELINE_MAX_ATTEMPTS:-2}"
PIPELINE_RETRY_DELAY_SECONDS="${PIPELINE_RETRY_DELAY_SECONDS:-5}"

require_non_negative_integer() {
  name="$1"
  value="$2"

  case "$value" in
    ''|*[!0-9]*)
      echo "$name must be a non-negative integer" >&2
      exit 2
      ;;
  esac
}

require_non_negative_integer "PIPELINE_MAX_ATTEMPTS" "$PIPELINE_MAX_ATTEMPTS"
require_non_negative_integer \
  "PIPELINE_RETRY_DELAY_SECONDS" \
  "$PIPELINE_RETRY_DELAY_SECONDS"

if [ "$PIPELINE_MAX_ATTEMPTS" -lt 1 ]; then
  echo "PIPELINE_MAX_ATTEMPTS must be at least 1" >&2
  exit 2
fi

if [ "$#" -eq 0 ]; then
  set -- bronze silver gold publish
fi

run_stage() {
  stage="$1"

  case "$stage" in
    bronze)
      job="/opt/project/src/bronze/load_product_reviews_bronze.py"
      ;;
    silver)
      job="/opt/project/src/silver/build_product_reviews_silver.py"
      ;;
    gold)
      job="/opt/project/src/gold/build_product_review_gold.py"
      ;;
    publish)
      job="/opt/project/src/serving/publish_gold_to_postgres.py"
      ;;
    *)
      echo "Unknown pipeline stage: $stage" >&2
      echo "Valid stages: bronze silver gold publish" >&2
      exit 2
      ;;
  esac

  attempt=1
  while [ "$attempt" -le "$PIPELINE_MAX_ATTEMPTS" ]; do
    echo "=== Starting $stage (attempt $attempt/$PIPELINE_MAX_ATTEMPTS) ==="
    if "$SPARK_SUBMIT_BIN" "$job"; then
      echo "=== Completed $stage ==="
      return 0
    else
      exit_code="$?"
    fi

    if [ "$attempt" -ge "$PIPELINE_MAX_ATTEMPTS" ]; then
      echo \
        "=== Failed $stage after $attempt attempt(s); exit code $exit_code ===" \
        >&2
      return "$exit_code"
    fi

    echo \
      "=== $stage failed with exit code $exit_code; retrying in ${PIPELINE_RETRY_DELAY_SECONDS}s ===" \
      >&2
    if [ "$PIPELINE_RETRY_DELAY_SECONDS" -gt 0 ]; then
      sleep "$PIPELINE_RETRY_DELAY_SECONDS"
    fi
    attempt=$((attempt + 1))
  done
}

for stage in "$@"; do
  run_stage "$stage"
done