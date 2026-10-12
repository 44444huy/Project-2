#!/bin/sh

set -eu

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

  echo "=== Starting $stage ==="
  /opt/spark/bin/spark-submit "$job"
  echo "=== Completed $stage ==="
}

for stage in "$@"; do
  run_stage "$stage"
done
