import argparse
import json
import os

from pyspark.sql import SparkSession


ICEBERG_TABLES = [
    "polaris.bronze.reviews_raw",
    "polaris.bronze.products_raw",
    "polaris.silver.reviews",
    "polaris.silver.products",
    "polaris.silver.rejected_rows",
    "polaris.silver.review_product",
    "polaris.silver.review_sentiment",
    "polaris.gold.monthly_review_metrics",
    "polaris.gold.product_review_metrics",
    "polaris.gold.product_monthly_review_metrics",
    "polaris.gold.category_rating_metrics",
    "polaris.gold.data_quality_summary",
    "polaris.gold.monthly_sentiment_metrics",
    "polaris.gold.product_sentiment_metrics",
]

SERVING_TABLES = [
    "serving.monthly_review_metrics",
    "serving.product_review_metrics",
    "serving.product_monthly_review_metrics",
    "serving.category_rating_metrics",
    "serving.data_quality_summary",
    "serving.monthly_sentiment_metrics",
    "serving.product_sentiment_metrics",
]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    return parser.parse_args()


def iceberg_metrics(spark):
    counts = {}
    snapshots = {}
    for table_name in ICEBERG_TABLES:
        counts[table_name] = spark.table(table_name).count()
        current = spark.sql(
            f"""
            SELECT snapshot_id
            FROM {table_name}.history
            WHERE is_current_ancestor
            ORDER BY made_current_at DESC
            LIMIT 1
            """
        ).first()
        snapshots[table_name] = current["snapshot_id"] if current else None
    return counts, snapshots


def open_postgres_connection(spark):
    driver = "org.postgresql.Driver"
    spark._jvm.org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry.register(
        driver
    )
    url = (
        f"jdbc:postgresql://{os.environ['POSTGRES_HOST']}:"
        f"{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
    )
    return spark._jvm.java.sql.DriverManager.getConnection(
        url,
        os.environ["POSTGRES_USER"],
        os.environ["POSTGRES_PASSWORD"],
    )


def serving_counts(connection):
    result = {}
    statement = connection.createStatement()
    try:
        for table_name in SERVING_TABLES:
            rows = statement.executeQuery(f"SELECT COUNT(*) FROM {table_name}")
            rows.next()
            result[table_name] = rows.getLong(1)
            rows.close()
    finally:
        statement.close()
    return result


def mark_success(connection, run_id, table_counts, snapshots):
    sql = """
        UPDATE serving.pipeline_runs
        SET status = 'SUCCESS',
            finished_at = NOW(),
            duration_seconds = EXTRACT(EPOCH FROM (NOW() - started_at)),
            table_counts = ?::jsonb,
            iceberg_snapshots = ?::jsonb,
            error_message = NULL,
            updated_at = NOW()
        WHERE run_id = ?
    """
    statement = connection.prepareStatement(sql)
    try:
        statement.setString(1, json.dumps(table_counts, sort_keys=True))
        statement.setString(2, json.dumps(snapshots, sort_keys=True))
        statement.setString(3, run_id)
        updated = statement.executeUpdate()
        if updated != 1:
            raise RuntimeError(f"No pipeline run row found for {run_id}")
    finally:
        statement.close()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("finalize-pipeline-run").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    connection = None
    try:
        table_counts, snapshots = iceberg_metrics(spark)
        connection = open_postgres_connection(spark)
        table_counts.update(serving_counts(connection))
        mark_success(connection, args.run_id, table_counts, snapshots)
        print(json.dumps({
            "run_id": args.run_id,
            "status": "SUCCESS",
            "table_counts": table_counts,
            "iceberg_snapshots": snapshots,
        }, sort_keys=True))
    finally:
        if connection is not None:
            connection.close()
        spark.stop()


if __name__ == "__main__":
    main()
