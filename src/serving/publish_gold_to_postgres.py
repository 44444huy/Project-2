import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


JDBC_URL = (
    f"jdbc:postgresql://{os.environ['POSTGRES_HOST']}:"
    f"{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
)
JDBC_OPTIONS = {
    "user": os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
    "driver": "org.postgresql.Driver",
}

TABLES = {
    "polaris.gold.monthly_review_metrics": "serving.monthly_review_metrics",
    "polaris.gold.product_review_metrics": "serving.product_review_metrics",
    "polaris.gold.category_rating_metrics": "serving.category_rating_metrics",
    "polaris.gold.data_quality_summary": "serving.data_quality_summary",
}

RECONCILIATION_RULES = {
    "polaris.gold.monthly_review_metrics": {
        "key_columns": ["review_month"],
        "sum_columns": [
            "review_count",
            "low_rating_count",
            "verified_purchase_count",
        ],
    },
    "polaris.gold.product_review_metrics": {
        "key_columns": ["parent_asin"],
        "sum_columns": [
            "review_count",
            "low_rating_count",
            "verified_purchase_count",
            "helpful_vote_total",
        ],
    },
    "polaris.gold.category_rating_metrics": {
        "key_columns": ["product_group", "rating"],
        "sum_columns": ["review_count", "verified_purchase_count"],
    },
    "polaris.gold.data_quality_summary": {
        "key_columns": ["source_name", "metric_type", "metric_name"],
        "sum_columns": ["row_count"],
    },
}


def prepare_for_postgres(dataframe, source_table):
    if source_table == "polaris.gold.product_review_metrics":
        return dataframe.select(
            *[
                F.to_json("categories").alias("categories_json")
                if column == "categories"
                else F.col(column)
                for column in dataframe.columns
            ]
        )
    return dataframe


def reconciliation_metrics(dataframe, key_columns, sum_columns):
    null_key = F.lit(False)
    for column in key_columns:
        null_key = null_key | F.col(column).isNull()

    expressions = [
        F.count("*").alias("row_count"),
        F.countDistinct(*key_columns).alias("distinct_key_count"),
        F.sum(F.when(null_key, 1).otherwise(0)).alias("null_key_rows"),
    ]
    expressions.extend(
        F.coalesce(F.sum(column), F.lit(0)).alias(f"{column}_sum")
        for column in sum_columns
    )
    return dataframe.agg(*expressions).first().asDict()


def assert_reconciled(source, published, target_table, rules):
    source_metrics = reconciliation_metrics(source, **rules)
    published_metrics = reconciliation_metrics(published, **rules)

    for label, metrics in (
        ("Gold source", source_metrics),
        ("PostgreSQL target", published_metrics),
    ):
        if metrics["null_key_rows"] != 0:
            raise RuntimeError(
                f"{target_table}: {label} has "
                f"{metrics['null_key_rows']} rows with a null grain key"
            )
        if metrics["distinct_key_count"] != metrics["row_count"]:
            raise RuntimeError(
                f"{target_table}: {label} grain key is not unique; "
                f"{metrics['distinct_key_count']} distinct keys for "
                f"{metrics['row_count']} rows"
            )

    if source_metrics != published_metrics:
        differences = {
            name: (source_metrics[name], published_metrics[name])
            for name in source_metrics
            if source_metrics[name] != published_metrics[name]
        }
        raise RuntimeError(
            f"{target_table}: reconciliation mismatch "
            f"(Gold, PostgreSQL) = {differences}"
        )

    return published_metrics


def write_jdbc(dataframe, target_table):
    (
        dataframe.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", target_table)
        .option("user", JDBC_OPTIONS["user"])
        .option("password", JDBC_OPTIONS["password"])
        .option("driver", JDBC_OPTIONS["driver"])
        .mode("overwrite")
        .save()
    )


def read_jdbc(spark, target_table):
    return (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", target_table)
        .option("user", JDBC_OPTIONS["user"])
        .option("password", JDBC_OPTIONS["password"])
        .option("driver", JDBC_OPTIONS["driver"])
        .load()
    )


def main():
    spark = SparkSession.builder.appName("publish-gold-to-postgres").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        for source_table, target_table in TABLES.items():
            outgoing = prepare_for_postgres(
                spark.table(source_table),
                source_table,
            ).cache()
            write_jdbc(outgoing, target_table)
            published = read_jdbc(spark, target_table)
            metrics = assert_reconciled(
                outgoing,
                published,
                target_table,
                RECONCILIATION_RULES[source_table],
            )

            checked_sums = ", ".join(
                f"{name}={value}"
                for name, value in metrics.items()
                if name.endswith("_sum")
            )
            print(
                f"Published {source_table} -> {target_table}: "
                f"{metrics['row_count']} rows; {checked_sums}"
            )
            outgoing.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
