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
            source_count = outgoing.count()

            write_jdbc(outgoing, target_table)
            published_count = read_jdbc(spark, target_table).count()

            if source_count != published_count:
                raise RuntimeError(
                    f"{target_table}: expected {source_count} rows, "
                    f"published {published_count}"
                )

            print(
                f"Published {source_table} -> {target_table}: "
                f"{published_count} rows"
            )
            outgoing.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
