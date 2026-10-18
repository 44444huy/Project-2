import json
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType


CONFIG_PATH = "/opt/project/config/dataset.json"
RAW_BUCKET = "lakehouse"
DEFAULT_DATA_MODE = "million"
MILLION_REVIEW_ROWS = 1_000_000


def configured_data_mode(value=None):
    data_mode = (
        value or os.environ.get("DATA_MODE", DEFAULT_DATA_MODE)
    ).strip().lower()
    if data_mode not in {"million", "full"}:
        raise ValueError("DATA_MODE must be 'million' or 'full'")
    return data_mode


DATA_MODE = configured_data_mode()
PIPELINE_RUN_ID = os.environ.get("ORCHESTRATION_RUN_ID") or (
    "full-source-batch"
    if DATA_MODE == "full"
    else "first-1000000-reviews-batch"
)

TABLES = {
    "reviews": "polaris.bronze.reviews_raw",
    "products": "polaris.bronze.products_raw",
}


def load_dataset_config(path=CONFIG_PATH):
    with open(path, encoding="utf-8") as config_file:
        config = json.load(config_file)

    sources = {source["name"]: source for source in config["sources"]}
    if set(sources) != set(TABLES):
        raise ValueError("Dataset config must contain reviews and products")
    return config, sources


def raw_path(dataset_id, source, data_mode=None):
    mode = configured_data_mode(data_mode)
    if mode == "full":
        return (
            f"s3a://{RAW_BUCKET}/raw/{dataset_id}/full/"
            f"{source['name']}/part-*.jsonl.gz"
        )
    if mode == "million":
        source_pattern = (
            "part-0000[01].jsonl.gz"
            if source["name"] == "reviews"
            else "part-*.jsonl.gz"
        )
        return (
            f"s3a://{RAW_BUCKET}/raw/{dataset_id}/full/"
            f"{source['name']}/{source_pattern}"
        )


def build_bronze_dataframe(
    spark,
    dataset,
    source,
    input_path,
    max_rows=None,
):
    indexed_lines = spark.sparkContext.textFile(str(input_path)).zipWithIndex()
    if max_rows is not None:
        indexed_lines = indexed_lines.filter(lambda item: item[1] < max_rows)
    rows = indexed_lines.map(lambda item: (item[0], item[1] + 1))
    input_schema = StructType([
        StructField("raw_json", StringType(), False),
        StructField("source_line_number", LongType(), False),
    ])

    dataframe = spark.createDataFrame(rows, input_schema)
    trimmed_json = F.trim(F.col("raw_json"))
    is_json_object = (
        F.get_json_object(F.col("raw_json"), "$").isNotNull()
        & trimmed_json.startswith("{")
        & trimmed_json.endswith("}")
    )
    record_key = F.to_json(F.array(
        F.lit(dataset["dataset_id"]),
        F.lit(source["name"]),
        F.lit(source["file_name"]),
        F.col("source_line_number").cast("string"),
    ))

    return dataframe.select(
        F.sha2(record_key, 256).alias("record_id"),
        F.lit(dataset["dataset_id"]).alias("dataset_id"),
        F.lit(source["name"]).alias("source_name"),
        F.lit(source["file_name"]).alias("source_file_name"),
        "source_line_number",
        F.lit(source["url"]).alias("source_url"),
        F.lit(dataset["source_reference"]).alias("source_reference"),
        F.lit(PIPELINE_RUN_ID).alias("first_pipeline_run_id"),
        F.current_timestamp().alias("first_ingested_at_utc"),
        "raw_json",
        F.when(is_json_object, F.lit("parseable"))
        .otherwise(F.lit("malformed"))
        .alias("parse_status"),
        F.when(is_json_object, F.lit(None).cast("string"))
        .otherwise(F.lit("not_valid_json_object"))
        .alias("parse_error"),
    )


def ensure_bronze_table(spark, table_name):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.bronze")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            record_id STRING NOT NULL,
            dataset_id STRING NOT NULL,
            source_name STRING NOT NULL,
            source_file_name STRING NOT NULL,
            source_line_number BIGINT NOT NULL,
            source_url STRING NOT NULL,
            source_reference STRING NOT NULL,
            first_pipeline_run_id STRING NOT NULL,
            first_ingested_at_utc TIMESTAMP NOT NULL,
            raw_json STRING NOT NULL,
            parse_status STRING NOT NULL,
            parse_error STRING
        )
        USING iceberg
        TBLPROPERTIES (
            'format-version' = '2',
            'write.format.default' = 'parquet'
        )
    """)


def merge_bronze(spark, dataframe, table_name, source_name):
    view_name = f"incoming_{source_name}_bronze"
    dataframe.createOrReplaceTempView(view_name)
    spark.sql(f"""
        MERGE INTO {table_name} AS target
        USING {view_name} AS incoming
        ON target.record_id = incoming.record_id
        WHEN NOT MATCHED THEN INSERT *
    """)


def main():
    dataset, sources = load_dataset_config()
    spark = (
        SparkSession.builder
        .appName("load-product-reviews-bronze")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        for source_name in ("reviews", "products"):
            source = sources[source_name]
            input_path = raw_path(dataset["dataset_id"], source)
            table_name = TABLES[source_name]
            dataframe = build_bronze_dataframe(
                spark,
                dataset,
                source,
                input_path,
                max_rows=(
                    MILLION_REVIEW_ROWS
                    if DATA_MODE == "million" and source_name == "reviews"
                    else None
                ),
            )

            input_metrics = dataframe.agg(
                F.count("*").alias("input_rows"),
                F.count_if(
                    F.col("parse_status") == "parseable"
                ).alias("parseable_rows"),
            ).first()
            input_rows = input_metrics["input_rows"]
            parseable_rows = input_metrics["parseable_rows"]
            malformed_rows = input_rows - parseable_rows

            if (
                DATA_MODE == "million"
                and source_name == "reviews"
                and input_rows != MILLION_REVIEW_ROWS
            ):
                raise RuntimeError(
                    f"reviews: expected {MILLION_REVIEW_ROWS} rows, "
                    f"got {input_rows}"
                )
            if input_rows == 0:
                raise RuntimeError(
                    f"{source_name}: {DATA_MODE} input is empty"
                )

            ensure_bronze_table(spark, table_name)
            merge_bronze(spark, dataframe, table_name, source_name)

            table_rows = (
                spark.table(table_name)
                .filter(F.col("dataset_id") == dataset["dataset_id"])
                .count()
            )
            if table_rows != input_rows:
                raise RuntimeError(
                    f"{source_name}: {DATA_MODE} Bronze has {table_rows} rows, "
                    f"expected exactly {input_rows}"
                )
            print(json.dumps({
                "source": source_name,
                "data_mode": DATA_MODE,
                "raw_path": input_path,
                "table": table_name,
                "input_rows": input_rows,
                "parseable_rows": parseable_rows,
                "malformed_rows": malformed_rows,
                "table_rows": table_rows,
            }))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
