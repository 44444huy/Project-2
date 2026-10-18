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
    "polaris.gold.product_monthly_review_metrics": (
        "serving.product_monthly_review_metrics"
    ),
    "polaris.gold.category_rating_metrics": "serving.category_rating_metrics",
    "polaris.gold.data_quality_summary": "serving.data_quality_summary",
}

PUBLISH_LOCK_SQL = (
    "SELECT pg_advisory_lock(hashtext('product-review-serving-publish'))"
)
PUBLISH_UNLOCK_SQL = (
    "SELECT pg_advisory_unlock(hashtext('product-review-serving-publish'))"
)

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
    "polaris.gold.product_monthly_review_metrics": {
        "key_columns": ["parent_asin", "review_month"],
        "sum_columns": [
            "review_count",
            "low_rating_count",
            "verified_purchase_count",
            "rating_1_count",
            "rating_2_count",
            "rating_3_count",
            "rating_4_count",
            "rating_5_count",
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
        ("PostgreSQL staging", published_metrics),
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
            f"(Gold, PostgreSQL staging) = {differences}"
        )

    return published_metrics


def qualified_table_parts(table_name):
    parts = table_name.split(".")
    if len(parts) != 2 or any(not part for part in parts):
        raise ValueError(f"Expected schema.table, received: {table_name}")
    return parts


def quote_identifier(identifier):
    return '"' + identifier.replace('"', '""') + '"'


def quote_table(table_name):
    return ".".join(
        quote_identifier(part) for part in qualified_table_parts(table_name)
    )


def staging_table_name(target_table):
    schema, table = qualified_table_parts(target_table)
    staging = f"{table}__staging"
    if len(staging) > 63:
        raise ValueError(f"PostgreSQL staging identifier is too long: {staging}")
    return f"{schema}.{staging}"


def atomic_swap_statements(staged_tables):
    statements = []
    previous_tables = []

    for target_table, staging_table in staged_tables.items():
        target_schema, target_name = qualified_table_parts(target_table)
        staging_schema, staging_name = qualified_table_parts(staging_table)
        if target_schema != staging_schema:
            raise ValueError("Staging and target tables must use the same schema")

        previous_name = f"{target_name}__previous"
        if len(previous_name) > 63:
            raise ValueError(
                f"PostgreSQL previous identifier is too long: {previous_name}"
            )
        previous_table = f"{target_schema}.{previous_name}"
        previous_tables.append(previous_table)

        statements.extend([
            f"DROP TABLE IF EXISTS {quote_table(previous_table)}",
            f"ALTER TABLE IF EXISTS {quote_table(target_table)} "
            f"RENAME TO {quote_identifier(previous_name)}",
            f"ALTER TABLE {quote_table(staging_table)} "
            f"RENAME TO {quote_identifier(target_name)}",
        ])

    statements.extend(
        f"DROP TABLE IF EXISTS {quote_table(previous_table)}"
        for previous_table in previous_tables
    )
    return statements


def staging_optimization_statements(staged_tables):
    monthly = quote_table(staged_tables["serving.monthly_review_metrics"])
    product = quote_table(staged_tables["serving.product_review_metrics"])
    product_monthly = quote_table(
        staged_tables["serving.product_monthly_review_metrics"]
    )
    category = quote_table(staged_tables["serving.category_rating_metrics"])
    quality = quote_table(staged_tables["serving.data_quality_summary"])

    statements = [
        "CREATE EXTENSION IF NOT EXISTS pg_trgm",
        f"CREATE UNIQUE INDEX ON {monthly} (review_month)",
        f"CREATE UNIQUE INDEX ON {product} (parent_asin)",
        f"CREATE INDEX ON {product} (review_count DESC, parent_asin)",
        f"CREATE INDEX ON {product} USING GIN (parent_asin gin_trgm_ops)",
        f"CREATE INDEX ON {product} USING GIN (product_title gin_trgm_ops)",
        f"CREATE UNIQUE INDEX ON {product_monthly} "
        "(parent_asin, review_month)",
        f"CREATE UNIQUE INDEX ON {category} (product_group, rating)",
        f"CREATE UNIQUE INDEX ON {quality} "
        "(source_name, metric_type, metric_name)",
    ]
    statements.extend(
        f"ANALYZE {table_name}"
        for table_name in (
            monthly,
            product,
            product_monthly,
            category,
            quality,
        )
    )
    return statements


def execute_statements(connection, statements):
    statement = connection.createStatement()
    try:
        for sql in statements:
            statement.execute(sql)
    finally:
        statement.close()


def execute_transaction(connection, statements):
    connection.setAutoCommit(False)
    try:
        execute_statements(connection, statements)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.setAutoCommit(True)




def execute_publish_transaction(connection, statements, run_id=None):
    connection.setAutoCommit(False)
    try:
        execute_statements(connection, statements)
        if run_id:
            prepared = connection.prepareStatement(
                """
                UPDATE serving.pipeline_runs
                SET status = 'PUBLISHED',
                    published_at = NOW(),
                    updated_at = NOW()
                WHERE run_id = ?
                """
            )
            try:
                prepared.setString(1, run_id)
                if prepared.executeUpdate() != 1:
                    raise RuntimeError(
                        f"No pipeline run row found for {run_id}"
                    )
            finally:
                prepared.close()
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.setAutoCommit(True)

def open_postgres_connection(spark):
    spark._jvm.org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry.register(
        JDBC_OPTIONS["driver"]
    )
    return spark._jvm.java.sql.DriverManager.getConnection(
        JDBC_URL,
        JDBC_OPTIONS["user"],
        JDBC_OPTIONS["password"],
    )


def write_jdbc(dataframe, target_table):
    (
        dataframe.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", target_table)
        .option("user", JDBC_OPTIONS["user"])
        .option("password", JDBC_OPTIONS["password"])
        .option("driver", JDBC_OPTIONS["driver"])
        .option("batchsize", "5000")
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
        .option("fetchsize", "10000")
        .load()
    )


def metric_summary(metrics):
    return ", ".join(
        f"{name}={value}"
        for name, value in metrics.items()
        if name.endswith("_sum")
    )


def main():
    spark = SparkSession.builder.appName("publish-gold-to-postgres").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    staged_tables = {
        target_table: staging_table_name(target_table)
        for target_table in TABLES.values()
    }
    connection = None

    try:
        connection = open_postgres_connection(spark)
        execute_statements(connection, [PUBLISH_LOCK_SQL])

        staged_metrics = {}
        for source_table, target_table in TABLES.items():
            staging_table = staged_tables[target_table]
            outgoing = prepare_for_postgres(
                spark.table(source_table),
                source_table,
            ).cache()
            try:
                write_jdbc(outgoing, staging_table)
                staging = read_jdbc(spark, staging_table)
                metrics = assert_reconciled(
                    outgoing,
                    staging,
                    target_table,
                    RECONCILIATION_RULES[source_table],
                )
                staged_metrics[target_table] = metrics
                print(
                    f"Staged {source_table} -> {staging_table}: "
                    f"{metrics['row_count']} rows; {metric_summary(metrics)}"
                )
            finally:
                outgoing.unpersist()

        execute_statements(
            connection,
            staging_optimization_statements(staged_tables),
        )
        execute_publish_transaction(
            connection,
            atomic_swap_statements(staged_tables),
            os.environ.get("ORCHESTRATION_RUN_ID"),
        )

        for source_table, target_table in TABLES.items():
            metrics = staged_metrics[target_table]
            print(
                f"Published {source_table} -> {target_table}: "
                f"{metrics['row_count']} rows; {metric_summary(metrics)}"
            )
    finally:
        if connection is not None:
            try:
                execute_transaction(
                    connection,
                    [
                        f"DROP TABLE IF EXISTS {quote_table(staging_table)}"
                        for staging_table in staged_tables.values()
                    ],
                )
            except Exception as cleanup_error:
                print(f"WARNING: failed to clean staging tables: {cleanup_error}")
            try:
                execute_statements(connection, [PUBLISH_UNLOCK_SQL])
            except Exception as unlock_error:
                print(f"WARNING: failed to release publish lock: {unlock_error}")
            connection.close()
        spark.stop()


if __name__ == "__main__":
    main()
