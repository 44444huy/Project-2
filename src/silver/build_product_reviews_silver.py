from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window


REVIEWS_BRONZE = "polaris.bronze.reviews_raw"
PRODUCTS_BRONZE = "polaris.bronze.products_raw"

REVIEWS_SILVER = "polaris.silver.reviews"
PRODUCTS_SILVER = "polaris.silver.products"
REJECTED_SILVER = "polaris.silver.rejected_rows"
REVIEW_PRODUCT_SILVER = "polaris.silver.review_product"


REVIEW_IMAGE_SCHEMA = StructType([
    StructField("small_image_url", StringType(), True),
    StructField("medium_image_url", StringType(), True),
    StructField("large_image_url", StringType(), True),
    StructField("attachment_type", StringType(), True),
])

PRODUCT_IMAGE_SCHEMA = StructType([
    StructField("thumb", StringType(), True),
    StructField("large", StringType(), True),
    StructField("variant", StringType(), True),
    StructField("hi_res", StringType(), True),
])

PRODUCT_VIDEO_SCHEMA = StructType([
    StructField("title", StringType(), True),
    StructField("url", StringType(), True),
    StructField("user_id", StringType(), True),
])

REVIEW_SCHEMA = StructType([
    StructField("rating", DoubleType(), True),
    StructField("title", StringType(), True),
    StructField("text", StringType(), True),
    StructField("images", ArrayType(REVIEW_IMAGE_SCHEMA), True),
    StructField("asin", StringType(), True),
    StructField("parent_asin", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("helpful_vote", LongType(), True),
    StructField("verified_purchase", BooleanType(), True),
])

PRODUCT_SCHEMA = StructType([
    StructField("main_category", StringType(), True),
    StructField("title", StringType(), True),
    StructField("average_rating", DoubleType(), True),
    StructField("rating_number", LongType(), True),
    StructField("features", ArrayType(StringType()), True),
    StructField("description", ArrayType(StringType()), True),
    StructField("price", DoubleType(), True),
    StructField("images", ArrayType(PRODUCT_IMAGE_SCHEMA), True),
    StructField("videos", ArrayType(PRODUCT_VIDEO_SCHEMA), True),
    StructField("store", StringType(), True),
    StructField("categories", ArrayType(StringType()), True),
    StructField("parent_asin", StringType(), True),
    StructField("subtitle", StringType(), True),
])


def normalize_text(column, allow_line_breaks=False):
    """Normalize leading, trailing, and repeated whitespace."""
    trimmed = F.regexp_replace(column, r"^\s+|\s+$", "")
    if allow_line_breaks:
        return F.regexp_replace(
            F.regexp_replace(trimmed, r"\r\n?", "\n"),
            r"[^\S\n]+",
            " ",
        )
    return F.regexp_replace(trimmed, r"\s+", " ")


def issue_array(*rules):
    """Build a compact issue array by removing null rule results."""
    return F.filter(
        F.array(*[
            F.when(condition, F.lit(issue))
            for condition, issue in rules
        ]),
        lambda issue: issue.isNotNull(),
    )


def is_blank(column):
    return column.isNull() | (F.trim(column) == F.lit(""))


def normalize_string_array(column):
    return F.transform(column, lambda value: normalize_text(value))


def build_reviews(bronze_df):
    parsed = (
        bronze_df
        .withColumn("data", F.from_json("raw_json", REVIEW_SCHEMA))
        .withColumn("normalized_title", normalize_text(F.col("data.title")))
        .withColumn(
            "normalized_text",
            normalize_text(F.col("data.text"), allow_line_breaks=True),
        )
        .withColumn(
            "reviewed_at",
            F.timestamp_millis(F.col("data.timestamp")),
        )
    )

    rejection_reasons = issue_array(
        (F.col("parse_status") != "parseable", "BRONZE_JSON_MALFORMED"),
        (F.col("data").isNull(), "REVIEW_JSON_SCHEMA_MISMATCH"),
        (is_blank(F.col("data.asin")), "ASIN_MISSING"),
        (is_blank(F.col("data.parent_asin")), "PARENT_ASIN_MISSING"),
        (is_blank(F.col("data.user_id")), "USER_ID_MISSING"),
        (F.col("data.rating").isNull(), "RATING_MISSING"),
        (
            F.col("data.rating").isNotNull()
            & ~F.col("data.rating").between(1.0, 5.0),
            "RATING_OUT_OF_RANGE",
        ),
        (F.col("data.timestamp").isNull(), "TIMESTAMP_MISSING"),
        (
            F.col("data.timestamp").isNotNull()
            & (
                (F.col("data.timestamp") <= 0)
                | (F.col("reviewed_at") > F.current_timestamp())
            ),
            "TIMESTAMP_INVALID",
        ),
        (F.col("data.helpful_vote").isNull(), "HELPFUL_VOTE_MISSING"),
        (
            F.col("data.helpful_vote").isNotNull()
            & (F.col("data.helpful_vote") < 0),
            "HELPFUL_VOTE_NEGATIVE",
        ),
        (
            F.col("data.verified_purchase").isNull(),
            "VERIFIED_PURCHASE_MISSING",
        ),
    )

    warning_reasons = issue_array(
        (is_blank(F.col("data.title")), "TITLE_MISSING_OR_BLANK"),
        (is_blank(F.col("data.text")), "TEXT_MISSING_OR_BLANK"),
        (
            F.col("data.title").isNotNull()
            & (F.col("normalized_title") != F.col("data.title")),
            "TITLE_WHITESPACE_NORMALIZED",
        ),
        (
            F.col("data.text").isNotNull()
            & (F.col("normalized_text") != F.col("data.text")),
            "TEXT_WHITESPACE_NORMALIZED",
        ),
    )

    quality = (
        parsed
        .withColumn("rejection_reasons", rejection_reasons)
        .withColumn("warning_reasons", warning_reasons)
        .withColumn(
            "quality_status",
            F.when(F.size("rejection_reasons") > 0, F.lit("REJECTED"))
            .when(F.size("warning_reasons") > 0, F.lit("WARNING"))
            .otherwise(F.lit("PASS")),
        )
    )

    clean = (
        quality
        .filter(F.col("quality_status") != "REJECTED")
        .select(
            "record_id",
            F.col("data.asin").alias("asin"),
            F.col("data.parent_asin").alias("parent_asin"),
            F.col("data.user_id").alias("user_id"),
            F.col("data.rating").alias("rating"),
            F.col("normalized_title").alias("title"),
            F.col("normalized_text").alias("text"),
            F.col("data.helpful_vote").alias("helpful_vote"),
            F.col("data.verified_purchase").alias("verified_purchase"),
            F.col("data.timestamp").alias("review_timestamp_ms"),
            "reviewed_at",
            F.col("data.images").alias("images"),
            "quality_status",
            F.col("warning_reasons").alias("quality_issues"),
            "source_file_name",
            "source_line_number",
            F.current_timestamp().alias("silver_updated_at"),
        )
    )

    rejected = (
        quality
        .filter(F.col("quality_status") == "REJECTED")
        .select(
            "record_id",
            F.lit("reviews").alias("source_name"),
            "source_file_name",
            "source_line_number",
            "raw_json",
            "rejection_reasons",
            F.current_timestamp().alias("rejected_at"),
        )
    )
    return clean, rejected


def build_products(bronze_df):
    parsed = (
        bronze_df
        .withColumn("data", F.from_json("raw_json", PRODUCT_SCHEMA))
        .withColumn("normalized_title", normalize_text(F.col("data.title")))
        .withColumn(
            "normalized_main_category",
            normalize_text(F.col("data.main_category")),
        )
        .withColumn("normalized_store", normalize_text(F.col("data.store")))
        .withColumn(
            "normalized_subtitle",
            normalize_text(F.col("data.subtitle")),
        )
        .withColumn("features", normalize_string_array(F.col("data.features")))
        .withColumn(
            "description",
            normalize_string_array(F.col("data.description")),
        )
        .withColumn(
            "categories",
            normalize_string_array(F.col("data.categories")),
        )
        .withColumn("details_json", F.get_json_object("raw_json", "$.details"))
        .withColumn("author_json", F.get_json_object("raw_json", "$.author"))
        .withColumn(
            "bought_together_json",
            F.get_json_object("raw_json", "$.bought_together"),
        )
    )

    product_window = Window.partitionBy("data.parent_asin")
    product_order = product_window.orderBy(F.col("source_line_number"))
    ranked = (
        parsed
        .withColumn("duplicate_count", F.count("*").over(product_window))
        .withColumn("duplicate_rank", F.row_number().over(product_order))
    )

    has_product_key = ~is_blank(F.col("data.parent_asin"))
    rejection_reasons = issue_array(
        (F.col("parse_status") != "parseable", "BRONZE_JSON_MALFORMED"),
        (F.col("data").isNull(), "PRODUCT_JSON_SCHEMA_MISMATCH"),
        (~has_product_key, "PARENT_ASIN_MISSING"),
        (is_blank(F.col("data.title")), "PRODUCT_TITLE_MISSING"),
        (
            F.col("data.average_rating").isNotNull()
            & ~F.col("data.average_rating").between(0.0, 5.0),
            "AVERAGE_RATING_OUT_OF_RANGE",
        ),
        (
            F.col("data.rating_number").isNotNull()
            & (F.col("data.rating_number") < 0),
            "RATING_NUMBER_NEGATIVE",
        ),
        (
            F.col("data.price").isNotNull()
            & (F.col("data.price") < 0),
            "PRICE_NEGATIVE",
        ),
        (
            has_product_key
            & (F.col("duplicate_count") > 1)
            & (F.col("duplicate_rank") > 1),
            "DUPLICATE_PARENT_ASIN",
        ),
    )

    warning_reasons = issue_array(
        (F.col("data.main_category").isNull(), "MAIN_CATEGORY_MISSING"),
        (F.col("data.store").isNull(), "STORE_MISSING"),
        (F.col("data.price").isNull(), "PRICE_MISSING"),
        (
            F.col("data.categories").isNull()
            | (F.size(F.col("data.categories")) == 0),
            "CATEGORIES_EMPTY",
        ),
        (
            F.col("data.title").isNotNull()
            & (F.col("normalized_title") != F.col("data.title")),
            "TITLE_WHITESPACE_NORMALIZED",
        ),
        (
            has_product_key
            & (F.col("duplicate_count") > 1)
            & (F.col("duplicate_rank") == 1),
            "DUPLICATE_PARENT_ASIN_FIRST_ROW_KEPT",
        ),
    )

    quality = (
        ranked
        .withColumn("rejection_reasons", rejection_reasons)
        .withColumn("warning_reasons", warning_reasons)
        .withColumn(
            "quality_status",
            F.when(F.size("rejection_reasons") > 0, F.lit("REJECTED"))
            .when(F.size("warning_reasons") > 0, F.lit("WARNING"))
            .otherwise(F.lit("PASS")),
        )
    )

    clean = (
        quality
        .filter(F.col("quality_status") != "REJECTED")
        .select(
            "record_id",
            F.col("data.parent_asin").alias("parent_asin"),
            F.col("normalized_main_category").alias("main_category"),
            F.col("normalized_title").alias("title"),
            F.col("data.average_rating").alias("average_rating"),
            F.col("data.rating_number").alias("rating_number"),
            "features",
            "description",
            F.col("data.price").cast("decimal(12,2)").alias("price"),
            F.col("data.images").alias("images"),
            F.col("data.videos").alias("videos"),
            F.col("normalized_store").alias("store"),
            "categories",
            "details_json",
            "author_json",
            "bought_together_json",
            F.col("normalized_subtitle").alias("subtitle"),
            "quality_status",
            F.col("warning_reasons").alias("quality_issues"),
            "source_file_name",
            "source_line_number",
            F.current_timestamp().alias("silver_updated_at"),
        )
    )

    rejected = (
        quality
        .filter(F.col("quality_status") == "REJECTED")
        .select(
            "record_id",
            F.lit("products").alias("source_name"),
            "source_file_name",
            "source_line_number",
            "raw_json",
            "rejection_reasons",
            F.current_timestamp().alias("rejected_at"),
        )
    )
    return clean, rejected


def build_review_product(reviews_df, products_df):
    return (
        reviews_df.alias("review")
        .join(
            products_df.alias("product"),
            F.col("review.parent_asin") == F.col("product.parent_asin"),
            "left",
        )
        .select(
            F.col("review.record_id").alias("review_record_id"),
            F.col("review.asin").alias("asin"),
            F.col("review.parent_asin").alias("parent_asin"),
            F.col("review.user_id").alias("user_id"),
            F.col("review.rating").alias("rating"),
            F.col("review.title").alias("review_title"),
            F.col("review.text").alias("review_text"),
            F.col("review.helpful_vote").alias("helpful_vote"),
            F.col("review.verified_purchase").alias("verified_purchase"),
            F.col("review.reviewed_at").alias("reviewed_at"),
            F.col("product.record_id").alias("product_record_id"),
            F.col("product.title").alias("product_title"),
            F.col("product.main_category").alias("main_category"),
            F.col("product.store").alias("store"),
            F.col("product.price").alias("price"),
            F.col("product.categories").alias("categories"),
            F.col("product.record_id").isNotNull().alias("product_found"),
            F.col("review.quality_status").alias("review_quality_status"),
            F.col("product.quality_status").alias("product_quality_status"),
            F.current_timestamp().alias("silver_updated_at"),
        )
    )


def create_silver_tables(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.silver")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {REVIEWS_SILVER} (
            record_id STRING NOT NULL,
            asin STRING NOT NULL,
            parent_asin STRING NOT NULL,
            user_id STRING NOT NULL,
            rating DOUBLE NOT NULL,
            title STRING,
            text STRING,
            helpful_vote BIGINT NOT NULL,
            verified_purchase BOOLEAN NOT NULL,
            review_timestamp_ms BIGINT NOT NULL,
            reviewed_at TIMESTAMP NOT NULL,
            images ARRAY<STRUCT<
                small_image_url: STRING,
                medium_image_url: STRING,
                large_image_url: STRING,
                attachment_type: STRING
            >>,
            quality_status STRING NOT NULL,
            quality_issues ARRAY<STRING> NOT NULL,
            source_file_name STRING NOT NULL,
            source_line_number BIGINT NOT NULL,
            silver_updated_at TIMESTAMP NOT NULL
        )
        USING iceberg
        PARTITIONED BY (months(reviewed_at))
        TBLPROPERTIES ('format-version' = '2')
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {PRODUCTS_SILVER} (
            record_id STRING NOT NULL,
            parent_asin STRING NOT NULL,
            main_category STRING,
            title STRING NOT NULL,
            average_rating DOUBLE,
            rating_number BIGINT,
            features ARRAY<STRING>,
            description ARRAY<STRING>,
            price DECIMAL(12,2),
            images ARRAY<STRUCT<
                thumb: STRING,
                large: STRING,
                variant: STRING,
                hi_res: STRING
            >>,
            videos ARRAY<STRUCT<title: STRING, url: STRING, user_id: STRING>>,
            store STRING,
            categories ARRAY<STRING>,
            details_json STRING,
            author_json STRING,
            bought_together_json STRING,
            subtitle STRING,
            quality_status STRING NOT NULL,
            quality_issues ARRAY<STRING> NOT NULL,
            source_file_name STRING NOT NULL,
            source_line_number BIGINT NOT NULL,
            silver_updated_at TIMESTAMP NOT NULL
        )
        USING iceberg
        TBLPROPERTIES ('format-version' = '2')
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {REJECTED_SILVER} (
            record_id STRING NOT NULL,
            source_name STRING NOT NULL,
            source_file_name STRING NOT NULL,
            source_line_number BIGINT NOT NULL,
            raw_json STRING NOT NULL,
            rejection_reasons ARRAY<STRING> NOT NULL,
            rejected_at TIMESTAMP NOT NULL
        )
        USING iceberg
        TBLPROPERTIES ('format-version' = '2')
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {REVIEW_PRODUCT_SILVER} (
            review_record_id STRING NOT NULL,
            asin STRING NOT NULL,
            parent_asin STRING NOT NULL,
            user_id STRING NOT NULL,
            rating DOUBLE NOT NULL,
            review_title STRING,
            review_text STRING,
            helpful_vote BIGINT NOT NULL,
            verified_purchase BOOLEAN NOT NULL,
            reviewed_at TIMESTAMP NOT NULL,
            product_record_id STRING,
            product_title STRING,
            main_category STRING,
            store STRING,
            price DECIMAL(12,2),
            categories ARRAY<STRING>,
            product_found BOOLEAN NOT NULL,
            review_quality_status STRING NOT NULL,
            product_quality_status STRING,
            silver_updated_at TIMESTAMP NOT NULL
        )
        USING iceberg
        PARTITIONED BY (months(reviewed_at))
        TBLPROPERTIES ('format-version' = '2')
    """)


def overwrite_table(spark, dataframe, table_name, view_name):
    dataframe.createOrReplaceTempView(view_name)
    spark.sql(f"INSERT OVERWRITE {table_name} SELECT * FROM {view_name}")


def main():
    spark = SparkSession.builder.appName("build-product-reviews-silver").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        reviews, review_rejected = build_reviews(
            spark.table(REVIEWS_BRONZE)
        )
        products, product_rejected = build_products(
            spark.table(PRODUCTS_BRONZE)
        )

        rejected = review_rejected.unionByName(product_rejected)

        create_silver_tables(spark)
        overwrite_table(spark, reviews, REVIEWS_SILVER, "incoming_reviews")
        overwrite_table(spark, products, PRODUCTS_SILVER, "incoming_products")
        overwrite_table(spark, rejected, REJECTED_SILVER, "incoming_rejected")
        review_product = build_review_product(reviews, products)
        overwrite_table(
            spark,
            review_product,
            REVIEW_PRODUCT_SILVER,
            "incoming_review_product",
        )

        reviews_table = spark.table(REVIEWS_SILVER)
        products_table = spark.table(PRODUCTS_SILVER)
        rejected_table = spark.table(REJECTED_SILVER)
        review_product_table = spark.table(REVIEW_PRODUCT_SILVER)

        review_count = reviews_table.count()
        product_count = products_table.count()
        rejected_count = rejected_table.count()
        joined_count = review_product_table.count()
        matched_count = review_product_table.filter("product_found").count()

        print("Silver reviews:", review_count)
        print("Silver products:", product_count)
        print("Silver rejected rows:", rejected_count)
        print("Silver review-product rows:", joined_count)
        print("Reviews matched to product metadata:", matched_count)

        reviews_table.groupBy("quality_status").count().show()
        products_table.groupBy("quality_status").count().show()
        rejected_table.groupBy("source_name").count().show()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
