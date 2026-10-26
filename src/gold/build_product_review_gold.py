from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


REVIEWS_SILVER = "polaris.silver.reviews"
PRODUCTS_SILVER = "polaris.silver.products"
REJECTED_SILVER = "polaris.silver.rejected_rows"
REVIEW_PRODUCT_SILVER = "polaris.silver.review_product"
SENTIMENT_SILVER = "polaris.silver.review_sentiment"

MONTHLY_GOLD = "polaris.gold.monthly_review_metrics"
PRODUCT_GOLD = "polaris.gold.product_review_metrics"
PRODUCT_MONTHLY_GOLD = "polaris.gold.product_monthly_review_metrics"
CATEGORY_GOLD = "polaris.gold.category_rating_metrics"
QUALITY_GOLD = "polaris.gold.data_quality_summary"
MONTHLY_SENTIMENT_GOLD = "polaris.gold.monthly_sentiment_metrics"
PRODUCT_SENTIMENT_GOLD = "polaris.gold.product_sentiment_metrics"

SENTIMENT_LABELS = ("positive", "neutral", "negative")
SENTIMENT_COUNT_COLUMNS = [
    "review_count",
    "scored_review_count",
    *[f"{label}_count" for label in SENTIMENT_LABELS],
    "high_rating_negative_count",
    "low_rating_positive_count",
]
SENTIMENT_RATE_COLUMNS = [
    *[f"{label}_rate" for label in SENTIMENT_LABELS],
    "high_rating_negative_rate",
    "low_rating_positive_rate",
]


def safe_rate(numerator, denominator):
    return F.when(
        denominator > 0,
        numerator.cast("double") / denominator,
    ).otherwise(F.lit(0.0))


def build_monthly_metrics(reviews_df):
    return (
        reviews_df
        .withColumn(
            "review_month",
            F.date_trunc("month", "reviewed_at").cast("date"),
        )
        .groupBy("review_month")
        .agg(
            F.count("*").alias("review_count"),
            F.avg("rating").alias("average_rating"),
            F.count_if(F.col("rating") <= 2).alias("low_rating_count"),
            F.count_if("verified_purchase").alias("verified_purchase_count"),
            F.avg("helpful_vote").alias("average_helpful_vote"),
        )
        .withColumn(
            "low_rating_rate",
            safe_rate(F.col("low_rating_count"), F.col("review_count")),
        )
        .withColumn(
            "verified_purchase_rate",
            safe_rate(
                F.col("verified_purchase_count"),
                F.col("review_count"),
            ),
        )
        .withColumn("gold_updated_at", F.current_timestamp())
        .select(
            "review_month",
            "review_count",
            "average_rating",
            "low_rating_count",
            "low_rating_rate",
            "verified_purchase_count",
            "verified_purchase_rate",
            "average_helpful_vote",
            "gold_updated_at",
        )
    )


def build_product_metrics(reviews_df, products_df):
    review_metrics = (
        reviews_df
        .groupBy("parent_asin")
        .agg(
            F.count("*").alias("review_count"),
            F.avg("rating").alias("average_review_rating"),
            F.count_if(F.col("rating") <= 2).alias("low_rating_count"),
            F.count_if("verified_purchase").alias("verified_purchase_count"),
            F.sum("helpful_vote").alias("helpful_vote_total"),
        )
    )

    product_dimension = products_df.select(
        "record_id",
        "parent_asin",
        "title",
        "main_category",
        "store",
        "price",
        "average_rating",
        "rating_number",
        "categories",
        "quality_status",
    )

    joined = review_metrics.alias("review").join(
        product_dimension.alias("product"),
        F.col("review.parent_asin") == F.col("product.parent_asin"),
        "full",
    )

    review_count = F.coalesce(F.col("review.review_count"), F.lit(0))
    low_rating_count = F.coalesce(
        F.col("review.low_rating_count"),
        F.lit(0),
    )
    verified_count = F.coalesce(
        F.col("review.verified_purchase_count"),
        F.lit(0),
    )

    return joined.select(
        F.coalesce(
            F.col("review.parent_asin"),
            F.col("product.parent_asin"),
        ).alias("parent_asin"),
        F.col("product.record_id").alias("product_record_id"),
        F.col("product.title").alias("product_title"),
        F.col("product.main_category").alias("main_category"),
        F.col("product.store").alias("store"),
        F.col("product.price").alias("price"),
        F.col("product.average_rating").alias("metadata_average_rating"),
        F.col("product.rating_number").alias("metadata_rating_number"),
        F.col("product.categories").alias("categories"),
        F.col("product.quality_status").alias("product_quality_status"),
        F.col("product.record_id").isNotNull().alias("product_found"),
        (review_count > 0).alias("has_reviews"),
        review_count.alias("review_count"),
        F.col("review.average_review_rating").alias("average_review_rating"),
        low_rating_count.alias("low_rating_count"),
        safe_rate(low_rating_count, review_count).alias("low_rating_rate"),
        verified_count.alias("verified_purchase_count"),
        safe_rate(verified_count, review_count).alias("verified_purchase_rate"),
        F.coalesce(F.col("review.helpful_vote_total"), F.lit(0)).alias(
            "helpful_vote_total"
        ),
        F.current_timestamp().alias("gold_updated_at"),
    )


def build_product_monthly_metrics(reviews_df):
    grouped = (
        reviews_df
        .withColumn(
            "review_month",
            F.date_trunc("month", "reviewed_at").cast("date"),
        )
        .groupBy("parent_asin", "review_month")
        .agg(
            F.count("*").alias("review_count"),
            F.avg("rating").alias("average_rating"),
            F.count_if(F.col("rating") <= 2).alias("low_rating_count"),
            F.count_if("verified_purchase").alias(
                "verified_purchase_count"
            ),
            *[
                F.count_if(F.col("rating") == rating).alias(
                    f"rating_{rating}_count"
                )
                for rating in range(1, 6)
            ],
        )
    )
    return (
        grouped
        .withColumn(
            "low_rating_rate",
            safe_rate(F.col("low_rating_count"), F.col("review_count")),
        )
        .withColumn(
            "verified_purchase_rate",
            safe_rate(
                F.col("verified_purchase_count"),
                F.col("review_count"),
            ),
        )
        .withColumn("gold_updated_at", F.current_timestamp())
        .select(
            "parent_asin",
            "review_month",
            "review_count",
            "average_rating",
            "low_rating_count",
            "low_rating_rate",
            "verified_purchase_count",
            "verified_purchase_rate",
            *[f"rating_{rating}_count" for rating in range(1, 6)],
            "gold_updated_at",
        )
    )


def build_category_rating_metrics(review_product_df):
    grouped = (
        review_product_df
        .withColumn(
            "product_group",
            F.when(~F.col("product_found"), F.lit("UNMATCHED_PRODUCT"))
            .when(F.col("main_category").isNull(), F.lit("UNKNOWN_CATEGORY"))
            .otherwise(F.col("main_category")),
        )
        .withColumn("rating", F.col("rating").cast("int"))
        .groupBy("product_group", "rating")
        .agg(
            F.count("*").alias("review_count"),
            F.count_if("verified_purchase").alias("verified_purchase_count"),
        )
    )

    group_window = Window.partitionBy("product_group")
    return (
        grouped
        .withColumn(
            "group_review_count",
            F.sum("review_count").over(group_window),
        )
        .withColumn(
            "rating_share_within_group",
            safe_rate(F.col("review_count"), F.col("group_review_count")),
        )
        .withColumn(
            "verified_purchase_rate",
            safe_rate(
                F.col("verified_purchase_count"),
                F.col("review_count"),
            ),
        )
        .withColumn("is_low_rating", F.col("rating") <= 2)
        .withColumn("gold_updated_at", F.current_timestamp())
        .select(
            "product_group",
            "rating",
            "is_low_rating",
            "review_count",
            "group_review_count",
            "rating_share_within_group",
            "verified_purchase_count",
            "verified_purchase_rate",
            "gold_updated_at",
        )
    )


def reviews_with_sentiment(reviews_df, sentiment_df):
    # Left join so a review without a score still counts in review_count;
    # the rates use scored reviews only.
    labels = sentiment_df.select(
        F.col("review_record_id").alias("record_id"),
        "sentiment_label",
    )
    return reviews_df.join(labels, on="record_id", how="left")


def sentiment_aggregations():
    label = F.col("sentiment_label")
    return [
        F.count("*").alias("review_count"),
        F.count(label).alias("scored_review_count"),
        *[
            F.count_if(label == name).alias(f"{name}_count")
            for name in SENTIMENT_LABELS
        ],
        # Text that disagrees with the stars: 4-5 stars but negative text, or
        # 1-2 stars but positive text.
        F.count_if((F.col("rating") >= 4) & (label == "negative")).alias(
            "high_rating_negative_count"
        ),
        F.count_if((F.col("rating") <= 2) & (label == "positive")).alias(
            "low_rating_positive_count"
        ),
        F.avg("rating").alias("average_rating"),
    ]


def with_sentiment_rates(grouped):
    scored = F.col("scored_review_count")
    for name in SENTIMENT_LABELS:
        grouped = grouped.withColumn(
            f"{name}_rate", safe_rate(F.col(f"{name}_count"), scored)
        )
    return (
        grouped
        .withColumn(
            "high_rating_negative_rate",
            safe_rate(F.col("high_rating_negative_count"), scored),
        )
        .withColumn(
            "low_rating_positive_rate",
            safe_rate(F.col("low_rating_positive_count"), scored),
        )
        .withColumn("gold_updated_at", F.current_timestamp())
    )


def build_monthly_sentiment_metrics(reviews_df, sentiment_df):
    grouped = (
        reviews_with_sentiment(reviews_df, sentiment_df)
        .withColumn(
            "review_month",
            F.date_trunc("month", "reviewed_at").cast("date"),
        )
        .groupBy("review_month")
        .agg(*sentiment_aggregations())
    )
    return with_sentiment_rates(grouped).select(
        "review_month",
        *SENTIMENT_COUNT_COLUMNS,
        *SENTIMENT_RATE_COLUMNS,
        "average_rating",
        "gold_updated_at",
    )


def build_product_sentiment_metrics(reviews_df, sentiment_df):
    grouped = (
        reviews_with_sentiment(reviews_df, sentiment_df)
        .groupBy("parent_asin")
        .agg(*sentiment_aggregations())
    )
    return with_sentiment_rates(grouped).select(
        "parent_asin",
        *SENTIMENT_COUNT_COLUMNS,
        *SENTIMENT_RATE_COLUMNS,
        "average_rating",
        "gold_updated_at",
    )


def status_metrics(dataframe, source_name):
    return (
        dataframe
        .groupBy("quality_status")
        .count()
        .select(
            F.lit(source_name).alias("source_name"),
            F.lit("STATUS").alias("metric_type"),
            F.col("quality_status").alias("metric_name"),
            F.col("count").alias("row_count"),
        )
    )


def issue_metrics(dataframe, source_name):
    return (
        dataframe
        .select(F.explode("quality_issues").alias("issue"))
        .groupBy("issue")
        .count()
        .select(
            F.lit(source_name).alias("source_name"),
            F.lit("ISSUE").alias("metric_type"),
            F.col("issue").alias("metric_name"),
            F.col("count").alias("row_count"),
        )
    )


def build_quality_summary(reviews_df, products_df, rejected_df):
    rejected_status = (
        rejected_df
        .groupBy("source_name")
        .count()
        .select(
            "source_name",
            F.lit("STATUS").alias("metric_type"),
            F.lit("REJECTED").alias("metric_name"),
            F.col("count").alias("row_count"),
        )
    )
    rejected_issues = (
        rejected_df
        .select(
            "source_name",
            F.explode("rejection_reasons").alias("issue"),
        )
        .groupBy("source_name", "issue")
        .count()
        .select(
            "source_name",
            F.lit("ISSUE").alias("metric_type"),
            F.col("issue").alias("metric_name"),
            F.col("count").alias("row_count"),
        )
    )

    return (
        status_metrics(reviews_df, "reviews")
        .unionByName(status_metrics(products_df, "products"))
        .unionByName(issue_metrics(reviews_df, "reviews"))
        .unionByName(issue_metrics(products_df, "products"))
        .unionByName(rejected_status)
        .unionByName(rejected_issues)
        .withColumn("gold_updated_at", F.current_timestamp())
    )


def create_gold_tables(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.gold")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {MONTHLY_GOLD} (
            review_month DATE NOT NULL,
            review_count BIGINT NOT NULL,
            average_rating DOUBLE,
            low_rating_count BIGINT NOT NULL,
            low_rating_rate DOUBLE NOT NULL,
            verified_purchase_count BIGINT NOT NULL,
            verified_purchase_rate DOUBLE NOT NULL,
            average_helpful_vote DOUBLE,
            gold_updated_at TIMESTAMP NOT NULL
        ) USING iceberg TBLPROPERTIES ('format-version' = '2')
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {PRODUCT_GOLD} (
            parent_asin STRING NOT NULL,
            product_record_id STRING,
            product_title STRING,
            main_category STRING,
            store STRING,
            price DECIMAL(12,2),
            metadata_average_rating DOUBLE,
            metadata_rating_number BIGINT,
            categories ARRAY<STRING>,
            product_quality_status STRING,
            product_found BOOLEAN NOT NULL,
            has_reviews BOOLEAN NOT NULL,
            review_count BIGINT NOT NULL,
            average_review_rating DOUBLE,
            low_rating_count BIGINT NOT NULL,
            low_rating_rate DOUBLE NOT NULL,
            verified_purchase_count BIGINT NOT NULL,
            verified_purchase_rate DOUBLE NOT NULL,
            helpful_vote_total BIGINT NOT NULL,
            gold_updated_at TIMESTAMP NOT NULL
        ) USING iceberg TBLPROPERTIES ('format-version' = '2')
    """)

    product_columns = set(spark.table(PRODUCT_GOLD).columns)
    if (
        "has_sample_reviews" in product_columns
        and "has_reviews" not in product_columns
    ):
        spark.sql(
            f"ALTER TABLE {PRODUCT_GOLD} "
            "RENAME COLUMN has_sample_reviews TO has_reviews"
        )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {PRODUCT_MONTHLY_GOLD} (
            parent_asin STRING NOT NULL,
            review_month DATE NOT NULL,
            review_count BIGINT NOT NULL,
            average_rating DOUBLE,
            low_rating_count BIGINT NOT NULL,
            low_rating_rate DOUBLE NOT NULL,
            verified_purchase_count BIGINT NOT NULL,
            verified_purchase_rate DOUBLE NOT NULL,
            rating_1_count BIGINT NOT NULL,
            rating_2_count BIGINT NOT NULL,
            rating_3_count BIGINT NOT NULL,
            rating_4_count BIGINT NOT NULL,
            rating_5_count BIGINT NOT NULL,
            gold_updated_at TIMESTAMP NOT NULL
        ) USING iceberg TBLPROPERTIES ('format-version' = '2')
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATEGORY_GOLD} (
            product_group STRING NOT NULL,
            rating INT NOT NULL,
            is_low_rating BOOLEAN NOT NULL,
            review_count BIGINT NOT NULL,
            group_review_count BIGINT NOT NULL,
            rating_share_within_group DOUBLE NOT NULL,
            verified_purchase_count BIGINT NOT NULL,
            verified_purchase_rate DOUBLE NOT NULL,
            gold_updated_at TIMESTAMP NOT NULL
        ) USING iceberg TBLPROPERTIES ('format-version' = '2')
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {QUALITY_GOLD} (
            source_name STRING NOT NULL,
            metric_type STRING NOT NULL,
            metric_name STRING NOT NULL,
            row_count BIGINT NOT NULL,
            gold_updated_at TIMESTAMP NOT NULL
        ) USING iceberg TBLPROPERTIES ('format-version' = '2')
    """)

    sentiment_columns = """
            review_count BIGINT NOT NULL,
            scored_review_count BIGINT NOT NULL,
            positive_count BIGINT NOT NULL,
            neutral_count BIGINT NOT NULL,
            negative_count BIGINT NOT NULL,
            high_rating_negative_count BIGINT NOT NULL,
            low_rating_positive_count BIGINT NOT NULL,
            positive_rate DOUBLE NOT NULL,
            neutral_rate DOUBLE NOT NULL,
            negative_rate DOUBLE NOT NULL,
            high_rating_negative_rate DOUBLE NOT NULL,
            low_rating_positive_rate DOUBLE NOT NULL,
            average_rating DOUBLE,
            gold_updated_at TIMESTAMP NOT NULL
    """

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {MONTHLY_SENTIMENT_GOLD} (
            review_month DATE NOT NULL,
            {sentiment_columns}
        ) USING iceberg TBLPROPERTIES ('format-version' = '2')
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {PRODUCT_SENTIMENT_GOLD} (
            parent_asin STRING NOT NULL,
            {sentiment_columns}
        ) USING iceberg TBLPROPERTIES ('format-version' = '2')
    """)


def overwrite_table(spark, dataframe, table_name, view_name):
    dataframe.createOrReplaceTempView(view_name)
    spark.sql(f"INSERT OVERWRITE {table_name} SELECT * FROM {view_name}")


def main():
    spark = SparkSession.builder.appName("build-product-review-gold").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        reviews = spark.table(REVIEWS_SILVER)
        products = spark.table(PRODUCTS_SILVER)
        rejected = spark.table(REJECTED_SILVER)
        review_product = spark.table(REVIEW_PRODUCT_SILVER)
        sentiment = spark.table(SENTIMENT_SILVER)

        monthly = build_monthly_metrics(reviews)
        product = build_product_metrics(reviews, products)
        product_monthly = build_product_monthly_metrics(reviews)
        category = build_category_rating_metrics(review_product)
        quality = build_quality_summary(reviews, products, rejected)
        monthly_sentiment = build_monthly_sentiment_metrics(reviews, sentiment)
        product_sentiment = build_product_sentiment_metrics(reviews, sentiment)

        create_gold_tables(spark)
        overwrite_table(spark, monthly, MONTHLY_GOLD, "incoming_monthly_gold")
        overwrite_table(spark, product, PRODUCT_GOLD, "incoming_product_gold")
        overwrite_table(
            spark,
            product_monthly,
            PRODUCT_MONTHLY_GOLD,
            "incoming_product_monthly_gold",
        )
        overwrite_table(spark, category, CATEGORY_GOLD, "incoming_category_gold")
        overwrite_table(spark, quality, QUALITY_GOLD, "incoming_quality_gold")
        overwrite_table(
            spark,
            monthly_sentiment,
            MONTHLY_SENTIMENT_GOLD,
            "incoming_monthly_sentiment_gold",
        )
        overwrite_table(
            spark,
            product_sentiment,
            PRODUCT_SENTIMENT_GOLD,
            "incoming_product_sentiment_gold",
        )

        monthly_table = spark.table(MONTHLY_GOLD)
        product_table = spark.table(PRODUCT_GOLD)
        product_monthly_table = spark.table(PRODUCT_MONTHLY_GOLD)
        category_table = spark.table(CATEGORY_GOLD)
        quality_table = spark.table(QUALITY_GOLD)
        monthly_sentiment_table = spark.table(MONTHLY_SENTIMENT_GOLD)
        product_sentiment_table = spark.table(PRODUCT_SENTIMENT_GOLD)

        print("Gold monthly rows:", monthly_table.count())
        print("Gold product rows:", product_table.count())
        print("Gold product-month rows:", product_monthly_table.count())
        print("Gold category-rating rows:", category_table.count())
        print("Gold quality rows:", quality_table.count())
        print("Gold monthly sentiment rows:", monthly_sentiment_table.count())
        print("Gold product sentiment rows:", product_sentiment_table.count())
        monthly_sentiment_table.agg(
            *[F.sum(column).alias(column) for column in SENTIMENT_COUNT_COLUMNS]
        ).show(truncate=False)
        product_table.orderBy(
            F.desc("review_count"), "parent_asin"
        ).show(10, False)
        category_table.orderBy("product_group", "rating").show(50, False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
