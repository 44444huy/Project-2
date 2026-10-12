from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


REVIEWS_SILVER = "polaris.silver.reviews"
PRODUCTS_SILVER = "polaris.silver.products"
REJECTED_SILVER = "polaris.silver.rejected_rows"
REVIEW_PRODUCT_SILVER = "polaris.silver.review_product"

MONTHLY_GOLD = "polaris.gold.monthly_review_metrics"
PRODUCT_GOLD = "polaris.gold.product_review_metrics"
CATEGORY_GOLD = "polaris.gold.category_rating_metrics"
QUALITY_GOLD = "polaris.gold.data_quality_summary"


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
        (review_count > 0).alias("has_sample_reviews"),
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
            has_sample_reviews BOOLEAN NOT NULL,
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


def overwrite_table(spark, dataframe, table_name, view_name):
    dataframe.createOrReplaceTempView(view_name)
    spark.sql(f"INSERT OVERWRITE {table_name} SELECT * FROM {view_name}")


def main():
    spark = SparkSession.builder.appName("build-product-review-gold").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        reviews = spark.table(REVIEWS_SILVER).cache()
        products = spark.table(PRODUCTS_SILVER).cache()
        rejected = spark.table(REJECTED_SILVER).cache()
        review_product = spark.table(REVIEW_PRODUCT_SILVER).cache()

        monthly = build_monthly_metrics(reviews).cache()
        product = build_product_metrics(reviews, products).cache()
        category = build_category_rating_metrics(review_product).cache()
        quality = build_quality_summary(reviews, products, rejected).cache()

        create_gold_tables(spark)
        overwrite_table(spark, monthly, MONTHLY_GOLD, "incoming_monthly_gold")
        overwrite_table(spark, product, PRODUCT_GOLD, "incoming_product_gold")
        overwrite_table(spark, category, CATEGORY_GOLD, "incoming_category_gold")
        overwrite_table(spark, quality, QUALITY_GOLD, "incoming_quality_gold")

        print("Gold monthly rows:", monthly.count())
        print("Gold product rows:", product.count())
        print("Gold category-rating rows:", category.count())
        print("Gold quality rows:", quality.count())
        product.orderBy(F.desc("review_count"), "parent_asin").show(10, False)
        category.orderBy("product_group", "rating").show(50, False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
