import os

import psycopg
import streamlit as st
from psycopg.rows import dict_row


DB_CONFIG = {
    "host": os.environ["POSTGRES_HOST"],
    "port": int(os.environ["POSTGRES_PORT"]),
    "dbname": os.environ["POSTGRES_DB"],
    "user": os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
}
DATA_MODE = os.environ.get("DATA_MODE", "million").strip().lower()
if DATA_MODE not in {"million", "full"}:
    raise ValueError("DATA_MODE must be 'million' or 'full'")
IS_FULL_DATASET = DATA_MODE == "full"


def query_uncached(sql, parameters=()):
    with psycopg.connect(**DB_CONFIG) as connection:
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(sql, parameters)
            return cursor.fetchall()


@st.cache_data(ttl=60)
def query(sql, parameters=(), data_version=None):
    del data_version
    return query_uncached(sql, parameters)


def load_month_bounds():
    return query(
        """
        SELECT MIN(review_month) AS first_month,
               MAX(review_month) AS last_month
        FROM serving.monthly_review_metrics
        """
    )[0]


def load_overview(first_month, last_month):
    return query(
        """
        SELECT
            MIN(review_month) AS first_month,
            MAX(review_month) AS last_month,
            SUM(review_count) AS review_count,
            SUM(low_rating_count) AS low_rating_count,
            SUM(verified_purchase_count) AS verified_purchase_count,
            SUM(average_rating * review_count) / NULLIF(SUM(review_count), 0)
                AS average_rating,
            MAX(gold_updated_at) AS gold_updated_at
        FROM serving.monthly_review_metrics
        WHERE review_month BETWEEN %s AND %s
        """,
        (first_month, last_month),
    )[0]


def load_monthly(first_month, last_month):
    return query(
        """
        SELECT
            review_month,
            review_count,
            average_rating,
            low_rating_count,
            100 * low_rating_rate AS low_rating_percent,
            100 * verified_purchase_rate AS verified_purchase_percent,
            average_helpful_vote
        FROM serving.monthly_review_metrics
        WHERE review_month BETWEEN %s AND %s
        ORDER BY review_month
        """,
        (first_month, last_month),
    )


def load_products(search_text, minimum_reviews, product_scope):
    return query(
        """
        SELECT
            parent_asin,
            product_title,
            main_category,
            price,
            product_found,
            review_count,
            average_review_rating,
            low_rating_count,
            100 * low_rating_rate AS low_rating_percent,
            100 * verified_purchase_rate AS verified_purchase_percent,
            helpful_vote_total
        FROM serving.product_review_metrics
        WHERE review_count >= %s
          AND (
              %s = ''
              OR parent_asin ILIKE '%%' || %s || '%%'
              OR COALESCE(product_title, '') ILIKE '%%' || %s || '%%'
          )
          AND (
              %s = 'All'
              OR (%s = 'Matched metadata' AND product_found)
              OR (%s = 'Review only' AND NOT product_found)
          )
        ORDER BY review_count DESC, parent_asin
        LIMIT 100
        """,
        (
            minimum_reviews,
            search_text,
            search_text,
            search_text,
            product_scope,
            product_scope,
            product_scope,
        ),
    )


def load_product_groups():
    rows = query(
        """
        SELECT DISTINCT product_group
        FROM serving.category_rating_metrics
        ORDER BY product_group
        """
    )
    return [row["product_group"] for row in rows]


def load_category_ratings(product_group):
    return query(
        """
        SELECT
            product_group,
            rating,
            review_count,
            100 * rating_share_within_group AS rating_share_percent,
            100 * verified_purchase_rate AS verified_purchase_percent
        FROM serving.category_rating_metrics
        WHERE product_group = %s
        ORDER BY rating
        """,
        (product_group,),
    )


def load_product_count():
    return query(
        """
        SELECT COUNT(*) AS product_count
        FROM serving.product_review_metrics
        WHERE product_found
        """
    )[0]["product_count"]


def load_rating_distribution(first_month, last_month):
    return query(
        """
        SELECT bucket.rating, SUM(bucket.review_count) AS review_count
        FROM serving.product_monthly_review_metrics AS metric
        CROSS JOIN LATERAL (
            VALUES
                (1, metric.rating_1_count),
                (2, metric.rating_2_count),
                (3, metric.rating_3_count),
                (4, metric.rating_4_count),
                (5, metric.rating_5_count)
        ) AS bucket(rating, review_count)
        WHERE metric.review_month BETWEEN %s AND %s
        GROUP BY bucket.rating
        ORDER BY bucket.rating
        """,
        (first_month, last_month),
    )


def load_product_monthly(parent_asin, first_month, last_month):
    return query(
        """
        SELECT review_month, review_count, average_rating,
               100 * low_rating_rate AS low_rating_percent,
               100 * verified_purchase_rate AS verified_purchase_percent
        FROM serving.product_monthly_review_metrics
        WHERE parent_asin = %s
          AND review_month BETWEEN %s AND %s
        ORDER BY review_month
        """,
        (parent_asin, first_month, last_month),
    )


def load_quality():
    return query(
        """
        SELECT source_name, metric_type, metric_name, row_count
        FROM serving.data_quality_summary
        ORDER BY source_name, metric_type DESC, row_count DESC, metric_name
        """
    )


def load_missing_metadata_reviews():
    return query(
        """
        SELECT
            COALESCE(
                SUM(review_count) FILTER (WHERE NOT product_found),
                0
            ) AS missing_review_count,
            COALESCE(SUM(review_count), 0) AS total_review_count
        FROM serving.product_review_metrics
        """
    )[0]


def load_pipeline_runs():
    return query_uncached(
        """
        SELECT run_id, status, data_mode, review_limit, code_version,
               started_at, published_at, finished_at, duration_seconds,
               table_counts, iceberg_snapshots, airflow_log_url,
               error_message
        FROM serving.pipeline_runs
        ORDER BY started_at DESC
        LIMIT 20
        """
    )


def load_published_run():
    rows = query_uncached(
        """
        SELECT run_id, status, data_mode, code_version, published_at
        FROM serving.pipeline_runs
        WHERE published_at IS NOT NULL
        ORDER BY published_at DESC
        LIMIT 1
        """
    )
    return rows[0] if rows else None


def percentage(numerator, denominator):
    if not denominator:
        return "0.0%"
    return f"{100 * numerator / denominator:.1f}%"


st.set_page_config(
    page_title="Product Review Lakehouse",
    page_icon="📊",
    layout="wide",
)

st.title("Product Review Lakehouse")
if IS_FULL_DATASET:
    st.caption("Electronics · complete source snapshot")
else:
    st.caption(
        "Electronics · 1,000,000 reviews with complete product metadata"
    )

try:
    published_run = load_published_run()
except psycopg.Error as error:
    st.error("The pipeline run history is unavailable.")
    st.caption(str(error))
    st.stop()

published_version = (
    published_run["run_id"] if published_run else "unversioned"
)
if st.session_state.get("published_version") != published_version:
    st.cache_data.clear()
    st.session_state["published_version"] = published_version

try:
    month_bounds = load_month_bounds()
except (psycopg.Error, IndexError) as error:
    st.error(
        "Serving tables are unavailable. Trigger the Airflow DAG and wait "
        "for publication to finish."
    )
    st.caption(str(error))
    st.stop()

available_first_month = month_bounds["first_month"]
available_last_month = month_bounds["last_month"]
if not available_first_month or not available_last_month:
    st.error("The monthly Serving table is empty.")
    st.stop()

with st.sidebar:
    st.header("How to read this dashboard")
    scope_note = (
        "This dashboard uses the complete configured source snapshot. "
        if IS_FULL_DATASET
        else "This dashboard uses a fixed one-million-review processing set. "
    )
    st.info(
        scope_note
        + "Review count is not sales volume, ratings 1–2 are low ratings "
        "rather than text sentiment, and price is the product metadata "
        "snapshot rather than the price at review time."
    )
    selected_period = st.date_input(
        "Observed review period",
        value=(available_first_month, available_last_month),
        min_value=available_first_month,
        max_value=available_last_month,
    )
    if st.button("Refresh data"):
        st.cache_data.clear()
        st.rerun()

if isinstance(selected_period, (tuple, list)) and len(selected_period) == 2:
    selected_first, selected_last = selected_period
else:
    selected_first = available_first_month
    selected_last = available_last_month

selected_first = selected_first.replace(day=1)
selected_last = selected_last.replace(day=1)

try:
    overview = load_overview(selected_first, selected_last)
    monthly = load_monthly(selected_first, selected_last)
    product_count = load_product_count()
    rating_distribution = load_rating_distribution(selected_first, selected_last)
except (psycopg.Error, IndexError) as error:
    st.error("The published Serving or operations tables are unavailable.")
    st.caption(str(error))
    st.stop()


review_count = overview["review_count"] or 0
low_rating_count = overview["low_rating_count"] or 0
verified_count = overview["verified_purchase_count"] or 0

metric_columns = st.columns(5)
metric_columns[0].metric("Reviews", f"{review_count:,}")
metric_columns[1].metric("Products", f"{product_count:,}")
metric_columns[2].metric(
    "Average rating", f"{(overview['average_rating'] or 0):.2f} / 5"
)
metric_columns[3].metric(
    "Low ratings (1–2)",
    f"{low_rating_count:,} ({percentage(low_rating_count, review_count)})",
)
metric_columns[4].metric(
    "Verified purchases",
    f"{verified_count:,} ({percentage(verified_count, review_count)})",
)

st.caption(
    f"Observed months: {overview['first_month']:%Y-%m} to "
    f"{overview['last_month']:%Y-%m} · Gold updated "
    f"{overview['gold_updated_at']:%Y-%m-%d %H:%M:%S}"
)
if published_run:
    st.caption(
        f"Displayed pipeline version: {published_run['run_id']} · "
        f"published {published_run['published_at']:%Y-%m-%d %H:%M:%S}"
    )

monthly_tab, products_tab, categories_tab, quality_tab, operations_tab = st.tabs(
    ["Monthly", "Products", "Category & rating", "Data quality", "Operations"]
)

with monthly_tab:
    st.subheader("Review activity by observed month")
    st.line_chart(
        monthly,
        x="review_month",
        y=["review_count", "low_rating_count"],
    )
    st.subheader("Average rating")
    st.line_chart(monthly, x="review_month", y="average_rating")
    st.subheader("Rating distribution")
    st.bar_chart(rating_distribution, x="rating", y="review_count")
    st.subheader("Low-rating and verified-purchase rates (%)")
    st.line_chart(
        monthly,
        x="review_month",
        y=["low_rating_percent", "verified_purchase_percent"],
    )
    st.dataframe(monthly, width="stretch", hide_index=True)

with products_tab:
    filter_columns = st.columns([2, 1, 1])
    search_text = filter_columns[0].text_input(
        "ASIN or product title",
        placeholder="Search the published Gold table",
    ).strip()
    minimum_reviews = filter_columns[1].number_input(
        "Minimum reviews",
        min_value=0,
        value=1,
        step=1,
    )
    product_scope = filter_columns[2].selectbox(
        "Metadata scope",
        ["All", "Matched metadata", "Review only"],
    )
    products = load_products(search_text, minimum_reviews, product_scope)
    st.caption(f"Showing {len(products)} rows, limited to the first 100 results.")
    st.dataframe(products, width="stretch", hide_index=True)

    if products:
        product_labels = {
            row["parent_asin"]: (
                f"{row['parent_asin']} — "
                f"{(row['product_title'] or 'Untitled product')[:80]}"
            )
            for row in products
        }
        selected_product = st.selectbox(
            "Product trend",
            list(product_labels),
            format_func=lambda value: product_labels[value],
        )
        product_monthly = load_product_monthly(
            selected_product,
            selected_first,
            selected_last,
        )
        if product_monthly:
            st.line_chart(
                product_monthly,
                x="review_month",
                y="review_count",
            )
            st.line_chart(
                product_monthly,
                x="review_month",
                y=[
                    "average_rating",
                    "low_rating_percent",
                    "verified_purchase_percent",
                ],
            )
            st.dataframe(
                product_monthly,
                width="stretch",
                hide_index=True,
            )
        else:
            st.info("No reviews for this product in the selected period.")

with categories_tab:
    product_groups = load_product_groups()
    selected_group = st.selectbox("Product group", product_groups)
    category_ratings = load_category_ratings(selected_group)
    st.bar_chart(category_ratings, x="rating", y="review_count")
    st.dataframe(category_ratings, width="stretch", hide_index=True)
    if selected_group == "UNMATCHED_PRODUCT":
        st.warning(
            "These reviews did not match a product in the complete "
            "metadata source. They are retained rather than silently dropped."
        )

with quality_tab:
    st.subheader("Accepted status and named issues")
    missing_metadata = load_missing_metadata_reviews()
    st.metric(
        "Reviews without matched product metadata",
        (
            f"{missing_metadata['missing_review_count']:,} "
            f"({percentage(missing_metadata['missing_review_count'], missing_metadata['total_review_count'])})"
        ),
    )
    st.caption(
        "Issue counts can overlap because one source row may carry more than "
        "one warning. Do not sum issue rows as a unique-record count."
    )
    st.dataframe(load_quality(), width="stretch", hide_index=True)

with operations_tab:
    st.subheader("Pipeline runs")
    st.link_button("Open Airflow", "http://localhost:8080")
    runs = load_pipeline_runs()
    if not runs:
        st.info("No Airflow pipeline run has been recorded yet.")
    else:
        latest = runs[0]
        missing_metadata = load_missing_metadata_reviews()
        operation_metrics = st.columns(5)
        operation_metrics[0].metric("Latest status", latest["status"])
        operation_metrics[1].metric("Data mode", latest["data_mode"])
        operation_metrics[2].metric(
            "Duration",
            (
                f"{latest['duration_seconds'] / 60:.1f} min"
                if latest["duration_seconds"] is not None
                else "Running"
            ),
        )
        operation_metrics[3].metric("Code version", latest["code_version"])
        operation_metrics[4].metric(
            "Missing metadata",
            (
                f"{missing_metadata['missing_review_count']:,} "
                f"({percentage(missing_metadata['missing_review_count'], missing_metadata['total_review_count'])})"
            ),
        )

        quality_rows = load_quality()
        status_counts = {
            (row["source_name"], row["metric_name"]): row["row_count"]
            for row in quality_rows
            if row["metric_type"] == "STATUS"
        }
        quality_metrics = st.columns(4)
        quality_metrics[0].metric(
            "Accepted reviews",
            f"{status_counts.get(('reviews', 'PASS'), 0) + status_counts.get(('reviews', 'WARNING'), 0):,}",
        )
        quality_metrics[1].metric(
            "Rejected reviews",
            f"{status_counts.get(('reviews', 'REJECTED'), 0):,}",
        )
        quality_metrics[2].metric(
            "Accepted products",
            f"{status_counts.get(('products', 'PASS'), 0) + status_counts.get(('products', 'WARNING'), 0):,}",
        )
        quality_metrics[3].metric(
            "Rejected products",
            f"{status_counts.get(('products', 'REJECTED'), 0):,}",
        )

        st.dataframe(
            [
                {
                    "run_id": row["run_id"],
                    "status": row["status"],
                    "data_mode": row["data_mode"],
                    "review_limit": row["review_limit"],
                    "started_at": row["started_at"],
                    "published_at": row["published_at"],
                    "finished_at": row["finished_at"],
                    "duration_seconds": row["duration_seconds"],
                    "error_message": row["error_message"],
                }
                for row in runs
            ],
            width="stretch",
            hide_index=True,
        )
        if latest["table_counts"]:
            st.subheader("Latest reconciled table counts")
            st.json(latest["table_counts"])
        if latest["iceberg_snapshots"]:
            st.subheader("Latest Iceberg snapshots")
            st.json(latest["iceberg_snapshots"])
