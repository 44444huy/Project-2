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


@st.cache_data(ttl=60)
def query(sql, parameters=()):
    with psycopg.connect(**DB_CONFIG) as connection:
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(sql, parameters)
            return cursor.fetchall()


def load_overview():
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
        """
    )[0]


def load_monthly():
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
        ORDER BY review_month
        """
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


def load_quality():
    return query(
        """
        SELECT source_name, metric_type, metric_name, row_count
        FROM serving.data_quality_summary
        ORDER BY source_name, metric_type DESC, row_count DESC, metric_name
        """
    )


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
st.caption("Electronics · first 5,000 review rows and 5,000 product rows")

with st.sidebar:
    st.header("How to read this dashboard")
    st.info(
        "This is an engineering sample. Review count is not sales volume, "
        "ratings 1–2 are low ratings rather than text sentiment, and price is "
        "the product metadata snapshot rather than the price at review time."
    )
    if st.button("Refresh data"):
        st.cache_data.clear()
        st.rerun()

try:
    overview = load_overview()
    monthly = load_monthly()
except (psycopg.Error, IndexError) as error:
    st.error(
        "Serving tables are unavailable. Run "
        "'docker compose run --rm spark-pipeline publish' before starting "
        "the dashboard."
    )
    st.caption(str(error))
    st.stop()

review_count = overview["review_count"] or 0
low_rating_count = overview["low_rating_count"] or 0
verified_count = overview["verified_purchase_count"] or 0

metric_columns = st.columns(4)
metric_columns[0].metric("Reviews", f"{review_count:,}")
metric_columns[1].metric("Average rating", f"{overview['average_rating']:.2f} / 5")
metric_columns[2].metric(
    "Low ratings (1–2)",
    f"{low_rating_count:,} ({percentage(low_rating_count, review_count)})",
)
metric_columns[3].metric(
    "Verified purchases",
    f"{verified_count:,} ({percentage(verified_count, review_count)})",
)

st.caption(
    f"Observed months: {overview['first_month']:%Y-%m} to "
    f"{overview['last_month']:%Y-%m} · Gold updated "
    f"{overview['gold_updated_at']:%Y-%m-%d %H:%M:%S}"
)

monthly_tab, products_tab, categories_tab, quality_tab = st.tabs(
    ["Monthly", "Products", "Category & rating", "Data quality"]
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
        "Minimum sample reviews",
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

with categories_tab:
    product_groups = load_product_groups()
    selected_group = st.selectbox("Product group", product_groups)
    category_ratings = load_category_ratings(selected_group)
    st.bar_chart(category_ratings, x="rating", y="review_count")
    st.dataframe(category_ratings, width="stretch", hide_index=True)
    if selected_group == "UNMATCHED_PRODUCT":
        st.warning(
            "These reviews did not match the independent first-5,000 product "
            "metadata sample. They are retained rather than silently dropped."
        )

with quality_tab:
    st.subheader("Accepted status and named issues")
    st.caption(
        "Issue counts can overlap because one source row may carry more than "
        "one warning. Do not sum issue rows as a unique-record count."
    )
    st.dataframe(load_quality(), width="stretch", hide_index=True)
