# Dashboard

`app/dashboard.py` is a thin read-only application over PostgreSQL schema
`serving`. It does not contain Bronze, Silver or Gold transformation logic and
does not query MinIO, Polaris or Iceberg directly.

## Pages and sources

The single page has four tabs:

| Tab | PostgreSQL source | Purpose |
| --- | --- | --- |
| Monthly | `serving.monthly_review_metrics` | review volume, average rating, low-rating and verified-purchase trends |
| Products | `serving.product_review_metrics` | search and rank product-level sample metrics |
| Category & rating | `serving.category_rating_metrics` | inspect rating distribution within one product group |
| Data quality | `serving.data_quality_summary` | inspect accepted statuses and named source issues |

Product search text and filters are passed as SQL parameters. The application
does not concatenate user input into SQL.

## Run and verify

Publish Gold before starting the dashboard:

```powershell
docker compose run --rm spark-pipeline publish
docker compose up -d --build dashboard
```

Open `http://localhost:8501`. The Compose healthcheck uses Streamlit's health
endpoint. The verified 5k run also executed the page with Streamlit AppTest;
it connected to PostgreSQL, rendered four tabs and produced no exception.

## Interpretation boundary

The dashboard always labels the current data as an engineering sample. Review
count is not sales, ratings 1–2 are not a text-sentiment model, and product
price is a metadata snapshot rather than a review-time price. The unmatched
product group is kept visible because the review and metadata first-5,000
samples are independent.

