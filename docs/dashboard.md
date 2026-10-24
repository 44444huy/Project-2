# Dashboard

`app/dashboard.py` is a thin read-only application over PostgreSQL schema
`serving`. It does not contain Bronze, Silver or Gold transformation logic and
does not query MinIO, Polaris or Iceberg directly.

## Pages and sources

The single page has six tabs. A global observed-month range filters the
overview and monthly series without changing the published dataset version.

| Tab | PostgreSQL source | Purpose |
| --- | --- | --- |
| Monthly | monthly and product-monthly Serving marts | overview counts, time-filtered review trends and rating distribution |
| Products | product and product-monthly Serving marts | search/rank products and inspect the selected product trend by month |
| Category & rating | `serving.category_rating_metrics` | inspect rating distribution within one product group |
| Sentiment | `serving.monthly_sentiment_metrics`, `serving.product_sentiment_metrics` (joined to the monthly and product marts) | label shares, positive/negative trend, negative text vs 1–2 stars, text that disagrees with the stars, products with the most negative text |
| Data quality | `serving.data_quality_summary` | inspect accepted statuses and named source issues |
| Operations | `serving.pipeline_runs`, `serving.schema_drift_events`, `serving.quality_guard_checks` | inspect the latest run status, duration, code version, Raw schema gate and quality guard results, reconciled counts and Iceberg snapshots |

The overview includes review and product counts. Its observed-month filter also
controls monthly metrics, rating distribution and the selected product trend.

Product search text and filters are passed as SQL parameters. The application
does not concatenate user input into SQL.

## Run and verify

Start the dashboard, trigger the Airflow DAG, and wait for it to finish:

```powershell
docker compose up -d --build dashboard
```

Open Airflow at `http://localhost:8080` to trigger
`product_review_lakehouse`, then open `http://localhost:8501`. The Compose
healthcheck uses Streamlit's health endpoint. The verified one-million run
returned `200 ok`, connected to the published PostgreSQL tables, and exposed
all five dashboard tabs.

## Interpretation boundary

The dashboard labels one-million and full profiles separately. It also
shows the run ID and publication time of the Serving version currently being
displayed. This remains distinct from the latest pipeline attempt: a failed or
still-running attempt does not replace the last fully published tables. A run
stopped by the Raw schema gate is shown as `SCHEMA_DRIFT_BLOCKED` with the held
changes, and a run stopped by the quality guard as `QUALITY_GUARD_BLOCKED` with
the blocked checks; in both cases the displayed tables stay on the last
published version.

Review count is not sales, ratings 1–2 are not a text-sentiment model, and
product price is a metadata snapshot rather than a review-time price. The
Sentiment tab states the model's measured agreement and that `neutral` is
weak; the product ranking uses the whole published dataset, not the selected
period. The
unmatched-product count and group remain visible instead of silently discarding
reviews without matched product metadata.
