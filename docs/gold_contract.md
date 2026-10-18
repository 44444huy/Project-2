# Gold data contract

Gold is implemented by `src/gold/build_product_review_gold.py`. The job reads
only committed Silver tables and rebuilds five Iceberg tables with
`INSERT OVERWRITE`.

## `polaris.gold.monthly_review_metrics`

Grain: one observed review month.

Measures include review count, average rating, low-rating count/rate,
verified-purchase count/rate and average helpful vote. Low rating means rating
1 or 2; it is not text sentiment. Review count is not sales volume.

The verified one-million profile contains 280 observed months. Counts sum to
1,000,000 reviews, including 136,736 low ratings and 903,914 verified
purchases.

## `polaris.gold.product_review_metrics`

Grain: one distinct `parent_asin` found in either reviews or product metadata.

A full join is deliberate:

- review-only products remain visible with `product_found = false`;
- metadata-only products remain visible with `has_reviews = false`;
- matched products carry both review measures and metadata attributes.

`price` remains a metadata-crawl snapshot, not a historical review-time price.
`metadata_average_rating` and `average_review_rating` are kept separate because
they describe different populations.

The verified profile contains 1,609,933 product rows and its review measures
sum to 1,000,000.

## `polaris.gold.product_monthly_review_metrics`

Grain: one `parent_asin` and one observed review month.

This mart supports the product trend view without making the dashboard query
Silver detail. It stores review count, average rating, low-rating and
verified-purchase counts/rates, plus rating 1–5 counts. The rating buckets can
also be summed across products for a time-filtered overview distribution.

The verified one-million profile contains 739,414 product-month rows. Its
review counts and its five rating buckets each reconcile to 1,000,000 reviews.

## `polaris.gold.category_rating_metrics`

Grain: product group and integer rating.

For matched products, product group is `main_category`. Missing metadata is
grouped as `UNMATCHED_PRODUCT`; matched products with null category use
`UNKNOWN_CATEGORY`. Measures include review count, share within group and
verified-purchase rate.

The verified profile contains 177 rows and reconciles to 1,000,000 reviews.

## `polaris.gold.data_quality_summary`

Grain: source, metric type and metric name.

- `STATUS` rows count `PASS`, `WARNING`, and rejected rows.
- `ISSUE` rows count each named issue. One source row may contribute to more
  than one issue, so issue counts are not unique-record counts.

The verified profile contains 14 summary rows. Status totals reconcile to
1,000,000 review rows and 1,610,012 product input rows.

## Reconciliation invariants

- monthly `SUM(review_count) = silver.reviews` count;
- product `SUM(review_count) = silver.reviews` count;
- product-monthly `SUM(review_count) = silver.reviews` count;
- product-monthly rating 1–5 sums equal its review count;
- category-rating `SUM(review_count) = silver.review_product` count;
- product Gold has one row per distinct unioned `parent_asin`;
- rates remain between 0 and 1;
- reruns replace rather than append aggregate rows.
