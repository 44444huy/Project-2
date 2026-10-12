# Gold data contract

Gold is implemented by `src/gold/build_product_review_gold.py`. The job reads
only verified Silver tables and rebuilds four Iceberg tables with
`INSERT OVERWRITE`.

These are provisional models for the first-5,000 engineering milestone. The
table shapes follow the stated analytical questions, but sample values must not
be presented as full-dataset findings.

## `polaris.gold.monthly_review_metrics`

Grain: one observed review month.

Measures include review count, average rating, low-rating count/rate,
verified-purchase count/rate and average helpful vote. Low rating means rating
1 or 2; it is not text sentiment. Review count is not sales volume.

The verified sample has 182 observed months from March 2003 through March 2023.
Counts sum to 5,000 reviews and low-rating counts sum to 591.

## `polaris.gold.product_review_metrics`

Grain: one distinct `parent_asin` found in either the review sample or product
sample.

A full join is deliberate:

- review-only products remain visible with `product_found = false`;
- metadata-only products remain visible with `has_sample_reviews = false`;
- matched products carry both review measures and metadata attributes.

This avoids silently discarding most rows from the two independent samples.
The sample table contains 9,474 distinct products: 5,000 metadata products,
4,509 products represented in reviews, and 35 distinct overlaps. The 40 matched
review rows therefore belong to 35 products.

`price` remains a metadata-crawl snapshot, not a historical review-time price.
`metadata_average_rating` and `average_review_rating` are kept separate because
they describe different populations.

## `polaris.gold.category_rating_metrics`

Grain: product group and integer rating.

For matched products, product group is `main_category`. Missing metadata is
explicitly grouped as `UNMATCHED_PRODUCT`; matched products with null category
use `UNKNOWN_CATEGORY`. Measures include review count, share within group and
verified-purchase rate.

The table's 22 sample rows sum to 5,000 reviews. Most reviews fall under
`UNMATCHED_PRODUCT` because first-N review and product samples are independent.

## `polaris.gold.data_quality_summary`

Grain: source, metric type and metric name.

- `STATUS` rows count accepted `PASS` and `WARNING` rows and any rejected rows.
- `ISSUE` rows count each named issue. One row may contribute to more than one
  issue, so issue counts must not be summed as though they were unique records.

The sample has 12 status/issue rows and no rejected source rows.

## Reconciliation invariants

- monthly `SUM(review_count) = silver.reviews` count;
- product `SUM(review_count) = silver.reviews` count;
- category-rating `SUM(review_count) = silver.review_product` count;
- product Gold has one row per distinct unioned `parent_asin`;
- reruns replace rather than append aggregate rows.
