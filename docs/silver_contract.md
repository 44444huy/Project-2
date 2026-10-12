# Silver data contract

Silver is implemented by one direct Spark job:
`src/silver/build_product_reviews_silver.py`.

The job reads the two Bronze Iceberg tables, parses the original JSON with
explicit schemas, applies visible quality rules, and rebuilds four Silver
tables. It uses only PySpark.

## Tables

| Table | Grain and purpose |
| --- | --- |
| `polaris.silver.reviews` | One accepted physical review line per `record_id`. |
| `polaris.silver.products` | One accepted product per `parent_asin`; if duplicates occur, the first source line is kept and later lines are rejected. |
| `polaris.silver.rejected_rows` | Rows that cannot safely enter a clean table, with explicit reasons and original JSON. |
| `polaris.silver.review_product` | One row per accepted review, left-joined to product metadata by `parent_asin`. |

`review_product` is a Silver integration table, not a Gold aggregate. A missing
product match leaves product fields null and sets `product_found = false`; it
does not remove the review.

## Normalization patterns

The job keeps two small transformation patterns explicit in the code:

- trim text and normalize repeated whitespace;
- construct issue arrays with `F.filter(F.array(...))` so null issue entries
  disappear.

The rules remain inline because this dataset does not need a generic registry,
metadata-contract engine, phone/email normalizers, or extra third-party
libraries.

## Review rules

A review is rejected when JSON is malformed, a core ID is blank, rating is not
within 1–5, timestamp is missing/invalid, helpful vote is negative, or verified
purchase is missing.

Blank title/text and whitespace normalization are warnings. Warning rows remain
in `silver.reviews` with `quality_status = 'WARNING'` and named
`quality_issues`.

`timestamp` is interpreted as Unix epoch milliseconds and becomes UTC
`reviewed_at`. The original millisecond value is also retained.

## Product rules

A product is rejected when its JSON is malformed, `parent_asin` or title is
blank, rating fields are invalid, price is negative, or it is a later duplicate
of an already-seen `parent_asin`.

Missing price, category, store, or category list is a warning, not a rejection.
Price is a product-metadata snapshot and is stored as `DECIMAL(12,2)`; it must
not be interpreted as the price at review time.

Regular arrays such as features, description, categories, images and videos
remain typed arrays. Irregular `details`, `author`, and `bought_together`
structures remain JSON strings so Silver does not discard nested values.

## Batch behavior

The two 5k source objects are fixed inputs. Silver therefore uses
`INSERT OVERWRITE` rather than CDC offsets or a run registry. Running the job
again rebuilds the same logical rows instead of appending duplicates.

## Verified sample results

| Check | Result |
| --- | ---: |
| Review rows / distinct IDs | 5,000 / 5,000 |
| Product rows / distinct IDs | 5,000 / 5,000 |
| Rejected rows | 0 |
| Review-product rows | 5,000 |
| Product matches | 40 (0.8%) |
| Review warning rows | 1,792 |
| Product warning rows | 2,939 |

The 40 matches describe two independent first-N engineering samples. They are
not the full dataset's join rate.
