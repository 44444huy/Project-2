# Initial 5k data findings

These findings describe the first 5,000 physical lines from each source. The
files are processed by the Compose Raw-to-Bronze pipeline under the
`first_n_lines: 5000` scope, but the observations below come from the initial
source survey. They are not representative or synchronized samples of the
complete Electronics dataset.

## Reviews

- All 5,000 lines are JSON objects and reach Bronze as `parseable`.
- `timestamp` values are 13-digit Unix epoch milliseconds.
- The observed UTC range is 2003-03-06 01:21:05 through
  2023-03-16 20:47:13.396.
- Ratings are all in 1-5: 395 one-star, 196 two-star, 326 three-star,
  812 four-star, and 3,271 five-star reviews.
- No negative `helpful_vote` was observed.
- Core review fields were present and non-null, but two `text` values contained
  whitespace only.
- Candidate review keys based on user, ASIN, timestamp, or full-row content had
  no duplicates in this sample. This does not establish a dataset-wide review
  primary key.

## Products

- All 5,000 lines are JSON objects and reach Bronze as `parseable`.
- All 5,000 `parent_asin` values were unique in this sample.
- `price` was null in 2,862 rows (57.24%).
- `main_category` was null in 53 rows and `categories` was empty in 362 rows.
- `price` is a product-metadata crawl snapshot, not the price when a review was
  posted or a purchase occurred.

## Independent-sample join

Only 40 of the 5,000 sampled reviews matched the independently sampled product
metadata by `parent_asin` (0.8%). This must not be reported as full-source join
coverage because the two first-N samples are neither random nor aligned. Join
coverage must be recomputed after complete Silver inputs are available.

## Consequences for later layers

- Bronze retains source fidelity and technical parse status; it must not infer
  business validity from this sample.
- Silver converts epoch milliseconds, normalizes text, retains missing price
  as a warning, reflects price snapshot semantics, and reports join coverage.
- The current Gold tables are provisional 5k models derived after Silver
  profiling. Their group coverage and interpretation must be revalidated on
  complete Silver data rather than fixed from the 0.8% sample join.
- Review count is not sales volume, and ratings of 1-2 are not a text-sentiment
  model.
