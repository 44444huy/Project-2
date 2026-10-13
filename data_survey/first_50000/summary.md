# Amazon Electronics: first 50,000 rows profiling

Samples contain the first N lines, not a random sample.
Missing/null percentages use valid JSON objects as denominator.
Independent samples cannot measure full-dataset join coverage.

## reviews

Scanned: 50000; valid: 50000; invalid: 0.

| Field | Types | Missing | Null | Empty | Missing/null % |
| --- | --- | ---: | ---: | ---: | ---: |
| asin | str: 50000 | 0 | 0 | 0 | 0.0 |
| helpful_vote | int: 50000 | 0 | 0 | 0 | 0.0 |
| images | list: 50000 | 0 | 0 | 45780 | 0.0 |
| parent_asin | str: 50000 | 0 | 0 | 0 | 0.0 |
| rating | float: 50000 | 0 | 0 | 0 | 0.0 |
| text | str: 50000 | 0 | 0 | 0 | 0.0 |
| timestamp | int: 50000 | 0 | 0 | 0 | 0.0 |
| title | str: 50000 | 0 | 0 | 0 | 0.0 |
| user_id | str: 50000 | 0 | 0 | 0 | 0.0 |
| verified_purchase | bool: 50000 | 0 | 0 | 0 | 0.0 |

## products

Scanned: 50000; valid: 50000; invalid: 0.

| Field | Types | Missing | Null | Empty | Missing/null % |
| --- | --- | ---: | ---: | ---: | ---: |
| author | NoneType: 9, dict: 11 | 49980 | 9 | 0 | 99.98 |
| average_rating | float: 50000 | 0 | 0 | 0 | 0.0 |
| bought_together | NoneType: 50000 | 0 | 50000 | 0 | 100.0 |
| categories | list: 50000 | 0 | 0 | 3647 | 0.0 |
| description | list: 50000 | 0 | 0 | 20986 | 0.0 |
| details | dict: 50000 | 0 | 0 | 127 | 0.0 |
| features | list: 50000 | 0 | 0 | 10999 | 0.0 |
| images | list: 50000 | 0 | 0 | 4 | 0.0 |
| main_category | str: 49428, NoneType: 572 | 0 | 572 | 0 | 1.14 |
| parent_asin | str: 50000 | 0 | 0 | 0 | 0.0 |
| price | NoneType: 28588, float: 21412 | 0 | 28588 | 0 | 57.18 |
| rating_number | int: 50000 | 0 | 0 | 0 | 0.0 |
| store | str: 49725, NoneType: 275 | 0 | 275 | 1 | 0.55 |
| subtitle | str: 13, NoneType: 7 | 49980 | 7 | 0 | 99.97 |
| title | str: 50000 | 0 | 0 | 0 | 0.0 |
| videos | list: 50000 | 0 | 0 | 28982 | 0.0 |
