# Amazon Electronics: initial data profiling

Samples contain the first N lines, not a random sample.
Missing/null percentages use valid JSON objects as denominator.
Independent samples cannot measure full-dataset join coverage.

## reviews

Scanned: 5000; valid: 5000; invalid: 0.

| Field | Types | Missing | Null | Empty | Missing/null % |
| --- | --- | ---: | ---: | ---: | ---: |
| asin | str: 5000 | 0 | 0 | 0 | 0.0 |
| helpful_vote | int: 5000 | 0 | 0 | 0 | 0.0 |
| images | list: 5000 | 0 | 0 | 4801 | 0.0 |
| parent_asin | str: 5000 | 0 | 0 | 0 | 0.0 |
| rating | float: 5000 | 0 | 0 | 0 | 0.0 |
| text | str: 5000 | 0 | 0 | 0 | 0.0 |
| timestamp | int: 5000 | 0 | 0 | 0 | 0.0 |
| title | str: 5000 | 0 | 0 | 0 | 0.0 |
| user_id | str: 5000 | 0 | 0 | 0 | 0.0 |
| verified_purchase | bool: 5000 | 0 | 0 | 0 | 0.0 |

## products

Scanned: 5000; valid: 5000; invalid: 0.

| Field | Types | Missing | Null | Empty | Missing/null % |
| --- | --- | ---: | ---: | ---: | ---: |
| author | NoneType: 2, dict: 2 | 4996 | 2 | 0 | 99.96 |
| average_rating | float: 5000 | 0 | 0 | 0 | 0.0 |
| bought_together | NoneType: 5000 | 0 | 5000 | 0 | 100.0 |
| categories | list: 5000 | 0 | 0 | 362 | 0.0 |
| description | list: 5000 | 0 | 0 | 2160 | 0.0 |
| details | dict: 5000 | 0 | 0 | 12 | 0.0 |
| features | list: 5000 | 0 | 0 | 1075 | 0.0 |
| images | list: 5000 | 0 | 0 | 0 | 0.0 |
| main_category | str: 4947, NoneType: 53 | 0 | 53 | 0 | 1.06 |
| parent_asin | str: 5000 | 0 | 0 | 0 | 0.0 |
| price | NoneType: 2862, float: 2138 | 0 | 2862 | 0 | 57.24 |
| rating_number | int: 5000 | 0 | 0 | 0 | 0.0 |
| store | str: 4965, NoneType: 35 | 0 | 35 | 0 | 0.7 |
| subtitle | str: 3, NoneType: 1 | 4996 | 1 | 0 | 99.94 |
| title | str: 5000 | 0 | 0 | 0 | 0.0 |
| videos | list: 5000 | 0 | 0 | 2881 | 0.0 |
