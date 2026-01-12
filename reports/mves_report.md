# MVES Report

- Total cases: 63
- Passed: 30
- Failed: 33
- Pass rate: 47.6%
- Critical failures: 55

## Failure breakdown
- expectation_transform: 24
- truth_matches: 27
- primary_value_mentioned: 3
- date_rules: 4

## Example failed cases
- **unrate_mom_0**
  - Question: What was the month-over-month change in Civilian Unemployment Rate in January 2015?
  - [critical] expectation_transform: Expected transform mom but got point.
  - [critical] truth_matches: Value 5.7 differs from truth 1.7857142857142954 (tol=1e-06).

- **cpiaucsl_max_1**
  - Question: What was the highest Consumer Price Index for All Urban Consumers: All Items between July 2000 and March 2011?
  - [critical] expectation_transform: Expected transform max but got ma.
  - [critical] truth_matches: Value 172.0333333333333 differs from truth 223.046 (tol=1e-06).

- **fedfunds_ma_2**
  - Question: What was the 6-period moving average of Effective Federal Funds Rate in March 2005?
  - [critical] truth_matches: Value 2.47 differs from truth 2.21 (tol=1e-06).

- **cpiaucsl_mom_2**
  - Question: What was the month-over-month change in Consumer Price Index for All Urban Consumers: All Items in April 2022?
  - [critical] expectation_transform: Expected transform mom but got point.
  - [critical] truth_matches: Value 288.582 differs from truth 0.3878706077567196 (tol=1e-06).

- **cpiaucsl_yoy_1**
  - Question: What was the year-over-year change in Consumer Price Index for All Urban Consumers: All Items in January 2004?
  - [critical] expectation_transform: Expected transform yoy but got point.
  - [critical] truth_matches: Value 186.3 differs from truth 2.0262869660460114 (tol=1e-06).
