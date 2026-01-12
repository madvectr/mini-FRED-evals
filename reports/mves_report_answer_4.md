# MVES Report

- Agent: answer_4
- Total cases: 63
- Passed: 54
- Failed: 9
- Pass rate: 85.7%
- Critical failures: 9

## Failure breakdown
- truth_matches: 5
- date_rules: 4

## Example failed cases
- **cpiaucsl_ma_7**
  - Question: What was the 6-period moving average of Consumer Price Index for All Urban Consumers: All Items in August 2018?
  - [critical] truth_matches: Value 251.29833333333332 differs from truth 250.7485 (tol=1e-06).

- **pcepi_ma_3**
  - Question: What was the 5-period moving average of Personal Consumption Expenditures Price Index in September 2023?
  - [critical] truth_matches: Value 120.99866666666667 differs from truth 120.723 (tol=1e-06).

- **unrate_ma_2**
  - Question: What was the 6-period moving average of Civilian Unemployment Rate in July 2021?
  - [critical] truth_matches: Value 5.7 differs from truth 5.916666666666667 (tol=1e-06).

- **cpiaucsl_ma_6**
  - Question: What was the 6-period moving average of Consumer Price Index for All Urban Consumers: All Items in August 2014?
  - [critical] truth_matches: Value 237.39633333333333 differs from truth 236.93383333333335 (tol=1e-06).

- **cpiaucsl_ma_1**
  - Question: What was the 5-period moving average of Consumer Price Index for All Urban Consumers: All Items in February 2001?
  - [critical] truth_matches: Value 175.4 differs from truth 174.85999999999999 (tol=1e-06).
