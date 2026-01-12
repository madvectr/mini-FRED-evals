# MVES Report

- Agent: answer_2
- Total cases: 63
- Passed: 44
- Failed: 19
- Pass rate: 69.8%
- Critical failures: 9

## Failure breakdown
- primary_value_mentioned: 11
- truth_matches: 4
- expectation_transform: 1
- date_rules: 4

## Example failed cases
- **unrate_mom_0**
  - Question: What was the month-over-month change in Civilian Unemployment Rate in January 2015?
  - [medium] primary_value_mentioned: Answer text does not appear to cite value 1.7857142857142954.

- **fedfunds_ma_2**
  - Question: What was the 6-period moving average of Effective Federal Funds Rate in March 2005?
  - [critical] truth_matches: Value 2.47 differs from truth 2.21 (tol=1e-06).

- **cpiaucsl_mom_2**
  - Question: What was the month-over-month change in Consumer Price Index for All Urban Consumers: All Items in April 2022?
  - [medium] primary_value_mentioned: Answer text does not appear to cite value 0.3878706077567196.

- **cpiaucsl_yoy_1**
  - Question: What was the year-over-year change in Consumer Price Index for All Urban Consumers: All Items in January 2004?
  - [medium] primary_value_mentioned: Answer text does not appear to cite value 2.0262869660460114.

- **pcepi_yoy_8**
  - Question: What was the year-over-year change in Personal Consumption Expenditures Price Index in April 2009?
  - [medium] primary_value_mentioned: Answer text does not appear to cite value -0.5675739591662123.
