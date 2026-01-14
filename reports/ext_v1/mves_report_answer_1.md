# MVES Report

- Agent: answer_1
- Total cases: 233
- Passed: 226
- Failed: 7
- Pass rate: 97.0%
- Critical failures: 14

## Failure breakdown
- expectation_transform: 7
- truth_matches: 7

## Example failed cases
- **UNRATE_mom_0__v1**
  - Question: How large was the month-over-month change in 2022-02 for Civilian Unemployment Rate?
  - [critical] expectation_transform: Expected transform mom but got point.
  - [critical] truth_matches: Value 3.9 differs from truth -2.500000000000002 (tol=1e-06).

- **UNRATE_mom_2__v1**
  - Question: What was the month-over-month change for Civilian Unemployment Rate in 2024-02?
  - [critical] expectation_transform: Expected transform mom but got point.
  - [critical] truth_matches: Value 3.9 differs from truth 5.405405405405398 (tol=1e-06).

- **FEDFUNDS_mom_1__v2**
  - Question: Provide the month-over-month change of Effective Federal Funds Rate for October 2022.?
  - [critical] expectation_transform: Expected transform mom but got point.
  - [critical] truth_matches: Value 3.08 differs from truth 20.3125 (tol=1e-06).

- **PCEPI_mom_1__v1**
  - Question: What was the month-over-month change for Personal Consumption Expenditures Price Index in 2015-12?
  - [critical] expectation_transform: Expected transform mom but got point.
  - [critical] truth_matches: Value 97.456 differs from truth -0.08406979843752552 (tol=1e-06).

- **PCEPI_mom_2__v1**
  - Question: Provide the month-over-month change of Personal Consumption Expenditures Price Index for March 2005.?
  - [critical] expectation_transform: Expected transform mom but got point.
  - [critical] truth_matches: Value 81.375 differs from truth 0.29951190652269755 (tol=1e-06).
