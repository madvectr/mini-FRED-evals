# MVES Report

- Agent: answer_1
- Total cases: 63
- Passed: 52
- Failed: 11
- Pass rate: 82.5%
- Critical failures: 22

## Failure breakdown
- expectation_transform: 10
- truth_matches: 10
- window_rules: 2

## Example failed cases
- **cpiaucsl_mom_9**
  - Question: What was the month-over-month change in Consumer Price Index for All Urban Consumers: All Items in February 2013?
  - [critical] expectation_transform: Expected transform mom but got point.
  - [critical] truth_matches: Value 232.937 differs from truth 0.542992675210101 (tol=1e-06).

- **cpiaucsl_mom_6**
  - Question: What was the month-over-month change in Consumer Price Index for All Urban Consumers: All Items in June 2017?
  - [critical] expectation_transform: Expected transform mom but got point.
  - [critical] truth_matches: Value 244.163 differs from truth 0.0651628661825299 (tol=1e-06).

- **cpiaucsl_mom_2**
  - Question: What was the month-over-month change in Consumer Price Index for All Urban Consumers: All Items in September 2019?
  - [critical] expectation_transform: Expected transform mom but got point.
  - [critical] truth_matches: Value 256.43 differs from truth 0.15388460997672415 (tol=1e-06).

- **fedfunds_mom_0**
  - Question: What was the month-over-month change in Effective Federal Funds Rate in June 2016?
  - [critical] expectation_transform: Expected transform mom but got point.
  - [critical] truth_matches: Value 0.38 differs from truth 2.7027027027027053 (tol=1e-06).

- **unrate_mom_3**
  - Question: What was the month-over-month change in Civilian Unemployment Rate in February 2021?
  - [critical] expectation_transform: Expected transform mom but got point.
  - [critical] truth_matches: Value 6.2 differs from truth -3.1250000000000027 (tol=1e-06).
