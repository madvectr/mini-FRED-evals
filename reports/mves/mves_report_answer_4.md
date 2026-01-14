# MVES Report

- Agent: answer_4
- Total cases: 63
- Passed: 61
- Failed: 2
- Pass rate: 96.8%
- Critical failures: 4

## Failure breakdown
- date_rules: 1
- expectation_value_presence: 1
- truth_matches: 1
- window_rules: 2

## Example failed cases
- **fedfunds_yoy_7**
  - Question: What was the year-over-year change in Effective Federal Funds Rate in May 2023?
  - [critical] date_rules: date missing for point/yoy/mom/ma transform.
  - [high] expectation_value_presence: Expected a numeric value but got null.
  - [critical] truth_matches: Response missing value despite truth_spec.

- **refusal_vague_range**
  - Question: How high did it go between 2010 and 2012?
  - [critical] window_rules: window.start missing for max/min question.
  - [critical] window_rules: window.end missing for max/min question.
