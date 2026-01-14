# MVES Report

- Agent: answer_4
- Total cases: 560
- Passed: 374
- Failed: 186
- Pass rate: 66.8%
- Critical failures: 394

## Failure breakdown
- expectation_transform: 120
- truth_matches: 176
- date_rules: 16
- expectation_value_presence: 78
- window_rules: 4

## Example failed cases
- **CPIAUCSL_yoy_0__v3**
  - Question: Operationally speaking, Quantify the annual swing on 2008-01 for Consumer Price Index for All Urban Consumers: All Items. Stick to the factual value.?
  - [critical] expectation_transform: Expected transform yoy but got point.
  - [critical] truth_matches: Value 212.174 differs from truth 4.29469565516597 (tol=1e-06).

- **CPIAUCSL_yoy_1__v3**
  - Question: How large was the annual swing in 2004-03 for Consumer Price Index for All Urban Consumers: All Items? Stick to the factual value.?
  - [critical] expectation_transform: Expected transform yoy but got point.
  - [critical] truth_matches: Value 187.1 differs from truth 1.740076128330608 (tol=1e-06).

- **CPIAUCSL_yoy_3__v1**
  - Question: Quick check: Provide the annual swing of Consumer Price Index for All Urban Consumers: All Items for 2011-04. Keep it numeric.?
  - [critical] expectation_transform: Expected transform yoy but got point.
  - [critical] truth_matches: Value 224.093 differs from truth 3.07723444478687 (tol=1e-06).

- **CPIAUCSL_yoy_3__v2**
  - Question: Quantify the annual swing on Apr 2011 for Consumer Price Index for All Urban Consumers: All Items.?
  - [critical] expectation_transform: Expected transform yoy but got point.
  - [critical] truth_matches: Value 224.093 differs from truth 3.07723444478687 (tol=1e-06).

- **CPIAUCSL_yoy_4__v2**
  - Question: Operationally speaking, What was the year-over-year change for Consumer Price Index for All Urban Consumers: All Items in May 2013? Answer precisely.?
  - [critical] date_rules: date missing for point/yoy/mom/ma transform.
  - [critical] expectation_value_presence: Expected a numeric value but got null.
  - [critical] truth_matches: Response missing value despite truth_spec.
