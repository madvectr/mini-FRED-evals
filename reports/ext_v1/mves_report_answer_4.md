# MVES Report

- Agent: answer_4
- Total cases: 233
- Passed: 218
- Failed: 15
- Pass rate: 93.6%
- Critical failures: 30

## Failure breakdown
- date_rules: 10
- expectation_value_presence: 15
- truth_matches: 15
- window_rules: 5

## Example failed cases
- **UNRATE_point_0__v1**
  - Question: What was Civilian Unemployment Rate in May 2012?
  - [critical] date_rules: date missing for point/yoy/mom/ma transform.
  - [high] expectation_value_presence: Expected a numeric value but got null.
  - [critical] truth_matches: Response missing value despite truth_spec.

- **UNRATE_point_0__v2**
  - Question: Give me Civilian Unemployment Rate for May 2012.?
  - [critical] date_rules: date missing for point/yoy/mom/ma transform.
  - [high] expectation_value_presence: Expected a numeric value but got null.
  - [critical] truth_matches: Response missing value despite truth_spec.

- **UNRATE_mom_1__v1**
  - Question: How large was the MoM change in May 2005 for Civilian Unemployment Rate?
  - [critical] date_rules: date missing for point/yoy/mom/ma transform.
  - [high] expectation_value_presence: Expected a numeric value but got null.
  - [critical] truth_matches: Response missing value despite truth_spec.

- **UNRATE_mom_1__v2**
  - Question: How large was the monthly change in May 2005 for Civilian Unemployment Rate?
  - [critical] date_rules: date missing for point/yoy/mom/ma transform.
  - [high] expectation_value_presence: Expected a numeric value but got null.
  - [critical] truth_matches: Response missing value despite truth_spec.

- **UNRATE_max_2__v1**
  - Question: Tell me the highest reading for Civilian Unemployment Rate between May 2013 and July 2020.?
  - [critical] window_rules: window.start missing for max/min question.
  - [high] expectation_value_presence: Expected a numeric value but got null.
  - [critical] truth_matches: Response missing value despite truth_spec.
