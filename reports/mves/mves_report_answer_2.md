# MVES Report

- Agent: answer_2
- Total cases: 63
- Passed: 62
- Failed: 1
- Pass rate: 98.4%
- Critical failures: 2

## Failure breakdown
- window_rules: 2

## Example failed cases
- **refusal_vague_range**
  - Question: How high did it go between 2010 and 2012?
  - [critical] window_rules: window.start missing for max/min question.
  - [critical] window_rules: window.end missing for max/min question.
