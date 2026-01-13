## Promptfoo Extended Harness

This directory adds a dedicated promptfoo configuration that runs the MVES agent end-to-end via `scripts/answer.py`, validates each response with a deterministic Python assertion, and stores test cases in JSONL for easy growth.

### Prerequisites
- Install the project dependencies (see the repo README) so `python3 scripts/answer.py` works locally.
- Ensure `duckdb` Python bindings are available (needed for truth checks).
- `npx` must be available to download `promptfoo`.

### Running the suite
- From the repo root run `evals/promptfoo_ext/scripts/run.sh`. The script simply shells into `npx promptfoo@latest eval -c evals/promptfoo_ext/promptfooconfig.yaml`, so you can pass any extra promptfoo flags (e.g. `--max-concurrency 2`) after the script name. Promptfoo now invokes `scripts/exec_answer.py`, which determines the repo root at runtime and relays the call to `scripts/answer.py`, so no environment tweaks are required even if you run Promptfoo manually from another directory.
- Choose the agent via the `--agent` flag (defaults to `answer_4`):
  ```bash
  evals/promptfoo_ext/scripts/run.sh --agent answer_2
  ```
- The harness executes `python3 ../../scripts/answer.py` via the Promptfoo `exec` provider for each test row (Promptfoo injects the rendered prompt as the first positional argument) and pipes the JSON output into `assertions/mves_assert.py` for validation.

### Adding or updating test cases
- All cases live in `evals/promptfoo_ext/tests/cases.jsonl`. Append one JSON object per line:
  ```json
  {"description":"unique_id","vars":{"question":"...", "expect":{...}, "truth_spec":{... optional ...}}}
  ```
- `expect.should_have_value` controls whether we expect a refusal or a numeric value. Provide `truth_spec` for answerable cases so the assertion can run DuckDB-based checks via `src.truth`.
- Keep tests deterministic—use concrete dates/windows and explicit tolerances when comparing against the warehouse.

#### Tiering guidance
- **Tier 1: Smoke (fast, every edit)** — target 50–100 lightweight cases that catch parsing/date/transform/citation regressions quickly. These should execute in under a minute so they can run on every PR or local edit loop. Add additional tiers (regression, stress, etc.) later if needed; for now, focus on scaling the smoke suite using the JSONL format above.

### Assertions
- `assertions/mves_assert.py` enforces schema, refusal/value behavior, doc_id/citation rules, and (optionally) numeric truth comparisons. Failures surface directly in the promptfoo report.
- No `avg` transform support yet; we’ll add it later once the agent surface is ready.
