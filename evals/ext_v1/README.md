## Extended MVES Evaluations (ext_v1)

The `ext_v1` pipeline builds large MVES eval suites automatically:

1. **Truth specs** are deterministically sampled from `data/warehouse.duckdb` using the existing `src.truth` helpers (no manual numbers).
2. **Questions** are produced from templates that mirror each `truth_spec` (no LLMs).
3. **Refusals** are generated from templates for underspecified questions (no DuckDB interaction).
4. **Evalset builder** merges answered + refusal cases into a single JSONL ready for MVES.

Everything is reproducible: every script accepts a `--seed`, logs it, and uses deterministic sampling.

### Default workflow

```bash
# 1) Sample deterministic truth specs (20 per series by default)
python evals/ext_v1/generate_golden_from_duckdb.py \
  --seed 123 --per-series 20

# 2) Expand each spec into multiple phrasing variants (2 per case by default)
python evals/ext_v1/generate_questions.py \
  --seed 123 --variants 2

# 3) Combine answered cases + refusal templates
python evals/ext_v1/build_evalset.py

# 4) Merge spec/verifier overrides and run MVES
python evals/ext_v1/scripts/run_ext_mves.py \
  --agent answer_4 \
  --eval evals/ext_v1/evalset.jsonl
```

### Scaling guidance

- `--per-series` controls how many truth specs we sample per series (shared across transforms). Default `20` keeps the total manageable (~5k cases with two variants). Increase cautiously (e.g., `--per-series 50`, `--variants 3`) as runtime and file size scale roughly linearly.
- Refusals default to ~30 template cases and are regenerated only when the template file changes.
- DuckDB sampling and truth computation typically finish in under a minute for the default settings on a laptop; expect proportionally longer runs as you scale up.

### Specs & verifiers

- Base MVES spec + verifier map stay in `mves/`. We only apply delta overrides:
  - `spec.override.json` relaxes date/window constraints for refusal expectations.
  - `verifiers.override.json` makes `date_rules`, `window_rules`, and `truth_matches` conditional on `expect.should_answer` / `expect.should_have_value`.
- `load_spec.py` deep-merges the base files with overrides and emits JSON (`spec.merged.json`, `verifiers.merged.json`). `run_ext_mves.py` runs this automatically before invoking the main MVES runner.

### Smoke testing

Run the bundled smoke test whenever you tweak the generation scripts:

```bash
python evals/ext_v1/smoke_test_ext_v1.py
```

It builds a tiny eval set (`--per-series 2`, `--variants 1`), validates JSONL structure, verifies that truth specs are computable, and ensures `evalset.jsonl` isn’t empty.
