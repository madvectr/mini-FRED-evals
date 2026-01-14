## Extended MVES Evaluations (ext_v2)

`ext_v2` is a tougher follow-on to `ext_v1`. It keeps the deterministic generation flow but biases toward the transforms and phrasing that historically tripped earlier agents:

1. **Truth specs** favor change-style transforms (MoM/YoY/MA) and wide extrema windows (36 requests per series by default, weighted toward non-point queries).
2. **Questions** use noisier templates, extra prefixes/suffixes, and rolling-average synonyms to stress the parser.
3. **Refusals** include vaguer multi-condition prompts (missing transforms, conflicting ranges) so we can spot hallucinated answers.
4. **Evalset builder** merges the answered + refusal sets just like ext_v1.

All steps are reproducible; every script takes a seed.

### Default workflow

```bash
# 1) Sample deterministic truth specs with heavier non-point coverage
python evals/ext_v2/generate_golden_from_duckdb.py \
  --seed 123 --per-series 36

# 2) Expand each spec into noisier question variants (3 per case by default)
python evals/ext_v2/generate_questions.py \
  --seed 123 --variants 3

# 3) Combine answered cases + tougher refusal templates
python evals/ext_v2/build_evalset.py

# 4) Merge spec/verifier overrides and run MVES
python evals/ext_v2/scripts/run_ext_mves.py \
  --agent answer_4 \
  --eval evals/ext_v2/evalset.jsonl
```

### Scaling guidance

- Raising `--per-series` increases the share of long-window extrema questions very quickly; keep an eye on runtime.
- Use `--variants` to dial up/down the amount of phrasing noise.
- Refusals are template driven; edit `refusals.jsonl` if you need different ambiguity styles.

### Specs & verifiers

- `spec.override.json` keeps the refusal relaxations from ext_v1 and adds commentary explaining the tougher expectations.
- `verifiers.override.json` mirrors the base map but can selectively disable/enable checks if we need to focus on certain behaviors.
- `load_spec.py` (copied locally for convenience) deep-merges overrides with the base spec + verifier map; the run script executes it automatically.

### Smoke testing

```bash
python evals/ext_v2/smoke_test_ext_v2.py
```

The smoke test samples a tiny eval set (`--per-series 2`, `--variants 1`), validates JSONL structure, and ensures we can compute every `truth_spec` before running full suites.
