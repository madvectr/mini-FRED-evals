## Mini-FRED Warehouse

Mini-FRED is a deterministic DuckDB snapshot containing a handful of macro series (CPIAUCSL, UNRATE, FEDFUNDS, PCEPI, GDPC1) curated for MVES specifications and promptfoo-based regressions. Everything runs locally—data, truth logic, and evaluation harnesses—so once you ingest snapshots you can test agents offline.

### Prerequisites
- Python ≥3.10 with virtualenv support  
  ```
  cd mini-fred
  python3 -m venv .venv && source .venv/bin/activate
  pip install -e .
  ```
- Node.js/npm for promptfoo (`brew install node` or equivalent)
- Optional: `FRED_API_KEY` in `.env` if you need to refresh data

### Core workflow (Python venv active)
1. Ingest + QC:
   ```
   python scripts/ingest_fred.py --refresh --export-snapshots
   python scripts/qc_checks.py
   python scripts/build_series_cards.py --last-n 12
   ```
2. Ask ad-hoc questions:
   ```
   python scripts/answer.py "What was the unemployment rate in April 2020?" --agent answer_4
   ```

### MVES specification
- Specs, verifiers, and custom Python checks live in `mves/`.  
- The canonical golden + refusal set lives in `evals/mves/`. Regenerate with:
  ```
  python scripts/generate_golden.py --out evals/mves/golden.jsonl
  ```
- Run the canonical MVES suite (golden + refusals) via the suite-local wrapper:
  ```bash
  for agent in answer_1 answer_2 answer_3 answer_4; do
    python evals/mves/scripts/run_mves.py --agent "$agent"
  done
  ```

### Additional evaluation suites
All commands assume the Python venv is active; promptfoo runs also require `npm` to be available.

1. **`evals/mves/` (golden + refusals)** – described above; produces reports under `reports/mves/mves_report_<agent>.*`.

2. **`evals/ext_v1/` (auto-generated large suite)**  
   - Rebuild merged spec/verifier overrides (see `evals/ext_v1/README.md`).  
   - Execute for every agent:
     ```bash
     for agent in answer_1 answer_2 answer_3 answer_4; do
       python evals/ext_v1/scripts/run_ext_mves.py --agent "$agent" --eval evals/ext_v1/evalset.jsonl
     done
     ```
3. **`evals/ext_v2/` (tough-mode suite with noisy prompts)**  
   - Biases toward MoM/YoY/MA + long-window extrema, adds noisy phrasing, and includes harder refusal templates (see `evals/ext_v2/README.md` for the full workflow).  
   - Regenerate via the same four scripts as ext_v1 (`generate_golden_from_duckdb.py`, `generate_questions.py`, `build_evalset.py`, then the wrapper below).  
   - Run it via:
     ```bash
     for agent in answer_1 answer_2 answer_3 answer_4; do
       python evals/ext_v2/scripts/run_ext_mves.py --agent "$agent" --eval evals/ext_v2/evalset.jsonl
     done
     ```
   - Reports land in `reports/ext_v2/mves_report_<agent>.*`.

4. **`evals/promptfoo_ext/` (promptfoo + Python assertions)**  
   - Requires `npm` (in addition to the Python venv).  
   - Run promptfoo for each agent via the helper script:
     ```bash
     for agent in answer_1 answer_2 answer_3 answer_4; do
       evals/promptfoo_ext/scripts/run.sh --agent "$agent"
     done
     ```
   - The script shells into `npx promptfoo@latest eval -c evals/promptfoo_ext/promptfooconfig.yaml`, so append any extra flags (e.g., `--max-concurrency 2`).

### Environment
- Keep the Python venv active for all scripts (`source .venv/bin/activate`).
- Ensure `npm` is on PATH before running promptfoo commands.
- Once snapshots are committed, no API keys are required for evaluation.
