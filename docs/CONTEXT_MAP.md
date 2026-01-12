# Mini-FRED Context Map

## What and Why
- **Mini-FRED** is a reproducible micro-warehouse that mirrors a small curated subset of FRED data (five macro series) for deterministic truth checks and citation-ready artifacts.
- It exists to unblock MVES verifiers and promptfoo experiments by providing high-signal, low-noise reference data that can live entirely inside git.
- **In scope (v1):** schema design, config-driven ingest plan, DuckDB warehouse, Markdown series cards, curated snapshots, light QC strategy.
- **Out of scope (v1):** orchestration layers, agents, promptfoo scenarios, embeddings/vector search, UI, historical vintages, or anything requiring API secrets at runtime once data is committed.

## Repository Layout
- `README.md` — quickstart + roadmap.
- `pyproject.toml` — pinned dependencies (DuckDB, pandas, PyYAML).
- `docs/CONTEXT_MAP.md` — this document.
- `config/series.yaml` — locked series IDs, names, date bounds, truth policy.
- `data/raw/` — raw CSV/JSON captures directly from FRED (kept small, one file per series).
- `data/snapshots/` — DuckDB files and manifest metadata for offline reuse.
- `corpus/series_cards/` — generated Markdown “series cards” derived from the warehouse.
- `scripts/` — CLI entry points for ingest, QC, card generation, answer runner, and MVES workflows.
- `src/` — Python package with clients (`fred_client`), warehouse helpers, card builders, parsing/truth utilities, and shared helpers.
- `rag_agent/` — versioned RAG agents (e.g., `answer_1.py`) that `scripts/answer.py` can load via `--agent`.
- `mves/` — evaluation spec + verifier map + Python verifiers.
- `eval/` — golden JSONL generated from DuckDB-derived snapshots plus refusal cases.
- `reports/` — artifacts emitted by `scripts/mves_run.py`.

## Data Model (DuckDB)
Two core tables make up the warehouse:
1. `series`
   - `series_id` (TEXT, PK)
   - `title` (TEXT)
   - `units` (TEXT)
   - `frequency` (TEXT)
   - `seasonal_adjustment` (TEXT)
   - `notes` (TEXT)
   - `last_updated` (TEXT)
2. `observations`
   - `series_id` (TEXT, FK → series.series_id)
   - `date` (DATE)
   - `value` (DOUBLE)

Both tables share a deterministic CSV import contract so they can be regenerated purely from committed source files. Truth policy v1 keeps only the latest available values (no vintages).

## Build Steps (current plan)
1. **Configure:** edit `config/series.yaml` if additional metadata is needed; series IDs/date range are frozen for v1.
2. **Ingest:** `scripts/ingest_fred.py` reads the config, downloads metadata + observations via `fred_client`, caches raw JSON under `data/raw/`, stores everything in `data/warehouse.duckdb`, and optionally exports CSV snapshots under `data/snapshots/`.
3. **QC:** `scripts/qc_checks.py` runs schema validation, coverage checks, row-count thresholds, and null-density warnings; it exits non-zero if critical checks fail.
4. **Series cards:** `scripts/build_series_cards.py` loads the latest snapshot, fetches the last _N_ observations for each series, and produces Markdown cards in `corpus/series_cards/` (`series_<SERIES>.md`).
5. **Answerer:** `scripts/answer.py` parses a natural-language question, computes the requested statistic from DuckDB (`src/truth.py`), runs a TF-IDF retriever over series cards (`src/retriever.py`) to infer/ground the series, and emits a JSON payload with citations + retrieved doc scores.
6. **MVES control plane:** `scripts/generate_golden.py` samples queries/dates/windows from the warehouse snapshots to produce `eval/golden.jsonl`; `scripts/mves_run.py` replays those questions through the answerer, applies verifiers in `mves/verifiers.py`, and writes reports under `reports/`.

## What Comes Next
- **Agent + MVES wiring:** promptfoo scenarios, verifier prompts, and any orchestration stay out of scope until the warehouse is fully reproducible.
- **Data freshness policy:** once snapshots are committed, add a CHANGELOG entry per refresh.
- **Extended datasets:** add more series only if they support the same offline guarantees.
- **Automation:** optional GitHub workflow later for scheduled refreshes (still manual for v1).
