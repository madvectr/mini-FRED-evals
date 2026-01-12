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
- `scripts/` — CLI entry points for ingest, QC, card generation (currently placeholders).
- `src/` — Python package with clients (`fred_client`), warehouse helpers, card builders, and shared utilities.

## Data Model (DuckDB)
Two core tables make up the warehouse:
1. `series_metadata`
   - `series_id` (TEXT, PK)
   - `title` (TEXT)
   - `units` (TEXT)
   - `frequency` (TEXT)
   - `seasonal_adjustment` (TEXT)
   - `notes` (TEXT)
   - `last_updated` (TIMESTAMP)
2. `series_observations`
   - `series_id` (TEXT, FK → series_metadata)
   - `observation_date` (DATE)
   - `value` (DOUBLE)
   - `status` (TEXT, optional flags such as “estimated”)

Both tables share a deterministic CSV import contract so they can be regenerated purely from committed source files. Truth policy v1 keeps only the latest available values (no vintages).

## Build Steps (current plan)
1. **Configure:** edit `config/series.yaml` if additional metadata is needed; series IDs/date range are frozen for v1.
2. **Ingest (future):** `scripts/ingest_fred.py` will read the config, download metadata + observations via `fred_client`, and write both raw CSV dumps and a DuckDB snapshot to `data/snapshots/`.
3. **QC (future):** `scripts/qc_checks.py` will run schema validation, range/freshness checks, and emit a simple report.
4. **Series cards (future):** `scripts/build_series_cards.py` will load the latest snapshot, fetch the last _N_ observations for each series, and produce Markdown cards in `corpus/series_cards/`.

Until ingest lands, the scripts simply validate configuration presence and document the expected workflow.

## What Comes Next
- **Agent + MVES wiring:** promptfoo scenarios, verifier prompts, and any orchestration stay out of scope until the warehouse is fully reproducible.
- **Data freshness policy:** once snapshots are committed, add a CHANGELOG entry per refresh.
- **Extended datasets:** add more series only if they support the same offline guarantees.
- **Automation:** optional GitHub workflow later for scheduled refreshes (still manual for v1).
