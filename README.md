## Mini-FRED Warehouse

Purpose is to create a deterministic, lightweight subset of key macroeconomic series that can be stored locally in DuckDB and reused for MVES verifiers and promptfoo experiments without requiring a network connection after the initial ingest.

### Quickstart (scaffold stage)
1. `cd mini-fred`
2. Create a virtual environment (example): `python3 -m venv .venv && source .venv/bin/activate`
3. Install dependencies: `pip install -e .`
4. Inspect `config/series.yaml` to confirm the five locked series and date bounds.
5. Run any placeholder script with `python scripts/<script>.py --help` to review the forthcoming workflow.

### Next milestones
- Implement `scripts/ingest_fred.py` to download metadata + observations into DuckDB snapshots.
- Add basic QC coverage in `scripts/qc_checks.py` (schema validation, range checks, freshness).
- Generate Markdown series cards and snapshot them into `corpus/series_cards`.
- Commit reproducible data snapshots for offline truth checks.

### Environment variables
- `FRED_API_KEY`: required for future online ingest. Not needed once data snapshots are committed, but the variable will be read by `src/fred_client.py` when networking is enabled.
