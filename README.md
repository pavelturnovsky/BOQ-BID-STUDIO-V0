# BoQ Bid Studio CLI

This repository contains a modular command-line tool for comparing supplier bids
against a master bill of quantities.  The application is implemented in pure
Python (3.10+) and focuses on working with large tabular data sets without any
cloud dependencies.  Semantic search is provided through a TF–IDF fallback that
runs locally.

## Project layout

```
.
├── bidstudio/           # Core application modules
├── config/              # YAML configuration used by the CLI
├── sample_data/         # Synthetic CSV data sets for development and tests
├── tests/               # Pytest-based regression tests
└── requirements.txt     # Python dependencies
```

## Sample data

Synthetic CSV inputs with 1,200+ rows are provided in `sample_data/` to exercise
the pipeline:

- `master.csv` – reference bill of quantities.
- `bid_alpha.csv` – supplier offer with a slight price increase, a subset of
  missing items, and several additional items.
- `bid_beta.csv` – supplier offer with lower prices, different dropped items,
  and additional scope entries.

All files share the same schema consisting of `ItemCode`, `Description`,
`Unit`, `Quantity`, `UnitPrice` and `TotalPrice`.  Paths and column mappings are
configured in `config/config.yaml`.

## CLI usage

Install dependencies and run the comparison directly from the repository root:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m bidstudio.cli --config config/config.yaml --output-dir outputs
```

Command-line arguments allow overriding the master file, bids, output
directory, semantic search provider, and processing chunk size.  Use `--help`
to see the complete list of options.

The CLI writes three CSV reports and one JSON audit file to the configured
output directory:

- `item_differences.csv` – per-item comparison for every supplier.
- `summary.csv` – supplier-level totals and counts of matched/missing items.
- `unmatched_items.csv` – items present only in the supplier offer together with
  TF–IDF based master suggestions.
- `comparison_audit.json` – metadata containing run statistics and the summary
  table in machine-readable form.

## Testing

Run the automated test-suite with:

```bash
pytest
```

The tests exercise configuration loading, the semantic search implementation and
an end-to-end CLI execution on the provided sample data.
