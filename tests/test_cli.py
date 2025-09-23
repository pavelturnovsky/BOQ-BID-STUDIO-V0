from pathlib import Path

import pandas as pd

from bidstudio.cli import main


def test_cli_creates_expected_reports(tmp_path):
    output_dir = tmp_path / "reports"
    exit_code = main([
        "--config",
        "config/config.yaml",
        "--output-dir",
        str(output_dir),
        "--quiet",
    ])

    assert exit_code == 0
    summary_path = output_dir / "summary.csv"
    items_path = output_dir / "item_differences.csv"
    unmatched_path = output_dir / "unmatched_items.csv"
    audit_path = output_dir / "comparison_audit.json"

    for path in (summary_path, items_path, unmatched_path, audit_path):
        assert path.exists()

    summary = pd.read_csv(summary_path)
    assert not summary.empty
    assert set(summary["supplier"]) == {"Supplier Alpha", "Supplier Beta"}

    unmatched = pd.read_csv(unmatched_path)
    if not unmatched.empty:
        assert {"supplier", "description"}.issubset(unmatched.columns)
