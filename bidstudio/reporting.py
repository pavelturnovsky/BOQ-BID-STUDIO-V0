"""Utilities for exporting comparison outputs to disk."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict

from .comparison import ComparisonResult
from .config import OutputConfig

logger = logging.getLogger(__name__)


def export_comparison(result: ComparisonResult, output: OutputConfig) -> Dict[str, Path]:
    """Persist comparison artefacts to the configured output directory."""

    output_dir = output.directory
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Writing reports to %s", output_dir)

    paths: Dict[str, Path] = {}

    items_path = output_dir / output.item_report
    result.items.to_csv(items_path, index=False)
    paths["items"] = items_path

    summary_path = output_dir / output.summary_report
    result.summary.to_csv(summary_path, index=False)
    paths["summary"] = summary_path

    unmatched_path = output_dir / output.unmatched_report
    result.unmatched.to_csv(unmatched_path, index=False)
    paths["unmatched"] = unmatched_path

    audit_payload = result.metadata.copy()
    audit_payload["summary"] = result.summary.to_dict(orient="records")
    audit_path = output_dir / output.audit_log
    with audit_path.open("w", encoding="utf-8") as handle:
        json.dump(audit_payload, handle, ensure_ascii=False, indent=2)
    paths["audit"] = audit_path

    return paths


__all__ = ["export_comparison"]
