"""Command line interface for the Bid Studio comparison pipeline."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Iterable, List, Optional

import numpy as np

from .comparison import compare_bids
from .config import AppConfig, BidConfig, load_config
from .io import load_bid_dataset, load_master_dataset
from .reporting import export_comparison
from .search import create_search_provider

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = Path("config/config.yaml")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compare supplier bids against a master BoQ")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH, help="Path to YAML configuration")
    parser.add_argument("--master", type=Path, help="Override path to the master dataset")
    parser.add_argument(
        "--bid",
        action="append",
        help="Override bid file in the format SupplierName=path/to/file.csv",
    )
    parser.add_argument("--output-dir", type=Path, help="Directory for generated reports")
    parser.add_argument("--search-provider", help="Name of the search provider to use (tfidf, none)")
    parser.add_argument("--top-k", type=int, help="Number of semantic search suggestions to keep")
    parser.add_argument("--chunk-size", type=int, help="Chunk size for CSV ingestion")
    parser.add_argument("--currency", help="Currency code to display in outputs")
    parser.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING)")
    parser.add_argument("--quiet", action="store_true", help="Suppress console summary output")
    return parser


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    try:
        config = load_config(args.config)
        _apply_overrides(config, args)
    except Exception as exc:  # pragma: no cover - CLI validation
        logger.error("Failed to load configuration: %s", exc)
        return 1

    if not config.bids:
        logger.warning("No supplier bids provided in configuration")
        return 0

    chunk_size = args.chunk_size if args.chunk_size is not None else config.comparison.chunk_size

    try:
        master_frame = load_master_dataset(
            config.master,
            config.columns,
            config.comparison.key_columns,
            chunk_size=chunk_size,
        )
        bid_frames = [
            load_bid_dataset(bid, config.columns, config.comparison.key_columns, chunk_size=chunk_size)
            for bid in config.bids
        ]
    except Exception as exc:
        logger.exception("Failed to load datasets: %s", exc)
        return 1

    provider_name = (args.search_provider or config.search.provider or "tfidf").lower()
    if provider_name in {"none", "off", "disabled"}:
        search_provider = None
    else:
        search_provider = create_search_provider(provider_name)

    try:
        result = compare_bids(
            master_frame,
            bid_frames,
            config.comparison,
            search_provider=search_provider,
            search_fields=config.search.fields,
            search_top_k=args.top_k or config.search.top_k,
            search_metadata_fields=config.search.metadata_fields,
        )
    except Exception as exc:
        logger.exception("Comparison failed: %s", exc)
        return 1

    try:
        export_comparison(result, config.output)
    except Exception as exc:
        logger.exception("Failed to export comparison results: %s", exc)
        return 1

    if not args.quiet:
        _print_summary(result, config.comparison.currency)

    return 0


def _apply_overrides(config: AppConfig, args: argparse.Namespace) -> None:
    if args.master:
        config.master = _resolve_override_path(args.master)

    if args.bid:
        bids: List[BidConfig] = []
        for entry in args.bid:
            name, path = _parse_bid_override(entry)
            bids.append(BidConfig(name=name, path=_resolve_override_path(Path(path))))
        config.bids = bids

    if args.output_dir:
        config.output.directory = _resolve_override_path(args.output_dir)

    if args.search_provider:
        config.search.provider = args.search_provider

    if args.top_k:
        config.search.top_k = args.top_k

    if args.chunk_size is not None:
        config.comparison.chunk_size = args.chunk_size

    if args.currency:
        config.comparison.currency = args.currency


def _parse_bid_override(value: str) -> tuple[str, str]:
    if "=" not in value:
        raise ValueError("Bid overrides must be in the format Supplier=path")
    name, path = value.split("=", 1)
    name = name.strip()
    path = path.strip()
    if not name or not path:
        raise ValueError("Bid override must include both supplier name and path")
    return name, path


def _resolve_override_path(path: Path) -> Path:
    path = Path(path).expanduser()
    if path.is_absolute():
        return path
    return (Path.cwd() / path).resolve()


def _print_summary(result, currency: Optional[str]) -> None:
    summary = result.summary.copy()
    if summary.empty:
        print("No supplier bids processed.")
        return

    numeric_columns = [
        column
        for column in ("master_total", "supplier_total", "difference", "difference_pct")
        if column in summary.columns
    ]
    for column in numeric_columns:
        summary[column] = summary[column].apply(_format_float)

    if currency:
        summary["currency"] = currency

    print("Supplier comparison summary:")
    print(summary.to_string(index=False))
    if not result.unmatched.empty:
        print(f"Unmatched bid items: {len(result.unmatched)}")


def _format_float(value: float) -> str:
    try:
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return "-"
        return f"{float(value):,.2f}"
    except Exception:  # pragma: no cover - formatting fallback
        return str(value)


if __name__ == "__main__":  # pragma: no cover - manual execution entry point
    raise SystemExit(main())
