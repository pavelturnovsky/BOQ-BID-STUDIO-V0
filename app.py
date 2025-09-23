"""Command line interface for the BOQ Bid Studio pipeline."""
from __future__ import annotations

import argparse

from tabulate import tabulate

from core import (
    assign_disciplines,
    build_wbs_index,
    ensure_directories,
    export_to_xlsx,
    flag_outliers,
    hybrid_search,
    load_config,
    load_offers,
    match_items,
    normalize_currency,
    normalize_units,
    rollup_by_discipline,
    rollup_by_wbs,
    validate_totals,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="BOQ Bid Studio CLI")
    parser.add_argument("--inputs", nargs="+", required=True, help="Input offer files (Excel/CSV)")
    parser.add_argument("--baseline", default="median", help="Baseline supplier or 'median'")
    parser.add_argument("--rollup", help="Roll-up target (discipline code or WBS:prefix)")
    parser.add_argument("--query", help="Natural language search query")
    parser.add_argument("--export", help="Output XLSX path (e.g. out/result.xlsx)")
    parser.add_argument("--config", default="config/config.yaml", help="Configuration file path")
    return parser.parse_args()


def run_pipeline(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    ensure_directories(["logs", "out"])

    offers = load_offers(args.inputs, config)
    offers = normalize_units(offers, config.get("unit_map", {}))
    currency_cfg = config.get("currency", {})
    offers = normalize_currency(
        offers,
        currency_cfg.get("base", "CZK"),
        currency_cfg.get("rates", {}),
        currency_cfg.get("default_vat", 0.21),
    )
    offers = validate_totals(offers)
    offers = assign_disciplines(offers, config)
    offers.reset_index(drop=True, inplace=True)
    offers["item_index"] = offers.index

    wbs_index = build_wbs_index(config)
    matches = match_items(offers, wbs_index, config)
    drop_cols = {"item_desc", "supplier", "sheet_name", "item_code"}
    offers = offers.merge(
        matches.drop(columns=[col for col in drop_cols if col in matches.columns]),
        on="item_index",
        how="left",
    )
    offers = flag_outliers(offers)

    summary = None
    if args.rollup:
        if args.rollup.upper().startswith("WBS:"):
            prefix = args.rollup.split(":", 1)[1]
            summary = rollup_by_wbs(offers, baseline=args.baseline, prefix=prefix)
        else:
            summary = rollup_by_discipline(offers, baseline=args.baseline)
    else:
        summary = rollup_by_discipline(offers, baseline=args.baseline)

    if summary is not None and not summary.empty:
        print("\n=== Roll-up Summary ===")
        print(tabulate(summary, headers="keys", tablefmt="github", floatfmt=".2f"))

    if args.query:
        print("\n=== Search Results ===")
        search_df = hybrid_search(offers, args.query, config)
        print(tabulate(search_df, headers="keys", tablefmt="github", floatfmt=".3f"))

    if args.export:
        unmatched = offers[offers["match_status"] == "unmatched"] if "match_status" in offers.columns else None
        output_file = export_to_xlsx(summary, offers, unmatched, args.export)
        print(f"\nExported report to {output_file}")


def main() -> None:
    args = parse_args()
    run_pipeline(args)


if __name__ == "__main__":
    main()
