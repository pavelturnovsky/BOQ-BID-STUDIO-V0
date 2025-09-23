"""Core pipeline package for the BOQ Bid Studio MVP."""

from .ingest import load_offers, read_workbook, auto_map_columns
from .normalize import normalize_currency, normalize_units, validate_totals
from .disciplines import detect_disciplines, assign_disciplines
from .matching import match_items, build_wbs_index
from .aggregate import rollup_by_discipline, rollup_by_wbs, flag_outliers
from .search import hybrid_search
from .export import export_to_xlsx
from .utils import load_config, ensure_directories

__all__ = [
    "load_offers",
    "read_workbook",
    "auto_map_columns",
    "normalize_currency",
    "normalize_units",
    "validate_totals",
    "detect_disciplines",
    "assign_disciplines",
    "match_items",
    "build_wbs_index",
    "rollup_by_discipline",
    "rollup_by_wbs",
    "flag_outliers",
    "hybrid_search",
    "export_to_xlsx",
    "load_config",
    "ensure_directories",
]
