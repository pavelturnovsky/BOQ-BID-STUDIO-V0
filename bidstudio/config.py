"""Configuration loading utilities for Bid Studio."""

from __future__ import annotations

from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

import yaml


@dataclass
class BidConfig:
    """Descriptor for a supplier bid file."""

    name: str
    path: Path

    def resolved(self, base_path: Path) -> "BidConfig":
        return BidConfig(name=self.name, path=_resolve_path(self.path, base_path))


@dataclass
class ColumnMapping:
    """Canonical column mapping used throughout the pipeline."""

    code: Optional[str] = None
    description: Optional[str] = None
    unit: Optional[str] = None
    quantity: Optional[str] = None
    unit_price: Optional[str] = None
    total_price: Optional[str] = None

    def as_dict(self) -> Dict[str, Optional[str]]:
        return {
            "code": self.code,
            "description": self.description,
            "unit": self.unit,
            "quantity": self.quantity,
            "unit_price": self.unit_price,
            "total_price": self.total_price,
        }


@dataclass
class ComparisonConfig:
    """Tweaks influencing the numeric comparison pipeline."""

    key_columns: List[str] = field(default_factory=lambda: ["code"])
    numeric_columns: List[str] = field(
        default_factory=lambda: ["quantity", "total_price"]
    )
    currency: Optional[str] = None
    chunk_size: Optional[int] = None


@dataclass
class SearchConfig:
    """Settings related to semantic search of bill items."""

    provider: str = "tfidf"
    fields: List[str] = field(default_factory=lambda: ["description"])
    top_k: int = 5
    metadata_fields: Optional[List[str]] = None


@dataclass
class OutputConfig:
    """Paths describing where reports should be written."""

    directory: Path = Path("output")
    item_report: str = "item_differences.csv"
    summary_report: str = "summary.csv"
    unmatched_report: str = "unmatched_items.csv"
    audit_log: str = "comparison_audit.json"

    def resolved(self, base_path: Path) -> "OutputConfig":
        directory = _resolve_path(self.directory, base_path)
        return OutputConfig(
            directory=directory,
            item_report=self.item_report,
            summary_report=self.summary_report,
            unmatched_report=self.unmatched_report,
            audit_log=self.audit_log,
        )


@dataclass
class AppConfig:
    """Container for all configuration required by the CLI pipeline."""

    master: Path
    bids: List[BidConfig]
    columns: ColumnMapping
    comparison: ComparisonConfig = field(default_factory=ComparisonConfig)
    search: SearchConfig = field(default_factory=SearchConfig)
    output: OutputConfig = field(default_factory=OutputConfig)

    def resolved(self, base_path: Path) -> "AppConfig":
        master_path = _resolve_path(self.master, base_path)
        bid_configs = [bid.resolved(base_path) for bid in self.bids]
        output_config = self.output.resolved(base_path)
        return AppConfig(
            master=master_path,
            bids=bid_configs,
            columns=self.columns,
            comparison=self.comparison,
            search=self.search,
            output=output_config,
        )


def load_config(path: Path) -> AppConfig:
    """Load :class:`AppConfig` from a YAML file."""

    config_path = Path(path).expanduser()
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file '{config_path}' does not exist")

    with config_path.open("r", encoding="utf-8") as stream:
        raw_config: Mapping[str, Any] = yaml.safe_load(stream) or {}

    if "paths" not in raw_config:
        raise ValueError("Configuration must include the 'paths' section")

    paths_section = raw_config["paths"]
    master = Path(paths_section["master"])
    bid_entries = paths_section.get("bids", [])
    if not isinstance(bid_entries, Iterable):
        raise ValueError("paths.bids must be a list of bid descriptors")

    bids = [
        BidConfig(name=entry["name"], path=Path(entry["path"]))
        for entry in bid_entries
    ]

    columns_section = raw_config.get("columns")
    if not columns_section:
        raise ValueError("Configuration must define the 'columns' mapping")

    columns = _parse_column_mapping(columns_section)

    comparison = ComparisonConfig(**raw_config.get("comparison", {}))
    search = SearchConfig(**raw_config.get("search", {}))
    output = OutputConfig(**_parse_output_section(raw_config.get("output", {})))

    config = AppConfig(
        master=master,
        bids=bids,
        columns=columns,
        comparison=comparison,
        search=search,
        output=output,
    )
    return config.resolved(config_path.parent)


def _parse_column_mapping(section: Mapping[str, Any]) -> ColumnMapping:
    parsed: Dict[str, Optional[str]] = {}
    for field_info in fields(ColumnMapping):
        value = section.get(field_info.name)
        parsed[field_info.name] = _normalise_column_value(value)
    return ColumnMapping(**parsed)


def _normalise_column_value(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        if stripped.lower() in {"auto", "autodetect", "automatic"}:
            return None
        return stripped
    return str(value)


def _parse_output_section(section: Mapping[str, Any]) -> Dict[str, Any]:
    parsed: Dict[str, Any] = {}
    if "directory" in section:
        parsed["directory"] = Path(section["directory"])
    for key in ("item_report", "summary_report", "unmatched_report", "audit_log"):
        if key in section:
            parsed[key] = section[key]
    return parsed


def _resolve_path(path: Path, base_path: Path) -> Path:
    expanded = Path(path).expanduser()
    if expanded.is_absolute():
        return expanded
    return (base_path / expanded).resolve()
