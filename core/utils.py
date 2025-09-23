"""Utility helpers shared across the core pipeline."""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Mapping, MutableMapping, Optional

import yaml

LOGGER = logging.getLogger("boq_bid_studio")
DEFAULT_CONFIG_PATH = Path("config") / "config.yaml"


def load_config(path: Optional[str | Path] = None) -> Dict[str, Any]:
    """Load the YAML configuration file.

    Parameters
    ----------
    path:
        Optional override for the configuration path. When omitted the default
        ``config/config.yaml`` is used.

    Returns
    -------
    dict
        Parsed configuration dictionary.
    """

    config_path = Path(path) if path else DEFAULT_CONFIG_PATH
    with config_path.open("r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    return config


def ensure_directories(paths: Iterable[str | Path]) -> None:
    """Create directories if they do not already exist."""

    for path in paths:
        Path(path).mkdir(parents=True, exist_ok=True)


def clean_text(value: Any) -> str:
    """Normalise textual values for matching/search."""

    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    text = re.sub(r"\s+", " ", text.strip())
    return text


def normalise_header(value: str) -> str:
    """Normalise header text to ease column matching."""

    return re.sub(r"[^a-z0-9]+", "", value.casefold())


@dataclass
class WBSNode:
    """Container representing a single WBS node."""

    code: str
    name: str
    description: str
    discipline: Optional[str]
    keywords: List[str]
    unit: Optional[str]
    parent: Optional[str]


def iter_wbs_nodes(tree: List[Mapping[str, Any]], parent: Optional[str] = None) -> Iterator[WBSNode]:
    """Yield :class:`WBSNode` instances from a nested configuration tree."""

    for node in tree:
        code = node.get("code")
        name = node.get("name", "")
        description = node.get("description", "")
        discipline = node.get("discipline")
        keywords = list(node.get("keywords", []))
        unit = node.get("unit")
        yield WBSNode(
            code=code,
            name=name,
            description=description,
            discipline=discipline,
            keywords=keywords,
            unit=unit,
            parent=parent,
        )
        children = node.get("children", [])
        if children:
            yield from iter_wbs_nodes(children, parent=code)


def build_wbs_table(config: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """Return a flattened table representation of the configured WBS tree."""

    nodes = []
    for node in iter_wbs_nodes(config.get("wbs", [])):
        searchable_text = " ".join(
            filter(None, [node.code, node.name, node.description, " ".join(node.keywords)])
        )
        nodes.append(
            {
                "wbs_code": node.code,
                "wbs_name": node.name,
                "wbs_description": node.description,
                "discipline": node.discipline,
                "keywords": node.keywords,
                "unit": node.unit,
                "parent_code": node.parent,
                "search_text": clean_text(searchable_text),
            }
        )
    return nodes


def log_match_decision(path: str | Path, record: Mapping[str, Any]) -> None:
    """Append a record to the JSONL matching log."""

    ensure_directories([Path(path).parent])
    with Path(path).open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def timestamp() -> str:
    """Return an ISO-8601 timestamp string."""

    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


__all__ = [
    "WBSNode",
    "load_config",
    "ensure_directories",
    "clean_text",
    "normalise_header",
    "iter_wbs_nodes",
    "build_wbs_table",
    "log_match_decision",
    "timestamp",
]
