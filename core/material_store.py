"""Material price collection and storage helpers."""

from __future__ import annotations

import hashlib
import json
import math
import os
import sqlite3
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

import pandas as pd


DEFAULT_DATABASE_PATH = Path.home() / ".boq_bid_studio" / "material_prices.sqlite"


def _json_default(value: object) -> object:
    if isinstance(value, (datetime,)):
        return value.isoformat()
    return value


def _safe_float(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric):
        return None
    return numeric


def _safe_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return text


def _sheet_tokens(name: str) -> List[str]:
    normalized = name.strip().casefold()
    cleaned = normalized.replace("_", " ")
    tokens = {normalized, cleaned, cleaned.replace(" ", "")}
    return [token for token in tokens if token]


def should_collect_sheet(sheet_name: str) -> bool:
    """Return True if ``sheet_name`` looks like a master material sheet."""

    if not sheet_name:
        return False
    tokens = _sheet_tokens(sheet_name)
    for token in tokens:
        if token.startswith("master") or token.startswith("mater"):
            return True
    return False


@dataclass
class MaterialRecord:
    item_code: str
    description: str
    unit: str
    quantity: Optional[float] = None
    unit_price_material: Optional[float] = None
    unit_price_install: Optional[float] = None
    total_price: Optional[float] = None
    calc_total: Optional[float] = None
    row_ref: Optional[str] = None
    row_index: Optional[int] = None
    metadata: Dict[str, object] = field(default_factory=dict)

    def combined_unit_price(self) -> Optional[float]:
        values = [
            v
            for v in (self.unit_price_material, self.unit_price_install)
            if v is not None
        ]
        if not values:
            return None
        return float(sum(values))

    def row_hash(self) -> str:
        payload = json.dumps(
            {
                "item_code": self.item_code,
                "description": self.description,
                "unit": self.unit,
                "quantity": self.quantity,
                "unit_price_material": self.unit_price_material,
                "unit_price_install": self.unit_price_install,
                "total_price": self.total_price,
                "calc_total": self.calc_total,
            },
            sort_keys=True,
            ensure_ascii=False,
            default=_json_default,
        )
        return hashlib.sha1(payload.encode("utf-8")).hexdigest()


@dataclass
class MaterialBatch:
    workbook_name: str
    sheet_name: str
    file_hash: str
    records: Sequence[MaterialRecord]
    country: Optional[str] = None
    currency: Optional[str] = None
    project_name: Optional[str] = None
    metadata: Dict[str, object] = field(default_factory=dict)

    @property
    def source_hash(self) -> str:
        base = f"{self.file_hash}|{self.sheet_name.casefold()}"
        return hashlib.sha1(base.encode("utf-8")).hexdigest()


def build_material_batch(
    table: pd.DataFrame,
    *,
    workbook_name: str,
    sheet_name: str,
    file_hash: str,
    country: Optional[str] = None,
    currency: Optional[str] = None,
    project_name: Optional[str] = None,
    metadata: Optional[Dict[str, object]] = None,
) -> Optional[MaterialBatch]:
    """Convert ``table`` into a :class:`MaterialBatch` if it contains usable rows."""

    if not isinstance(table, pd.DataFrame) or table.empty:
        return None
    desc_series = table.get("description")
    if desc_series is None:
        return None

    summary_mask = table.get("is_summary")
    if summary_mask is not None:
        working = table.loc[~summary_mask.fillna(False)].copy()
    else:
        working = table.copy()

    if working.empty:
        return None

    records: List[MaterialRecord] = []
    price_columns = ["unit_price_material", "unit_price_install", "total_price", "calc_total"]

    for idx, row in working.iterrows():
        description = _safe_text(row.get("description"))
        if not description:
            continue

        prices = [
            _safe_float(row.get(col))
            for col in price_columns
            if col in working.columns
        ]
        if not prices or all(v is None or v <= 0 for v in prices):
            continue
        if any(v is not None and abs(v) > 1e9 for v in prices):
            continue

        quantity = _safe_float(row.get("quantity"))
        if quantity is not None and quantity < 0:
            continue

        record = MaterialRecord(
            item_code=_safe_text(row.get("code")),
            description=description,
            unit=_safe_text(row.get("unit")),
            quantity=quantity,
            unit_price_material=_safe_float(row.get("unit_price_material")),
            unit_price_install=_safe_float(row.get("unit_price_install")),
            total_price=_safe_float(row.get("total_price")),
            calc_total=_safe_float(row.get("calc_total")),
            row_ref=_safe_text(row.get("row_ref")) or None,
            row_index=int(idx) if isinstance(idx, (int, float)) else None,
            metadata={
                "section_total": _safe_float(row.get("section_total")),
                "summary_type": _safe_text(row.get("summary_type")),
            },
        )
        records.append(record)

    if not records:
        return None

    batch_meta = dict(metadata or {})
    if "row_count" not in batch_meta:
        batch_meta["row_count"] = len(records)

    return MaterialBatch(
        workbook_name=workbook_name,
        sheet_name=sheet_name,
        file_hash=file_hash,
        records=records,
        country=_safe_text(country) or None,
        currency=_safe_text(currency) or None,
        project_name=_safe_text(project_name) or None,
        metadata=batch_meta,
    )


class MaterialPriceDatabase:
    """Store material price records in a SQLite database."""

    def __init__(self, path: Optional[Path] = None) -> None:
        self.path = Path(path) if path else DEFAULT_DATABASE_PATH
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(self.path.parent, 0o700)
        except OSError:
            pass
        self._lock = threading.Lock()
        self._initialized = False

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.path))
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA journal_mode = WAL;")
        return conn

    def _ensure_schema(self) -> None:
        if self._initialized:
            return
        with self._lock:
            if self._initialized:
                return
            conn = self._connect()
            try:
                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS sources (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        source_hash TEXT UNIQUE NOT NULL,
                        file_hash TEXT NOT NULL,
                        workbook_name TEXT,
                        sheet_name TEXT,
                        country TEXT,
                        currency TEXT,
                        project_name TEXT,
                        captured_at TEXT NOT NULL,
                        metadata TEXT
                    );

                    CREATE TABLE IF NOT EXISTS material_entries (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
                        item_code TEXT,
                        description TEXT NOT NULL,
                        unit TEXT,
                        quantity REAL,
                        unit_price_material REAL,
                        unit_price_install REAL,
                        unit_price_total REAL,
                        total_price REAL,
                        calc_total REAL,
                        row_ref TEXT,
                        row_index INTEGER,
                        row_hash TEXT NOT NULL,
                        metadata TEXT,
                        UNIQUE(source_id, row_hash)
                    );

                    CREATE INDEX IF NOT EXISTS idx_material_entries_item_code
                        ON material_entries(item_code);
                    CREATE INDEX IF NOT EXISTS idx_material_entries_description
                        ON material_entries(description);
                    CREATE INDEX IF NOT EXISTS idx_material_entries_source
                        ON material_entries(source_id);
                    """
                )
                conn.commit()
            finally:
                conn.close()
            try:
                if self.path.exists():
                    os.chmod(self.path, 0o600)
            except OSError:
                pass
            self._initialized = True

    def store_batch(self, batch: MaterialBatch) -> int:
        if not batch.records:
            return 0
        if not batch.file_hash:
            return 0
        self._ensure_schema()
        inserted = 0
        with self._lock:
            conn = self._connect()
            try:
                captured_at = datetime.now(timezone.utc).isoformat()
                metadata_json = json.dumps(
                    batch.metadata, ensure_ascii=False, sort_keys=True, default=_json_default
                )
                conn.execute(
                    """
                    INSERT INTO sources (
                        source_hash, file_hash, workbook_name, sheet_name,
                        country, currency, project_name, captured_at, metadata
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(source_hash) DO UPDATE SET
                        file_hash = excluded.file_hash,
                        workbook_name = excluded.workbook_name,
                        sheet_name = excluded.sheet_name,
                        country = excluded.country,
                        currency = excluded.currency,
                        project_name = excluded.project_name,
                        captured_at = excluded.captured_at,
                        metadata = excluded.metadata;
                    """,
                    (
                        batch.source_hash,
                        batch.file_hash,
                        batch.workbook_name,
                        batch.sheet_name,
                        batch.country,
                        batch.currency,
                        batch.project_name,
                        captured_at,
                        metadata_json,
                    ),
                )
                source_id = conn.execute(
                    "SELECT id FROM sources WHERE source_hash = ?",
                    (batch.source_hash,),
                ).fetchone()
                if not source_id:
                    conn.commit()
                    return 0
                source_pk = int(source_id[0])
                conn.execute(
                    "DELETE FROM material_entries WHERE source_id = ?",
                    (source_pk,),
                )

                for record in batch.records:
                    combined_price = record.combined_unit_price()
                    metadata_payload = json.dumps(
                        record.metadata,
                        ensure_ascii=False,
                        sort_keys=True,
                        default=_json_default,
                    )
                    conn.execute(
                        """
                        INSERT INTO material_entries (
                            source_id, item_code, description, unit, quantity,
                            unit_price_material, unit_price_install, unit_price_total,
                            total_price, calc_total, row_ref, row_index, row_hash, metadata
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(source_id, row_hash) DO UPDATE SET
                            item_code = excluded.item_code,
                            description = excluded.description,
                            unit = excluded.unit,
                            quantity = excluded.quantity,
                            unit_price_material = excluded.unit_price_material,
                            unit_price_install = excluded.unit_price_install,
                            unit_price_total = excluded.unit_price_total,
                            total_price = excluded.total_price,
                            calc_total = excluded.calc_total,
                            row_ref = excluded.row_ref,
                            row_index = excluded.row_index,
                            metadata = excluded.metadata;
                        """,
                        (
                            source_pk,
                            record.item_code or None,
                            record.description,
                            record.unit or None,
                            record.quantity,
                            record.unit_price_material,
                            record.unit_price_install,
                            combined_price,
                            record.total_price,
                            record.calc_total,
                            record.row_ref,
                            record.row_index,
                            record.row_hash(),
                            metadata_payload,
                        ),
                    )
                    inserted += 1
                conn.commit()
            finally:
                conn.close()
        return inserted


class MaterialCollector:
    """Collect material records from workbooks and persist them."""

    def __init__(self, database: Optional[MaterialPriceDatabase] = None) -> None:
        self.database = database or MaterialPriceDatabase()

    def collect_from_workbook(
        self,
        workbook: "WorkbookData",
        *,
        file_hash: str,
        country: Optional[str] = None,
        currency: Optional[str] = None,
        project_name: Optional[str] = None,
        metadata: Optional[Dict[str, object]] = None,
        sheet_whitelist: Optional[Iterable[str]] = None,
    ) -> int:
        if not file_hash:
            return 0
        allowed = {s.casefold() for s in sheet_whitelist} if sheet_whitelist else None
        total = 0
        for sheet, payload in getattr(workbook, "sheets", {}).items():
            if allowed is not None and sheet.casefold() not in allowed:
                continue
            if not should_collect_sheet(sheet):
                continue
            table = payload.get("table") if isinstance(payload, dict) else None
            if not isinstance(table, pd.DataFrame) or table.empty:
                continue
            batch = build_material_batch(
                table,
                workbook_name=workbook.name,
                sheet_name=sheet,
                file_hash=file_hash,
                country=country,
                currency=currency,
                project_name=project_name,
                metadata=metadata,
            )
            if batch is None:
                continue
            total += self.database.store_batch(batch)
        return total


__all__ = [
    "MaterialBatch",
    "MaterialCollector",
    "MaterialPriceDatabase",
    "MaterialRecord",
    "build_material_batch",
    "should_collect_sheet",
]

