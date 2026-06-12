"""Shared runtime configuration for local and hosted deployments."""

from __future__ import annotations

import os
from pathlib import Path


def get_storage_dir() -> Path:
    """Return the persistent application storage directory.

    Hosted deployments can set ``BOQ_STORAGE_DIR`` to a mounted persistent
    volume. Local development keeps the existing per-user default.
    """

    configured_dir = os.getenv("BOQ_STORAGE_DIR")
    if configured_dir:
        return Path(configured_dir).expanduser()
    return Path.home() / ".boq_bid_studio"


DEFAULT_STORAGE_DIR = get_storage_dir()
