from dataclasses import dataclass, field
from typing import Dict

@dataclass
class WorkbookData:
    """Container for parsed workbook data."""
    name: str
    sheets: Dict[str, Dict] = field(default_factory=dict)
