from __future__ import annotations
import csv
from pathlib import Path
from typing import Dict, Any

def write_single_row_csv(out_dir: str, symbol: str, run_date: str, row: Dict[str, Any]) -> str:
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    out_path = Path(out_dir) / f"{symbol.upper()}_earnings_edge_{run_date}.csv"
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        w.writeheader()
        w.writerow(row)
    return str(out_path)