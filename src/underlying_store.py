from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import List, Tuple, Dict, Optional
import re

from src.ibkr_client import UnderlyingBar

DDL = """
CREATE TABLE IF NOT EXISTS underlying_daily (
    trade_date TEXT NOT NULL,
    symbol TEXT NOT NULL,
    close REAL NOT NULL,
    volume INTEGER NOT NULL,
    PRIMARY KEY (trade_date, symbol)
);
"""

# Matches:
# - 20260116
# - 20260116 2
# - 2026-01-16
_RE_YYYYMMDD = re.compile(r"^\s*(\d{8})\b")
_RE_YYYY_MM_DD = re.compile(r"^\s*(\d{4}-\d{2}-\d{2})\b")


def _normalize_trade_date(s: str) -> Optional[str]:
    if not s:
        return None
    s = str(s).strip()

    m = _RE_YYYY_MM_DD.match(s)
    if m:
        return m.group(1)

    m = _RE_YYYYMMDD.match(s)
    if m:
        ymd = m.group(1)  # YYYYMMDD
        return f"{ymd[0:4]}-{ymd[4:6]}-{ymd[6:8]}"

    return None


def init_db(db_path: str) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(DDL)
        conn.commit()
    finally:
        conn.close()


def upsert_bars(db_path: str, symbol: str, bars: List[UnderlyingBar]) -> None:
    """
    We normalize dates on write too, but we do NOT delete/alter older rows here.
    """
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(DDL)

        rows = []
        for b in bars:
            nd = _normalize_trade_date(b.date)
            if not nd:
                # Skip weird dates rather than poisoning the DB
                continue
            rows.append((nd, symbol.upper(), float(b.close), int(b.volume)))

        conn.executemany(
            """
            INSERT INTO underlying_daily(trade_date, symbol, close, volume)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(trade_date, symbol) DO UPDATE SET
                close=excluded.close,
                volume=excluded.volume
            """,
            rows,
        )
        conn.commit()
    finally:
        conn.close()


def load_recent(db_path: str, symbol: str, limit: int = 120) -> List[Tuple[str, float, int]]:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(
            """
            SELECT trade_date, close, volume
            FROM underlying_daily
            WHERE symbol = ?
            ORDER BY trade_date DESC
            LIMIT ?
            """,
            (symbol.upper(), limit),
        )
        raw = cur.fetchall()

        # Normalize and keep only parseable dates
        out = []
        for d, c, v in raw:
            nd = _normalize_trade_date(d)
            if not nd:
                continue
            out.append((nd, float(c), int(v)))

        out.sort(key=lambda x: x[0])  # oldest->newest
        return out
    finally:
        conn.close()


def load_all_dates_closes(db_path: str, symbol: str) -> Tuple[List[str], Dict[str, float]]:
    """
    Returns:
      dates_sorted (ascending YYYY-MM-DD) and close_by_date mapping.
    Normalizes any legacy rows that used '20260116 2' or '20260116'.
    """
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(
            """
            SELECT trade_date, close
            FROM underlying_daily
            WHERE symbol = ?
            ORDER BY trade_date ASC
            """,
            (symbol.upper(),),
        )
        rows = cur.fetchall()

        close_by_date: Dict[str, float] = {}
        for d, c in rows:
            nd = _normalize_trade_date(d)
            if not nd:
                continue
            # last write wins; should be identical anyway
            close_by_date[nd] = float(c)

        dates_sorted = sorted(close_by_date.keys())
        return dates_sorted, close_by_date
    finally:
        conn.close()


def has_min_history(db_path: str, symbol: str, min_rows: int = 260) -> bool:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(
            "SELECT COUNT(1) FROM underlying_daily WHERE symbol = ?",
            (symbol.upper(),),
        )
        n = cur.fetchone()[0]
        return int(n) >= int(min_rows)
    finally:
        conn.close()
