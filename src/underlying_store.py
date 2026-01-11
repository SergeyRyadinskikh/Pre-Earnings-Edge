from __future__ import annotations
import sqlite3
from typing import List
from pathlib import Path
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

def init_db(db_path: str) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(DDL)
        conn.commit()
    finally:
        conn.close()

def upsert_bars(db_path: str, symbol: str, bars: List[UnderlyingBar]) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(DDL)
        conn.executemany(
            """
            INSERT INTO underlying_daily(trade_date, symbol, close, volume)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(trade_date, symbol) DO UPDATE SET
                close=excluded.close,
                volume=excluded.volume
            """,
            [(b.date, symbol.upper(), float(b.close), int(b.volume)) for b in bars],
        )
        conn.commit()
    finally:
        conn.close()

def load_recent(db_path: str, symbol: str, limit: int = 60):
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
        rows = cur.fetchall()
        rows.reverse()  # oldest -> newest
        return rows
    finally:
        conn.close()