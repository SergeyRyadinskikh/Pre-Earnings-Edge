from __future__ import annotations
import sqlite3
from dataclasses import dataclass
from typing import List, Optional
from datetime import date, datetime

@dataclass(frozen=True)
class SkewRow:
    trade_date: str     # YYYY-MM-DD
    symbol: str
    expiry: str         # YYYYMMDD
    dte: int
    spot: float
    atm_strike: float
    atm_iv: float

def _expiry_to_date(expiry_yyyymmdd: str) -> date:
    return datetime.strptime(expiry_yyyymmdd, "%Y%m%d").date()

def get_latest_trade_date(conn: sqlite3.Connection, symbol: str) -> Optional[str]:
    cur = conn.execute(
        "SELECT MAX(trade_date) FROM skew_daily WHERE symbol = ?",
        (symbol.upper(),),
    )
    row = cur.fetchone()
    return row[0] if row and row[0] else None

def load_skew_rows_for_date(db_path: str, symbol: str, trade_date: str) -> List[SkewRow]:
    conn = sqlite3.connect(db_path)
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            """
            SELECT trade_date, symbol, expiry, dte, spot, atm_strike, atm_iv
            FROM skew_daily
            WHERE symbol = ? AND trade_date = ? AND atm_iv IS NOT NULL AND atm_strike IS NOT NULL
            """,
            (symbol.upper(), trade_date),
        )
        out: List[SkewRow] = []
        for r in cur.fetchall():
            out.append(
                SkewRow(
                    trade_date=r["trade_date"],
                    symbol=r["symbol"],
                    expiry=str(r["expiry"]),
                    dte=int(r["dte"]),
                    spot=float(r["spot"]),
                    atm_strike=float(r["atm_strike"]),
                    atm_iv=float(r["atm_iv"]),
                )
            )
        return out
    finally:
        conn.close()

def pick_front_back(rows: List[SkewRow], earnings_date_yyyy_mm_dd: str) -> tuple[SkewRow, SkewRow]:
    e_date = datetime.strptime(earnings_date_yyyy_mm_dd, "%Y-%m-%d").date()

    # expiries on/after earnings date
    eligible = [r for r in rows if _expiry_to_date(r.expiry) >= e_date]
    if len(eligible) < 2:
        raise RuntimeError(
            f"Not enough expiries in skew DB on/after earnings date {earnings_date_yyyy_mm_dd}. "
            f"Have eligible={len(eligible)} total_rows={len(rows)}"
        )

    eligible.sort(key=lambda r: _expiry_to_date(r.expiry))
    front = eligible[0]

    target = _expiry_to_date(front.expiry).toordinal() + 30
    back = min(
        eligible[1:],  # must be after front
        key=lambda r: abs(_expiry_to_date(r.expiry).toordinal() - target),
    )
    return front, back