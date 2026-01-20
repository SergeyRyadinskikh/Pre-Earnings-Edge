from __future__ import annotations

import requests
from dataclasses import dataclass
from typing import Optional, Any, Dict, List, Tuple
from datetime import date, timedelta, datetime


@dataclass(frozen=True)
class EarningsInfo:
    symbol: str
    next_earnings_date: Optional[str]             # YYYY-MM-DD
    next_time_hint: Optional[str]
    last_earnings: List[Tuple[str, str]]          # [(date, timing)] oldest->newest
    source: str
    error: Optional[str] = None


def _get(url: str, params: Dict[str, Any], timeout: int = 20) -> Any:
    r = requests.get(url, params=params, timeout=timeout)
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code} for {r.url} | body={r.text[:350]}")
    return r.json()


def _normalize_timing(x: Optional[str]) -> str:
    if not x:
        return "UNKNOWN"
    s = str(x).strip().upper()
    if "AMC" in s or "AFTER" in s:
        return "AMC"
    if "BMO" in s or "BEFORE" in s:
        return "BMO"
    return "UNKNOWN"


def _to_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _fetch_window(sym: str, api_key: str, frm: str, to: str) -> List[Dict[str, Any]]:
    url = "https://financialmodelingprep.com/stable/earnings-calendar"
    data = _get(url, {"symbol": sym, "from": frm, "to": to, "apikey": api_key})
    if not isinstance(data, list):
        return []
    return data


def fetch_earnings_bundle(symbol: str, api_key: str, n_last: int = 12) -> EarningsInfo:
    """
    Pulls earnings with SMALL windows to avoid plan/bandwidth gating.
    Strategy:
      1) Next earnings: today-7d to today+365d
      2) Last earnings: pull past in chunks until we have n_last (default 12)
    """
    sym = symbol.upper()
    today = date.today()
    source = "fmp:stable/earnings-calendar"
    try:
        # --- 1) Next earnings (small future window) ---
        future_from = (today - timedelta(days=7)).strftime("%Y-%m-%d")
        future_to = (today + timedelta(days=365)).strftime("%Y-%m-%d")
        fut_rows = _fetch_window(sym, api_key, future_from, future_to)

        future = []
        for row in fut_rows:
            d = row.get("date")
            if not d:
                continue
            dd = _to_date(d)
            if dd >= today:
                timing = _normalize_timing(row.get("time") or row.get("earningsTime"))
                future.append((d, timing, row))
        future.sort(key=lambda x: x[0])

        next_date = future[0][0] if future else None
        next_time_hint = None
        if future:
            raw = future[0][2]
            th = raw.get("time") or raw.get("earningsTime")
            next_time_hint = str(th).strip() if th else None

        # --- 2) Last earnings (pull backwards in chunks) ---
        collected: List[Tuple[str, str]] = []
        # We try 3 chunks: last 18 months, then 3 years, then 5 years (still much smaller than before+future)
        chunks = [
            (today - timedelta(days=548), today - timedelta(days=1)),      # ~18 months
            (today - timedelta(days=365 * 3), today - timedelta(days=1)),  # 3 years
            (today - timedelta(days=365 * 5), today - timedelta(days=1)),  # 5 years
        ]

        seen_dates = set()
        for a, b in chunks:
            frm = a.strftime("%Y-%m-%d")
            to = b.strftime("%Y-%m-%d")
            rows = _fetch_window(sym, api_key, frm, to)

            past = []
            for row in rows:
                d = row.get("date")
                if not d:
                    continue
                dd = _to_date(d)
                if dd < today:
                    t = _normalize_timing(row.get("time") or row.get("earningsTime"))
                    past.append((d, t))

            past.sort(key=lambda x: x[0])  # oldest->newest
            for d, t in past:
                if d not in seen_dates:
                    seen_dates.add(d)
                    collected.append((d, t))

            # Keep only last n_last (most recent)
            collected.sort(key=lambda x: x[0])
            if len(collected) > n_last:
                collected = collected[-n_last:]

            if len(collected) >= n_last:
                break

        # Ensure chronological for your event study
        collected.sort(key=lambda x: x[0])

        return EarningsInfo(
            symbol=sym,
            next_earnings_date=next_date,
            next_time_hint=next_time_hint,
            last_earnings=collected,
            source=source,
            error=None if (next_date or collected) else "FMP returned no earnings rows in tested windows",
        )

    except Exception as e:
        return EarningsInfo(
            symbol=sym,
            next_earnings_date=None,
            next_time_hint=None,
            last_earnings=[],
            source=source,
            error=str(e),
        )
