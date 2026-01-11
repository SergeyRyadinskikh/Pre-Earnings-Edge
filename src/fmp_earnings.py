from __future__ import annotations
import requests
from dataclasses import dataclass
from typing import Optional, Any, Dict
from datetime import date, timedelta

@dataclass(frozen=True)
class EarningsInfo:
    symbol: str
    earnings_date: Optional[str]  # YYYY-MM-DD
    time_hint: Optional[str] = None
    source: Optional[str] = None
    error: Optional[str] = None

def _get(url: str, params: Dict[str, Any], timeout: int = 20) -> Any:
    r = requests.get(url, params=params, timeout=timeout)
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code} for {r.url} | body={r.text[:300]}")
    return r.json()

def fetch_next_earnings(symbol: str, api_key: str) -> EarningsInfo:
    sym = symbol.upper()

    # Stable API requires from/to to get future earnings. :contentReference[oaicite:2]{index=2}
    today = date.today()
    frm = today.strftime("%Y-%m-%d")
    to = (today + timedelta(days=400)).strftime("%Y-%m-%d")

    try:
        url = "https://financialmodelingprep.com/stable/earnings-calendar"
        data = _get(url, {"symbol": sym, "from": frm, "to": to, "apikey": api_key})
        if isinstance(data, list) and data:
            # pick nearest date (data is usually sorted; still be safe)
            dates = [row.get("date") for row in data if row.get("date")]
            dates = sorted(dates)
            if dates:
                return EarningsInfo(sym, dates[0], None, "fmp:stable/earnings-calendar")
        return EarningsInfo(sym, None, None, "fmp:stable/earnings-calendar", error="No rows returned")
    except Exception as e:
        return EarningsInfo(sym, None, None, None, error=str(e))
