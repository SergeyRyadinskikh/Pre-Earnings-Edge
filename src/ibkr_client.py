from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Tuple, List
from datetime import datetime

from ib_insync import IB, Stock, Option, util


@dataclass(frozen=True)
class UnderlyingBar:
    date: str  # YYYY-MM-DD
    close: float
    volume: int


def connect_ib(host: str, port: int, client_id: int) -> IB:
    ib = IB()
    ib.connect(host, port, clientId=client_id, readonly=True, timeout=10)
    return ib


def _duration_str_from_days(days: int) -> str:
    """
    IBKR rule: >365 days must be requested in years (Error 321).
    We'll map:
      <=365  -> "XYZ D"
      >365   -> "N Y" rounded up (min 1Y)
    """
    d = int(max(days, 30))
    if d <= 365:
        return f"{d} D"
    years = (d + 364) // 365  # ceil
    return f"{max(years, 1)} Y"


def fetch_underlying_daily_bars(ib: IB, symbol: str, days: int = 60) -> List[UnderlyingBar]:
    stk = Stock(symbol.upper(), "SMART", "USD")
    ib.qualifyContracts(stk)

    duration = _duration_str_from_days(days)

    bars = ib.reqHistoricalData(
        stk,
        endDateTime="",
        durationStr=duration,
        barSizeSetting="1 day",
        whatToShow="TRADES",
        useRTH=True,
        formatDate=1,
        keepUpToDate=False,
    )

    out: List[UnderlyingBar] = []
    for b in bars:
        d = util.formatIBDatetime(b.date)[:10]
        out.append(UnderlyingBar(date=d, close=float(b.close), volume=int(b.volume)))

    if len(out) == 0:
        raise RuntimeError(f"IBKR returned no daily bars for {symbol} (durationStr={duration})")

    return out


def _valid_price(x: Optional[float]) -> bool:
    try:
        return x is not None and float(x) > 0
    except Exception:
        return False


def fetch_spot_with_fallback(
    ib: IB,
    symbol: str,
    bars_fallback: Optional[List[UnderlyingBar]] = None,
) -> Tuple[float, str]:
    sym = symbol.upper()
    stk = Stock(sym, "SMART", "USD")
    ib.qualifyContracts(stk)

    # 1) live snapshot
    ib.reqMarketDataType(1)  # LIVE
    t = ib.reqMktData(stk, "", snapshot=True, regulatorySnapshot=False)
    ib.sleep(1.5)

    spot = None
    try:
        mp = t.marketPrice()
        if _valid_price(mp):
            spot = float(mp)
    except Exception:
        pass

    if not spot and _valid_price(getattr(t, "last", None)):
        spot = float(t.last)

    ib.cancelMktData(stk)

    if spot:
        return spot, "snapshot_live"

    # 2) delayed snapshot
    ib.reqMarketDataType(4)  # DELAYED_FROZEN
    t2 = ib.reqMktData(stk, "", snapshot=True, regulatorySnapshot=False)
    ib.sleep(1.5)

    try:
        mp = t2.marketPrice()
        if _valid_price(mp):
            spot = float(mp)
    except Exception:
        pass

    if not spot and _valid_price(getattr(t2, "last", None)):
        spot = float(t2.last)

    if not spot and _valid_price(getattr(t2, "close", None)):
        spot = float(t2.close)

    ib.cancelMktData(stk)

    if spot:
        return spot, "snapshot_delayed"

    # 3) bars fallback
    if bars_fallback and len(bars_fallback) > 0:
        close = float(bars_fallback[-1].close)
        if close > 0:
            return close, "hist_close_fallback"

    raise RuntimeError(f"Could not determine spot for {sym} (live+delayed snapshots empty, no bars fallback)")


def _mid(bid: Optional[float], ask: Optional[float]) -> Optional[float]:
    if bid and ask and bid > 0 and ask > 0 and ask >= bid:
        return (bid + ask) / 2.0
    return None


def fetch_atm_straddle_mid(
    ib: IB,
    symbol: str,
    expiry_yyyymmdd: str,
    strike: float,
) -> Tuple[Optional[float], str]:
    sym = symbol.upper()
    call = Option(sym, expiry_yyyymmdd, strike, "C", "SMART")
    put = Option(sym, expiry_yyyymmdd, strike, "P", "SMART")
    ib.qualifyContracts(call, put)

    def _snap_pair(mkt_type: int):
        ib.reqMarketDataType(mkt_type)
        tc = ib.reqMktData(call, "", snapshot=True, regulatorySnapshot=False)
        tp = ib.reqMktData(put, "", snapshot=True, regulatorySnapshot=False)
        ib.sleep(2.0)
        return tc, tp

    tc, tp = _snap_pair(1)
    c_mid = _mid(tc.bid, tc.ask) or (float(tc.last) if tc.last and tc.last > 0 else None)
    p_mid = _mid(tp.bid, tp.ask) or (float(tp.last) if tp.last and tp.last > 0 else None)

    quality_parts = []
    quality_parts.append("C:mid" if _mid(tc.bid, tc.ask) else "C:last" if (tc.last and tc.last > 0) else "C:na")
    quality_parts.append("P:mid" if _mid(tp.bid, tp.ask) else "P:last" if (tp.last and tp.last > 0) else "P:na")

    ib.cancelMktData(call)
    ib.cancelMktData(put)

    if c_mid is None or p_mid is None:
        tc, tp = _snap_pair(4)
        c_mid = _mid(tc.bid, tc.ask) or (float(tc.last) if tc.last and tc.last > 0 else None)
        p_mid = _mid(tp.bid, tp.ask) or (float(tp.last) if tp.last and tp.last > 0 else None)
        quality_parts.append("retry:delayed")
        quality_parts.append("C2:mid" if _mid(tc.bid, tc.ask) else "C2:last" if (tc.last and tc.last > 0) else "C2:na")
        quality_parts.append("P2:mid" if _mid(tp.bid, tp.ask) else "P2:last" if (tp.last and tp.last > 0) else "P2:na")

        ib.cancelMktData(call)
        ib.cancelMktData(put)

    if c_mid is None or p_mid is None:
        return None, "|".join(quality_parts)

    return float(c_mid + p_mid), "|".join(quality_parts)


def _yyyymmdd_to_date(exp: str):
    return datetime.strptime(exp, "%Y%m%d").date()


def pick_expiries_from_chain(expirations: List[str], target_date_yyyy_mm_dd: Optional[str]) -> Tuple[str, str]:
    exps = sorted(expirations)
    if len(exps) < 2:
        raise RuntimeError("IBKR option chain has <2 expirations")

    if target_date_yyyy_mm_dd:
        tgt = datetime.strptime(target_date_yyyy_mm_dd, "%Y-%m-%d").date()
        eligible = [e for e in exps if _yyyymmdd_to_date(e) >= tgt]
        if len(eligible) >= 2:
            front = eligible[0]
            target = _yyyymmdd_to_date(front).toordinal() + 30
            back = min(eligible[1:], key=lambda e: abs(_yyyymmdd_to_date(e).toordinal() - target))
            return front, back

    front = exps[0]
    target = _yyyymmdd_to_date(front).toordinal() + 30
    back = min(exps[1:], key=lambda e: abs(_yyyymmdd_to_date(e).toordinal() - target))
    return front, back


def fetch_atm_iv_for_expiry(ib: IB, symbol: str, expiry_yyyymmdd: str, spot: float) -> Tuple[Optional[float], float]:
    """
    Snapshot-based IV from modelGreeks (no generic ticks).
    Try LIVE then DELAYED_FROZEN.
    """
    sym = symbol.upper()
    stk = Stock(sym, "SMART", "USD")
    ib.qualifyContracts(stk)

    chains = ib.reqSecDefOptParams(sym, "", "STK", stk.conId)
    if not chains:
        raise RuntimeError(f"reqSecDefOptParams returned empty for {sym}")

    chain = max(chains, key=lambda c: len(c.strikes))
    strikes = sorted([s for s in chain.strikes if s > 0])
    if not strikes:
        raise RuntimeError(f"No strikes for {sym} chain")

    atm_strike = float(min(strikes, key=lambda k: abs(k - spot)))
    opt = Option(sym, expiry_yyyymmdd, atm_strike, "C", "SMART")
    ib.qualifyContracts(opt)

    def _snap_iv(mkt_type: int) -> Optional[float]:
        ib.reqMarketDataType(mkt_type)
        t = ib.reqMktData(opt, "", snapshot=True, regulatorySnapshot=False)
        ib.sleep(2.0)
        iv = None
        if t.modelGreeks and t.modelGreeks.impliedVol and t.modelGreeks.impliedVol > 0:
            iv = float(t.modelGreeks.impliedVol)
        elif getattr(t, "impliedVolatility", None) and t.impliedVolatility > 0:
            iv = float(t.impliedVolatility)
        ib.cancelMktData(opt)
        return iv

    iv = _snap_iv(1) or _snap_iv(4)
    return iv, atm_strike


def fetch_term_structure_live(
    ib: IB,
    symbol: str,
    earnings_date_yyyy_mm_dd: Optional[str],
    spot: float,
) -> Tuple[str, str, float, float, float]:
    sym = symbol.upper()
    stk = Stock(sym, "SMART", "USD")
    ib.qualifyContracts(stk)

    chains = ib.reqSecDefOptParams(sym, "", "STK", stk.conId)
    if not chains:
        raise RuntimeError(f"reqSecDefOptParams returned empty for {sym}")

    chain = max(chains, key=lambda c: len(c.expirations))
    expirations = sorted(chain.expirations)
    front_exp, back_exp = pick_expiries_from_chain(expirations, earnings_date_yyyy_mm_dd)

    iv_front, atm_strike = fetch_atm_iv_for_expiry(ib, sym, front_exp, spot)
    iv_back, _ = fetch_atm_iv_for_expiry(ib, sym, back_exp, spot)

    if iv_front is None or iv_back is None:
        raise RuntimeError(
            f"Live IV snapshot returned None (market may be closed / no option greeks). "
            f"{sym}: front={front_exp} iv_front={iv_front} | back={back_exp} iv_back={iv_back}"
        )

    return front_exp, back_exp, float(atm_strike), float(iv_front), float(iv_back)
