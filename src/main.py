from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime

from src.config import load_config
from src.fmp_earnings import fetch_next_earnings
from src.skew_reader import get_latest_trade_date, load_skew_rows_for_date, pick_front_back
from src.ibkr_client import (
    connect_ib,
    fetch_underlying_daily_bars,
    fetch_spot_with_fallback,
    fetch_term_structure_live,
    fetch_atm_straddle_mid,
)
from src.underlying_store import init_db, upsert_bars, load_recent
from src.analytics import avg, realized_vol_annualized, term_slope_ratio
from src.csv_output import write_single_row_csv


def log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def pick_front_back_no_earnings(rows):
    rows_sorted = sorted(rows, key=lambda r: r.expiry)
    if len(rows_sorted) < 2:
        raise RuntimeError("Need >=2 expiries in skew DB to compute term structure")
    front = rows_sorted[0]

    from datetime import datetime as _dt
    def to_ord(exp):
        return _dt.strptime(exp, "%Y%m%d").date().toordinal()

    target = to_ord(front.expiry) + 30
    back = min(rows_sorted[1:], key=lambda r: abs(to_ord(r.expiry) - target))
    return front, back


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", required=True)
    args = ap.parse_args()
    symbol = args.symbol.upper()

    cfg = load_config()
    run_date = datetime.now().strftime("%Y-%m-%d")

    log(f"START Phase0 | symbol={symbol}")
    log(f"Skew DB (READ ONLY): {cfg.skew_db_path}")
    log(f"Underlying DB (OWNED): {cfg.underlying_db_path}")
    log(f"IBKR: {cfg.ib_host}:{cfg.ib_port} clientId={cfg.ib_client_id}")

    # -------------------------
    # 1) Earnings (FMP stable)
    # -------------------------
    log("Fetching next earnings from FMP...")
    earnings = fetch_next_earnings(symbol, cfg.fmp_api_key)
    if earnings.earnings_date:
        log(f"Earnings date: {earnings.earnings_date} (source={earnings.source})")
    else:
        log(f"[WARN] Earnings date unavailable. Details: {earnings.error}")

    # -------------------------
    # 2) Read latest skew snapshot (READ ONLY)
    # -------------------------
    log("Reading latest trade_date from skew DB...")
    conn = sqlite3.connect(cfg.skew_db_path)
    try:
        latest_td = get_latest_trade_date(conn, symbol)
    finally:
        conn.close()

    if not latest_td:
        log(f"[ERROR] No trade_date found in skew DB for {symbol}. Exiting.")
        return

    log(f"Latest skew trade_date for {symbol}: {latest_td}")
    log("Loading skew rows for that date...")
    skew_rows = load_skew_rows_for_date(cfg.skew_db_path, symbol, latest_td)
    log(f"Skew rows loaded: {len(skew_rows)}")

    # -------------------------
    # 3) IBKR connect (needed for bars/RV + (maybe) live IV)
    # -------------------------
    log("Connecting to IBKR...")
    ib = connect_ib(cfg.ib_host, cfg.ib_port, cfg.ib_client_id)

    # Defaults (so we can still write a CSV even if term structure fails)
    term_source = ""
    front_expiry = ""
    back_expiry = ""
    atm_strike_used = None
    iv_front = None
    iv_back = None
    slope = None
    ratio = None
    straddle_mid = None
    implied_move_pct = None
    quote_quality = ""
    spot = None
    spot_src = ""

    avg_vol_30 = None
    rv20 = None
    rv30 = None
    iv_rv20 = None
    iv_rv30 = None

    try:
        # -------------------------
        # 4) Underlying daily bars (for RV, avg volume, and spot fallback)
        # -------------------------
        log("Fetching underlying daily bars (for RV + volume + spot fallback)...")
        bars = fetch_underlying_daily_bars(ib, symbol, days=90)
        log(f"Underlying bars fetched: {len(bars)} (last={bars[-1].date} close={bars[-1].close} vol={bars[-1].volume})")

        log("Fetching spot (live -> delayed -> historical close fallback)...")
        spot, spot_src = fetch_spot_with_fallback(ib, symbol, bars_fallback=bars)
        log(f"Spot={spot} (source={spot_src})")

        # -------------------------
        # 5) Update underlying_daily DB
        # -------------------------
        log("Updating underlying_daily.sqlite with bars...")
        init_db(cfg.underlying_db_path)
        upsert_bars(cfg.underlying_db_path, symbol, bars)

        log("Loading recent underlying history for rolling metrics...")
        recent = load_recent(cfg.underlying_db_path, symbol, limit=120)
        closes = [float(r[1]) for r in recent]
        vols = [int(r[2]) for r in recent]
        log(f"Underlying rows in DB used: {len(recent)}")

        avg_vol_30 = avg([float(v) for v in vols[-30:]]) if len(vols) >= 30 else None
        rv20 = realized_vol_annualized(closes, 20)
        rv30 = realized_vol_annualized(closes, 30)

        log(f"avg_vol_30={avg_vol_30} | rv20={rv20} | rv30={rv30}")

        # -------------------------
        # 6) Term structure (prefer skew DB; fallback to live IBKR)
        # -------------------------
        if len(skew_rows) >= 2:
            log("Computing term structure from skew DB...")
            if earnings.earnings_date:
                front, back = pick_front_back(skew_rows, earnings.earnings_date)
            else:
                front, back = pick_front_back_no_earnings(skew_rows)

            front_expiry = front.expiry
            back_expiry = back.expiry
            atm_strike_used = float(front.atm_strike)
            iv_front = float(front.atm_iv)
            iv_back = float(back.atm_iv)
            term_source = "skew_db"

            slope, ratio = term_slope_ratio(iv_front, iv_back)
            log(f"Term slope={slope} | term ratio={ratio} (source={term_source})")

        else:
            term_source = "live_ibkr_greeks_snapshot"
            log("Skew DB has <2 expiries -> falling back to LIVE IBKR term structure.")
            log("NOTE: If market is closed, IB often returns no option IV/greeks on snapshot. In that case we'll write CSV with blank IV fields.")

            try:
                front_expiry, back_expiry, atm_strike_used, iv_front, iv_back = fetch_term_structure_live(
                    ib, symbol, earnings.earnings_date, spot
                )
                slope, ratio = term_slope_ratio(iv_front, iv_back)
                log(f"Live TS OK: front={front_expiry} back={back_expiry} strike={atm_strike_used} iv_front={iv_front} iv_back={iv_back}")
                log(f"Term slope={slope} | term ratio={ratio} (source={term_source})")
            except Exception as e:
                # Friendly: keep running, write CSV, but term structure fields will remain blank
                log(f"[WARN] Live term structure failed: {e}")
                log("[WARN] Likely causes: market closed, no option quotes/greeks available, or no permissions for option computations.")
                log("[WARN] Continuing without term structure. (You can re-run during market hours.)")

        # -------------------------
        # 7) IV/RV ratios (only if we have iv_front)
        # -------------------------
        if iv_front is not None and rv20 is not None and rv20 > 0:
            iv_rv20 = iv_front / rv20
        if iv_front is not None and rv30 is not None and rv30 > 0:
            iv_rv30 = iv_front / rv30

        log(f"iv_rv20={iv_rv20} | iv_rv30={iv_rv30}")

        # -------------------------
        # 8) Implied move (requires front expiry + strike)
        # -------------------------
        if front_expiry and atm_strike_used is not None:
            log(f"Fetching ATM straddle mid for expiry={front_expiry} strike={atm_strike_used} ...")
            straddle_mid, quote_quality = fetch_atm_straddle_mid(
                ib, symbol, front_expiry, float(atm_strike_used)
            )
            implied_move_pct = (straddle_mid / spot) if (straddle_mid and spot and spot > 0) else None
            log(f"straddle_mid={straddle_mid} | implied_move_pct={implied_move_pct} | quote_quality={quote_quality}")
        else:
            log("[WARN] Skipping straddle/implied move because front_expiry/ATM strike are unavailable.")

    finally:
        log("Disconnecting from IBKR...")
        ib.disconnect()

    # -------------------------
    # 9) Write CSV (always)
    # -------------------------
    log("Writing CSV output...")

    row = {
        "symbol": symbol,
        "run_date": run_date,

        "skew_trade_date_used": latest_td,

        "spot": round(spot, 4) if spot is not None else "",
        "spot_source": spot_src or "",

        "earnings_date": earnings.earnings_date or "",
        "earnings_time_hint": earnings.time_hint or "",
        "earnings_source": earnings.source or "",
        "earnings_error": earnings.error or "",

        "front_expiry": front_expiry,
        "back_expiry": back_expiry,
        "atm_strike_used": round(atm_strike_used, 4) if atm_strike_used is not None else "",

        "atm_iv_front": round(iv_front, 6) if iv_front is not None else "",
        "atm_iv_back": round(iv_back, 6) if iv_back is not None else "",

        "term_slope": round(slope, 6) if slope is not None else "",
        "term_ratio": round(ratio, 6) if ratio is not None else "",
        "term_structure_source": term_source or "",

        "atm_straddle_mid": round(straddle_mid, 6) if straddle_mid is not None else "",
        "implied_move_pct": round(implied_move_pct, 6) if implied_move_pct is not None else "",
        "quote_quality": quote_quality or "",

        "avg_vol_30": round(avg_vol_30, 2) if avg_vol_30 is not None else "",
        "rv20": round(rv20, 6) if rv20 is not None else "",
        "rv30": round(rv30, 6) if rv30 is not None else "",
        "iv_rv20": round(iv_rv20, 6) if iv_rv20 is not None else "",
        "iv_rv30": round(iv_rv30, 6) if iv_rv30 is not None else "",

        "rv_window_ok": "1" if (rv20 is not None and rv30 is not None) else "0",
        "vol_window_ok": "1" if (avg_vol_30 is not None) else "0",
        "term_structure_ok": "1" if (iv_front is not None and iv_back is not None) else "0",
        "implied_move_ok": "1" if (implied_move_pct is not None) else "0",
    }

    out_path = write_single_row_csv(cfg.out_dir, symbol, run_date, row)
    log(f"[OK] Wrote: {out_path}")
    log("DONE")


if __name__ == "__main__":
    main()