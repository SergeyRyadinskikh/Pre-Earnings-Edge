from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime
from pathlib import Path
import csv

from src.config import load_config
from src.fmp_earnings import fetch_earnings_bundle
from src.skew_reader import get_latest_trade_date, load_skew_rows_for_date, pick_front_back
from src.ibkr_client import (
    connect_ib,
    fetch_underlying_daily_bars,
    fetch_spot_with_fallback,
    fetch_term_structure_live,
    fetch_atm_straddle_mid,
)
from src.underlying_store import init_db, upsert_bars, load_recent, has_min_history
from src.analytics import avg, realized_vol_annualized, term_slope_ratio
from src.csv_output import write_single_row_csv
from src.earnings_moves import compute_earnings_moves_from_db, summarize_earnings_moves


def log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def _dated_out_dir(run_date_yyyy_mm_dd: str) -> str:
    """
    Returns: data/out/dd_mm_yyyy
    (No CSVs subfolder)
    """
    dt = datetime.strptime(run_date_yyyy_mm_dd, "%Y-%m-%d")
    ddmmyyyy = dt.strftime("%d_%m_%Y")
    return str(Path("data") / "out" / ddmmyyyy)


def _write_moves_csv(out_dir: str, symbol: str, run_date: str, moves) -> str:
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    out_path = Path(out_dir) / f"{symbol.upper()}_earnings_moves_{run_date}.csv"

    fieldnames = [
        "earnings_date", "timing",
        "d_m1", "d0", "d_p1",
        "close_dm1", "close_d0", "close_dp1",
        "move_bmo_like_pct", "move_amc_like_pct",
        "move_used_pct", "used_window",
        "note",
    ]

    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for m in moves:
            w.writerow({
                "earnings_date": m.earnings_date,
                "timing": m.timing,
                "d_m1": m.d_m1 or "",
                "d0": m.d0 or "",
                "d_p1": m.d_p1 or "",
                "close_dm1": "" if m.close_dm1 is None else round(m.close_dm1, 6),
                "close_d0": "" if m.close_d0 is None else round(m.close_d0, 6),
                "close_dp1": "" if m.close_dp1 is None else round(m.close_dp1, 6),
                "move_bmo_like_pct": "" if m.move_bmo_like_pct is None else round(m.move_bmo_like_pct, 6),
                "move_amc_like_pct": "" if m.move_amc_like_pct is None else round(m.move_amc_like_pct, 6),
                "move_used_pct": "" if m.move_used_pct is None else round(m.move_used_pct, 6),
                "used_window": m.used_window or "",
                "note": m.note or "",
            })

    return str(out_path)


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


def _parse_last_earnings_dates_arg(s: str):
    """
    Accepts: "YYYY-MM-DD,YYYY-MM-DD,..."
    Returns: [(date,"UNKNOWN"), ...] sorted ascending.
    """
    items = [x.strip() for x in (s or "").split(",") if x.strip()]
    out = [(d, "UNKNOWN") for d in items]
    out.sort(key=lambda x: x[0])
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", required=True)

    # Manual overrides (to bypass FMP 402 / missing data)
    ap.add_argument("--next-earnings-date", default="", help="Override next earnings date YYYY-MM-DD")
    ap.add_argument("--last-earnings-dates", default="", help="Comma list of past earnings dates YYYY-MM-DD,... (timing assumed UNKNOWN)")

    args = ap.parse_args()
    symbol = args.symbol.upper()

    cfg = load_config()
    run_date = datetime.now().strftime("%Y-%m-%d")
    out_dir = _dated_out_dir(run_date)

    log(f"START V2 | symbol={symbol}")
    log(f"Outputs folder: {out_dir}")
    log(f"Skew DB (READ ONLY): {cfg.skew_db_path}")
    log(f"Underlying DB (OWNED): {cfg.underlying_db_path}")
    log(f"IBKR: {cfg.ib_host}:{cfg.ib_port} clientId={cfg.ib_client_id}")

    # -------- Earnings bundle (FMP) --------
    # We'll still call it, but manual overrides can fully replace it.
    log("Fetching earnings bundle from FMP (stable)...")
    eb = fetch_earnings_bundle(symbol, cfg.fmp_api_key, n_last=12)

    # Apply overrides if provided
    if args.next_earnings_date.strip():
        eb = eb.__class__(
            symbol=eb.symbol,
            next_earnings_date=args.next_earnings_date.strip(),
            next_time_hint=eb.next_time_hint,
            last_earnings=eb.last_earnings,
            source=eb.source,
            error=eb.error,
        )

    if args.last_earnings_dates.strip():
        manual_last = _parse_last_earnings_dates_arg(args.last_earnings_dates.strip())
        eb = eb.__class__(
            symbol=eb.symbol,
            next_earnings_date=eb.next_earnings_date,
            next_time_hint=eb.next_time_hint,
            last_earnings=manual_last,
            source=eb.source,
            error=eb.error,
        )

    if eb.error:
        log(f"[WARN] Earnings source error: {eb.error}")
    log(f"Next earnings: {eb.next_earnings_date or ''} (source={eb.source})")
    log(f"Historical earnings count: {len(eb.last_earnings)}")

    # -------- Skew DB latest rows (read-only) --------
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
    skew_rows = load_skew_rows_for_date(cfg.skew_db_path, symbol, latest_td)
    log(f"Skew rows loaded: {len(skew_rows)}")

    # -------- Defaults so we can always write CSV --------
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

    moves_csv_path = ""
    earnings_stats = None

    # -------- IBKR work --------
    log("Connecting to IBKR...")
    ib = connect_ib(cfg.ib_host, cfg.ib_port, cfg.ib_client_id)
    try:
        init_db(cfg.underlying_db_path)

        if has_min_history(cfg.underlying_db_path, symbol, min_rows=800):
            log("Underlying DB already has substantial history; fetching last ~140 days for refresh...")
            bars = fetch_underlying_daily_bars(ib, symbol, days=140)
        else:
            log("Underlying DB has limited history; fetching ~5 years of daily bars (one-time build-up)...")
            bars = fetch_underlying_daily_bars(ib, symbol, days=365 * 5)

        log(f"Fetched bars: {len(bars)} (last={bars[-1].date} close={bars[-1].close})")
        upsert_bars(cfg.underlying_db_path, symbol, bars)

        log("Fetching spot (live -> delayed -> historical close fallback)...")
        spot, spot_src = fetch_spot_with_fallback(ib, symbol, bars_fallback=bars)
        log(f"Spot={spot} (source={spot_src})")

        recent = load_recent(cfg.underlying_db_path, symbol, limit=150)
        closes = [float(r[1]) for r in recent]
        vols = [int(r[2]) for r in recent]
        avg_vol_30 = avg([float(v) for v in vols[-30:]]) if len(vols) >= 30 else None
        rv20 = realized_vol_annualized(closes, 20)
        rv30 = realized_vol_annualized(closes, 30)

        # -------- Term structure (DB first, else live) --------
        if len(skew_rows) >= 2 and eb.next_earnings_date:
            log("Computing term structure from skew DB using earnings anchor...")
            front, back = pick_front_back(skew_rows, eb.next_earnings_date)
            front_expiry = front.expiry
            back_expiry = back.expiry
            atm_strike_used = float(front.atm_strike)
            iv_front = float(front.atm_iv)
            iv_back = float(back.atm_iv)
            term_source = "skew_db"
        elif len(skew_rows) >= 2:
            log("Computing term structure from skew DB (no earnings anchor)...")
            front, back = pick_front_back_no_earnings(skew_rows)
            front_expiry = front.expiry
            back_expiry = back.expiry
            atm_strike_used = float(front.atm_strike)
            iv_front = float(front.atm_iv)
            iv_back = float(back.atm_iv)
            term_source = "skew_db"
        else:
            term_source = "live_ibkr_greeks_snapshot"
            log("Skew DB has <2 expiries -> LIVE IBKR fallback for term structure.")
            try:
                front_expiry, back_expiry, atm_strike_used, iv_front, iv_back = fetch_term_structure_live(
                    ib, symbol, eb.next_earnings_date, spot
                )
            except Exception as e:
                log(f"[WARN] Live term structure failed (often market closed): {e}")

        if iv_front is not None and iv_back is not None:
            slope, ratio = term_slope_ratio(iv_front, iv_back)
        log(f"Term: slope={slope} ratio={ratio} (source={term_source})")

        if iv_front is not None and rv20 is not None and rv20 > 0:
            iv_rv20 = iv_front / rv20
        if iv_front is not None and rv30 is not None and rv30 > 0:
            iv_rv30 = iv_front / rv30

        # -------- Implied move (needs option quotes) --------
        if front_expiry and atm_strike_used is not None:
            log(f"Fetching ATM straddle mid for implied move (expiry={front_expiry} strike={atm_strike_used})...")
            straddle_mid, quote_quality = fetch_atm_straddle_mid(ib, symbol, front_expiry, float(atm_strike_used))
            if straddle_mid and spot and spot > 0:
                implied_move_pct = float(straddle_mid) / float(spot)
        else:
            log("[WARN] Skipping straddle/implied move (missing front_expiry or atm_strike).")

    finally:
        log("Disconnecting from IBKR...")
        ib.disconnect()

    # -------- Historical earnings move percentile --------
    log("Computing historical earnings moves from underlying DB...")
    moves = compute_earnings_moves_from_db(cfg.underlying_db_path, symbol, eb.last_earnings)
    moves_csv_path = _write_moves_csv(out_dir, symbol, run_date, moves)
    log(f"[OK] Wrote moves CSV: {moves_csv_path}")

    earnings_stats = summarize_earnings_moves(moves, implied_move_pct, min_valid_events=8)

    log(
        f"Earnings move stats: used={earnings_stats.n_events_used}/{earnings_stats.n_events_total} "
        f"mean={earnings_stats.mean_move} median={earnings_stats.median_move} p75={earnings_stats.p75_move} max={earnings_stats.max_move} "
        f"implied_rank={earnings_stats.implied_percentile_rank}"
    )

    # -------- Write summary CSV --------
    log("Writing summary CSV...")
    row = {
        "symbol": symbol,
        "run_date": run_date,

        "skew_trade_date_used": latest_td,

        "spot": round(spot, 4) if spot is not None else "",
        "spot_source": spot_src or "",

        "next_earnings_date": eb.next_earnings_date or "",
        "next_earnings_time_hint": eb.next_time_hint or "",
        "earnings_source": eb.source or "",
        "earnings_error": eb.error or "",

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

        "earnings_n_total": earnings_stats.n_events_total,
        "earnings_n_used": earnings_stats.n_events_used,
        "earnings_move_mean": round(earnings_stats.mean_move, 6) if earnings_stats.mean_move is not None else "",
        "earnings_move_median": round(earnings_stats.median_move, 6) if earnings_stats.median_move is not None else "",
        "earnings_move_p75": round(earnings_stats.p75_move, 6) if earnings_stats.p75_move is not None else "",
        "earnings_move_max": round(earnings_stats.max_move, 6) if earnings_stats.max_move is not None else "",
        "implied_percentile_rank": round(earnings_stats.implied_percentile_rank, 2) if earnings_stats.implied_percentile_rank is not None else "",
        "moves_csv_path": moves_csv_path,

        "vol_window_ok": "1" if avg_vol_30 is not None else "0",
        "rv_window_ok": "1" if (rv20 is not None and rv30 is not None) else "0",
        "term_structure_ok": "1" if (iv_front is not None and iv_back is not None) else "0",
        "implied_move_ok": "1" if (implied_move_pct is not None) else "0",
        "earnings_hist_ok": "1" if earnings_stats.earnings_hist_ok else "0",
    }

    out_path = write_single_row_csv(out_dir, symbol, run_date, row)
    log(f"[OK] Wrote summary CSV: {out_path}")
    log("DONE")


if __name__ == "__main__":
    main()
