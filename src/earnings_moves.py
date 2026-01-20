from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Dict, Tuple
import math

from src.underlying_store import load_all_dates_closes


@dataclass(frozen=True)
class EarningsEventMove:
    earnings_date: str                 # YYYY-MM-DD
    timing: str                        # "AMC" / "BMO" / "UNKNOWN"
    d_m1: Optional[str]                # trading date used for D-1 close
    d0: Optional[str]                  # trading date used for D0 close
    d_p1: Optional[str]                # trading date used for D+1 close
    close_dm1: Optional[float]
    close_d0: Optional[float]
    close_dp1: Optional[float]
    move_bmo_like_pct: Optional[float] # abs(D0/D-1 - 1)
    move_amc_like_pct: Optional[float] # abs(D+1/D0 - 1)
    move_used_pct: Optional[float]     # chosen based on timing or max if unknown
    used_window: Optional[str]         # "BMO_like" / "AMC_like" / None
    note: str                          # freeform quality note


@dataclass(frozen=True)
class EarningsMoveStats:
    n_events_total: int
    n_events_used: int
    mean_move: Optional[float]
    median_move: Optional[float]
    p75_move: Optional[float]
    max_move: Optional[float]
    implied_percentile_rank: Optional[float]  # 0..100 percentile of implied vs realized
    earnings_hist_ok: bool


def _parse_date(d: str):
    return datetime.strptime(d, "%Y-%m-%d").date()


def _percentile(sorted_vals: List[float], p: float) -> Optional[float]:
    """
    p in [0,1]. Linear interpolation.
    """
    if not sorted_vals:
        return None
    if p <= 0:
        return sorted_vals[0]
    if p >= 1:
        return sorted_vals[-1]
    n = len(sorted_vals)
    idx = (n - 1) * p
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return sorted_vals[lo]
    w = idx - lo
    return sorted_vals[lo] * (1 - w) + sorted_vals[hi] * w


def _median(sorted_vals: List[float]) -> Optional[float]:
    return _percentile(sorted_vals, 0.5)


def _nearest_trading_dates(dates_sorted: List[str], target: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Given trading dates sorted ascending (YYYY-MM-DD), and a calendar target date,
    return (D-1, D0, D+1) where:
      - D0 is the greatest trading date <= target (or None if target before first trading date)
      - D-1 is trading date immediately before D0
      - D+1 is trading date immediately after D0
    """
    if not dates_sorted:
        return None, None, None

    t = _parse_date(target)

    # Find rightmost date <= target
    idx = None
    for i in range(len(dates_sorted) - 1, -1, -1):
        if _parse_date(dates_sorted[i]) <= t:
            idx = i
            break
    if idx is None:
        return None, None, None

    d0 = dates_sorted[idx]
    dm1 = dates_sorted[idx - 1] if idx - 1 >= 0 else None
    dp1 = dates_sorted[idx + 1] if idx + 1 < len(dates_sorted) else None
    return dm1, d0, dp1


def compute_earnings_moves_from_db(
    underlying_db_path: str,
    symbol: str,
    earnings_dates_with_timing: List[Tuple[str, str]],  # [(date, timing)]
) -> List[EarningsEventMove]:
    """
    Uses underlying_daily.sqlite closes to compute realized moves around each earnings date.
    Requires underlying_daily to contain historical closes for those dates.
    """
    dates_sorted, close_by_date = load_all_dates_closes(underlying_db_path, symbol)

    out: List[EarningsEventMove] = []
    for edate, timing in earnings_dates_with_timing:
        dm1, d0, dp1 = _nearest_trading_dates(dates_sorted, edate)

        c_dm1 = close_by_date.get(dm1) if dm1 else None
        c_d0 = close_by_date.get(d0) if d0 else None
        c_dp1 = close_by_date.get(dp1) if dp1 else None

        move_bmo = None
        move_amc = None
        used = None
        used_window = None
        note_parts = []

        if c_dm1 and c_d0 and c_dm1 > 0 and c_d0 > 0:
            move_bmo = abs((c_d0 / c_dm1) - 1.0)
        else:
            note_parts.append("missing_bmo_inputs")

        if c_d0 and c_dp1 and c_d0 > 0 and c_dp1 > 0:
            move_amc = abs((c_dp1 / c_d0) - 1.0)
        else:
            note_parts.append("missing_amc_inputs")

        t = (timing or "UNKNOWN").upper()
        if t == "BMO":
            used = move_bmo
            used_window = "BMO_like"
        elif t == "AMC":
            used = move_amc
            used_window = "AMC_like"
        else:
            # Unknown timing: use whichever is larger (if both exist), else whichever exists
            if move_bmo is not None and move_amc is not None:
                if move_bmo >= move_amc:
                    used, used_window = move_bmo, "BMO_like"
                else:
                    used, used_window = move_amc, "AMC_like"
            elif move_bmo is not None:
                used, used_window = move_bmo, "BMO_like"
            elif move_amc is not None:
                used, used_window = move_amc, "AMC_like"
            else:
                used, used_window = None, None

        out.append(
            EarningsEventMove(
                earnings_date=edate,
                timing=t,
                d_m1=dm1,
                d0=d0,
                d_p1=dp1,
                close_dm1=c_dm1,
                close_d0=c_d0,
                close_dp1=c_dp1,
                move_bmo_like_pct=move_bmo,
                move_amc_like_pct=move_amc,
                move_used_pct=used,
                used_window=used_window,
                note="|".join(note_parts) if note_parts else "",
            )
        )

    return out


def summarize_earnings_moves(
    moves: List[EarningsEventMove],
    current_implied_move_pct: Optional[float],
    min_valid_events: int = 8,
) -> EarningsMoveStats:
    used_vals = [m.move_used_pct for m in moves if m.move_used_pct is not None]
    used_vals = [float(x) for x in used_vals if x is not None]
    used_vals.sort()

    n_total = len(moves)
    n_used = len(used_vals)

    mean_move = sum(used_vals) / n_used if n_used else None
    median_move = _median(used_vals)
    p75_move = _percentile(used_vals, 0.75)
    max_move = used_vals[-1] if used_vals else None

    implied_rank = None
    if current_implied_move_pct is not None and used_vals:
        # Percentile rank of implied relative to realized distribution:
        # fraction of realized <= implied
        le = sum(1 for x in used_vals if x <= current_implied_move_pct)
        implied_rank = 100.0 * (le / len(used_vals))

    ok = n_used >= min_valid_events

    return EarningsMoveStats(
        n_events_total=n_total,
        n_events_used=n_used,
        mean_move=mean_move,
        median_move=median_move,
        p75_move=p75_move,
        max_move=max_move,
        implied_percentile_rank=implied_rank,
        earnings_hist_ok=ok,
    )
