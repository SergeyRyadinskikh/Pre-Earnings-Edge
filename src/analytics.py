from __future__ import annotations

import math
from typing import Optional, Sequence, Tuple, List


def avg(values: Sequence[float]) -> Optional[float]:
    if not values:
        return None
    return sum(values) / len(values)


def realized_vol_annualized(closes: Sequence[float], window: int) -> Optional[float]:
    if len(closes) < window + 1:
        return None
    rets = []
    for i in range(-window, 0):
        c0 = closes[i - 1]
        c1 = closes[i]
        if c0 <= 0 or c1 <= 0:
            return None
        rets.append(math.log(c1 / c0))
    if len(rets) < 2:
        return None
    m = sum(rets) / len(rets)
    var = sum((x - m) ** 2 for x in rets) / (len(rets) - 1)
    return math.sqrt(var) * math.sqrt(252)


def term_slope_ratio(iv_front: float, iv_back: float) -> Tuple[Optional[float], Optional[float]]:
    if iv_front is None or iv_back is None:
        return None, None
    if iv_back <= 0:
        return None, None
    return iv_front - iv_back, iv_front / iv_back


def percentile(sorted_vals: List[float], p: float) -> Optional[float]:
    """
    p in [0,1], linear interpolation. sorted_vals must be sorted ascending.
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
