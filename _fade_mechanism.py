"""FADE MECHANISM — pourquoi le fade-soutenu PERD Jan-Fev (-7..-18 bps) et GAGNE Mar-Mai (+2..+16) ?

HYPOTHESE : en mois DOWNTREND, la jambe LONG-dip (acheter le creux dans un marche qui tombe)
attrape des couteaux qui tombent et saigne ; la jambe SHORT-rip tient. La regime-dependence
= le DRIFT macro qui tue la jambe qui le combat. On mesure aussi si la REVERSION conditionnelle
apres trigger fade faiblit/s'inverse en mois de trend.

LE FADE (les 2 jambes parient sur reversion) :
  SHORT-rip  : drift_30>0 & drift_8h>0  (d30>=p75)  -> on SHORT le rip soutenu
  LONG-dip   : drift_30<0 & drift_8h<0  (d30<=p25)  -> on LONG le dip soutenu

Filtre commun : vol_30min>=P75 & d30 extreme & d8 MEME signe ; rising-edge ; no-overlap 8h.
Hold 8h, net 3 bps. Par MOIS (Jan,Fev,Mar,Avr) + fenetre daily MAI. LONG vs SHORT split OBLIGATOIRE.

Sorties par mois x jambe :
  (a) net bps/trade @ 8h (net 3 bps) + n
  (b) reversion conditionnelle E[retour fwd EN direction du pari] @ 1h/2h/4h/8h
  (c) drift net du marche (le trend) + MAE mediane (max adverse excursion sur le hold 8h)
"""
import os, zipfile, time
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

BINANCE_DIR = r"C:\Users\erast\hl_data\binance"
DAILY_DIR = r"C:\Users\erast\hl_data\binance\daily"
MIN = 60 * 1000
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]
MONTHS = ["2026-01", "2026-02", "2026-03", "2026-04"]
# May daily window (two segments per spec: 04-30..05-07 and 05-15..05-22)
MAY_DAYS = ["2026-04-30", "2026-05-01", "2026-05-02", "2026-05-03", "2026-05-04",
            "2026-05-05", "2026-05-06", "2026-05-07",
            "2026-05-15", "2026-05-16", "2026-05-17", "2026-05-18", "2026-05-19",
            "2026-05-20", "2026-05-21", "2026-05-22"]
MAY_LABEL = "2026-05"
FEE = 3.0
HOLD_MIN = 480          # 8h hold for the trade PnL
HORIZONS = [60, 120, 240, 480]  # conditional reversion horizons


# ---------- VERBATIM from _binance_quadrant.py ----------
def load(zp):
    with zipfile.ZipFile(zp) as z:
        name = z.namelist()[0]
        with z.open(name) as f:
            first = f.readline().decode("utf-8", "ignore")
        hh = "price" in first.lower()
        with z.open(name) as f:
            df = pd.read_csv(f, header=0 if hh else None)
    price = df.iloc[:, 1].values.astype(np.float64)
    T = df.iloc[:, 5].values.astype(np.int64)
    m = df.iloc[:, 6].astype(str).str.lower().values
    o = np.argsort(T, kind="stable")
    T = T[o]; price = price[o]; m = m[o]
    ibt = (m == "false")
    ask = pd.Series(np.where(ibt, price, np.nan)).ffill().values
    bid = pd.Series(np.where(~ibt, price, np.nan)).ffill().values
    mid = (ask + bid) / 2.0
    ok = ~np.isnan(mid)
    return T[ok], mid[ok]


def drift(ts, mid, W):
    p = np.searchsorted(ts, ts - W, side="left")
    d = (mid - mid[p]) / mid[p] * 1e4
    return np.where((ts - ts[p]) >= 0.5 * W, d, np.nan)


def vol30(ts, mid):
    n = len(ts); lm = np.log(mid); lr = np.diff(lm, prepend=lm[0]) * 1e4
    C1 = np.concatenate([[0], np.cumsum(lr)]); C2 = np.concatenate([[0], np.cumsum(lr * lr)])
    j = np.searchsorted(ts, ts - 30 * MIN, side="left"); cnt = np.arange(n) - j
    s1 = C1[np.arange(n) + 1] - C1[j + 1]; s2 = C2[np.arange(n) + 1] - C2[j + 1]
    with np.errstate(invalid="ignore", divide="ignore"):
        mean = s1 / cnt; v = np.sqrt(np.maximum(s2 / cnt - mean * mean, 0))
    v[cnt < 10] = np.nan
    return v


def no_overlap(mask, ts):
    idx = np.where(mask & ~np.concatenate([[False], mask[:-1]]))[0]
    out = []; last = -np.inf
    for i in idx:
        if ts[i] - last >= 8 * 60 * MIN:
            out.append(i); last = ts[i]
    return np.array(out, dtype=int)


def fwd(ts, mid, idx, H_ms, is_long):
    if len(idx) == 0:
        return np.array([])
    j = np.searchsorted(ts, ts[idx] + H_ms, side="left")
    ok = j < len(ts)
    d = np.full(len(idx), np.nan)
    d[ok] = (mid[j[ok]] - mid[idx[ok]]) / mid[idx[ok]] * 1e4
    return d if is_long else -d


# ---------- NEW: max adverse excursion over the hold path ----------
def mae_path(ts, mid, idx, H_ms, is_long):
    """Max ADVERSE excursion (bps, positive = adverse) over [entry, entry+H] for each trade.
    LONG : adverse = mid goes DOWN  -> mae = (entry - min_mid)/entry
    SHORT: adverse = mid goes UP    -> mae = (max_mid - entry)/entry
    Returns NaN if the hold window runs off the end of the series.
    """
    if len(idx) == 0:
        return np.array([])
    jend = np.searchsorted(ts, ts[idx] + H_ms, side="left")
    out = np.full(len(idx), np.nan)
    n = len(ts)
    for k in range(len(idx)):
        i = idx[k]; je = jend[k]
        if je >= n or je <= i:
            continue
        seg = mid[i:je + 1]
        entry = mid[i]
        if is_long:
            adverse = (entry - seg.min()) / entry * 1e4
        else:
            adverse = (seg.max() - entry) / entry * 1e4
        out[k] = max(adverse, 0.0)
    return out


# ---------- May loader: concat daily zips per symbol into one continuous series ----------
def load_may(sym):
    parts_ts = []; parts_mid = []
    for day in MAY_DAYS:
        zp = os.path.join(DAILY_DIR, f"{sym}-aggTrades-{day}.zip")
        if not os.path.exists(zp):
            continue
        t, mid = load(zp)
        parts_ts.append(t); parts_mid.append(mid)
    if not parts_ts:
        return np.array([], dtype=np.int64), np.array([], dtype=np.float64)
    ts = np.concatenate(parts_ts); mid = np.concatenate(parts_mid)
    o = np.argsort(ts, kind="stable")
    return ts[o], mid[o]


# FADE legs: (d30 condition, d8 condition, is_long)
LEGS = {
    "SHORT_rip": ("hi", "up", False),   # d30>=p75 & d8>0 -> SHORT the sustained rip
    "LONG_dip":  ("lo", "dn", True),    # d30<=p25 & d8<0 -> LONG the sustained dip
}


def process_period(label, ts, mid, agg):
    """Compute fade legs for one (symbol, period) and accumulate into agg.
    agg[label][leg]['pnl']     -> list of net-8h-hold bps per trade
    agg[label][leg]['rev'][H]  -> list of fwd-in-bet-direction bps @ horizon H
    agg[label][leg]['mae']     -> list of MAE bps over 8h hold
    agg[label]['mkt_drift']    -> list of (sym total drift bps) for market-trend readout
    agg[label]['n_pts']        -> list of n ticks (for context)
    """
    n = len(ts)
    if n < 5000:
        return
    v = vol30(ts, mid)
    d30 = drift(ts, mid, 30 * MIN)
    d8 = drift(ts, mid, 480 * MIN)
    val = ~(np.isnan(v) | np.isnan(d30) | np.isnan(d8))
    if val.sum() < 1000:
        return
    vP = np.nanpercentile(v[val], 75)
    p25 = np.nanpercentile(d30[val], 25)
    p75 = np.nanpercentile(d30[val], 75)
    hv = val & (v >= vP)
    cond30 = {"hi": d30 >= p75, "lo": d30 <= p25}
    cond8 = {"up": d8 > 0, "dn": d8 < 0}

    # market net drift over the whole period (the trend) in bps, first->last mid
    mkt = (mid[-1] - mid[0]) / mid[0] * 1e4
    agg[label].setdefault("mkt_drift", []).append(mkt)
    agg[label].setdefault("n_pts", []).append(n)

    line = [f"{label} {agg['_sym']}"]
    for leg, (c3, c8, isl) in LEGS.items():
        mask = hv & cond30[c3] & cond8[c8]
        idx = no_overlap(mask, ts)
        line.append(f"{leg}={len(idx)}")
        # trade PnL @ 8h hold, in bet direction (gross; fee applied at report)
        pnl = fwd(ts, mid, idx, HOLD_MIN * MIN, isl)
        pnl = pnl[~np.isnan(pnl)]
        agg[label][leg]["pnl"].extend(pnl.tolist())
        # conditional reversion per horizon
        for H in HORIZONS:
            f = fwd(ts, mid, idx, H * MIN, isl)
            f = f[~np.isnan(f)]
            agg[label][leg]["rev"][H].extend(f.tolist())
        # MAE over the 8h hold
        m = mae_path(ts, mid, idx, HOLD_MIN * MIN, isl)
        m = m[~np.isnan(m)]
        agg[label][leg]["mae"].extend(m.tolist())
    print("  " + " | ".join(line), flush=True)


def new_agg_label():
    return {leg: {"pnl": [], "rev": {H: [] for H in HORIZONS}, "mae": []} for leg in LEGS}


def main():
    t0 = time.time()
    print("=== FADE MECHANISM : LONG-dip vs SHORT-rip, par mois (Binance OOS, 4 majors, tick-level, ZERO sampling) ===\n")
    print(f"hold={HOLD_MIN}min  fee={FEE}bps  legs: SHORT_rip(d30>=p75 & d8>0), LONG_dip(d30<=p25 & d8<0)\n")

    labels = MONTHS + [MAY_LABEL]
    agg = {lab: new_agg_label() for lab in labels}

    # monthly files
    for sym in SYMBOLS:
        agg["_sym"] = sym
        for mo in MONTHS:
            zp = os.path.join(BINANCE_DIR, f"{sym}-aggTrades-{mo}.zip")
            if not os.path.exists(zp):
                continue
            ts, mid = load(zp)
            process_period(mo, ts, mid, agg)
        # May daily window concatenated
        ts, mid = load_may(sym)
        if len(ts):
            process_period(MAY_LABEL, ts, mid, agg)

    # ============ REPORT ============
    def stats(arr, fee=0.0):
        a = np.asarray(arr, dtype=np.float64) - fee
        if len(a) == 0:
            return (0, float('nan'), float('nan'))
        mean = a.mean()
        t = mean / (a.std(ddof=1) / np.sqrt(len(a))) if len(a) > 1 else float('nan')
        return (len(a), mean, t)

    print("\n" + "=" * 100)
    print("TABLE 1 — FADE NET bps/trade @ 8h hold (net 3 bps), LONG-dip vs SHORT-rip, PER MONTH")
    print("  perte si net<0 ; gagne si net>0.  mkt_drift = trend du marche sur la periode (moyenne 4 majors)")
    print("=" * 100)
    hdr = f"  {'period':>8} | {'mkt_drift':>9} | {'LONG_dip n':>10} {'net':>7} {'t':>6} | {'SHORT_rip n':>11} {'net':>7} {'t':>6} | {'COMBINED net':>12}"
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))
    for lab in labels:
        md = np.mean(agg[lab].get("mkt_drift", [np.nan]))
        nL, mL, tL = stats(agg[lab]["LONG_dip"]["pnl"], FEE)
        nS, mS, tS = stats(agg[lab]["SHORT_rip"]["pnl"], FEE)
        comb = agg[lab]["LONG_dip"]["pnl"] + agg[lab]["SHORT_rip"]["pnl"]
        nC, mC, tC = stats(comb, FEE)
        print(f"  {lab:>8} | {md:>+9.0f} | {nL:>10d} {mL:>+7.1f} {tL:>+6.1f} | "
              f"{nS:>11d} {mS:>+7.1f} {tS:>+6.1f} | {mC:>+7.1f} (n={nC})")

    print("\n" + "=" * 100)
    print("TABLE 2 — CONDITIONAL REVERSION : E[retour fwd EN direction du pari] (bps, GROSS), PER MONTH x LEG")
    print("  >0 = le marche reverse en notre faveur (reversion) ; <0 = continuation (le trend continue, on saigne)")
    print("=" * 100)
    for leg in LEGS:
        print(f"\n  --- {leg} ---")
        print(f"    {'period':>8} | {'n':>6} | " + " | ".join(f"{'+'+str(H)+'m':>8}" for H in HORIZONS))
        for lab in labels:
            rev = agg[lab][leg]["rev"]
            n0 = len(rev[HORIZONS[0]])
            cells = []
            for H in HORIZONS:
                a = np.asarray(rev[H], dtype=np.float64)
                cells.append(f"{a.mean():>+8.1f}" if len(a) >= 5 else f"{'·':>8}")
            print(f"    {lab:>8} | {n0:>6d} | " + " | ".join(cells))

    print("\n" + "=" * 100)
    print("TABLE 3 — MEDIAN MAE (max adverse excursion sur le hold 8h, bps), PER MONTH x LEG  +  mkt_drift")
    print("  knife-catching => MAE PROFONDE sur LONG_dip dans les mois downtrend (mkt_drift<0)")
    print("=" * 100)
    hdr3 = f"  {'period':>8} | {'mkt_drift':>9} | {'LONG_dip medMAE':>15} {'(n)':>7} | {'SHORT_rip medMAE':>16} {'(n)':>7}"
    print(hdr3)
    print("  " + "-" * (len(hdr3) - 2))
    for lab in labels:
        md = np.mean(agg[lab].get("mkt_drift", [np.nan]))
        aL = np.asarray(agg[lab]["LONG_dip"]["mae"], dtype=np.float64)
        aS = np.asarray(agg[lab]["SHORT_rip"]["mae"], dtype=np.float64)
        medL = np.median(aL) if len(aL) else float('nan')
        medS = np.median(aS) if len(aS) else float('nan')
        print(f"  {lab:>8} | {md:>+9.0f} | {medL:>15.1f} {len(aL):>7d} | {medS:>16.1f} {len(aS):>7d}")

    # also dump per-symbol market drift for transparency
    print("\n" + "=" * 100)
    print("APPENDIX — market net drift per period (bps), individual majors (trend direction)")
    print("=" * 100)
    # recompute per-sym drift display: we only stored the mean; re-list raw values
    for lab in labels:
        vals = agg[lab].get("mkt_drift", [])
        print(f"  {lab:>8} : " + "  ".join(f"{x:>+8.0f}" for x in vals) + f"   (mean {np.mean(vals):+.0f})" if vals else f"  {lab:>8} : -")

    print(f"\n[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
