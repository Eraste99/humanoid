"""FADE-SUSTAINED x REGIME MAP — weekly granularity (Binance OOS, 4 majors, Jan->May 2026, tick-level, ZERO sampling).

GOAL: explain WHY the fade-sustained mean-reversion strategy was profitable in some 2026 periods
and lost in others. Monthly fade net was a MONOTONE gradient (Jan -18 -> May +10..+16 bps/trade) =>
regime evolution. We map weekly fade PnL against quantitative REGIME descriptors computed from each
week's own tick path, find the regime axis that explains it, quartile-bucket + correlate, and print
the weekly Jan->May time series.

THE FADE (enters AGAINST a sustained extended move):
  vol_30min >= P75  AND  drift_30min in extreme tail (>=P75 => SHORT the rip, <=P25 => LONG the dip)
  AND  drift_8h SAME SIGN as drift_30min  (i.e. the 30min spike is WITH the 8h trend = sustained move)
  -> SHORT a sustained rip / LONG a sustained dip.
  Rising-edge detection + no-overlap 8h. PnL/trade = forward return in fade direction over a fixed
  8h hold, NET of 3 bps (maker-maker, 1.5+1.5). Percentile thresholds (vol P75, drift P25/P75)
  calibrated PER ASSET-MONTH (May = all available May daily files concatenated as one "month").

Functions load/drift/vol30/no_overlap/fwd reused VERBATIM from _binance_quadrant.py.

REGIME DESCRIPTORS per (asset, week), from that week's tick path:
  net_drift_bps  = signed total log return of the week (bps)
  abs_drift_bps  = |net_drift_bps|
  trend_persist  = |sum hourly rets| / sum|hourly rets|  (efficiency of the week, 0..1)
  rvol_bps       = realized vol = std of 1-min log returns (bps), the week's micro vol
  vr_1min        = variance ratio: Var(k-min ret)/(k * Var(1-min ret)), k=15. >1 trending, <1 mean-revert
  kaufman_er     = Kaufman efficiency ratio on 1-min closes: |close_last-close_first| / sum|1-min moves|

OUTPUT:
  - per (asset, week) table: fade net bps/trade, n, + all regime descriptors
  - quartile buckets of weekly fade PnL by each regime descriptor (pooled across assets) + Pearson &
    Spearman correlation (n-weighted and unweighted) -> which variable best & most monotonically explains
  - weekly time series Jan->May (fade PnL + best regime variable) to show the monotone evolution
"""
import os, sys, zipfile, time, datetime
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings('ignore')
# force UTF-8 stdout so redirect to a file on a cp1252 console can't crash on any glyph
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

BINANCE_DIR = r"C:\Users\erast\hl_data\binance"
DAILY_DIR = os.path.join(BINANCE_DIR, "daily")
MIN = 60 * 1000
HOUR = 60 * MIN
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]
MONTHS = ["2026-01", "2026-02", "2026-03", "2026-04"]  # monthly zips
MAY_DAYS = ["2026-04-30", "2026-05-01", "2026-05-02", "2026-05-03", "2026-05-04",
            "2026-05-05", "2026-05-06", "2026-05-07",
            "2026-05-15", "2026-05-16", "2026-05-17", "2026-05-18",
            "2026-05-19", "2026-05-20", "2026-05-21", "2026-05-22"]
FEE = 3.0
HOLD_MIN = 480          # 8h fixed hold
NO_OVERLAP_MS = 8 * HOUR


# ---------------- VERBATIM from _binance_quadrant.py ----------------
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


# ---------------- helpers ----------------
def iso_week_key(ts_ms):
    """Map an epoch-ms timestamp to an ISO (year, week) -> 'YYYY-Www' string (UTC)."""
    dt = datetime.datetime.utcfromtimestamp(ts_ms / 1000.0)
    iso = dt.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"


def week_start_ms(week_key):
    """Monday 00:00 UTC of an ISO week -> epoch ms (for chronological sort & labeling)."""
    yr = int(week_key[:4]); wk = int(week_key[6:])
    monday = datetime.date.fromisocalendar(yr, wk, 1)
    dt = datetime.datetime(monday.year, monday.month, monday.day, tzinfo=datetime.timezone.utc)
    return int(dt.timestamp() * 1000)


def monday_ms_vec(ts):
    """VECTORIZED Monday-00:00-UTC-of-week (epoch ms) for every tick — replaces the per-tick datetime
    call that was O(N) Python (44M-84M datetime conversions/file = the hang). Epoch day0=Thursday;
    Monday=0 via (days+3)%7. Weekly grouping is IDENTICAL to ISO weeks (both Monday-aligned UTC);
    only the computation changes (pure int64 ops, O(N) vectorized). Returns int64 ms array."""
    days = ts // 86400000
    dow = (days + 3) % 7            # Monday=0 (epoch 1970-01-01 = Thursday)
    return (days - dow) * 86400000


def minute_closes(ts, mid):
    """Last mid in each 1-min bucket -> (bucket_index, close). ZERO sampling: uses every tick to form bars."""
    b = (ts // MIN).astype(np.int64)
    # last tick per bucket: since ts sorted, the last occurrence per bucket is the close
    # find boundaries where bucket changes
    chg = np.concatenate([np.diff(b) != 0, [True]])
    return b[chg], mid[chg]


def hourly_closes(ts, mid):
    b = (ts // HOUR).astype(np.int64)
    chg = np.concatenate([np.diff(b) != 0, [True]])
    return b[chg], mid[chg]


def regime_descriptors(ts, mid):
    """Compute regime descriptors from a week's tick path. Returns dict or None if too sparse."""
    if len(ts) < 200:
        return None
    # --- minute closes ---
    mb, mc = minute_closes(ts, mid)
    if len(mc) < 30:
        return None
    lmc = np.log(mc)
    r1 = np.diff(lmc) * 1e4  # 1-min log returns (bps), only across consecutive *present* minutes
    if len(r1) < 20:
        return None
    # net signed drift over the week (bps) = total log return
    net_drift = (lmc[-1] - lmc[0]) * 1e4
    abs_drift = abs(net_drift)
    # realized vol = std of 1-min log returns (bps)
    rvol = float(np.std(r1, ddof=1))
    # Kaufman efficiency ratio on minute closes: |net move| / sum|1-min moves|
    denom_k = float(np.sum(np.abs(r1)))
    kaufman_er = abs(net_drift) / denom_k if denom_k > 1e-9 else np.nan
    # --- hourly returns for trend persistence ---
    hb, hc = hourly_closes(ts, mid)
    if len(hc) >= 3:
        lhc = np.log(hc)
        rh = np.diff(lhc) * 1e4
        denom_h = float(np.sum(np.abs(rh)))
        trend_persist = abs(float(np.sum(rh))) / denom_h if denom_h > 1e-9 else np.nan
    else:
        trend_persist = np.nan
    # --- variance ratio (k=15 min) on contiguous minute grid ---
    # build a *contiguous* minute series by forward-filling missing minutes within the week span,
    # so k-step sums are well defined. (ffill of close = flat across gaps; conservative.)
    full_idx = np.arange(mb[0], mb[-1] + 1)
    s = pd.Series(index=mb, data=mc).reindex(full_idx).ffill()
    lf = np.log(s.values)
    rr1 = np.diff(lf) * 1e4
    k = 15
    if len(rr1) > 5 * k:
        var1 = np.var(rr1, ddof=1)
        # non-overlapping k-min returns
        m = (len(rr1) // k) * k
        rk = lf[k:m + 1:k] - lf[0:m - k + 1:k]
        rk = rk * 1e4
        vark = np.var(rk, ddof=1)
        vr = (vark / (k * var1)) if var1 > 1e-12 else np.nan
    else:
        vr = np.nan
    return dict(net_drift=net_drift, abs_drift=abs_drift, trend_persist=trend_persist,
                rvol=rvol, vr_1min=vr, kaufman_er=kaufman_er, n_min=len(r1))


def fade_entries(ts, mid):
    """Return (idx_short, idx_long): rising-edge no-overlap fade entries over the WHOLE series.
    Percentiles calibrated on this whole series (per asset-month / per asset-May)."""
    v = vol30(ts, mid)
    d30 = drift(ts, mid, 30 * MIN)
    d8 = drift(ts, mid, 480 * MIN)
    val = ~(np.isnan(v) | np.isnan(d30) | np.isnan(d8))
    if val.sum() < 1000:
        return np.array([], int), np.array([], int)
    vP = np.nanpercentile(v[val], 75)
    p25 = np.nanpercentile(d30[val], 25)
    p75 = np.nanpercentile(d30[val], 75)
    hv = val & (v >= vP)
    # SHORT the rip: d30>=p75 & d8>0 (same sign, up); LONG the dip: d30<=p25 & d8<0 (same sign, down)
    mask_short = hv & (d30 >= p75) & (d8 > 0)
    mask_long = hv & (d30 <= p25) & (d8 < 0)
    idx_short = no_overlap(mask_short, ts)
    idx_long = no_overlap(mask_long, ts)
    return idx_short, idx_long


def build_series_for_period(sym, period):
    """period is a month string ('2026-01'..) -> load monthly zip; or 'MAY' -> concat May daily files."""
    if period == "MAY":
        parts_ts = []; parts_mid = []
        for d in MAY_DAYS:
            zp = os.path.join(DAILY_DIR, f"{sym}-aggTrades-{d}.zip")
            if not os.path.exists(zp):
                continue
            t, m = load(zp)
            parts_ts.append(t); parts_mid.append(m)
        if not parts_ts:
            return None, None
        ts = np.concatenate(parts_ts); mid = np.concatenate(parts_mid)
        o = np.argsort(ts, kind="stable")
        return ts[o], mid[o]
    else:
        zp = os.path.join(BINANCE_DIR, f"{sym}-aggTrades-{period}.zip")
        if not os.path.exists(zp):
            return None, None
        return load(zp)


def main():
    t0 = time.time()
    print("=== FADE-SUSTAINED x REGIME MAP (weekly) — Binance OOS, 4 majors, Jan->May 2026, tick-level ===\n")
    print("FADE = vol30>=P75 & drift30 tail (>=P75 SHORT / <=P25 LONG) & drift8h SAME SIGN as drift30.")
    print(f"PnL/trade = forward return in fade direction over fixed {HOLD_MIN}min hold, NET {FEE} bps maker-maker.")
    print("Rising-edge + no-overlap 8h. Percentiles per asset-month (May=concat daily). ZERO price sampling.\n")

    periods = MONTHS + ["MAY"]
    # rows: one per (asset, week)
    # we accumulate fade trade PnLs per (asset, week) and regime descriptors per (asset, week)
    week_trades = {}   # (sym, week) -> list of net pnl
    week_regime = {}   # (sym, week) -> regime dict
    week_seen_ticks = {}  # (sym, week) -> count of ticks (to confirm coverage)

    # cache of the assembled per-(asset,week) table (tiny, 84 rows) so a cosmetic crash in the
    # ANALYSIS stage never forces a 15-min re-parse of 9GB of ticks.
    CACHE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_fade_regime_cache.pkl")
    if os.path.exists(CACHE):
        df = pd.read_pickle(CACHE)
        print(f"[cache] loaded per-(asset,week) table from {CACHE} ({len(df)} rows) — skipping tick parse.\n")
        return analyze(df, t0)

    for sym in SYMBOLS:
        for period in periods:
            ts, mid = build_series_for_period(sym, period)
            if ts is None or len(ts) < 5000:
                print(f"  [skip] {sym} {period}: no/short data")
                continue
            n = len(ts)
            wk_keys = monday_ms_vec(ts)   # vectorized Monday-00:00-UTC week key (int64 ms) — replaces per-tick datetime
            # --- fade entries on the whole period series (consistent percentile calibration) ---
            idx_short, idx_long = fade_entries(ts, mid)
            f_short = fwd(ts, mid, idx_short, HOLD_MIN * MIN, is_long=False)
            f_long = fwd(ts, mid, idx_long, HOLD_MIN * MIN, is_long=True)
            # assign each entry to its calendar week, store NET pnl
            for idx_arr, f_arr in [(idx_short, f_short), (idx_long, f_long)]:
                for i, fv in zip(idx_arr, f_arr):
                    if np.isnan(fv):
                        continue
                    wk = int(wk_keys[i])
                    week_trades.setdefault((sym, wk), []).append(float(fv) - FEE)

            # --- regime descriptors per calendar week from THIS period's ticks ---
            for wk_val in np.unique(wk_keys):
                wk = int(wk_val)
                sel = wk_keys == wk_val
                if sel.sum() < 200:
                    continue
                reg = regime_descriptors(ts[sel], mid[sel])
                if reg is None:
                    continue
                # if a week appears in two periods (e.g. 04-30 in April monthly AND May daily),
                # keep the one with more ticks (more complete coverage)
                key = (sym, wk)
                prev = week_seen_ticks.get(key, -1)
                if sel.sum() > prev:
                    week_regime[key] = reg
                    week_seen_ticks[key] = int(sel.sum())
            print(f"  {sym} {period}: ticks={n:>9,}  short_entries={len(idx_short):>3}  long_entries={len(idx_long):>3}")

    # ---------------- assemble per (asset, week) table ----------------
    rows = []
    for (sym, wk), reg in week_regime.items():
        pnls = week_trades.get((sym, wk), [])
        nn = len(pnls)
        rows.append(dict(
            sym=sym, week=iso_week_key(wk), wk_ms=int(wk),
            n=nn,
            fade_pnl=(float(np.mean(pnls)) if nn > 0 else np.nan),
            net_drift=reg["net_drift"], abs_drift=reg["abs_drift"],
            trend_persist=reg["trend_persist"], rvol=reg["rvol"],
            vr_1min=reg["vr_1min"], kaufman_er=reg["kaufman_er"], n_min=reg["n_min"],
        ))
    df = pd.DataFrame(rows).sort_values(["wk_ms", "sym"]).reset_index(drop=True)
    try:
        df.to_pickle(CACHE)
        print(f"\n[cache] saved per-(asset,week) table to {CACHE} ({len(df)} rows).")
    except Exception as e:
        print(f"\n[cache] WARN could not save cache: {e}")
    return analyze(df, t0)


def analyze(df, t0):
    REGVARS = [
        ("net_drift", "net signed drift (bps)"),
        ("abs_drift", "|net drift| (bps)"),
        ("trend_persist", "trend persistence |sum hr|/sum|hr|"),
        ("rvol", "realized vol 1min (bps)"),
        ("vr_1min", "variance ratio k=15"),
        ("kaufman_er", "Kaufman efficiency ratio"),
    ]

    # months for context label
    def mlabel(wk_ms):
        return datetime.datetime.utcfromtimestamp(wk_ms / 1000.0).strftime("%Y-%m-%d")

    print("\n" + "=" * 120)
    print("PER (ASSET, WEEK) — fade net bps/trade + regime descriptors (week's own tick path)")
    print("=" * 120)
    hdr = f"{'week':>9} {'asset':>7} {'wkstart':>11} | {'n':>3} {'fadePnL':>8} | {'netDrift':>9} {'|drift|':>8} {'persist':>7} {'rvol':>6} {'VR15':>6} {'KER':>5}"
    print(hdr)
    print("-" * len(hdr))
    for _, r in df.iterrows():
        fp = f"{r['fade_pnl']:+8.1f}" if not np.isnan(r['fade_pnl']) else f"{'·':>8}"
        print(f"{r['week']:>9} {r['sym']:>7} {mlabel(r['wk_ms']):>11} | {int(r['n']):>3} {fp} | "
              f"{r['net_drift']:>+9.0f} {r['abs_drift']:>8.0f} {r['trend_persist']:>7.3f} "
              f"{r['rvol']:>6.1f} {r['vr_1min']:>6.2f} {r['kaufman_er']:>5.3f}")

    # ---------------- analysis: only weeks with enough trades ----------------
    MIN_N = 3  # require >=3 fade trades for a stable weekly PnL
    da = df[(df['n'] >= MIN_N) & df['fade_pnl'].notna()].copy()
    print(f"\n[analysis universe] weeks with >= {MIN_N} fade trades: {len(da)} of {len(df)} (asset-weeks)")
    print(f"  total fade trades in universe: {int(da['n'].sum())}")

    def spearman(x, y):
        rx = pd.Series(x).rank().values
        ry = pd.Series(y).rank().values
        if np.std(rx) < 1e-9 or np.std(ry) < 1e-9:
            return np.nan
        return float(np.corrcoef(rx, ry)[0, 1])

    def pearson(x, y):
        if np.std(x) < 1e-9 or np.std(y) < 1e-9:
            return np.nan
        return float(np.corrcoef(x, y)[0, 1])

    print("\n" + "=" * 120)
    print("CORRELATION of weekly fade PnL vs each regime descriptor (pooled across assets)")
    print("  n-weighted Pearson weights each asset-week by its #fade-trades (more trades = more reliable PnL)")
    print("=" * 120)
    print(f"  {'regime var':>34} | {'Pearson':>8} {'Spearman':>9} {'wPearson':>9}")
    print("  " + "-" * 64)
    corr_summary = {}
    y = da['fade_pnl'].values
    w = da['n'].values.astype(float)
    for col, lbl in REGVARS:
        x = da[col].values
        m = ~(np.isnan(x) | np.isnan(y))
        pr = pearson(x[m], y[m])
        sp = spearman(x[m], y[m])
        # weighted pearson
        xm = x[m]; ym = y[m]; wm = w[m]
        wmean_x = np.sum(wm * xm) / np.sum(wm); wmean_y = np.sum(wm * ym) / np.sum(wm)
        cov = np.sum(wm * (xm - wmean_x) * (ym - wmean_y)) / np.sum(wm)
        vx = np.sum(wm * (xm - wmean_x) ** 2) / np.sum(wm)
        vy = np.sum(wm * (ym - wmean_y) ** 2) / np.sum(wm)
        wpr = cov / np.sqrt(vx * vy) if vx > 1e-12 and vy > 1e-12 else np.nan
        corr_summary[col] = (pr, sp, wpr, lbl)
        print(f"  {lbl:>34} | {pr:>+8.3f} {sp:>+9.3f} {wpr:>+9.3f}")

    # ---------------- quartile buckets per regime var ----------------
    print("\n" + "=" * 120)
    print("QUARTILE BUCKETS — mean weekly fade PnL by each regime variable (pooled asset-weeks)")
    print("  Q1=lowest quartile of the regime var ... Q4=highest. trade-weighted mean PnL per bucket.")
    print("=" * 120)
    for col, lbl in REGVARS:
        x = da[col].values
        m = ~np.isnan(x)
        sub = da[m].copy()
        xv = sub[col].values
        try:
            q = pd.qcut(xv, 4, labels=["Q1", "Q2", "Q3", "Q4"], duplicates="drop")
        except Exception:
            continue
        sub['q'] = q
        print(f"\n  --- {lbl} ---")
        print(f"    {'bucket':>6} | {'range':>22} | {'#wk':>4} {'#trd':>5} | {'meanPnL':>8} {'wmeanPnL':>9}")
        for qb in ["Q1", "Q2", "Q3", "Q4"]:
            g = sub[sub['q'] == qb]
            if len(g) == 0:
                continue
            lo = g[col].min(); hi = g[col].max()
            mp = g['fade_pnl'].mean()
            wmp = np.sum(g['n'] * g['fade_pnl']) / np.sum(g['n'])
            print(f"    {qb:>6} | [{lo:>+8.2f},{hi:>+8.2f}] | {len(g):>4} {int(g['n'].sum()):>5} | {mp:>+8.1f} {wmp:>+9.1f}")

    # ---------------- pick best regime var: monotone + |corr| ----------------
    # score = |spearman| (monotonic) primarily
    best = sorted(corr_summary.items(), key=lambda kv: -abs(kv[1][1]) if not np.isnan(kv[1][1]) else 1e9)
    best_col = best[0][0]; best_lbl = best[0][1][3]
    print("\n" + "=" * 120)
    print(f"BEST (most monotonic) regime variable by |Spearman|: '{best_lbl}'  "
          f"(Pearson {corr_summary[best_col][0]:+.3f}, Spearman {corr_summary[best_col][1]:+.3f}, "
          f"wPearson {corr_summary[best_col][2]:+.3f})")
    print("=" * 120)

    # ---------------- weekly time series Jan->May (PnL + best var), pooled across assets ----------------
    print("\n" + "=" * 120)
    print("WEEKLY TIME SERIES Jan->May (pooled across assets): trade-weighted fade PnL + regime context")
    print("  shows the MONOTONE evolution. pooledPnL = trade-weighted mean over the 4 assets that week.")
    print("=" * 120)
    g = da.groupby("week")
    rows2 = []
    for wk, grp in g:
        wms = grp['wk_ms'].iloc[0]
        npool = int(grp['n'].sum())
        wpnl = np.sum(grp['n'] * grp['fade_pnl']) / npool
        # regime context: pooled (trade-weighted where sensible, simple mean for ratios)
        ctx = {}
        for col, _ in REGVARS:
            ctx[col] = float(np.nanmean(grp[col].values))
        rows2.append(dict(week=wk, wk_ms=wms, n=npool, wpnl=wpnl, nassets=len(grp), **ctx))
    ts_df = pd.DataFrame(rows2).sort_values("wk_ms").reset_index(drop=True)

    hdr2 = (f"{'week':>9} {'wkstart':>11} {'#a':>3} {'#trd':>5} {'pooledPnL':>10} | "
            f"{'netDrift':>9} {'|drift|':>8} {'persist':>7} {'rvol':>6} {'VR15':>6} {'KER':>5}")
    print(hdr2)
    print("-" * len(hdr2))
    for _, r in ts_df.iterrows():
        print(f"{r['week']:>9} {mlabel(r['wk_ms']):>11} {int(r['nassets']):>3} {int(r['n']):>5} "
              f"{r['wpnl']:>+10.1f} | {r['net_drift']:>+9.0f} {r['abs_drift']:>8.0f} "
              f"{r['trend_persist']:>7.3f} {r['rvol']:>6.1f} {r['vr_1min']:>6.2f} {r['kaufman_er']:>5.3f}")

    # ---------------- monthly rollup (sanity vs user's cited gradient) ----------------
    print("\n" + "=" * 120)
    print("MONTHLY ROLLUP (sanity vs cited gradient Jan -18 -> May +10..+16): trade-weighted fade PnL")
    print("=" * 120)
    da2 = da.copy()
    da2['month'] = da2['wk_ms'].apply(lambda x: datetime.datetime.utcfromtimestamp(x / 1000.0).strftime("%Y-%m"))
    for mo, grp in da2.groupby('month'):
        wpnl = np.sum(grp['n'] * grp['fade_pnl']) / np.sum(grp['n'])
        print(f"  {mo}: pooled fade PnL = {wpnl:+6.1f} bps/trade   (#trades={int(grp['n'].sum())}, #asset-weeks={len(grp)}, "
              f"mean netDrift={grp['net_drift'].mean():+.0f}, mean persist={grp['trend_persist'].mean():.3f}, "
              f"mean KER={grp['kaufman_er'].mean():.3f})")

    print(f"\n[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
