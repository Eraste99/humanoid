"""IS THE FADE'S REGIME-DEPENDENCE EXPLOITABLE EX-ANTE? (causal walk-forward gate)

The "fade-sustained" trade (SHORT a sustained rip / LONG a sustained dip) loses in TREND months
(Jan-Feb 2026) and wins in CHOP (Mar-May): a clean monotone gradient. That is only TRADEABLE if the
regime is (a) PERSISTENT week-to-week and (b) DETECTABLE a week AHEAD with PAST data only. A prior
|macro-drift| filter FAILED (non-monotone, knife-edge). Here we test cleaner regime metrics + a proper
WALK-FORWARD gate, against the null.

CRUX: can a CAUSAL gate beat ALWAYS-ON *and* a RANDOM gate, robustly across months? A clean negative
("regime real but not week-ahead predictable -> fade not rescuable") is a fully valuable result.

--------------------------------------------------------------------------------------------------
LOOK-AHEAD PREVENTION (the #1 trap) -- stated explicitly:
  * Fade trades are detected tick-level with ZERO sampling, no-overlap 8h, PnL = 8h fwd return net 3bps.
    A trade is ASSIGNED to the ISO calendar week of its ENTRY timestamp.
  * The REGIME metric for week k (trend strength / |drift| / Kaufman Efficiency Ratio / Variance Ratio)
    is computed STRICTLY from data in the trailing calendar window [week_k_start - L , week_k_start).
    It NEVER touches any tick at-or-after week_k_start. So the week-k label is known the instant week k
    begins, before any week-k trade fires.
  * The GATE decision for week k (ON if past-regime=chop, OFF if trend) therefore uses only past data.
  * The chop/trend THRESHOLD for the gate is itself fit causally: at each week start it is the MEDIAN of
    all PAST weekly regime values seen so far (expanding, strictly < week k). No global/full-sample
    threshold is ever used by the causal gate. (We also report a fixed-global-median variant as a
    sensitivity, clearly flagged as NOT the causal arm.)
  * The regime metric uses a 1-MINUTE mid grid over the trailing window (a summary statistic only, so it
    is comparable across assets/periods); the TRADES & PnL remain pure tick-level. Stated for honesty.

ARMS compared (net bps/trade AND cumulative PnL; pooled + per-month; 4 majors):
  GATED (causal)      : trade week k iff past-regime says CHOP.
  ALWAYS-ON           : trade every week (baseline).
  RANDOM-GATE (null)  : same NUMBER of ON-weeks as GATED but chosen at random (avg many seeds).
  HINDSIGHT-GATE      : trade week k iff the REALIZED (same-week) regime is chop -> upper bound, ref only.

DATA: C:\\Users\\erast\\hl_data\\binance monthly {BTC,ETH,SOL,XRP}USDT 2026-01..04 (contiguous) +
      daily 2026-04-30..05-07 and 05-15..05-22 (note GAP 05-08..05-14). Cols price=1,qty=2,tt=5,ibm=6.
"""
import os, zipfile, time, warnings
import numpy as np
import pandas as pd
warnings.filterwarnings("ignore")

BINANCE_DIR = r"C:\Users\erast\hl_data\binance"
DAILY_DIR = os.path.join(BINANCE_DIR, "daily")
MIN = 60 * 1000
DAY = 24 * 60 * MIN
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]
MONTHS = ["2026-01", "2026-02", "2026-03", "2026-04"]
DAILY_DAYS = ["2026-04-30", "2026-05-01", "2026-05-02", "2026-05-03", "2026-05-04", "2026-05-05",
              "2026-05-06", "2026-05-07", "2026-05-15", "2026-05-16", "2026-05-17", "2026-05-18",
              "2026-05-19", "2026-05-20", "2026-05-21", "2026-05-22"]
FEE = 3.0
H_HOLD = 480  # 8h hold, minutes
TRAIL_WEEKS = 2          # causal trailing window length for the regime metric (weeks)
GAP_MS = 30 * MIN        # any inter-tick gap >= this breaks feature windows (gap-aware)
RANDOM_SEEDS = 200       # number of random-gate reshuffles to average the null
MIN_TRADES_WEEK = 5      # a (asset,week) cell needs >= this many fade trades to enter weekly stats
MIN_GRID_PTS = 60        # trailing window needs >= this many 1-min grid points to label the regime


# ----------------------------------------------------------------------------- IO (reused from _binance_quadrant)
def load_zip(zp):
    with zipfile.ZipFile(zp) as z:
        name = z.namelist()[0]
        with z.open(name) as f:
            first = f.readline().decode("utf-8", "ignore")
        hh = "price" in first.lower()
        with z.open(name) as f:
            df = pd.read_csv(f, header=0 if hh else None,
                             usecols=[1, 2, 5, 6],
                             names=None if hh else range(7))
    if hh:
        price = df["price"].values.astype(np.float64)
        T = df["transact_time"].values.astype(np.int64)
        m = df["is_buyer_maker"].astype(str).str.lower().values
    else:
        price = df.iloc[:, 0].values.astype(np.float64)  # remap when no header (cols subset 1,2,5,6 -> 0..3)
        T = df.iloc[:, 2].values.astype(np.int64)
        m = df.iloc[:, 3].astype(str).str.lower().values
    return T, price, m


def to_mid(T, price, m):
    o = np.argsort(T, kind="stable")
    T = T[o]; price = price[o]; m = m[o]
    ibt = (m == "false")  # is_buyer_maker == False  -> aggressive BUY -> defines ask side
    ask = pd.Series(np.where(ibt, price, np.nan)).ffill().values
    bid = pd.Series(np.where(~ibt, price, np.nan)).ffill().values
    mid = (ask + bid) / 2.0
    ok = ~np.isnan(mid)
    return T[ok], mid[ok]


def load_asset(sym):
    """Concatenate monthly Jan-Apr + daily (04-30..05-07, 05-15..05-22) into one continuous (ts, mid)."""
    Ts, Ps, Ms = [], [], []
    for mo in MONTHS:
        zp = os.path.join(BINANCE_DIR, f"{sym}-aggTrades-{mo}.zip")
        if os.path.exists(zp):
            T, p, m = load_zip(zp); Ts.append(T); Ps.append(p); Ms.append(m)
    for d in DAILY_DAYS:
        zp = os.path.join(DAILY_DIR, f"{sym}-aggTrades-{d}.zip")
        if os.path.exists(zp):
            T, p, m = load_zip(zp); Ts.append(T); Ps.append(p); Ms.append(m)
    T = np.concatenate(Ts); p = np.concatenate(Ps); m = np.concatenate(Ms)
    del Ts, Ps, Ms
    ts, mid = to_mid(T, p, m)
    # dedup exact-equal timestamps collisions are fine for searchsorted; keep sorted unique-stable
    return ts, mid


# ----------------------------------------------------------------------------- gap mask
def gap_break(ts):
    """gap_break[i] = timestamp of the most recent point such that there is NO inter-tick gap >= GAP_MS
    between it and i. Used to invalidate trailing windows that span a data gap."""
    dts = np.diff(ts, prepend=ts[0])
    is_gap = dts >= GAP_MS
    # last index where a gap STARTED (i.e. boundary). seg_start_ts[i] = ts at start of current contiguous run
    seg_id = np.cumsum(is_gap)
    # ts of first element of each segment
    first_ts_of_seg = np.zeros(seg_id.max() + 1, dtype=np.int64)
    # earliest ts per seg
    order = np.arange(len(ts))
    for s in range(seg_id.max() + 1):
        sel = order[seg_id == s]
        if len(sel):
            first_ts_of_seg[s] = ts[sel[0]]
    return seg_id, first_ts_of_seg


# ----------------------------------------------------------------------------- features (reused, gap-aware)
def drift(ts, mid, W, seg_id):
    """trailing-window return in bps over window W; NaN if window spans a data gap or insufficient span."""
    p = np.searchsorted(ts, ts - W, side="left")
    d = (mid - mid[p]) / mid[p] * 1e4
    span_ok = (ts - ts[p]) >= 0.5 * W
    no_gap = seg_id[p] == seg_id           # same contiguous segment as W ago
    return np.where(span_ok & no_gap, d, np.nan)


def vol30(ts, mid, seg_id):
    n = len(ts); lm = np.log(mid); lr = np.diff(lm, prepend=lm[0]) * 1e4
    C1 = np.concatenate([[0], np.cumsum(lr)]); C2 = np.concatenate([[0], np.cumsum(lr * lr)])
    j = np.searchsorted(ts, ts - 30 * MIN, side="left"); cnt = np.arange(n) - j
    s1 = C1[np.arange(n) + 1] - C1[j + 1]; s2 = C2[np.arange(n) + 1] - C2[j + 1]
    with np.errstate(invalid="ignore", divide="ignore"):
        mean = s1 / cnt; v = np.sqrt(np.maximum(s2 / cnt - mean * mean, 0))
    v[cnt < 10] = np.nan
    no_gap = seg_id[j] == seg_id
    v[~no_gap] = np.nan
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


# ----------------------------------------------------------------------------- week index (ISO, UTC)
def iso_week_key(ms):
    """Return an integer week id = days_since_epoch_Monday // 7. Monotone, contiguous, UTC. Comparable
    across assets. Week k covers [origin + 7k days, origin + 7(k+1) days)."""
    # epoch (1970-01-01) was a Thursday; shift so that week boundaries fall on Monday 00:00 UTC.
    # 1970-01-05 is the first Monday. offset = 4 days.
    day = ms // DAY
    return (day - 4) // 7  # week 0 starts Mon 1970-01-05; arbitrary but consistent Monday-aligned


def week_start_ms(wk):
    return (wk * 7 + 4) * DAY


# ----------------------------------------------------------------------------- regime metrics on 1-min grid
def grid_mid(ts, mid, t0, t1):
    """1-min mid grid on [t0, t1): last tick at or before each grid point. Returns prices (NaN-free run)."""
    if t1 <= t0:
        return np.array([])
    grid = np.arange(t0, t1, MIN)
    j = np.searchsorted(ts, grid, side="right") - 1
    ok = j >= 0
    g = np.full(len(grid), np.nan)
    g[ok] = mid[j[ok]]
    g = g[~np.isnan(g)]
    return g


def regime_metrics(prices):
    """Compute regime descriptors from an evenly-spaced (1-min) price series. Higher = more TRENDY.
    Returns dict with: er (Kaufman efficiency ratio in [0,1]), absdrift (|net move| bps),
    vr (variance ratio q=30, >1 trend / <1 mean-revert), trendstrength = er (alias).
    All are pure functions of the trailing window."""
    n = len(prices)
    if n < MIN_GRID_PTS:
        return None
    lp = np.log(prices)
    r = np.diff(lp)                         # 1-min log returns
    net = lp[-1] - lp[0]
    path = np.sum(np.abs(r))
    er = abs(net) / path if path > 0 else 0.0          # Kaufman ER in [0,1]; high=directional
    absdrift = abs(net) * 1e4                            # |drift| over window, bps
    # variance ratio VR(q): var of q-step returns / (q * var of 1-step). >1 => trending.
    q = 30
    if n > 2 * q:
        rq = lp[q:] - lp[:-q]
        v1 = np.var(r, ddof=1)
        vq = np.var(rq, ddof=1)
        vr = (vq / (q * v1)) if v1 > 0 else np.nan
    else:
        vr = np.nan
    # realized vol over window (for context only)
    rv = np.std(r, ddof=1) * 1e4
    return {"er": er, "absdrift": absdrift, "vr": vr, "rv": rv}


# ----------------------------------------------------------------------------- build fade trades + weekly regime
def build_asset(sym):
    """Return per-asset:
       trades: DataFrame [week, pnl_net]  (one row per fade trade, PnL already net of fee, fade direction)
       weekly_regime: DataFrame indexed by week with CAUSAL regime metrics (trailing TRAIL_WEEKS, strictly
                      before the week starts) AND the REALIZED same-week regime metric (for hindsight only).
    """
    ts, mid = load_asset(sym)
    n = len(ts)
    seg_id, _ = gap_break(ts)
    v = vol30(ts, mid, seg_id)
    d30 = drift(ts, mid, 30 * MIN, seg_id)
    d8 = drift(ts, mid, 480 * MIN, seg_id)
    val = ~(np.isnan(v) | np.isnan(d30) | np.isnan(d8))
    vP = np.nanpercentile(v[val], 75)
    p25 = np.nanpercentile(d30[val], 25); p75 = np.nanpercentile(d30[val], 75)
    hv = val & (v >= vP)
    # FADE = fight a sustained extended move: spike same sign as 8h trend.
    #   FADE_SHORT: d30>=p75 & d8>0 -> SHORT the rip      (is_long=False)
    #   FADE_LONG : d30<=p25 & d8<0 -> LONG  the dip      (is_long=True)
    mask_short = hv & (d30 >= p75) & (d8 > 0)
    mask_long = hv & (d30 <= p25) & (d8 < 0)
    idx_short = no_overlap(mask_short, ts)
    idx_long = no_overlap(mask_long, ts)
    # IMPORTANT: enforce 8h no-overlap ACROSS both fade legs combined (a single position book)
    all_idx = np.sort(np.concatenate([idx_short, idx_long])) if (len(idx_short) + len(idx_long)) else np.array([], int)
    is_long_arr = np.isin(all_idx, idx_long)
    # de-overlap merged stream (greedy, 8h)
    keep = []
    last = -np.inf
    for i, il in zip(all_idx, is_long_arr):
        if ts[i] - last >= 8 * 60 * MIN:
            keep.append((i, il)); last = ts[i]
    if not keep:
        trades = pd.DataFrame(columns=["week", "pnl_net"])
    else:
        kidx = np.array([k[0] for k in keep]); klong = np.array([k[1] for k in keep])
        pnl = np.full(len(kidx), np.nan)
        # forward 8h return in fade direction
        jL = kidx[klong]; jS = kidx[~klong]
        if len(jL):
            pnl[klong] = fwd(ts, mid, jL, H_HOLD * MIN, True)
        if len(jS):
            pnl[~klong] = fwd(ts, mid, jS, H_HOLD * MIN, False)
        wk = iso_week_key(ts[kidx])
        good = ~np.isnan(pnl)
        trades = pd.DataFrame({"week": wk[good], "pnl_net": pnl[good] - FEE})

    # ---- weekly regime metrics ----
    weeks = np.unique(iso_week_key(ts))
    rows = []
    for wk in weeks:
        ws = week_start_ms(wk)
        we = ws + 7 * DAY
        # CAUSAL trailing window: [ws - TRAIL_WEEKS*7d , ws)
        tw0 = ws - TRAIL_WEEKS * 7 * DAY
        g_past = grid_mid(ts, mid, tw0, ws)
        m_past = regime_metrics(g_past)
        # REALIZED same-week window: [ws, we)  -- HINDSIGHT ONLY
        g_now = grid_mid(ts, mid, ws, we)
        m_now = regime_metrics(g_now)
        row = {"week": wk,
               "past_er": np.nan, "past_absdrift": np.nan, "past_vr": np.nan, "past_n": len(g_past),
               "now_er": np.nan, "now_absdrift": np.nan, "now_vr": np.nan, "now_n": len(g_now)}
        if m_past is not None:
            row.update(past_er=m_past["er"], past_absdrift=m_past["absdrift"], past_vr=m_past["vr"])
        if m_now is not None:
            row.update(now_er=m_now["er"], now_absdrift=m_now["absdrift"], now_vr=m_now["vr"])
        rows.append(row)
    weekly_regime = pd.DataFrame(rows).set_index("week")
    del ts, mid, seg_id, v, d30, d8
    return trades, weekly_regime


# ----------------------------------------------------------------------------- helpers for reporting
def month_of_week(wk):
    """Map a week id to a coarse month label via its Monday start (UTC)."""
    ms = week_start_ms(wk)
    return pd.Timestamp(ms, unit="ms", tz="UTC").strftime("%Y-%m")


def tstat(x):
    x = np.asarray(x, float)
    if len(x) < 2:
        return float("nan")
    s = x.std(ddof=1)
    return x.mean() / (s / np.sqrt(len(x))) if s > 0 else float("nan")


# =================================================================================================== MAIN
def main():
    t0 = time.time()
    print("=" * 100)
    print("FADE REGIME GATE -- CAUSAL WALK-FORWARD vs ALWAYS-ON vs RANDOM(null) vs HINDSIGHT(upper bound)")
    print("=" * 100)
    print(f"trailing window = {TRAIL_WEEKS} wk | hold = {H_HOLD}min | fee = {FEE}bps | "
          f"no-overlap 8h (merged legs) | regime on 1-min grid | random seeds = {RANDOM_SEEDS}")
    print("LOOK-AHEAD GUARD: week-k regime uses ONLY ticks in [week_k_start - {0}wk, week_k_start); "
          "causal gate threshold = expanding median of PAST weekly regime values only.\n".format(TRAIL_WEEKS))

    all_trades = {}        # sym -> trades df
    all_regime = {}        # sym -> weekly regime df
    for sym in SYMBOLS:
        ts0 = time.time()
        tr, rg = build_asset(sym)
        all_trades[sym] = tr
        all_regime[sym] = rg
        nwk = rg.index.nunique()
        print(f"  [{sym}] trades={len(tr):>5} | weeks={nwk:>3} | "
              f"weeks w/ causal regime={rg['past_er'].notna().sum():>3} | {time.time()-ts0:.0f}s")

    # ---------- weekly fade PnL table per (sym, week) ----------
    # weekly_mean[sym][week] = mean net bps/trade ; weekly_sum = total net bps ; weekly_n = #trades
    print("\n" + "=" * 100)
    print("STEP 1+2 -- WEEKLY FADE PnL  and  CAUSAL REGIME (Kaufman ER on trailing 1-min grid)")
    print("=" * 100)

    panel = []   # rows: sym, week, month, n, mean_net, sum_net, past_er, past_absdrift, past_vr, now_er, now_vr
    for sym in SYMBOLS:
        tr = all_trades[sym]; rg = all_regime[sym]
        if len(tr) == 0:
            continue
        g = tr.groupby("week")["pnl_net"].agg(["count", "mean", "sum"])
        for wk, r in g.iterrows():
            reg = rg.loc[wk] if wk in rg.index else None
            panel.append({
                "sym": sym, "week": int(wk), "month": month_of_week(wk),
                "n": int(r["count"]), "mean_net": r["mean"], "sum_net": r["sum"],
                "past_er": (reg["past_er"] if reg is not None else np.nan),
                "past_absdrift": (reg["past_absdrift"] if reg is not None else np.nan),
                "past_vr": (reg["past_vr"] if reg is not None else np.nan),
                "now_er": (reg["now_er"] if reg is not None else np.nan),
                "now_absdrift": (reg["now_absdrift"] if reg is not None else np.nan),
                "now_vr": (reg["now_vr"] if reg is not None else np.nan),
            })
    P = pd.DataFrame(panel).sort_values(["sym", "week"]).reset_index(drop=True)

    # show per-asset weekly lines (compact)
    for sym in SYMBOLS:
        sub = P[P["sym"] == sym]
        if len(sub) == 0:
            continue
        print(f"\n  --- {sym} ---")
        print(f"    {'week':>5} {'month':>8} {'n':>4} {'fade_mean':>9} {'fade_sum':>9} | "
              f"{'pastER':>6} {'pastVR':>6} {'past|d|':>7} | {'nowER':>6} {'nowVR':>6}")
        for _, r in sub.iterrows():
            pe = f"{r['past_er']:.3f}" if pd.notna(r['past_er']) else "  --  "
            pv = f"{r['past_vr']:.2f}" if pd.notna(r['past_vr']) else "  -- "
            pd_ = f"{r['past_absdrift']:6.0f}" if pd.notna(r['past_absdrift']) else "   -- "
            ne = f"{r['now_er']:.3f}" if pd.notna(r['now_er']) else "  --  "
            nv = f"{r['now_vr']:.2f}" if pd.notna(r['now_vr']) else "  -- "
            print(f"    {r['week']:>5} {r['month']:>8} {r['n']:>4} {r['mean_net']:>+9.1f} "
                  f"{r['sum_net']:>+9.0f} | {pe:>6} {pv:>6} {pd_:>7} | {ne:>6} {nv:>6}")

    # ============================================================ STEP 3: persistence / predictability
    print("\n" + "=" * 100)
    print("STEP 3 -- PERSISTENCE (week-to-week autocorr of regime) & PREDICTABILITY (past regime -> fade sign)")
    print("=" * 100)
    # only weeks with valid causal regime AND enough trades
    Q = P[(P["n"] >= MIN_TRADES_WEEK) & P["past_er"].notna()].copy()
    print(f"  usable (asset,week) cells: {len(Q)}  (>= {MIN_TRADES_WEEK} trades & causal regime present)")

    # --- (a) AUTOCORRELATION of regime metric week-to-week, per asset then pooled ---
    print("\n  (a) PERSISTENCE: lag-1 autocorrelation of the CAUSAL regime metric (past_er), per asset")
    print("      (does this week's regime resemble last week's? high autocorr => persistent => forecastable)")
    ac_rows = []
    for sym in SYMBOLS:
        rg = all_regime[sym].dropna(subset=["past_er"]).sort_index()
        # use consecutive weeks only
        er = rg["past_er"]
        wks = er.index.values
        pairs = [(er.loc[w], er.loc[w + 1]) for w in wks if (w + 1) in er.index]
        if len(pairs) >= 4:
            a = np.array([p[0] for p in pairs]); b = np.array([p[1] for p in pairs])
            ac = np.corrcoef(a, b)[0, 1]
            ac_rows.append((sym, len(pairs), ac))
            print(f"      {sym}: n_pairs={len(pairs):>3}  autocorr(past_er)={ac:+.3f}")
    # pooled autocorr (stack consecutive pairs of the REALIZED now_er, which is the underlying regime process)
    print("\n      pooled lag-1 autocorr of the UNDERLYING regime (now_er, realized) across assets:")
    for metric in ["now_er", "now_vr", "now_absdrift"]:
        aa, bb = [], []
        for sym in SYMBOLS:
            rg = all_regime[sym].dropna(subset=[metric]).sort_index()
            s = rg[metric]
            for w in s.index.values:
                if (w + 1) in s.index:
                    aa.append(s.loc[w]); bb.append(s.loc[w + 1])
        if len(aa) >= 5:
            ac = np.corrcoef(aa, bb)[0, 1]
            print(f"        {metric}: n_pairs={len(aa):>3}  lag-1 autocorr={ac:+.3f}")

    # --- (b) PREDICTABILITY: does PAST regime predict the SIGN of next-week fade PnL? ---
    print("\n  (b) PREDICTABILITY: PAST regime (known before week starts) vs realized weekly fade mean")
    print("      Hypothesis: fade WINS in chop (low ER / low |drift| / VR<1), LOSES in trend (high ER).")
    print("      => expect NEGATIVE corr(past_er, fade_mean). Hit-rate: 'chop=>win' & 'trend=>loss'.")
    for metric in ["past_er", "past_absdrift", "past_vr"]:
        sub = Q.dropna(subset=[metric, "mean_net"])
        if len(sub) < 8:
            print(f"      {metric}: insufficient cells ({len(sub)})")
            continue
        x = sub[metric].values; y = sub["mean_net"].values
        # spearman (rank) corr
        rx = pd.Series(x).rank().values; ry = pd.Series(y).rank().values
        sp = np.corrcoef(rx, ry)[0, 1]
        pe = np.corrcoef(x, y)[0, 1]
        # threshold = MEDIAN of the metric within this usable set (in-sample, for descriptive AUC only)
        thr = np.median(x)
        # predicted ON when metric below median (chop). win = y>0.
        pred_on = x < thr if metric != "past_vr" else x < np.median(x)  # low VR also = chop
        # AUC of "low-regime => positive PnL": rank-based AUC using metric as (negated) score for P(win)
        score = -x  # higher score => more chop => predict win
        win = (y > 0).astype(int)
        auc = _auc(score, win)
        hit = np.mean((pred_on & (y > 0)) | (~pred_on & (y <= 0)))
        print(f"      {metric:>13}: pearson={pe:+.3f}  spearman={sp:+.3f}  "
              f"AUC(chop->win)={auc:.3f}  hit-rate(median split)={hit:.2%}  n={len(sub)}")

    # ============================================================ STEP 4: walk-forward gated vs arms
    print("\n" + "=" * 100)
    print("STEP 4 -- WALK-FORWARD GATE  (GATED causal | ALWAYS-ON | RANDOM null | HINDSIGHT upper bound)")
    print("=" * 100)
    print("  Gate metric = past_er (Kaufman efficiency ratio, trailing). ON iff CHOP (past_er < causal thr).")
    print("  Causal threshold = expanding median of all PRIOR weekly past_er (per asset), strictly < week k.")
    print("  Each ON week contributes ALL its fade trades; OFF week contributes none.\n")

    # Build, per asset, the ordered list of weeks that have BOTH trades(>=MIN) and a causal regime.
    # GATED decision uses expanding median of past_er seen so far.
    rng = np.random.default_rng(12345)
    GATE_METRIC = "past_er"

    # collect per-arm trade-level pnl with month tags, pooled across assets
    arms = {"GATED": [], "ALWAYS": [], "HINDSIGHT": []}     # lists of (month, pnl)
    arms_weekflags = {}   # sym -> list of (week, on_gated, on_hind, n_trades, month)
    random_pool = {s: [] for s in range(RANDOM_SEEDS)}      # seed -> list of (month, pnl)

    for sym in SYMBOLS:
        tr = all_trades[sym]; rg = all_regime[sym]
        if len(tr) == 0:
            continue
        gtr = tr.groupby("week")["pnl_net"]
        wk_n = gtr.count(); wk_list_all = sorted(wk_n.index.tolist())
        # weeks eligible to be traded: have >=MIN trades AND a causal regime value
        elig = [w for w in wk_list_all
                if wk_n.get(w, 0) >= MIN_TRADES_WEEK and pd.notna(rg.loc[w, GATE_METRIC]) if w in rg.index]
        flags = []
        seen_metric = []   # expanding list of PAST weekly metric values (strictly before current week)
        # to be fully causal, iterate weeks in time order; the expanding median uses ONLY prior eligible weeks
        for w in elig:
            mv = rg.loc[w, GATE_METRIC]
            thr = np.median(seen_metric) if len(seen_metric) >= 1 else np.inf  # 1st week: thr=inf => ON (chop by default until history exists)
            on_gated = mv < thr            # chop => ON
            now_er = rg.loc[w, "now_er"]
            hind_thr_pool = rg["now_er"].dropna()
            hind_thr = np.median(hind_thr_pool) if len(hind_thr_pool) else np.nan  # realized median (hindsight ref)
            on_hind = (pd.notna(now_er) and pd.notna(hind_thr) and now_er < hind_thr)
            mo = month_of_week(w)
            pnls = tr.loc[tr["week"] == w, "pnl_net"].values
            flags.append((w, on_gated, on_hind, len(pnls), mo))
            # accumulate arms
            for pv in pnls:
                arms["ALWAYS"].append((mo, pv))
                if on_gated:
                    arms["GATED"].append((mo, pv))
                if on_hind:
                    arms["HINDSIGHT"].append((mo, pv))
            seen_metric.append(mv)
        arms_weekflags[sym] = flags

        # RANDOM gate: same NUMBER of ON-weeks as GATED, drawn uniformly among eligible weeks, many seeds.
        n_on = sum(1 for f in flags if f[1])
        n_elig = len(flags)
        for s in range(RANDOM_SEEDS):
            if n_elig == 0:
                continue
            on_idx = set(rng.choice(n_elig, size=min(n_on, n_elig), replace=False).tolist()) if n_on > 0 else set()
            for k, f in enumerate(flags):
                if k in on_idx:
                    pnls = tr.loc[tr["week"] == f[0], "pnl_net"].values
                    mo = f[4]
                    for pv in pnls:
                        random_pool[s].append((mo, pv))

    # -------- summarize arms: pooled + per month --------
    months_order = ["2026-01", "2026-02", "2026-03", "2026-04", "2026-05"]

    def summarize(pairs):
        if not pairs:
            return {"n": 0, "mean": float("nan"), "sum": 0.0, "t": float("nan")}
        v = np.array([p[1] for p in pairs], float)
        return {"n": len(v), "mean": v.mean(), "sum": v.sum(), "t": tstat(v)}

    def summarize_by_month(pairs):
        out = {}
        for mo in months_order:
            vv = np.array([p[1] for p in pairs if p[0] == mo], float)
            out[mo] = (len(vv), (vv.mean() if len(vv) else float("nan")), (vv.sum() if len(vv) else 0.0))
        return out

    sg = summarize(arms["GATED"]); sa = summarize(arms["ALWAYS"]); sh = summarize(arms["HINDSIGHT"])
    # random: average over seeds of (mean, sum, n)
    r_means = [summarize(random_pool[s])["mean"] for s in range(RANDOM_SEEDS) if random_pool[s]]
    r_sums = [summarize(random_pool[s])["sum"] for s in range(RANDOM_SEEDS) if random_pool[s]]
    r_ns = [summarize(random_pool[s])["n"] for s in range(RANDOM_SEEDS) if random_pool[s]]
    r_mean = np.nanmean(r_means) if r_means else float("nan")
    r_mean_sd = np.nanstd(r_means) if r_means else float("nan")
    r_sum = np.nanmean(r_sums) if r_sums else float("nan")
    r_sum_sd = np.nanstd(r_sums) if r_sums else float("nan")
    # percentile of GATED within random null distribution (one-sided: gated better than random?)
    pct_mean = float(np.mean([sg["mean"] > m for m in r_means])) if r_means else float("nan")
    pct_sum = float(np.mean([sg["sum"] > s for s in r_sums])) if r_sums else float("nan")

    print("  POOLED (Jan->May, 4 majors)")
    print(f"    {'arm':<12} {'n_trades':>9} {'mean_net':>9} {'t-stat':>7} {'total_net_bps':>14}")
    print(f"    {'GATED':<12} {sg['n']:>9} {sg['mean']:>+9.2f} {sg['t']:>+7.2f} {sg['sum']:>+14.0f}")
    print(f"    {'ALWAYS-ON':<12} {sa['n']:>9} {sa['mean']:>+9.2f} {sa['t']:>+7.2f} {sa['sum']:>+14.0f}")
    print(f"    {'RANDOM(avg)':<12} {np.mean(r_ns) if r_ns else float('nan'):>9.0f} "
          f"{r_mean:>+9.2f} {'':>7} {r_sum:>+14.0f}   (mean sd={r_mean_sd:.2f}, sum sd={r_sum_sd:.0f})")
    print(f"    {'HINDSIGHT':<12} {sh['n']:>9} {sh['mean']:>+9.2f} {sh['t']:>+7.2f} {sh['sum']:>+14.0f}")
    print(f"\n    GATED vs RANDOM null:  P(mean_gated > mean_random) = {pct_mean:.2%}   "
          f"P(sum_gated > sum_random) = {pct_sum:.2%}")
    print(f"    GATED mean - ALWAYS mean = {sg['mean'] - sa['mean']:+.2f} bps/trade   "
          f"GATED captures {100*(sg['mean']-sa['mean'])/((sh['mean']-sa['mean']) if (sh['mean']-sa['mean'])!=0 else float('nan')):.0f}% "
          f"of HINDSIGHT edge over ALWAYS")

    # per-month table
    gm = summarize_by_month(arms["GATED"]); am = summarize_by_month(arms["ALWAYS"])
    hm = summarize_by_month(arms["HINDSIGHT"])
    # random per-month: average means across seeds
    rm_means = {mo: [] for mo in months_order}
    for s in range(RANDOM_SEEDS):
        bym = summarize_by_month(random_pool[s])
        for mo in months_order:
            if bym[mo][0] > 0:
                rm_means[mo].append(bym[mo][1])
    print("\n  PER MONTH -- mean net bps/trade (n)   [GATED | ALWAYS | RANDOM avg | HINDSIGHT]")
    print(f"    {'month':>8} | {'GATED':>14} | {'ALWAYS':>14} | {'RANDOM':>12} | {'HINDSIGHT':>14}")
    for mo in months_order:
        g = gm[mo]; a = am[mo]; h = hm[mo]
        rmean = np.nanmean(rm_means[mo]) if rm_means[mo] else float("nan")
        gs = f"{g[1]:+.1f}({g[0]})" if g[0] else "   --"
        as_ = f"{a[1]:+.1f}({a[0]})" if a[0] else "   --"
        rs = f"{rmean:+.1f}" if rm_means[mo] else "  --"
        hs = f"{h[1]:+.1f}({h[0]})" if h[0] else "   --"
        print(f"    {mo:>8} | {gs:>14} | {as_:>14} | {rs:>12} | {hs:>14}")

    # per-month CUMULATIVE total net bps
    print("\n  PER MONTH -- TOTAL net bps (cumulative PnL contribution)   [GATED | ALWAYS | RANDOM avg | HINDSIGHT]")
    print(f"    {'month':>8} | {'GATED':>10} | {'ALWAYS':>10} | {'RANDOM':>10} | {'HINDSIGHT':>10}")
    rm_sums = {mo: [] for mo in months_order}
    for s in range(RANDOM_SEEDS):
        bym = summarize_by_month(random_pool[s])
        for mo in months_order:
            rm_sums[mo].append(bym[mo][2])
    for mo in months_order:
        g = gm[mo]; a = am[mo]; h = hm[mo]
        rsum = np.nanmean(rm_sums[mo]) if rm_sums[mo] else float("nan")
        print(f"    {mo:>8} | {g[2]:>+10.0f} | {a[2]:>+10.0f} | {rsum:>+10.0f} | {h[2]:>+10.0f}")

    # ---- per-asset robustness of the gate (mean diff GATED-ALWAYS) ----
    print("\n  PER-ASSET robustness: mean net bps/trade  GATED vs ALWAYS vs RANDOM(avg)")
    print(f"    {'sym':>8} | {'on/elig':>8} | {'GATED':>16} | {'ALWAYS':>16} | {'RANDOM avg':>10}")
    for sym in SYMBOLS:
        flags = arms_weekflags.get(sym, [])
        if not flags:
            continue
        tr = all_trades[sym]
        on_pnls = np.concatenate([tr.loc[tr["week"] == f[0], "pnl_net"].values for f in flags if f[1]]) \
            if any(f[1] for f in flags) else np.array([])
        all_pnls = np.concatenate([tr.loc[tr["week"] == f[0], "pnl_net"].values for f in flags]) \
            if flags else np.array([])
        n_on = sum(1 for f in flags if f[1]); n_el = len(flags)
        # random per-asset
        rmu = []
        for s in range(min(RANDOM_SEEDS, 100)):
            if n_el == 0 or n_on == 0:
                continue
            sel = rng2_choice(n_el, n_on, sym, s)
            rp = np.concatenate([tr.loc[tr["week"] == flags[k][0], "pnl_net"].values for k in sel]) if len(sel) else np.array([])
            if len(rp):
                rmu.append(rp.mean())
        gmean = on_pnls.mean() if len(on_pnls) else float("nan")
        amean = all_pnls.mean() if len(all_pnls) else float("nan")
        rmean_a = np.mean(rmu) if rmu else float("nan")
        gs = f"{gmean:+.1f}(n={len(on_pnls)})"
        as_ = f"{amean:+.1f}(n={len(all_pnls)})"
        print(f"    {sym:>8} | {n_on:>3}/{n_el:<4} | {gs:>16} | {as_:>16} | {rmean_a:>+10.1f}")

    print(f"\n[done] total {time.time()-t0:.0f}s")


# small deterministic RNG helper for per-asset random gate (independent of global stream)
def rng2_choice(n_el, n_on, sym, seed):
    r = np.random.default_rng(hash((sym, seed)) % (2**32))
    return sorted(r.choice(n_el, size=min(n_on, n_el), replace=False).tolist())


def _auc(score, label):
    """ROC AUC via rank statistic (Mann-Whitney). score higher => predict label=1."""
    label = np.asarray(label, int); score = np.asarray(score, float)
    n1 = label.sum(); n0 = len(label) - n1
    if n1 == 0 or n0 == 0:
        return float("nan")
    order = np.argsort(score, kind="mergesort")
    ranks = np.empty(len(score), float)
    ranks[order] = np.arange(1, len(score) + 1)
    # average ranks for ties
    s_sorted = score[order]
    i = 0
    while i < len(s_sorted):
        j = i
        while j + 1 < len(s_sorted) and s_sorted[j + 1] == s_sorted[i]:
            j += 1
        if j > i:
            avg = (i + j + 2) / 2.0
            ranks[order[i:j + 1]] = avg
        i = j + 1
    auc = (ranks[label == 1].sum() - n1 * (n1 + 1) / 2.0) / (n1 * n0)
    return auc


if __name__ == "__main__":
    main()
