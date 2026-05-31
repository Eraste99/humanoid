"""TWO-SIDED REGIME SWITCH keyed on TREND MAGNITUDE (l'idee user, Binance OOS, 4 majors, Jan->May, tick-level, ZERO sampling).

User : "Jan/Fev = falling knife -> SHORTE (ride) au lieu de fader ; chop -> fade." = un FLIP fade<->trend-follow
decide par l'AMPLITUDE du trend (pas par ER/VR, que l'agent 2 a deja teste et qui a echoue).
La mecanique a montre : ALWAYS-TREND gagne Jan/Fev (+27/+20), le fade gagne en chop (Mar/Mai). Anti-correles.
Agent 3 a montre : |drift| est le meilleur predicteur de regime (mais persistance faible). Donc on switche dessus.

EVENT : vol30>=P75 & |drift_30| extreme (>=P75 ou <=P25), rising-edge, no-overlap 8h.
SIGNAL DE SWITCH (causal) : trend_mag = |drift(W)| a l'event, W in {8h,24h,72h}. Seuil = percentile EXPANSIF
des trend_mag des events ANTERIEURS du MEME actif (no look-ahead, comme le gate de l'agent 3).
  trend_mag > seuil  => STRONG trend -> RIDE (parie AVEC le move : drift30>0 LONG, <0 SHORT)
  trend_mag <= seuil => chop          -> FADE (parie CONTRE le move)
PnL = retour fwd 8h en direction du pari, net 3 bps maker-maker.

4 BRAS sur les MEMES events : SWITCH / ALWAYS-FADE / ALWAYS-TREND / RANDOM(coin-flip, 200 seeds).
Par mois + poole + t-stat, pour chaque (W, percentile-seuil). Le switch ne "marche" que s'il bat
ALWAYS-FADE *et* ALWAYS-TREND *et* RANDOM, robuste cross-mois.
"""
import os, zipfile, time, datetime
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

BINANCE_DIR = r"C:\Users\erast\hl_data\binance"
DAILY_DIR = os.path.join(BINANCE_DIR, "daily")
MIN = 60 * 1000
HOUR = 60 * MIN
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]
MONTHS = ["2026-01", "2026-02", "2026-03", "2026-04"]
MAY_DAYS = ["2026-04-30", "2026-05-01", "2026-05-02", "2026-05-03", "2026-05-04",
            "2026-05-05", "2026-05-06", "2026-05-07", "2026-05-15", "2026-05-16",
            "2026-05-17", "2026-05-18", "2026-05-19", "2026-05-20", "2026-05-21", "2026-05-22"]
FEE = 3.0
HOLD = 480 * MIN
TREND_WINDOWS = {"8h": 8 * HOUR, "24h": 24 * HOUR, "72h": 72 * HOUR}
PCTLS = [50, 75]            # seuil expansif : rider si trend_mag > ce percentile des events anterieurs
NULL_SEEDS = 200
RNG = np.random.default_rng(7)


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
    o = np.argsort(T, kind="stable"); T = T[o]; price = price[o]; m = m[o]
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
        if ts[i] - last >= 8 * HOUR:
            out.append(i); last = ts[i]
    return np.array(out, dtype=int)


def month_label(ts_ms):
    return datetime.datetime.utcfromtimestamp(ts_ms / 1000.0).strftime("%Y-%m")


def build_series(sym, period):
    if period == "MAY":
        pts, pmd = [], []
        for d in MAY_DAYS:
            zp = os.path.join(DAILY_DIR, f"{sym}-aggTrades-{d}.zip")
            if os.path.exists(zp):
                t, m = load(zp); pts.append(t); pmd.append(m)
        if not pts:
            return None, None
        ts = np.concatenate(pts); mid = np.concatenate(pmd)
        o = np.argsort(ts, kind="stable"); return ts[o], mid[o]
    zp = os.path.join(BINANCE_DIR, f"{sym}-aggTrades-{period}.zip")
    return load(zp) if os.path.exists(zp) else (None, None)


def main():
    t0 = time.time()
    print("=== TWO-SIDED SWITCH keyed on TREND MAGNITUDE (l'idee user) — Binance OOS 4 majors Jan->May ===")
    print("RIDE si |trend| fort (falling knife -> on suit) ; FADE si chop. Switch causal (seuil expansif, no-lookahead).")
    print(f"PnL = fwd {HOLD//HOUR}h net {FEE}bps. 4 bras memes events : SWITCH / ALWAYS-FADE / ALWAYS-TREND / RANDOM.\n")

    # per-asset chronological event records
    ev = {s: [] for s in SYMBOLS}   # list of dict(t, month, sgn, raw_fwd, tm8, tm24, tm72)
    for sym in SYMBOLS:
        for period in MONTHS + ["MAY"]:
            ts, mid = build_series(sym, period)
            if ts is None or len(ts) < 5000:
                print(f"  [skip] {sym} {period}"); continue
            v = vol30(ts, mid); d30 = drift(ts, mid, 30 * MIN)
            tm = {k: drift(ts, mid, W) for k, W in TREND_WINDOWS.items()}
            val = ~(np.isnan(v) | np.isnan(d30))
            if val.sum() < 1000:
                continue
            vP = np.nanpercentile(v[val], 75)
            p25 = np.nanpercentile(d30[val], 25); p75 = np.nanpercentile(d30[val], 75)
            evt = val & (v >= vP) & ((d30 <= p25) | (d30 >= p75))
            idx = no_overlap(evt, ts)
            if len(idx) == 0:
                continue
            j = np.searchsorted(ts, ts[idx] + HOLD, side="left")
            ok = j < len(ts)
            for kk, i in enumerate(idx):
                if not ok[kk]:
                    continue
                raw = (mid[j[kk]] - mid[i]) / mid[i] * 1e4   # signed fwd return
                sgn = 1.0 if d30[i] > 0 else -1.0
                rec = dict(t=int(ts[i]), month=month_label(ts[i]), sgn=sgn, raw=float(raw),
                           tm8=abs(tm["8h"][i]), tm24=abs(tm["24h"][i]), tm72=abs(tm["72h"][i]))
                ev[sym].append(rec)
            print(f"  {sym} {period}: events={len(idx)}")

    # flatten, per-asset chronological; build arms
    months = MONTHS + ["2026-05"]
    print("\n" + "=" * 100)
    print("RESULTATS — net bps/trade @8h, par bras. SWITCH doit battre ALWAYS-FADE & ALWAYS-TREND & RANDOM.")
    print("=" * 100)

    # ALWAYS arms + RANDOM (independent of switch var)
    allrec = []
    for s in SYMBOLS:
        allrec += sorted(ev[s], key=lambda r: r["t"])
    if not allrec:
        print("  aucun event"); return
    raw = np.array([r["raw"] for r in allrec]); sgn = np.array([r["sgn"] for r in allrec])
    mon = np.array([r["month"] for r in allrec])
    ride_gross = sgn * raw            # parier AVEC le move
    fade_gross = -ride_gross          # parier CONTRE
    always_fade = fade_gross - FEE
    always_trend = ride_gross - FEE
    # RANDOM : coin-flip ride/fade par event, moyenne sur seeds
    rnd = np.zeros(len(allrec))
    for _ in range(NULL_SEEDS):
        flip = RNG.random(len(allrec)) < 0.5
        rnd += np.where(flip, ride_gross, fade_gross) - FEE
    rnd /= NULL_SEEDS

    def line(name, arr):
        a = arr[~np.isnan(arr)]
        t = a.mean() / (a.std(ddof=1) / np.sqrt(len(a))) if len(a) > 1 else float('nan')
        permo = []
        for m in months:
            sel = (mon == m) & ~np.isnan(arr)
            permo.append(f"{arr[sel].mean():+6.1f}" if sel.sum() >= 5 else f"{'.':>6}")
        print(f"  {name:>26} | n={len(a):>4} | net={a.mean():>+6.2f} t={t:>+5.2f} | " + " ".join(permo))

    print(f"  {'(par mois ->)':>26} |        |               | " + " ".join(f"{m[-2:]:>6}" for m in months))
    line("ALWAYS-FADE", always_fade)
    line("ALWAYS-TREND", always_trend)
    line("RANDOM(null,200seeds)", rnd)

    # SWITCH for each (trend var, percentile) with per-asset EXPANDING percentile threshold (causal)
    for tmkey in ["tm8", "tm24", "tm72"]:
        for pct in PCTLS:
            switch_pnl = np.full(len(allrec), np.nan)
            pos = 0
            for s in SYMBOLS:
                recs = sorted(ev[s], key=lambda r: r["t"])
                seen = []
                for r in recs:
                    tmv = r[tmkey]
                    if not np.isnan(tmv) and len(seen) >= 10:
                        thr = np.percentile(seen, pct)
                        ride = tmv > thr
                    else:
                        ride = False   # avant assez d'historique -> defaut fade
                    g = (r["sgn"] * r["raw"]) if ride else (-r["sgn"] * r["raw"])
                    # map back to global allrec order: recompute index via running pointer per asset
                    switch_pnl[pos] = g - FEE
                    pos += 1
                    if not np.isnan(tmv):
                        seen.append(tmv)
            # NB: allrec was built per-asset in SYMBOLS order then concatenated, so pos aligns with allrec
            line(f"SWITCH[{tmkey}>P{pct}]", switch_pnl)

    print(f"\n[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
