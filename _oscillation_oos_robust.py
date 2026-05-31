"""OOS BINANCE robuste — meme construction oscillation, mais STATS ROBUSTES (median, hit-rate, winsor).

Pourquoi : la 1ere passe (mean) etait contaminee par des ticks aberrants de la reconstruction de-bounce
sur les majors moins liquides (SOL/XRP/ETH) -> fwd moyens absurdes (+60-90 bps/30m). La MOYENNE n'est
pas robuste aux outliers. Ici on regarde la TENDANCE CENTRALE : median(FADE), hit-rate P(FADE>0) et
P(FADE>fee), winsorized-mean (clip 1%/99%). Si en BEAR le fade a median>0 ET hit>50% sur les 4 majors
-> direction mean-reversion ROBUSTE (pas un artefact d'outliers). On cache aussi les arrays en .npz.

mid reconstruit de-bounce (buy-taker=ask/sell-taker=bid/ffill). tick-level, ZERO sampling. Jan->Mai.
"""
import os, time, zipfile
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

BINANCE_DIR = r"C:\Users\erast\hl_data\binance"
DAILY_DIR = os.path.join(BINANCE_DIR, "daily")
OUTDIR = os.path.dirname(os.path.abspath(__file__))
MIN = 60 * 1000
GAP = 10 * MIN
FEE = 3.0
MACRO = 4 * 60 * MIN
HS = {"30m": 30 * MIN, "2h": 120 * MIN}
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]
MONTHS = ["2026-01", "2026-02", "2026-03", "2026-04"]
MAY_A = [f"2026-05-{d:02d}" for d in range(1, 8)]
MAY_B = [f"2026-05-{d:02d}" for d in range(15, 23)]


def load_zip(zp):
    with zipfile.ZipFile(zp) as z:
        name = z.namelist()[0]
        with z.open(name) as f:
            first = f.readline().decode("utf-8", "ignore")
        hh = "price" in first.lower()
        with z.open(name) as f:
            df = pd.read_csv(f, header=0 if hh else None)
    price = df.iloc[:, 1].values.astype(np.float64); T = df.iloc[:, 5].values.astype(np.int64)
    m = df.iloc[:, 6].astype(str).str.lower().values
    o = np.argsort(T, kind="stable"); T = T[o]; price = price[o]; m = m[o]
    ibt = (m == "false")
    ask = pd.Series(np.where(ibt, price, np.nan)).ffill().values
    bid = pd.Series(np.where(~ibt, price, np.nan)).ffill().values
    mid = (ask + bid) / 2.0; ok = ~np.isnan(mid)
    return T[ok], mid[ok]


def load_block(paths):
    A = []; B = []
    for zp in paths:
        if os.path.exists(zp):
            t, m = load_zip(zp); A.append(t); B.append(m)
    if not A:
        return None
    ts = np.concatenate(A); mid = np.concatenate(B)
    o = np.argsort(ts, kind="stable")
    return ts[o], mid[o]


def drift_over(ts, mid, W):
    p = np.searchsorted(ts, ts - W, side="left")
    d = (mid - mid[p]) / mid[p] * 1e4
    return np.where((ts - ts[p]) >= 0.5 * W, d, np.nan)


def leg_chain(ts, mid, thr_bps=50.0):
    thr = thr_bps / 1e4; n = len(ts); Bs = []; ds = []
    i = 0
    while i < n - 1:
        p = mid[i]; up = p * (1 + thr); dn = p * (1 - thr)
        j = i + 1; found = -1; fdir = 0; win = 4096
        while j < n:
            end = min(n, j + win); seg = mid[j:end]
            hit = (seg >= up) | (seg <= dn)
            if hit.any():
                k = int(np.argmax(hit)); found = j + k
                fdir = 1 if mid[found] >= up else -1; break
            j = end; win = min(win * 4, 4_000_000)
        if found < 0:
            break
        Bs.append(found); ds.append(fdir); i = found
    return np.array(Bs, dtype=int), np.array(ds, dtype=float)


def block_collect(ts, mid):
    Bs, ds = leg_chain(ts, mid)
    if len(Bs) < 10:
        return None, 0
    dmac = drift_over(ts, mid, MACRO)[Bs]
    out = dict(d=ds, dmac=dmac)
    n = len(ts)
    for hk, H in HS.items():
        jf = np.searchsorted(ts, ts[Bs] + H, side="left")
        ok = jf < n
        f = np.full(len(Bs), np.nan)
        f[ok] = (mid[jf[ok]] - mid[Bs[ok]]) / mid[Bs[ok]] * 1e4
        tf = np.zeros(len(Bs), bool); tf[ok] = (ts[jf[ok]] - ts[Bs[ok]]) > (H + GAP)
        f[tf] = np.nan
        out["fwd_" + hk] = f
    return out, len(Bs)


def winmean(x, lo=1, hi=99):
    if len(x) < 10:
        return float(np.mean(x)) if len(x) else float('nan')
    a, b = np.percentile(x, [lo, hi])
    return float(np.mean(np.clip(x, a, b)))


def main():
    t0 = time.time()
    print("=== OOS BINANCE ROBUSTE — fade par regime : median / hit-rate / winsor (anti-outlier) — 4 majors Jan->Mai ===")
    print("question : en BEAR, fade a-t-il median>0 ET hit>50% sur les 4 majors ? (mean contaminee par ticks aberrants)\n")
    A = {s: {"d": [], "dmac": [], "fwd_30m": [], "fwd_2h": []} for s in SYMBOLS}
    for sym in SYMBOLS:
        blocks = [(mo, [os.path.join(BINANCE_DIR, f"{sym}-aggTrades-{mo}.zip")]) for mo in MONTHS]
        blocks.append(("MAY-A", [os.path.join(DAILY_DIR, f"{sym}-aggTrades-{d}.zip") for d in MAY_A]))
        blocks.append(("MAY-B", [os.path.join(DAILY_DIR, f"{sym}-aggTrades-{d}.zip") for d in MAY_B]))
        for label, paths in blocks:
            lb = load_block(paths)
            if lb is None:
                continue
            ts, mid = lb
            if len(ts) < 5000:
                continue
            r, nlegs = block_collect(ts, mid)
            if r is None:
                continue
            for k in A[sym]:
                A[sym][k].append(r[k])
            print(f"  {sym}/{label}: {nlegs} jambes  ({len(ts):,} ticks)")
            del ts, mid
        for k in A[sym]:
            A[sym][k] = np.concatenate(A[sym][k]) if A[sym][k] else np.array([])
        np.savez(os.path.join(OUTDIR, f"_oos_osc_{sym}.npz"),
                 d=A[sym]["d"], dmac=A[sym]["dmac"], fwd_30m=A[sym]["fwd_30m"], fwd_2h=A[sym]["fwd_2h"])

    for hk in ("fwd_30m", "fwd_2h"):
        H = hk.split("_")[1]
        print("\n" + "=" * 116)
        print(f"HORIZON H = {H}  | FADE robuste par regime (net 3bps) : mean (contaminee) vs WINSOR / MEDIAN / HIT%   [OOS]")
        print("=" * 116)
        print(f"  {'actif':>9} {'regime':>6} | {'n':>5} | {'FADEmean':>8} {'FADEwins':>8} {'FADEmed':>8} | {'hit>0%':>7} {'hit>fee%':>8} | {'RIDEmed':>8}")
        print("  " + "-" * 92)
        for s in SYMBOLS:
            d = A[s]["d"]; dmac = A[s]["dmac"]; fwd = A[s][hk]
            if len(d) == 0:
                continue
            ok = ~(np.isnan(fwd) | np.isnan(dmac))
            d = d[ok]; dmac = dmac[ok]; fwd = fwd[ok]
            if len(d) < 30:
                continue
            q1, q2 = np.quantile(dmac, [1 / 3, 2 / 3])
            regimes = [("bear", dmac <= q1), ("chop", (dmac > q1) & (dmac < q2)), ("bull", dmac >= q2)]
            for rname, rmask in regimes:
                if rmask.sum() < 10:
                    continue
                dd = d[rmask]; ff = fwd[rmask]
                fade_raw = -dd * ff                       # fee-free directional pnl du fade
                ride_raw = dd * ff
                fmean = float(np.mean(fade_raw)) - FEE
                fwins = winmean(fade_raw) - FEE
                fmed = float(np.median(fade_raw)) - FEE
                hit0 = 100.0 * np.mean(fade_raw > 0)
                hitf = 100.0 * np.mean(fade_raw > FEE)
                rmed = float(np.median(ride_raw)) - FEE
                print(f"  {s:>9} {rname:>6} | {int(rmask.sum()):>5} | {fmean:>+8.1f} {fwins:>+8.1f} {fmed:>+8.1f} | "
                      f"{hit0:>7.1f} {hitf:>8.1f} | {rmed:>+8.1f}")

    print("\n  LECTURE robuste : ignore FADEmean (outliers). Regarde FADEmed (median net) et hit%.")
    print("    BEAR : si FADEmed>0 ET hit>0%>50 sur les 4 majors -> mean-reversion ROBUSTE OOS (vrai edge directionnel).")
    print("    si FADEmed~0 / hit~50% alors que mean>0 -> c'etait juste des ticks aberrants (PAS d'edge).")
    print(f"\n[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
