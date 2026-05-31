"""OOS — EXIT COURT (hold 5/15/30min + trailing) sur Binance 5 mois. Le hold-15min in-sample (+6-10, bps/h +24-39)
survit-il hors-echantillon ? bear fade, jambes 50bps, sortie maker RT3. mid reconstruit de-bounce (fade conservateur).
trailing VECTORISE (np.maximum.accumulate) car les fenetres Binance sont denses (~57k ticks/30min sur BTC).
tick-level, ZERO sampling. cap 30min (t60/t120 deja juges inefficients in-sample).
"""
import os, time, zipfile
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

BIN_DIR = r"C:\Users\erast\hl_data\binance"
DAILY = os.path.join(BIN_DIR, "daily")
MIN = 60 * 1000
GAP = 10 * MIN
MACRO = 4 * 60 * MIN
CAP = 30 * MIN
FEE_MK = 3.0
TIMES = [5, 15, 30]
TRAILS = [(8.0, 6.0)]
SYMS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]
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


def main():
    t0 = time.time()
    print("=== OOS BINANCE — EXIT COURT (5/15/30min + trailing) bear fade, bps/HEURE — 50bps, maker RT3 ===")
    print("le hold-15min in-sample survit-il OOS ? mid reconstruit (fade conservateur). cap 30min.\n")
    print(f"  {'actif':>9} {'exit':>9} | {'n':>5} {'net':>6} {'win%':>5} {'holdmean':>8} | {'bps/HEURE':>9}")
    print("  " + "-" * 70)
    for sym in SYMS:
        blocks = [(mo, [os.path.join(BIN_DIR, f"{sym}-aggTrades-{mo}.zip")]) for mo in MONTHS]
        blocks.append(("MAY-A", [os.path.join(DAILY, f"{sym}-aggTrades-{d}.zip") for d in MAY_A]))
        blocks.append(("MAY-B", [os.path.join(DAILY, f"{sym}-aggTrades-{d}.zip") for d in MAY_B]))
        bdata = []; dms = []
        for label, paths in blocks:
            lb = load_block(paths)
            if lb is None:
                continue
            ts, mid = lb
            if len(ts) < 5000:
                continue
            Bs, ds = leg_chain(ts, mid, 50.0)
            if len(Bs) < 10:
                continue
            dm = drift_over(ts, mid, MACRO)[Bs]
            bdata.append((ts, mid, Bs, ds, dm)); dms.append(dm)
            del ts, mid
        if not dms:
            continue
        alld = np.concatenate(dms); v = ~np.isnan(alld)
        if v.sum() < 30:
            continue
        q1 = np.quantile(alld[v], 1 / 3)
        T_net = {T: [] for T in TIMES}; T_hold = {T: [] for T in TIMES}
        R_net = {tr: [] for tr in TRAILS}; R_hold = {tr: [] for tr in TRAILS}
        for ts, mid, Bs, ds, dm in bdata:
            n = len(ts)
            for k in range(len(Bs)):
                if np.isnan(dm[k]) or dm[k] > q1:
                    continue
                ib = int(Bs[k]); pos = -float(ds[k]); entry = mid[ib]; tB = ts[ib]
                jend = np.searchsorted(ts, tB + CAP, side="right")
                seg_ts = ts[ib + 1:jend]; seg = mid[ib + 1:jend]
                if len(seg) < 1:
                    continue
                g = np.where(np.diff(np.concatenate([[tB], seg_ts])) > GAP)[0]
                if len(g):
                    seg_ts = seg_ts[:g[0]]; seg = seg[:g[0]]
                if len(seg) < 1:
                    continue
                fav = pos * (seg - entry) / entry * 1e4
                rel = (seg_ts - tB) / float(MIN)
                for T in TIMES:
                    i = np.searchsorted(rel, T, side="left")
                    if i >= len(rel):
                        i = len(rel) - 1
                    T_net[T].append(fav[i] - FEE_MK); T_hold[T].append(rel[i])
                for (arm, trail) in TRAILS:
                    peak = np.maximum.accumulate(fav)
                    cond = (peak >= arm) & (fav <= peak - trail)
                    w = np.where(cond)[0]
                    if len(w):
                        R_net[(arm, trail)].append(fav[w[0]] - FEE_MK); R_hold[(arm, trail)].append(rel[w[0]])
                    else:
                        R_net[(arm, trail)].append(fav[-1] - FEE_MK); R_hold[(arm, trail)].append(rel[-1])

        def line(name, nets, holds):
            if len(nets) < 5:
                return
            nets = np.array(nets); holds = np.array(holds)
            mn = float(np.mean(nets)); win = 100.0 * np.mean(nets > 0); hmn = float(np.mean(holds))
            bph = mn / (hmn / 60.0) if hmn > 0 else float('nan')
            print(f"  {sym:>9} {name:>9} | {len(nets):>5} {mn:>+6.1f} {win:>5.0f} {hmn:>7.1f}m | {bph:>+9.1f}")

        for T in TIMES:
            line(f"t{T}m", T_net[T], T_hold[T])
        for (arm, trail) in TRAILS:
            line(f"tr{int(arm)}/{int(trail)}", R_net[(arm, trail)], R_hold[(arm, trail)])
        print("  " + "." * 70)
    print("\n  LECTURE : si t15m reste net>0 OOS sur BTC/SOL/XRP avec bps/h eleve -> le hold court tient hors-echantillon.")
    print("    rappel de-bounce : fade conservateur (vrai book-mid >=).")
    print(f"\n[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
