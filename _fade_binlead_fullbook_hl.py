"""VALIDATION LIVRE COMPLET du filtre BINANCE-LEAD — le breakthrough ETH generalise-t-il a BTC/SOL/XRP/ETH, cross-week ?
Filtre : skip le fade HL si Binance bouge >=THR dans le sens de la jambe sur [T-120s, T] (lead causal). Binance = SIGNAL only.
2 variantes : same-asset (Binance de la meme paire) ET BTC-marche (Binance BTC pour toutes). Sur fade+FLUX (config deployable).
Compare baseline vs +binSAME vs +binBTC, par paire + POOL, cross-week. net/worst/maxDD/%garde. mid Binance reconstruit.
"""
import os, sys, time, zipfile
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import leg_chain, drift_over, WEEKS, MACRO, MIN, GAP

HOUR = 60 * MIN; FEE_MK = 3.0; HOLD = 30 * MIN; W120 = 120000
HLMAJ = ["BTCUSDC", "ETHUSDC", "SOLUSDC", "XRPUSDC"]
BIN = {"BTCUSDC": "BTCUSDT", "ETHUSDC": "ETHUSDT", "SOLUSDC": "SOLUSDT", "XRPUSDC": "XRPUSDT"}
BIN_DAILY = os.path.join(r"C:\Users\erast\hl_data\binance", "daily")
DAYS = [f"2026-05-{d:02d}" for d in list(range(1, 8)) + list(range(15, 23))]


def load_zip(zp):
    with zipfile.ZipFile(zp) as z:
        name = z.namelist()[0]
        with z.open(name) as f:
            hh = "price" in f.readline().decode("utf-8", "ignore").lower()
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


def load_bin(sym):
    A = []; B = []
    for d in DAYS:
        zp = os.path.join(BIN_DAILY, f"{sym}-aggTrades-{d}.zip")
        if os.path.exists(zp):
            t, m = load_zip(zp); A.append(t); B.append(m)
    if not A:
        return None
    ts = np.concatenate(A); mid = np.concatenate(B); o = np.argsort(ts, kind="stable")
    return ts[o], mid[o]


def bdrift(B, T):
    if B is None:
        return np.nan
    bts, bmid = B
    i = np.searchsorted(bts, T, side="left"); j = np.searchsorted(bts, T - W120, side="left")
    if i >= len(bts) or j >= len(bts) or i <= j:
        return np.nan
    return (bmid[i] - bmid[j]) / bmid[j] * 1e4


def fwd_at(ts, mid, Bs, H):
    n = len(ts); jf = np.searchsorted(ts, ts[Bs] + H, side="left"); ok = jf < n
    f = np.full(len(Bs), np.nan); f[ok] = (mid[jf[ok]] - mid[Bs[ok]]) / mid[Bs[ok]] * 1e4
    tf = np.zeros(len(Bs), bool); tf[ok] = (ts[jf[ok]] - ts[Bs[ok]]) > (H + GAP); f[tf] = np.nan
    return f


def tail(rows):
    if len(rows) < 6:
        return None
    rows = sorted(rows, key=lambda r: r[0]); net = np.array([r[1] for r in rows])
    eq = np.cumsum(net); return float(np.mean(net)), float(np.min(net)), float(np.max(np.maximum.accumulate(eq) - eq)), len(net)


def main():
    print("=== VALIDATION LIVRE filtre BINANCE-LEAD (skip si Binance court >=30bps dir jambe, 120s) — BTC/SOL/XRP/ETH ===\n")
    binD = {s: load_bin(s) for s in set(BIN.values())}
    res = {}   # (scope, cfg) -> list (T, net)
    THR = 30.0
    for pair in HLMAJ:
        for wk, ddir in WEEKS.items():
            try:
                ticks, trades, l2 = load_all(pair, ddir)
                ts, mid, feats = vectorized_features(ticks, trades, l2)
            except Exception:
                continue
            ts = np.asarray(ts, np.int64); mid = np.asarray(mid, np.float64)
            Bs, ds = leg_chain(ts, mid, 50.0)
            if len(Bs) < 5:
                continue
            dmac = drift_over(ts, mid, MACRO)[Bs]; d1 = drift_over(ts, mid, HOUR)[Bs]
            tfi = np.asarray(feats.get("tfi_5min"), float)[Bs]; fwd = fwd_at(ts, mid, Bs, HOLD)
            ok = ~np.isnan(dmac)
            if ok.sum() < 15:
                continue
            q1, q2 = np.quantile(dmac[ok], [1 / 3, 2 / 3])
            bm = ok & (dmac >= q2) & ~np.isnan(d1); m1 = np.median(d1[bm]) if bm.sum() else 0.0
            ents = []
            for k in range(len(Bs)):
                dm = dmac[k]
                if np.isnan(dm) or np.isnan(fwd[k]):
                    continue
                d = float(ds[k])
                if dm <= q1:
                    if d != -1.0:
                        continue
                elif dm >= q2:
                    if d != 1.0 or np.isnan(d1[k]) or d1[k] < m1:
                        continue
                else:
                    continue
                ents.append((int(Bs[k]), d, tfi[k], -d * fwd[k] - FEE_MK))
            if len(ents) < 8:
                continue
            qtfi = np.nanquantile(np.abs(np.array([e[2] for e in ents])), 0.70)
            for ib, d, tfival, net in ents:
                if (not np.isnan(tfival)) and (np.sign(tfival) == d) and (abs(tfival) >= qtfi):
                    continue   # FLUX skip
                T = ts[ib]
                bs = bdrift(binD.get(BIN[pair]), T); bb = bdrift(binD.get("BTCUSDT"), T)
                fire_same = (not np.isnan(bs)) and (np.sign(bs) == d) and (abs(bs) >= THR)
                fire_btc = (not np.isnan(bb)) and (np.sign(bb) == d) and (abs(bb) >= THR)
                for sc in (pair, "POOL", f"POOL_{wk}"):
                    res.setdefault((sc, "baseline"), []).append((T, net))
                    if not fire_same:
                        res.setdefault((sc, "binSAME"), []).append((T, net))
                    if not fire_btc:
                        res.setdefault((sc, "binBTC"), []).append((T, net))
                    if not (fire_same or fire_btc):
                        res.setdefault((sc, "binEITHER"), []).append((T, net))

    for sc in HLMAJ + ["POOL", "POOL_W507", "POOL_W516"]:
        base = res.get((sc, "baseline"))
        if not base:
            continue
        nb = len(base)
        print(f"=== {sc} ===")
        print(f"  {'config':>10} | {'%gardé':>6} {'net/tr':>6} | {'worst':>7} {'maxDD':>7}")
        for cfg in ("baseline", "binSAME", "binBTC", "binEITHER"):
            st = tail(res.get((sc, cfg), []))
            if st is None:
                continue
            mn, w1, mdd, n = st
            print(f"  {cfg:>10} | {100*n/nb:>5.0f}% {mn:>+6.1f} | {w1:>+7.0f} {mdd:>7.0f}")
        print()
    print("  LECTURE : si binSAME/binBTC AMELIORE net ET baisse worst/maxDD sur la plupart des paires + les 2 semaines")
    print("    -> le filtre Binance-lead generalise = vrai composant anti-queue (ameliore l'EV, contrairement aux single-venue).")


if __name__ == "__main__":
    main()
