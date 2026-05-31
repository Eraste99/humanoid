"""CROSS-VENUE LEAD au moment des CATASTROPHES — Binance a-t-il MENE HL avant le -237 ? (signal flagge par ma propre
recherche comme "le lead causal le plus fort", jamais teste). Si Binance bougeait DEJA dans le sens de la jambe
AVANT l'entree HL (alors que la microstructure HL etait calme), alors le -237 etait DETECTABLE cross-venue.

Diagnostic : pour les pires trades fade HL ETH, drift Binance ETH & BTC sur [T-120s, T] (PRE-entree, causal) vs HL calme.
+ filtre Binance-lead sur TOUS les fades HL ETH : skip si |drift Binance 120s|>=THR & sign==jambe -> attrape le -237 ? EV ?
Binance = SIGNAL seulement (pas execution ; cross-venue exec interdit MiCA, mais signal OK). mid reconstruit de-bounce.
"""
import os, sys, time, zipfile
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import leg_chain, drift_over, WEEKS, MACRO, MIN, GAP

HOUR = 60 * MIN; FEE_MK = 3.0; HOLD = 30 * MIN
BIN_DAILY = os.path.join(r"C:\Users\erast\hl_data\binance", "daily")
DAYS = [f"2026-05-{d:02d}" for d in list(range(1, 8)) + list(range(15, 23))]


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


def bdrift(bts, bmid, T, W):
    i = np.searchsorted(bts, T, side="left"); j = np.searchsorted(bts, T - W, side="left")
    if i >= len(bts) or j >= len(bts) or i <= j:
        return np.nan
    return (bmid[i] - bmid[j]) / bmid[j] * 1e4


def fwd_at(ts, mid, Bs, H):
    n = len(ts); jf = np.searchsorted(ts, ts[Bs] + H, side="left"); ok = jf < n
    f = np.full(len(Bs), np.nan); f[ok] = (mid[jf[ok]] - mid[Bs[ok]]) / mid[Bs[ok]] * 1e4
    tf = np.zeros(len(Bs), bool); tf[ok] = (ts[jf[ok]] - ts[Bs[ok]]) > (H + GAP); f[tf] = np.nan
    return f


def main():
    print("=== CROSS-VENUE LEAD aux CATASTROPHES — Binance a-t-il mene HL avant le -237 ? ===\n")
    binE = load_bin("ETHUSDT"); binB = load_bin("BTCUSDT")
    if binE is None:
        print("pas de data Binance"); return
    bEts, bEmid = binE; bBts, bBmid = binB
    # HL ETH fade entries (config unifiee)
    ent = []
    for wk, ddir in WEEKS.items():
        try:
            ticks, trades, l2 = load_all("ETHUSDC", ddir)
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
            T = int(ts[Bs[k]])
            ent.append(dict(T=T, d=d, net=-d * fwd[k] - FEE_MK, tfi=tfi[k]))
    if not ent:
        print("pas d'entrees"); return
    ent.sort(key=lambda e: e["net"])
    print("  --- 6 PIRES trades HL ETH : Binance bougeait-il AVANT l'entree HL (PRE = [T-120s,T]) ? ---")
    print(f"  {'date':>15} {'net':>6} {'d':>3} | {'binETH 120s':>11} {'binETH 60s':>10} | {'binBTC 120s':>11} | meme sens jambe ?")
    for e in ent[:6]:
        dt = time.strftime("%m-%d %H:%M:%S", time.gmtime(e["T"] / 1000))
        be120 = bdrift(bEts, bEmid, e["T"], 120000); be60 = bdrift(bEts, bEmid, e["T"], 60000)
        bb120 = bdrift(bBts, bBmid, e["T"], 120000)
        # "meme sens jambe" : Binance bouge dans le sens d (= le sens qui ecrase le fade)
        lead = (not np.isnan(be120)) and (np.sign(be120) == e["d"]) and abs(be120) >= 20
        print(f"  {dt:>15} {e['net']:>+6.0f} {e['d']:>+3.0f} | {be120:>+9.0f}bps {be60:>+8.0f}bps | {bb120:>+9.0f}bps | "
              f"{'OUI Binance LEAD' if lead else 'non'}")
    # filtre Binance-lead sur tous les fades HL ETH
    net = np.array([e["net"] for e in ent]); d = np.array([e["d"] for e in ent])
    be = np.array([bdrift(bEts, bEmid, e["T"], 120000) for e in ent])
    bb = np.array([bdrift(bBts, bBmid, e["T"], 120000) for e in ent])

    def cut(keep, lab):
        a = net[keep]
        if len(a) < 6:
            print(f"  {lab:>20} | n<6"); return
        eq = np.cumsum(a); mdd = np.max(np.maximum.accumulate(eq) - eq)
        print(f"  {lab:>20} | gardé {100*keep.mean():>3.0f}% | net {np.mean(a):>+5.1f} | worst {np.min(a):>+7.0f} | maxDD {mdd:>5.0f}")
    print("\n  --- filtre Binance-lead sur TOUS les fades HL ETH (skip si Binance bouge >=THR dans le sens jambe, 120s) ---")
    cut(np.ones(len(net), bool), "baseline")
    for THR in (20, 30, 50):
        fire = (np.sign(be) == d) & (np.abs(be) >= THR)
        cut(~fire, f"binETH-lead>={THR}")
    for THR in (20, 30, 50):
        fire = (np.sign(bb) == d) & (np.abs(bb) >= THR)
        cut(~fire, f"binBTC-lead>={THR}")
    print("\n  LECTURE : si au -237 Binance bougeait deja >=20-30bps dans le sens de la jambe AVANT l'entree HL")
    print("    -> DETECTABLE cross-venue (mon 'indetectable' etait faux). Si filtre coupe worst SANS tuer EV -> le vrai missing piece.")


if __name__ == "__main__":
    main()
