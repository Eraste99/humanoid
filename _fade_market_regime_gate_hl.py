"""PAUSE DE REGIME sur les ENTREES (idee user) — quand le MARCHE (BTC) fait un move DIRECTIONNEL, on n'OUVRE pas
de nouveau fade (on reste flat, on surveille) ; on REPREND quand c'est calme. != stop mid-trade (qui tue l'EV).
C'est une selection AVANT entree (comme FLUX), pas un bail sur position ouverte.

Setup : fade unifie + filtre FLUX, 4 majors. GATE : skip l'entree si |drift BTC sur 15min a l'entree| >= THR
(marche en mouvement directionnel = regime informe). Pas de flatten des positions ouvertes (hold 30min maker normal).
Compare baseline vs gated(THR). net, worst1, maxDD, %entrees-gardees. HL true-mid, causal, ZERO sampling.
"""
import os, sys, time
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import leg_chain, drift_over, WEEKS, MACRO, MIN, GAP

HOUR = 60 * MIN; W15 = 15 * MIN; FEE_MK = 3.0; HOLD = 30 * MIN
MAJ = ["BTCUSDC", "ETHUSDC", "SOLUSDC", "XRPUSDC"]
THRS = [30.0, 50.0, 75.0]


def btc_d15(bts, bmid, t0):
    i = np.searchsorted(bts, t0, side="left"); j = np.searchsorted(bts, t0 - W15, side="left")
    if i >= len(bts) or j >= len(bts) or i <= j:
        return np.nan
    if bts[i] - bts[j] > W15 + GAP:
        return np.nan
    return (bmid[i] - bmid[j]) / bmid[j] * 1e4


def tail(net):
    a = np.array(net)
    if len(a) < 6:
        return None
    eq = np.cumsum(a); return float(np.mean(a)), float(np.min(a)), float(np.max(np.maximum.accumulate(eq) - eq)), float(eq[-1])


def main():
    print("=== PAUSE REGIME sur ENTREES (skip si BTC bouge directionnel) — fade+FLUX, 4 majors, HL ===\n")
    D = {}
    for p in MAJ:
        for wk, ddir in WEEKS.items():
            try:
                ticks, trades, l2 = load_all(p, ddir)
                ts, mid, feats = vectorized_features(ticks, trades, l2)
            except Exception:
                continue
            D.setdefault(p, {})[wk] = (np.asarray(ts, np.int64), np.asarray(mid, np.float64), feats)
    res = {}
    for asset in MAJ:
        for wk in WEEKS:
            if wk not in D.get(asset, {}) or wk not in D.get("BTCUSDC", {}):
                continue
            ts, mid, feats = D[asset][wk]; bts, bmid, _ = D["BTCUSDC"][wk]
            Bs, ds = leg_chain(ts, mid, 50.0)
            if len(Bs) < 5:
                continue
            dmac = drift_over(ts, mid, MACRO)[Bs]; d1 = drift_over(ts, mid, HOUR)[Bs]
            tfi = np.asarray(feats.get("tfi_5min"), float)[Bs]
            ok = ~np.isnan(dmac)
            if ok.sum() < 15:
                continue
            q1, q2 = np.quantile(dmac[ok], [1 / 3, 2 / 3])
            bm = ok & (dmac >= q2) & ~np.isnan(d1); m1 = np.median(d1[bm]) if bm.sum() else 0.0
            ents = []
            for k in range(len(Bs)):
                dm = dmac[k]
                if np.isnan(dm):
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
                ents.append((int(Bs[k]), d, tfi[k]))
            if len(ents) < 8:
                continue
            qtfi = np.nanquantile(np.abs(np.array([e[2] for e in ents])), 0.70)
            for ib, d, tfival in ents:
                if (not np.isnan(tfival)) and (np.sign(tfival) == d) and (abs(tfival) >= qtfi):
                    continue  # FLUX skip
                j = np.searchsorted(ts, ts[ib] + HOLD, side="left"); j = min(j, len(ts) - 1)
                net = (-d) * (mid[j] - mid[ib]) / mid[ib] * 1e4 - FEE_MK
                bd = abs(btc_d15(bts, bmid, ts[ib]))
                for sc in ("POOL", asset):
                    res.setdefault((sc, "baseline"), []).append(net)
                    for THR in THRS:
                        if np.isnan(bd) or bd < THR:        # gate OUVERT (marche calme) -> on prend
                            res.setdefault((sc, f"gate{int(THR)}"), []).append(net)

    for sc in ["POOL"] + MAJ:
        base = res.get((sc, "baseline"))
        if not base:
            continue
        nb = len(base)
        print(f"=== {sc} ===")
        print(f"  {'config':>10} | {'n':>4} {'%gardé':>6} {'net/tr':>6} | {'worst1':>7} {'maxDD':>7} | {'total':>7}")
        for cfg in ["baseline"] + [f"gate{int(t)}" for t in THRS]:
            lst = res.get((sc, cfg))
            if not lst:
                continue
            st = tail(lst)
            if st is None:
                continue
            mn, w1, mdd, tot = st
            print(f"  {cfg:>10} | {len(lst):>4} {100*len(lst)/nb:>5.0f}% {mn:>+6.1f} | {w1:>+7.1f} {mdd:>7.0f} | {tot:>+7.0f}")
        print()
    print("  LECTURE : si gate BAISSE worst1/maxDD SANS tuer net/tr (contrairement au stop mid-trade) -> la PAUSE D'ENTREE marche.")
    print("    %gardé = entrees prises (le reste = sautees car marche en mouvement). On reprend des que BTC se calme.")


if __name__ == "__main__":
    main()
