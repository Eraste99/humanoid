"""PAUSE sur CO-MOVE SYSTEMIQUE (la VRAIE version, critique user) — pauser TOUTES les entrees seulement quand
le MARCHE ENTIER bouge ensemble (>=K majors alignes), PAS quand BTC isole wiggle (mon erreur : ca frappait les calmes).

Signal = LARGEUR : a l'instant T, compter les majors avec |drift 10min|>=THR_pair, alignes meme sens. systemic = count>=K.
Si systemic -> pause TOUTES les entrees (couper tout, re-prendre quand la largeur retombe). != stop mid-trade, != BTC-isole.
Hypothese (user) : ca s'allume RAREMENT (vrais events type 17mai 23:40) -> coupe le cluster catastrophe SANS toucher
les bons fades des paires calmes (qui, en temps normal, ne bougent PAS toutes ensemble) -> assurance ciblee, EV-preservee.
Setup : fade unifie + filtre FLUX, 4 majors. HL true-mid, causal, ZERO sampling.
"""
import os, sys, time
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import leg_chain, drift_over, WEEKS, MACRO, MIN, GAP

HOUR = 60 * MIN; W10 = 10 * MIN; FEE_MK = 3.0; HOLD = 30 * MIN
MAJ = ["BTCUSDC", "ETHUSDC", "SOLUSDC", "XRPUSDC"]
CONFIGS = [(2, 30.0), (3, 30.0), (2, 50.0), (3, 50.0)]   # (K, THR_pair)


def drift10(ts, mid, T):
    i = np.searchsorted(ts, T, side="left"); j = np.searchsorted(ts, T - W10, side="left")
    if i >= len(ts) or j >= len(ts) or i <= j or (ts[i] - ts[j]) > W10 + GAP:
        return np.nan
    return (mid[i] - mid[j]) / mid[j] * 1e4


def breadth(series, T, THR):
    """nb de majors alignes (meme sens) bougeant >=THR sur 10min a T."""
    up = dn = 0
    for ts, mid in series:
        d = drift10(ts, mid, T)
        if np.isnan(d):
            continue
        if d >= THR:
            up += 1
        elif d <= -THR:
            dn += 1
    return max(up, dn)


def tail(net):
    a = np.array(net)
    if len(a) < 6:
        return None
    eq = np.cumsum(a)
    return float(np.mean(a)), float(np.min(a)), float(np.max(np.maximum.accumulate(eq) - eq)), float(eq[-1])


def main():
    print("=== PAUSE CO-MOVE SYSTEMIQUE (>=K majors bougent ensemble) — fade+FLUX, 4 majors, HL ===\n")
    D = {}
    for p in MAJ:
        for wk, ddir in WEEKS.items():
            try:
                ticks, trades, l2 = load_all(p, ddir)
                ts, mid, feats = vectorized_features(ticks, trades, l2)
            except Exception:
                continue
            D.setdefault(p, {})[wk] = (np.asarray(ts, np.int64), np.asarray(mid, np.float64), feats)
    res = {}; fire = {c: [0, 0] for c in CONFIGS}    # firing rate sur entrees
    for asset in MAJ:
        for wk in WEEKS:
            if wk not in D.get(asset, {}):
                continue
            series = [(D[p][wk][0], D[p][wk][1]) for p in MAJ if wk in D.get(p, {})]
            ts, mid, feats = D[asset][wk]
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
            thrs = sorted(set(t for _, t in CONFIGS))
            for ib, d, tfival in ents:
                if (not np.isnan(tfival)) and (np.sign(tfival) == d) and (abs(tfival) >= qtfi):
                    continue
                T = ts[ib]; j = np.searchsorted(ts, T + HOLD, side="left"); j = min(j, len(ts) - 1)
                net = (-d) * (mid[j] - mid[ib]) / mid[ib] * 1e4 - FEE_MK
                br = {th: breadth(series, T, th) for th in thrs}
                for sc in ("POOL", asset):
                    res.setdefault((sc, "baseline"), []).append(net)
                    for (K, TH) in CONFIGS:
                        if br[TH] < K:                      # PAS systemique -> on prend
                            res.setdefault((sc, f"K{K}_{int(TH)}"), []).append(net)
                for (K, TH) in CONFIGS:
                    fire[(K, TH)][1] += 1
                    if br[TH] >= K:
                        fire[(K, TH)][0] += 1

    print("  firing rate du gate (% des entrees ou le marche est systemique) :")
    for (K, TH) in CONFIGS:
        f = fire[(K, TH)]
        print(f"     K>={K} & |drift10|>={int(TH)} : {100*f[0]/max(f[1],1):.0f}% des entrees")
    for sc in ["POOL"] + MAJ:
        base = res.get((sc, "baseline"))
        if not base:
            continue
        nb = len(base)
        print(f"\n=== {sc} ===")
        print(f"  {'config':>10} | {'%gardé':>6} {'net/tr':>6} | {'worst1':>7} {'maxDD':>7} | {'total':>7}")
        for cfg in ["baseline"] + [f"K{K}_{int(TH)}" for (K, TH) in CONFIGS]:
            lst = res.get((sc, cfg))
            if not lst:
                continue
            st = tail(lst)
            if st is None:
                continue
            mn, w1, mdd, tot = st
            print(f"  {cfg:>10} | {100*len(lst)/nb:>5.0f}% {mn:>+6.1f} | {w1:>+7.1f} {mdd:>7.0f} | {tot:>+7.0f}")
    print("\n  LECTURE : si le gate co-move s'allume RAREMENT (firing% bas) ET coupe worst1/maxDD SANS tuer net/tr")
    print("    -> assurance CIBLEE = la version correcte de l'idee user (couper tout SEULEMENT sur vrai event systemique).")


if __name__ == "__main__":
    main()
