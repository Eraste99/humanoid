"""CROSS-WEEK sur HL TRUE-MID — le fade-extension (bear ET bull, hold 15min ET 2h) est-il consistant entre
les 2 semaines HL distinctes (W507 vs W516) ? = test de robustesse SUR DONNEES PROPRES (vrai mid L2, zero artefact
de-bounce), pour ne PAS dependre du Binance reconstruit (qui penalise le fade, surtout aux holds courts).

Question user : (1) Binance OOS est-il serieux ? -> ici on s'en passe, on regarde la consistance cross-semaine en clean.
(2) est-ce SEULEMENT bear ? -> on mesure bear ET bull cote a cote.
regime = tercile drift_4h (pooled par actif pour coherence), net maker RT3. tick-level, ZERO sampling.
"""
import os, sys, time
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import leg_chain, drift_over, PAIRS, WEEKS, MACRO, MIN, GAP

CAP = 120 * MIN
FEE_MK = 3.0
HOLDS = [15, 120]      # minutes


def net_at(ts, mid, ib, pos, Tmin):
    entry = mid[ib]; t0 = ts[ib]
    j = np.searchsorted(ts, t0 + Tmin * MIN, side="left")
    if j >= len(ts):
        j = len(ts) - 1
    sub = ts[ib:j + 1]
    if len(sub) > 1:
        g = np.where(np.diff(sub) > GAP)[0]
        if len(g):
            j = ib + g[0]
    if j <= ib:
        return None
    return pos * (mid[j] - entry) / entry * 1e4 - FEE_MK


def main():
    t0 = time.time()
    print("=== CROSS-WEEK HL TRUE-MID — fade bear & bull, hold 15min & 2h, par SEMAINE (W507 vs W516) — donnees propres ===")
    print("teste si l'edge tient sur les 2 semaines distinctes SANS dependre du Binance reconstruit.\n")
    print(f"  {'actif':>9} {'regime':>6} {'wk':>5} | {'n':>4} | {'net15':>6} {'win15':>5} | {'net2h':>6} {'win2h':>5}")
    print("  " + "-" * 66)
    for pair in PAIRS:
        wk_data = {}; dmac_pool = []
        for wk, ddir in WEEKS.items():
            try:
                ticks, trades, l2 = load_all(pair, ddir)
                ts, mid, _ = vectorized_features(ticks, trades, l2)
            except Exception as e:
                print(f"  [skip] {pair}/{wk}: {e}"); continue
            ts = np.asarray(ts, np.int64); mid = np.asarray(mid, np.float64)
            Bs, ds = leg_chain(ts, mid, 50.0)
            if len(Bs) < 5:
                continue
            dmac = drift_over(ts, mid, MACRO)[Bs]
            wk_data[wk] = (ts, mid, Bs, ds, dmac); dmac_pool.append(dmac)
        if not dmac_pool:
            continue
        alld = np.concatenate(dmac_pool); v = ~np.isnan(alld)
        if v.sum() < 30:
            continue
        q1, q2 = np.quantile(alld[v], [1 / 3, 2 / 3])
        for regime in ("bear", "bull"):
            for wk in WEEKS:
                if wk not in wk_data:
                    continue
                ts, mid, Bs, ds, dmac = wk_data[wk]
                n15 = []; w15 = []; n2 = []; w2 = []
                for k in range(len(Bs)):
                    dm = dmac[k]
                    if np.isnan(dm):
                        continue
                    if regime == "bear" and dm > q1:
                        continue
                    if regime == "bull" and dm < q2:
                        continue
                    pos = -float(ds[k]); ib = int(Bs[k])
                    a = net_at(ts, mid, ib, pos, 15); b = net_at(ts, mid, ib, pos, 120)
                    if a is not None:
                        n15.append(a)
                    if b is not None:
                        n2.append(b)
                if len(n15) < 8:
                    continue
                net15 = np.mean(n15); win15 = 100 * np.mean(np.array(n15) > 0)
                net2 = np.mean(n2) if n2 else float('nan'); win2 = 100 * np.mean(np.array(n2) > 0) if n2 else float('nan')
                print(f"  {pair:>9} {regime:>6} {wk:>5} | {len(n15):>4} | {net15:>+6.1f} {win15:>5.0f} | {net2:>+6.1f} {win2:>5.0f}")
            print("  " + "." * 66)
    print("\n  LECTURE : net15/net2h = bps/trade (maker RT3) par SEMAINE. Si net15 BEAR > 0 sur W507 ET W516 -> pas un fluke")
    print("            d'une semaine, et le Binance reconstruit etait juste aveugle au hold court (artefact). Idem pour BULL.")
    print(f"\n[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
