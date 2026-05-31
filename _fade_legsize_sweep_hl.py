"""SWEEP SEUIL DE JAMBE — la frequence n'est PAS gravee : 50bps etait un choix arbitraire. Combien de trades/jour
et quel net si on fade des jambes de 20/30/50/75 bps ? (bear, hold 30m & 2h, sortie maker RT3, SANS stop = mode deployable)

User (a raison) : pourquoi seulement ~10 trades/jour ? -> c'est le seuil 50bps + bear-only + 3 majors. Ici on teste
le seuil. Tradeoff attendu : petites jambes = + de trades mais rebond + petit (peut passer sous les frais) ;
grosses jambes = - de trades mais edge + gros. On veut le point qui maximise le TOTAL (n x net) en restant + net/trade.
HL vrai mid L2, in-sample, tick-level, ZERO sampling. mid-proxy (fill~mid valide sur HL, spreads ~0.35bps).
"""
import os, sys, time
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import leg_chain, drift_over, PAIRS, WEEKS, MACRO, MIN, GAP

THRS = [20.0, 30.0, 50.0, 75.0]
HS = {"30m": 30 * MIN, "2h": 120 * MIN}
FEE_MK = 3.0


def fwd_at(ts, mid, Bs, H):
    n = len(ts)
    jf = np.searchsorted(ts, ts[Bs] + H, side="left")
    ok = jf < n
    f = np.full(len(Bs), np.nan)
    f[ok] = (mid[jf[ok]] - mid[Bs[ok]]) / mid[Bs[ok]] * 1e4
    tf = np.zeros(len(Bs), bool); tf[ok] = (ts[jf[ok]] - ts[Bs[ok]]) > (H + GAP)
    f[tf] = np.nan
    return f


def main():
    t0 = time.time()
    print("=== SWEEP SEUIL DE JAMBE — bear fade, hold 30m & 2h (maker RT3, SANS stop) — HL in-sample ===")
    print("trades/jour + net/trade + TOTAL(n x net) par seuil. La frequence est un BOUTON, pas une loi.\n")
    print(f"  {'actif':>9} {'thr':>4} | {'legs':>5} {'bear_n':>6} {'tr/j':>5} | {'net30':>6} {'tot30':>7} | {'net2h':>6} {'tot2h':>7} {'win2h%':>6}")
    print("  " + "-" * 82)
    for pair in PAIRS:
        weeks = []
        span_days = 0.0
        for wk, ddir in WEEKS.items():
            try:
                ticks, trades, l2 = load_all(pair, ddir)
                ts, mid, _ = vectorized_features(ticks, trades, l2)
            except Exception as e:
                print(f"  [skip] {pair}/{wk}: {e}"); continue
            ts = np.asarray(ts, np.int64); mid = np.asarray(mid, np.float64)
            weeks.append((ts, mid))
            span_days += (ts[-1] - ts[0]) / 86400000.0
        if not weeks or span_days < 1:
            continue
        for thr in THRS:
            D = []; DM = []; F = {hk: [] for hk in HS}
            for ts, mid in weeks:
                Bs, ds = leg_chain(ts, mid, thr_bps=thr)
                if len(Bs) < 5:
                    continue
                D.append(ds); DM.append(drift_over(ts, mid, MACRO)[Bs])
                for hk, H in HS.items():
                    F[hk].append(fwd_at(ts, mid, Bs, H))
            if not D:
                continue
            d = np.concatenate(D); dmac = np.concatenate(DM)
            f30 = np.concatenate(F["30m"]); f2 = np.concatenate(F["2h"])
            legs = len(d)
            ok = ~np.isnan(dmac)
            if ok.sum() < 15:
                continue
            q1 = np.quantile(dmac[ok], 1 / 3)
            bear = ok & (dmac <= q1)
            # net fade (maker RT3, no stop), per horizon, bear only (fwd valide)
            res = {}
            for hk, farr in (("30m", f30), ("2h", f2)):
                m = bear & ~np.isnan(farr)
                if m.sum() < 5:
                    res[hk] = (0, float('nan'), float('nan'), float('nan')); continue
                fade = -d[m] * farr[m] - FEE_MK
                res[hk] = (int(m.sum()), float(np.mean(fade)), float(np.sum(fade)), 100.0 * np.mean(fade > 0))
            n30, net30, tot30, _ = res["30m"]
            n2, net2, tot2, win2 = res["2h"]
            trday = n2 / span_days
            print(f"  {pair:>9} {int(thr):>4} | {legs:>5} {n2:>6} {trday:>5.1f} | {net30:>+6.1f} {tot30:>+7.0f} | "
                  f"{net2:>+6.1f} {tot2:>+7.0f} {win2:>6.0f}")
        print("  " + "." * 82)
    print("\n  LECTURE : tr/j = trades bear/jour (ce seuil). net = bps/trade (maker, no-stop). TOT = n x net = capture totale.")
    print("    si net reste + a 20-30bps -> bien + de trades/jour possible (la frequence montait). si net s'ecroule -> 50bps justifie.")
    print(f"\n[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
