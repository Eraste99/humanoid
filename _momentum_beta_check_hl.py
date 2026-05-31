"""ALPHA vs BETA du momentum — le HYPE-trend, est-ce du timing ou juste "HYPE a pompe" ? (scepticisme user, juste)

Le test momentum precedent comparait a un NULL direction-aleatoire, qui NE controle PAS le beta. Ici on compare au
BUY-AND-HOLD de l'actif, et on split le PnL momentum en LONGS vs SHORTS :
  - si mom ~ buy-hold ET ~tout le PnL vient des LONGS -> c'est du BETA (l'actif a monte), PAS de l'alpha de timing.
  - si mom bat buy-hold (surtout via des SHORTS gagnants en periodes baissieres) -> vrai timing alpha.
HL true-mid, par paire x semaine. entree=trend aligne 1h+4h, exit trailing (cf _momentum_majors_hl). taker RT6.
"""
import os, sys, time
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _momentum_majors_hl import sim_trend, drift_over, THR, HOUR, FEE_TK
from _oscillation_capture import PAIRS, WEEKS, MACRO, MIN, GAP


def buyhold_bps(ts, mid):
    # gap-aware : somme des retours de segments continus (evite de compter un saut de gap comme retour)
    seg_ret = 0.0; i0 = 0
    for i in range(1, len(ts)):
        if ts[i] - ts[i - 1] > GAP:
            seg_ret += (mid[i - 1] - mid[i0]) / mid[i0]; i0 = i
    seg_ret += (mid[-1] - mid[i0]) / mid[i0]
    return seg_ret * 1e4


def main():
    print("=== ALPHA vs BETA — momentum (trend aligne) vs BUY-AND-HOLD, split long/short — HL ===\n")
    print(f"  {'actif':>9} {'wk':>5} | {'buy&hold':>9} | {'MOM net':>8} {'MOM-B&H':>8} | {'LONG net(n)':>13} {'SHORT net(n)':>13}")
    print("  " + "-" * 78)
    for pair in PAIRS:
        for wk, ddir in WEEKS.items():
            try:
                ticks, trades, l2 = load_all(pair, ddir)
                ts, mid, _ = vectorized_features(ticks, trades, l2)
            except Exception:
                continue
            ts = np.asarray(ts, np.int64); mid = np.asarray(mid, np.float64); n = len(ts)
            if n < 1000:
                continue
            bh = buyhold_bps(ts, mid)
            d1 = drift_over(ts, mid, HOUR); d4 = drift_over(ts, mid, MACRO)
            sig = np.zeros(n)
            sig[(~np.isnan(d1)) & (~np.isnan(d4)) & (d1 > 0) & (d4 >= THR)] = 1.0
            sig[(~np.isnan(d1)) & (~np.isnan(d4)) & (d1 < 0) & (d4 <= -THR)] = -1.0
            prev = np.concatenate([[0.0], sig[:-1]])
            idxs = np.where((sig != 0) & (sig != prev))[0]
            longs = []; shorts = []; last_exit = -1
            for i in idxs:
                if ts[i] <= last_exit:
                    continue
                d = sig[i]; rt = sim_trend(ts, mid, int(i), d)
                if rt is None:
                    continue
                last_exit = rt[1]; net = rt[0] - FEE_TK
                (longs if d > 0 else shorts).append(net)
            momtot = sum(longs) + sum(shorts)
            nl, ns = len(longs), len(shorts)
            if nl + ns < 3:
                continue
            print(f"  {pair:>9} {wk:>5} | {bh:>+9.0f} | {momtot:>+8.0f} {momtot-bh:>+8.0f} | "
                  f"{sum(longs):>+8.0f}({nl:>2}) {sum(shorts):>+8.0f}({ns:>2})")
    print("\n  LECTURE : si MOM < buy&hold et SHORT net <=0 -> momentum = BETA (l'actif a monte), pas d'alpha de timing.")
    print("    si MOM > buy&hold (et SHORT net > 0 en semaines baissieres) -> vrai timing. (HYPE = le test cle.)")


if __name__ == "__main__":
    main()
