"""MOMENTUM LONG-SCALE sur TOUTES les paires (le vrai trend-following, PAS le leg-ride 30min).

Question : le trend-following paie-t-il sur les MAJORS (pas que HYPE) ? = la 2e sleeve (momentum) de la vision tout-terrain.
Instrument facon HYPE-trend winner : ENTREE = trend aligne (drift_1h ET drift_4h meme sens, |drift_4h|>=THR) ;
position AVEC la tendance ; EXIT = trailing sur MFE (arm/trail) + hard stop + max-hold long. Entree event-driven
(rising-edge du signal, no-overlap). Frais TAKER RT (momentum = chase). Par paire x semaine, vs NULL (direction alea).
Controle cle : TREND bat-il NULL ? (sinon c'est juste "etre long dans un bull" = capture triviale du drift).
HL true-mid, tick-level, ZERO sampling (entrees event-driven, pas de grille).
"""
import os, sys, time
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import leg_chain, drift_over, PAIRS, WEEKS, MACRO, MIN, GAP

HOUR = 60 * MIN
FEE_TK = 6.0           # momentum chase = taker entry + taker/trail exit
THR = 100.0            # |drift_4h| >= 100 bps = vraie tendance
ARM, TRAIL, SL = 50.0, 30.0, 120.0
MAXHOLD = 480 * MIN    # 8h


def sim_trend(ts, mid, i, d, maxhold=MAXHOLD):
    entry = mid[i]; t0 = ts[i]
    jend = np.searchsorted(ts, t0 + maxhold, side="right")
    seg_ts = ts[i + 1:jend]; seg = mid[i + 1:jend]
    if len(seg) < 1:
        return None
    g = np.where(np.diff(np.concatenate([[t0], seg_ts])) > GAP)[0]
    if len(g):
        seg_ts = seg_ts[:g[0]]; seg = seg[:g[0]]
    if len(seg) < 1:
        return None
    fav = d * (seg - entry) / entry * 1e4
    peak = np.maximum.accumulate(fav)
    stop = np.where(fav <= -SL)[0]
    trail = np.where((peak >= ARM) & (fav <= peak - TRAIL))[0]
    si = stop[0] if len(stop) else 10**9
    ti = trail[0] if len(trail) else 10**9
    if si == 10**9 and ti == 10**9:
        return float(fav[-1]), int(seg_ts[-1])
    if si <= ti:
        return float(-SL), int(seg_ts[si])
    return float(fav[ti]), int(seg_ts[ti])


def main():
    t0 = time.time()
    print("=== MOMENTUM LONG-SCALE (trend aligne 1h+4h, exit trailing, hold 8h) — 5 paires HL, par semaine ===")
    print(f"entry |drift_4h|>={THR:.0f} & drift_1h meme sens ; arm{ARM:.0f}/trail{TRAIL:.0f}/sl{SL:.0f} ; taker RT{FEE_TK:.0f}. vs NULL(dir alea).\n")
    print(f"  {'actif':>9} {'wk':>5} | {'n':>4} | {'TREND net':>9} {'win%':>5} | {'NULL':>6} | {'TREND-NULL':>10} | {'total':>7}")
    print("  " + "-" * 80)
    for pair in PAIRS:
        for wk, ddir in WEEKS.items():
            try:
                ticks, trades, l2 = load_all(pair, ddir)
                ts, mid, _ = vectorized_features(ticks, trades, l2)
            except Exception:
                continue
            ts = np.asarray(ts, np.int64); mid = np.asarray(mid, np.float64)
            n = len(ts)
            if n < 1000:
                continue
            d1 = drift_over(ts, mid, HOUR); d4 = drift_over(ts, mid, MACRO)
            sig = np.zeros(n)
            sig[(~np.isnan(d1)) & (~np.isnan(d4)) & (d1 > 0) & (d4 >= THR)] = 1.0
            sig[(~np.isnan(d1)) & (~np.isnan(d4)) & (d1 < 0) & (d4 <= -THR)] = -1.0
            prev = np.concatenate([[0.0], sig[:-1]])
            enter = (sig != 0) & (sig != prev)        # onset d'un nouveau signal
            idxs = np.where(enter)[0]
            tnet = []; nullv = []; last_exit = -1
            for i in idxs:
                if ts[i] <= last_exit:
                    continue
                d = sig[i]
                rt = sim_trend(ts, mid, int(i), d)
                ra = sim_trend(ts, mid, int(i), -d)
                if rt is None:
                    continue
                tnet.append(rt[0] - FEE_TK)
                last_exit = rt[1]
                if ra is not None:
                    nullv.append(0.5 * ((rt[0] - FEE_TK) + (ra[0] - FEE_TK)))
            if len(tnet) < 5:
                print(f"  {pair:>9} {wk:>5} | n={len(tnet):>2} (insuf)")
                continue
            a = np.array(tnet); nm = np.mean(nullv) if nullv else float('nan')
            print(f"  {pair:>9} {wk:>5} | {len(a):>4} | {np.mean(a):>+9.1f} {100*np.mean(a>0):>5.0f} | {nm:>+6.1f} | {np.mean(a)-nm:>+10.1f} | {np.sum(a):>+7.0f}")
        print("  " + "." * 80)
    print("\n  LECTURE : TREND net>0 ET TREND-NULL>0 sur les 2 semaines (majors) -> le momentum long-scale PAIE sur majors,")
    print("    et le TIMING ajoute (pas juste le drift). Sinon -> momentum reste HYPE-only / pas un edge majors.")
    print(f"\n[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
