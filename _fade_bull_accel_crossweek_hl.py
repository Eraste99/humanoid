"""VERIF CROSS-WEEK du fade-bull conditionne ACCEL — le 'fade le rip quand 1h accelere' (+7.3 poole) tient-il
sur W507 ET W516 separement ? (le bull brut flippait cross-week ; le conditionnement le sauve-t-il VRAIMENT ?)

bull = tercile haut drift_4h (cutoff POOLE par paire) ; accel = drift_1h >= mediane(d1 dans bull, poolee).
fade = short up-leg, hold 30min, maker RT3. net/win/n PAR SEMAINE, par paire + majors pooles.
HL true-mid, tick-level, ZERO sampling. mid-proxy.
"""
import os, sys, time
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import leg_chain, drift_over, PAIRS, WEEKS, MACRO, MIN, GAP

HOUR = 60 * MIN
FEE_MK = 3.0
HOLD = 30 * MIN


def fwd_at(ts, mid, Bs, H):
    n = len(ts)
    jf = np.searchsorted(ts, ts[Bs] + H, side="left")
    ok = jf < n
    f = np.full(len(Bs), np.nan)
    f[ok] = (mid[jf[ok]] - mid[Bs[ok]]) / mid[Bs[ok]] * 1e4
    tf = np.zeros(len(Bs), bool); tf[ok] = (ts[jf[ok]] - ts[Bs[ok]]) > (H + GAP)
    f[tf] = np.nan
    return f


def cell(fade, mask):
    m = mask & ~np.isnan(fade)
    n = int(m.sum())
    if n < 6:
        return f"n={n:>3} (insuff)"
    a = fade[m]
    return f"n={n:>3} net{np.mean(a):>+6.1f} win{100*np.mean(a>0):>3.0f}%"


def main():
    t0 = time.time()
    print("=== CROSS-WEEK du FADE-BULL ACCEL — 'fade le rip quand 1h accelere' tient-il W507 ET W516 ? — HL true-mid ===")
    print("bull=tercile haut drift_4h ; accel=drift_1h>=mediane(bull). fade short up-leg, hold 30min, maker RT3.\n")
    MAJ = []
    for pair in PAIRS:
        rows = []
        for wk, ddir in WEEKS.items():
            try:
                ticks, trades, l2 = load_all(pair, ddir)
                ts, mid, _ = vectorized_features(ticks, trades, l2)
            except Exception:
                continue
            ts = np.asarray(ts, np.int64); mid = np.asarray(mid, np.float64)
            Bs, ds = leg_chain(ts, mid, 50.0)
            if len(Bs) < 5:
                continue
            dmac = drift_over(ts, mid, MACRO)[Bs]
            d1 = drift_over(ts, mid, HOUR)[Bs]
            fwd = fwd_at(ts, mid, Bs, HOLD)
            fade = -ds * fwd - FEE_MK
            wkarr = np.full(len(Bs), wk, dtype=object)
            rows.append((fade, dmac, d1, wkarr))
        if not rows:
            continue
        fade = np.concatenate([r[0] for r in rows]); dmac = np.concatenate([r[1] for r in rows])
        d1 = np.concatenate([r[2] for r in rows]); wkall = np.concatenate([r[3] for r in rows])
        ok = ~(np.isnan(dmac) | np.isnan(fade) | np.isnan(d1))
        if ok.sum() < 30:
            continue
        q2 = np.quantile(dmac[ok], 2 / 3)
        bull = ok & (dmac >= q2)
        if bull.sum() < 12:
            continue
        m1 = np.median(d1[bull])
        accel = bull & (d1 >= m1); decel = bull & (d1 < m1)
        print(f"--- {pair} ---  (bull n={int(bull.sum())}, d1 median={m1:>+.0f})")
        for wk in WEEKS:
            wm = (wkall == wk)
            if wm.sum() == 0:
                continue
            print(f"    {wk}: ACCEL {cell(fade, accel & wm)}  |  DECEL {cell(fade, decel & wm)}")
        if pair != "HYPEUSDC":
            MAJ.append((fade, dmac, d1, wkall))
    if MAJ:
        fade = np.concatenate([r[0] for r in MAJ]); dmac = np.concatenate([r[1] for r in MAJ])
        d1 = np.concatenate([r[2] for r in MAJ]); wkall = np.concatenate([r[3] for r in MAJ])
        ok = ~(np.isnan(dmac) | np.isnan(fade) | np.isnan(d1))
        q2 = np.quantile(dmac[ok], 2 / 3); bull = ok & (dmac >= q2); m1 = np.median(d1[bull])
        accel = bull & (d1 >= m1); decel = bull & (d1 < m1)
        print(f"--- MAJORS POOLES ---  (bull n={int(bull.sum())})")
        for wk in WEEKS:
            wm = (wkall == wk)
            if wm.sum() == 0:
                continue
            print(f"    {wk}: ACCEL {cell(fade, accel & wm)}  |  DECEL {cell(fade, decel & wm)}")
    print("\n  LECTURE : si ACCEL net>0 sur W507 ET W516 (majors) -> le fade-bull-accel est ROBUSTE cross-week (pas un artefact pool).")
    print("    si ACCEL flippe par semaine -> le conditionnement n'a PAS sauve le bull, c'etait du pooling/chance.")
    print(f"\n[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
