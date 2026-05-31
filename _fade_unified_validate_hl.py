"""VALIDATION end-to-end du BOT UNIFIE (regime-aware fade), fills CARNET REELS, splitte par REGIME x SEMAINE x PAIRE.
Repond a la question user : "es-tu sure que ca marche robuste sur bear ET bull ?" -> la donnee repond, pas moi.

Regle unifiee :
  - regime tendanciel : drift_4h tercile bas (BEAR) ou haut (BULL) ; chop exclu.
  - on fade la jambe d'EXTENSION (d == sign(drift_4h)) : down-leg en bear = buy dip ; up-leg en bull = short rip.
  - filtre BULL only : accel-1h (drift_1h >= mediane(d1 dans bull)) [le bull brut etait instable].
  - execution : post maker 'best', fill via vrai carnet L2 (miss=adverse-sel), HOLD 30min, sortie maker RT3.
Sortie : par paire x regime x semaine -> sig, fill%, net/fill, win%, NET/SIGNAL. + majors pooles. HYPE = controle.
HL true-mid + vrai L2, tick-level, ZERO sampling.
"""
import os, sys, time
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _fade_maker_fill_hl import (leg_chain, drift_over, detect_fill, post_price,
                                 MIN, GAP, MACRO, FEE_MK, PAIRS, WEEKS)

HOUR = 60 * MIN
HOLD = 30 * MIN


def exit_at(ts, mid, t_fill, P, pos, Tmin):
    i0 = np.searchsorted(ts, t_fill, side="left")
    if i0 >= len(ts):
        return None
    j = np.searchsorted(ts, t_fill + Tmin * MIN, side="left")
    if j >= len(ts):
        j = len(ts) - 1
    sub = ts[i0:j + 1]
    if len(sub) > 1:
        g = np.where(np.diff(sub) > GAP)[0]
        if len(g):
            j = i0 + g[0]
    if j < i0:
        return None
    return pos * (mid[j] - P) / P * 1e4 - FEE_MK


def main():
    t0 = time.time()
    print("=== BOT UNIFIE (regime-aware fade) — fills CARNET REELS, splitte REGIME x SEMAINE x PAIRE — HL ===")
    print("bear=fade down-leg en downtrend (buy dip) ; bull=fade up-leg accel-1h en uptrend (short rip). hold 30min, maker.\n")
    print(f"  {'actif':>9} {'reg':>4} {'wk':>5} | {'sig':>4} {'fill%':>5} | {'net/fill':>8} {'win%':>4} | {'NET/SIG':>7}")
    print("  " + "-" * 74)
    acc = {}   # (scope, regime, wk) -> list of net  ;  cnt: (scope,regime,wk)->[sig,fill]
    cnt = {}

    def add(scope, regime, wk, net_or_none, filled, signaled):
        c = cnt.setdefault((scope, regime, wk), [0, 0])
        if signaled:
            c[0] += 1
        if filled:
            c[1] += 1
            if net_or_none is not None:
                acc.setdefault((scope, regime, wk), []).append(net_or_none)

    for pair in PAIRS:
        wk_data = {}; pool = []
        for wk, ddir in WEEKS.items():
            try:
                ticks, trades, l2 = load_all(pair, ddir)
                ts, mid, _ = vectorized_features(ticks, trades, l2)
            except Exception:
                continue
            ts = np.asarray(ts, np.int64); mid = np.asarray(mid, np.float64)
            l2_ts = l2["ts_ms"].values.astype(np.int64)
            b1p = l2["b1p"].values.astype(np.float64); a1p = l2["a1p"].values.astype(np.float64)
            okl = (b1p > 0) & (a1p > 0) & (a1p >= b1p)
            l2_ts = l2_ts[okl]; b1p = b1p[okl]; a1p = a1p[okl]
            if len(l2_ts) < 100:
                continue
            Bs, ds = leg_chain(ts, mid, 50.0)
            if len(Bs) < 5:
                continue
            dmac = drift_over(ts, mid, MACRO)[Bs]
            d1 = drift_over(ts, mid, HOUR)[Bs]
            wk_data[wk] = (ts, mid, Bs, ds, dmac, d1, l2_ts, b1p, a1p); pool.append((dmac, d1))
        if not pool:
            continue
        alld = np.concatenate([p[0] for p in pool]); alld1 = np.concatenate([p[1] for p in pool])
        v = ~np.isnan(alld)
        if v.sum() < 30:
            continue
        q1, q2 = np.quantile(alld[v], [1 / 3, 2 / 3])
        bullmask_pool = v & (alld >= q2)
        m1 = np.median(alld1[bullmask_pool & ~np.isnan(alld1)]) if (bullmask_pool & ~np.isnan(alld1)).sum() else 0.0
        is_major = pair != "HYPEUSDC"
        for wk in WEEKS:
            if wk not in wk_data:
                continue
            ts, mid, Bs, ds, dmac, d1, l2_ts, b1p, a1p = wk_data[wk]
            for k in range(len(Bs)):
                dm = dmac[k]
                if np.isnan(dm):
                    continue
                d = float(ds[k])
                if dm <= q1:
                    regime = "bear"
                    if d != -1.0:                       # extension = down-leg
                        continue
                elif dm >= q2:
                    regime = "bull"
                    if d != 1.0:                        # extension = up-leg
                        continue
                    if np.isnan(d1[k]) or d1[k] < m1:    # filtre accel-1h (bull only)
                        continue
                else:
                    continue                            # chop exclu
                ib = int(Bs[k]); pos = -d; tB = ts[ib]
                jl = np.searchsorted(l2_ts, tB, side="left")
                if jl >= len(l2_ts):
                    continue
                b1 = b1p[jl]; a1 = a1p[jl]; midl = 0.5 * (b1 + a1)
                P, is_long = post_price(pos, b1, a1, midl, "best")
                tf = detect_fill(l2_ts, b1p, a1p, tB, P, is_long)
                filled = tf is not None
                net = exit_at(ts, mid, tf, P, pos, 30) if filled else None
                add(pair, regime, wk, net, filled, True)
                if is_major:
                    add("MAJ", regime, wk, net, filled, True)

    def line(scope, regime, wk):
        c = cnt.get((scope, regime, wk))
        if not c or c[0] < 6:
            return
        lst = acc.get((scope, regime, wk), [])
        fr = 100.0 * c[1] / c[0]
        if lst:
            a = np.array(lst); nf = np.mean(a); win = 100 * np.mean(a > 0); ps = np.sum(a) / c[0]
            tag = scope if scope != "MAJ" else "MAJORS"
            print(f"  {tag:>9} {regime:>4} {wk:>5} | {c[0]:>4} {fr:>5.0f} | {nf:>+8.1f} {win:>4.0f} | {ps:>+7.1f}")

    for pair in PAIRS:
        for regime in ("bear", "bull"):
            for wk in WEEKS:
                line(pair, regime, wk)
        print("  " + "." * 74)
    for regime in ("bear", "bull"):
        for wk in WEEKS:
            line("MAJ", regime, wk)
    print("\n  LECTURE : robuste SSI bear NET/SIG>0 sur W507 ET W516, ET bull NET/SIG>0 sur W507 ET W516 (majors).")
    print("    chaque cellule = un cote sur une semaine, fills reels. Si un cote flippe une semaine -> PAS robuste, on le dit.")
    print(f"\n[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
