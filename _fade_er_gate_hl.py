"""EFFICIENCY RATIO (Kaufman) comme détecteur CHOPPY vs TREND-INFORMÉ — l'intuition skew/buy-hold du user, quantifiée.
ER = |déplacement net sur W| / (somme des |moves| sur W). ER→1 trend (fade-hostile) ; ER→0 choppy (fade-friendly).
Question : (1) ER discrimine-t-il HYPE (haut) vs majors (bas) ? (2) le fade gagne à ER bas / perd à ER haut ?
(3) un ER-GATE (fade seulement si ER bas) gère-t-il HYPE tout seul (sans hardcode) + garde les majors ?
INCLUT HYPE (pas d'exclusion statique). Fade unifié (extension régime), hold 30min maker. W=4h. HL true-mid, causal.
"""
import os, sys, time
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import leg_chain, drift_over, PAIRS, WEEKS, MACRO, MIN, GAP

HOUR = 60 * MIN; FEE_MK = 3.0; HOLD = 30 * MIN


def fwd_at(ts, mid, Bs, H):
    n = len(ts); jf = np.searchsorted(ts, ts[Bs] + H, side="left"); ok = jf < n
    f = np.full(len(Bs), np.nan); f[ok] = (mid[jf[ok]] - mid[Bs[ok]]) / mid[Bs[ok]] * 1e4
    tf = np.zeros(len(Bs), bool); tf[ok] = (ts[jf[ok]] - ts[Bs[ok]]) > (H + GAP); f[tf] = np.nan
    return f


def collect(pair):
    rows = []
    for wk, ddir in WEEKS.items():
        try:
            ticks, trades, l2 = load_all(pair, ddir)
            ts, mid, _ = vectorized_features(ticks, trades, l2)
        except Exception:
            continue
        ts = np.asarray(ts, np.int64); mid = np.asarray(mid, np.float64)
        cum_abs = np.concatenate([[0.0], np.cumsum(np.abs(np.diff(mid)))])
        Bs, ds = leg_chain(ts, mid, 50.0)
        if len(Bs) < 5:
            continue
        dmac = drift_over(ts, mid, MACRO)[Bs]; d1 = drift_over(ts, mid, HOUR)[Bs]
        fwd = fwd_at(ts, mid, Bs, HOLD)
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
            ib = int(Bs[k]); j = np.searchsorted(ts, ts[ib] - MACRO, side="left")
            if j >= ib or (ts[ib] - ts[j]) < 0.5 * MACRO:
                continue
            path = cum_abs[ib] - cum_abs[j]; net_move = abs(mid[ib] - mid[j])
            er = net_move / path if path > 0 else np.nan
            if np.isnan(er):
                continue
            rows.append((er, -d * fwd[k] - FEE_MK))
    return rows


def main():
    print("=== ER (Kaufman) détecteur choppy/trend — l'intuition skew du user — fade unifié, W=4h, HL ===\n")
    data = {p: collect(p) for p in PAIRS}
    print("--- 1. ER moyen par actif (HYPE doit être HAUT=trending ; majors BAS=choppy) ---")
    print(f"  {'actif':>9} | {'n':>4} | {'ER moy':>6} {'ER med':>6} | {'fade net (tous)':>15}")
    allrows = []
    for p in PAIRS:
        r = data[p]
        if len(r) < 8:
            continue
        er = np.array([x[0] for x in r]); net = np.array([x[1] for x in r])
        allrows += r
        print(f"  {p:>9} | {len(r):>4} | {np.mean(er):>6.2f} {np.median(er):>6.2f} | {np.mean(net):>+15.1f}")
    er = np.array([x[0] for x in allrows]); net = np.array([x[1] for x in allrows])
    # 2. fade net par tercile d'ER (pooled)
    q1, q2 = np.quantile(er, [1 / 3, 2 / 3])
    print(f"\n--- 2. fade net par tercile d'ER (pooled, seuils {q1:.2f}/{q2:.2f}) ---")
    for lab, m in (("ER BAS (choppy)", er <= q1), ("ER MID", (er > q1) & (er < q2)), ("ER HAUT (trend)", er >= q2)):
        a = net[m]
        print(f"  {lab:>18} | n={len(a):>4} | net {np.mean(a):>+6.1f} | win {100*np.mean(a>0):>3.0f}%")
    # 3. ER-gate par actif : fade seulement si ER <= q2 (skip le tercile haut=trend)
    print(f"\n--- 3. ER-GATE (fade seulement si ER < {q2:.2f}) — baseline vs gated, par actif (+ HYPE !) ---")
    print(f"  {'actif':>9} | {'base net':>8} {'base n':>6} | {'gated net':>9} {'gated n':>7} {'%gardé':>6}")
    for p in PAIRS:
        r = data[p]
        if len(r) < 8:
            continue
        e = np.array([x[0] for x in r]); nt = np.array([x[1] for x in r])
        keep = e < q2
        gn = nt[keep]
        gstr = f"{np.mean(gn):>+9.1f} {len(gn):>7} {100*keep.mean():>5.0f}%" if len(gn) >= 5 else f"{'n<5':>9}"
        print(f"  {p:>9} | {np.mean(nt):>+8.1f} {len(nt):>6} | {gstr}")
    print("\n  LECTURE : (1) si ER_HYPE >> ER_majors -> ER discrimine. (2) si fade net BAISSE de ER-bas à ER-haut -> ER prédit")
    print("    la fade-hostilité. (3) si l'ER-gate rend HYPE neutre/+ SANS casser les majors -> détecteur dynamique = TROUVÉ (sans hardcode).")


if __name__ == "__main__":
    main()
