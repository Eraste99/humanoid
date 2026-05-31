"""EXIT COURT vs 2H — on ne tient PAS 2h pour 5 bps. Le rebond est rapide (+7 en ~0.8min, MFE median +72).
Quel exit capture l'edge en MINUTES, pas en heures ? Metrique cle = bps/HEURE de capital (pas juste bps/trade).

bear fade (BTC/SOL/XRP majeurs ; ETH/HYPE controle), jambes 50bps, sortie maker RT3. On compare :
  - time-stops : 5 / 15 / 30 / 60 / 120 min ;
  - trailing : arm a +A, sortir si retrace de R depuis le pic (chevauche le rebond, sort au stall).
On sort net/trade, win%, hold median (min), et bps/HEURE = mean(net)/mean(hold_h). Le gagnant = bon net ET hold court.
HL vrai mid L2, in-sample, tick-level, ZERO sampling. mid-proxy (fill~mid valide HL).
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
TIMES = [5, 15, 30, 60, 120]
TRAILS = [(5.0, 4.0), (8.0, 6.0), (10.0, 8.0)]


def walk(ts, mid, ib, pos, cap=CAP):
    entry = mid[ib]; t0 = ts[ib]
    jend = np.searchsorted(ts, t0 + cap, side="right")
    seg_ts = ts[ib + 1:jend]; seg = mid[ib + 1:jend]
    if len(seg) < 1:
        return None
    g = np.where(np.diff(np.concatenate([[t0], seg_ts])) > GAP)[0]
    if len(g):
        seg_ts = seg_ts[:g[0]]; seg = seg[:g[0]]
    if len(seg) < 1:
        return None
    fav = pos * (seg - entry) / entry * 1e4
    rel = (seg_ts - t0) / float(MIN)
    return fav, rel


def main():
    t0 = time.time()
    print("=== EXIT COURT vs 2H — bear fade, bps/HEURE (capital) — HL in-sample, jambes 50bps, maker RT3 ===")
    print("on cherche l'exit qui capture l'edge en MINUTES. bps/h = mean(net)/mean(hold_h).\n")
    print(f"  {'actif':>9} {'exit':>10} | {'n':>4} {'net':>6} {'win%':>5} {'holdmed':>7} {'holdmean':>8} | {'bps/HEURE':>9}")
    print("  " + "-" * 78)
    for pair in PAIRS:
        weeks = []
        for wk, ddir in WEEKS.items():
            try:
                ticks, trades, l2 = load_all(pair, ddir)
                ts, mid, _ = vectorized_features(ticks, trades, l2)
            except Exception as e:
                print(f"  [skip] {pair}/{wk}: {e}"); continue
            ts = np.asarray(ts, np.int64); mid = np.asarray(mid, np.float64)
            weeks.append((ts, mid))
        if not weeks:
            continue
        # bear tercile pooled
        dms = []
        legs_all = []
        for ts, mid in weeks:
            Bs, ds = leg_chain(ts, mid, 50.0)
            if len(Bs) < 5:
                legs_all.append((ts, mid, np.array([], int), np.array([])))
                continue
            dm = drift_over(ts, mid, MACRO)[Bs]
            legs_all.append((ts, mid, Bs, ds, dm)); dms.append(dm)
        if not dms:
            continue
        alld = np.concatenate(dms); v = ~np.isnan(alld)
        if v.sum() < 15:
            continue
        q1 = np.quantile(alld[v], 1 / 3)
        # collect per-exit (net, hold) lists
        T_net = {T: [] for T in TIMES}; T_hold = {T: [] for T in TIMES}
        R_net = {tr: [] for tr in TRAILS}; R_hold = {tr: [] for tr in TRAILS}
        for item in legs_all:
            if len(item) < 5:
                continue
            ts, mid, Bs, ds, dm = item
            for k in range(len(Bs)):
                if np.isnan(dm[k]) or dm[k] > q1:
                    continue
                w = walk(ts, mid, int(Bs[k]), -float(ds[k]))
                if w is None:
                    continue
                fav, rel = w
                for T in TIMES:
                    i = np.searchsorted(rel, T, side="left")
                    if i >= len(rel):
                        i = len(rel) - 1
                    T_net[T].append(fav[i] - FEE_MK); T_hold[T].append(rel[i])
                for (arm, trail) in TRAILS:
                    peak = -1e9; armed = False; exornet = None
                    for kk in range(len(fav)):
                        f = fav[kk]
                        if f > peak:
                            peak = f
                        if peak >= arm:
                            armed = True
                        if armed and f <= peak - trail:
                            exornet = (f - FEE_MK, rel[kk]); break
                    if exornet is None:
                        exornet = (fav[-1] - FEE_MK, rel[-1])
                    R_net[(arm, trail)].append(exornet[0]); R_hold[(arm, trail)].append(exornet[1])

        def line(name, nets, holds):
            if len(nets) < 5:
                return
            nets = np.array(nets); holds = np.array(holds)
            mn = float(np.mean(nets)); win = 100.0 * np.mean(nets > 0)
            hmd = float(np.median(holds)); hmn = float(np.mean(holds))
            bph = mn / (hmn / 60.0) if hmn > 0 else float('nan')
            print(f"  {pair:>9} {name:>10} | {len(nets):>4} {mn:>+6.1f} {win:>5.0f} {hmd:>6.1f}m {hmn:>7.1f}m | {bph:>+9.1f}")

        for T in TIMES:
            line(f"t{T}m", T_net[T], T_hold[T])
        for (arm, trail) in TRAILS:
            line(f"tr{int(arm)}/{int(trail)}", R_net[(arm, trail)], R_hold[(arm, trail)])
        print("  " + "." * 78)
    print("\n  LECTURE : bps/HEURE = efficience capital. Tenir 2h pour +5 = +2.5/h ; capter +8 en 15min = +32/h.")
    print("    on veut net>0 ROBUSTE avec le hold le plus court / le meilleur bps-heure. trailing = capture le rebond rapide.")
    print(f"\n[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
