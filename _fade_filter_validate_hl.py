"""VALIDATION cross-week du filtre FLOW + sweep STOP DUR — boucle la deployabilite du fade.

(1) Le kill-switch FLOW (skip si sign(tfi_5min)==jambe & |tfi|>=Q70) coupe-t-il la queue sur W507 ET W516 (pas juste poole) ?
(2) Un stop dur LARGE (catastrophe-cap, taker) plafonne-t-il le single-worst que FLOW ne coupe pas, sans tuer l'EV ?
Setup = fade unifie (bear down-leg / bull up-leg+accel), hold 30min maker, stop=taker RT6. Par semaine + poole, MAJORS + par paire.
HL true-mid + feats causaux, tick-level, ZERO sampling.
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
FEE_TK = 6.0
HOLD = 30 * MIN
SLS = [None, 40.0, 50.0, 60.0, 80.0]


def fade_net(ts, mid, ib, d, SL):
    pos = -d; entry = mid[ib]; t0 = ts[ib]
    jend = np.searchsorted(ts, t0 + HOLD, side="right")
    seg_ts = ts[ib + 1:jend]; seg = mid[ib + 1:jend]
    if len(seg) < 1:
        return None
    g = np.where(np.diff(np.concatenate([[t0], seg_ts])) > GAP)[0]
    if len(g):
        seg = seg[:g[0]]
    if len(seg) < 1:
        return None
    fav = pos * (seg - entry) / entry * 1e4
    if SL is not None:
        h = np.where(fav <= -SL)[0]
        if len(h):
            return float(-SL) - FEE_TK
    return float(fav[-1]) - FEE_MK


def tail(ts, net):
    if len(net) < 6:
        return None
    o = np.argsort(ts); a = net[o]
    eq = np.cumsum(a); mdd = float(np.max(np.maximum.accumulate(eq) - eq))
    return float(np.mean(a)), 100 * np.mean(a > 0), float(np.min(a)), mdd, float(eq[-1])


def main():
    t0 = time.time()
    print("=== VALIDATION cross-week FILTRE FLOW + sweep STOP — fade unifie, HL true-mid ===")
    print("FLOW skip si flux taker soutient la jambe (|tfi|>=Q70). stop=catastrophe-cap taker. par semaine + poole.\n")
    ALL = []   # (scope_major?, pair, wk, ts, d, tfi, dict_netSL)
    rows = []  # entries: dict per entry
    for pair in PAIRS:
        wkA = {}; poolD = []; poolD1 = []
        for wk, ddir in WEEKS.items():
            try:
                ticks, trades, l2 = load_all(pair, ddir)
                ts, mid, feats = vectorized_features(ticks, trades, l2)
            except Exception:
                continue
            ts = np.asarray(ts, np.int64); mid = np.asarray(mid, np.float64)
            Bs, ds = leg_chain(ts, mid, 50.0)
            if len(Bs) < 5:
                continue
            dmac = drift_over(ts, mid, MACRO)[Bs]; d1 = drift_over(ts, mid, HOUR)[Bs]
            tfi = np.asarray(feats.get("tfi_5min", np.full(len(ts), np.nan)), np.float64)[Bs]
            wkA[wk] = (ts, mid, Bs, ds, dmac, d1, tfi); poolD.append(dmac); poolD1.append(d1)
        if not wkA:
            continue
        dd = np.concatenate(poolD); v = ~np.isnan(dd)
        if v.sum() < 30:
            continue
        q1, q2 = np.quantile(dd[v], [1 / 3, 2 / 3])
        d1p = np.concatenate(poolD1); bp = v & (dd >= q2) & ~np.isnan(d1p)
        m1 = np.median(d1p[bp]) if bp.sum() else 0.0
        # pass 1 : entries + tfi for qtfi
        ent = []
        for wk, (ts, mid, Bs, ds, dmac, d1, tfi) in wkA.items():
            for k in range(len(Bs)):
                dm = dmac[k]
                if np.isnan(dm):
                    continue
                dd_ = float(ds[k])
                if dm <= q1:
                    if dd_ != -1.0:
                        continue
                elif dm >= q2:
                    if dd_ != 1.0 or np.isnan(d1[k]) or d1[k] < m1:
                        continue
                else:
                    continue
                ent.append((wk, int(Bs[k]), dd_, tfi[k]))
        if len(ent) < 12:
            continue
        qtfi = np.nanquantile(np.abs(np.array([e[3] for e in ent])), 0.70)
        # pass 2 : walk net for each SL + flow flag
        for wk, ib, dd_, tfival in ent:
            ts, mid = wkA[wk][0], wkA[wk][1]
            fire = (not np.isnan(tfival)) and (np.sign(tfival) == dd_) and (abs(tfival) >= qtfi)
            nets = {}
            for SL in SLS:
                nets[SL] = fade_net(ts, mid, ib, dd_, SL)
            rows.append(dict(pair=pair, wk=wk, ts=ts[ib], fire=fire, nets=nets))

    def emit(scope, sel):
        sub = [r for r in rows if sel(r)]
        if len(sub) < 8:
            return
        print(f"\n=== {scope} (n={len(sub)}) ===")
        print(f"  {'config':>14} | {'%gardé':>6} | {'net/tr':>6} {'win%':>5} | {'worst1':>7} {'maxDD':>7} | {'total':>7}")
        print("  " + "-" * 68)
        # baseline = all, no stop
        for cfg, useflow, SL in [("BASELINE", False, None), ("FLOW", True, None),
                                 ("FLOW+SL80", True, 80.0), ("FLOW+SL60", True, 60.0),
                                 ("FLOW+SL50", True, 50.0), ("FLOW+SL40", True, 40.0)]:
            keep = [r for r in sub if (not useflow) or (not r["fire"])]
            ts = np.array([r["ts"] for r in keep], float)
            net = np.array([r["nets"][SL] for r in keep if r["nets"][SL] is not None], float)
            tsv = np.array([r["ts"] for r in keep if r["nets"][SL] is not None], float)
            st = tail(tsv, net)
            if st is None:
                continue
            mn, win, w1, mdd, tot = st
            print(f"  {cfg:>14} | {100*len(keep)/len(sub):>5.0f}% | {mn:>+6.1f} {win:>5.0f} | {w1:>+7.1f} {mdd:>7.0f} | {tot:>+7.0f}")

    MAJ = ("BTCUSDC", "ETHUSDC", "SOLUSDC", "XRPUSDC")
    for wk in WEEKS:
        emit(f"MAJORS {wk}", lambda r, wk=wk: r["pair"] in MAJ and r["wk"] == wk)
    emit("MAJORS POOL (2 sem)", lambda r: r["pair"] in MAJ)
    for p in PAIRS:
        emit(f"{p} (2 sem)", lambda r, p=p: r["pair"] == p)
    print("\n  LECTURE : (1) FLOW coupe maxDD/worst sur W507 ET W516 (pas juste poole) ? (2) quel SL plafonne worst1 au moindre cout net ?")
    print("    cible : FLOW+SL garde net ~ baseline, worst1 borne (~-SL), maxDD effondre. = fade deployable.")
    print(f"\n[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
