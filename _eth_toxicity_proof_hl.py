"""PREUVE : le -237 d'ETH etait-il un MOVE INFORME, et la toxicite est-elle QUANTIFIABLE ?
(question user). On prend les pires trades fade d'ETH et on regarde, A L'ENTREE (causal) :
  - le move qui nous a ecrase (continuation en bps) ;
  - les signatures de toxicite (recherche : |tfi_5min| flux soutenu, vol_5/vol_30 expansion, intensity surge,
    n_large, spread) + leur PERCENTILE dans la distribution des entrees ETH (elevees = detectable) ;
  - le FLOW filter (|tfi|>=Q70 & sign==jambe) l'aurait-il coupe ?
  - CROSS-ASSET : a cet instant, BTC/SOL/XRP ont-ils bouge AUSSI (=evenement de marche informe) ?
HL true-mid + feats causaux, tick-level, ZERO sampling.
"""
import os, sys, time
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import leg_chain, drift_over, WEEKS, MACRO, MIN, GAP

HOUR = 60 * MIN; FEE_MK = 3.0; HOLD = 30 * MIN
PAIRS = ["BTCUSDC", "ETHUSDC", "SOLUSDC", "XRPUSDC"]


def fwd_at(ts, mid, Bs, H):
    n = len(ts); jf = np.searchsorted(ts, ts[Bs] + H, side="left"); ok = jf < n
    f = np.full(len(Bs), np.nan); f[ok] = (mid[jf[ok]] - mid[Bs[ok]]) / mid[Bs[ok]] * 1e4
    tf = np.zeros(len(Bs), bool); tf[ok] = (ts[jf[ok]] - ts[Bs[ok]]) > (H + GAP); f[tf] = np.nan
    return f


def move_at(ts, mid, t0, H=HOLD):
    i = np.searchsorted(ts, t0, side="left"); j = np.searchsorted(ts, t0 + H, side="left")
    if i >= len(ts) or j >= len(ts) or i == j:
        return np.nan
    return (mid[j] - mid[i]) / mid[i] * 1e4


def main():
    print("=== PREUVE : ETH -237 = move informe ? toxicite quantifiable ? — HL true-mid ===\n")
    D = {}
    for p in PAIRS:
        TS = []; MID = []
        for wk, ddir in WEEKS.items():
            try:
                ticks, trades, l2 = load_all(p, ddir)
                ts, mid, feats = vectorized_features(ticks, trades, l2)
            except Exception:
                continue
            D.setdefault(p, {})[wk] = (np.asarray(ts, np.int64), np.asarray(mid, np.float64), feats)
    # ETH entrees (config unifiee) + features a l'entree
    ent = []
    for wk, (ts, mid, feats) in D["ETHUSDC"].items():
        Bs, ds = leg_chain(ts, mid, 50.0)
        if len(Bs) < 5:
            continue
        dmac = drift_over(ts, mid, MACRO)[Bs]; d1 = drift_over(ts, mid, HOUR)[Bs]
        fwd = fwd_at(ts, mid, Bs, HOLD)
        tfi = np.asarray(feats.get("tfi_5min"), float)[Bs]
        v5 = np.asarray(feats.get("vol_5min"), float)[Bs]; v30 = np.asarray(feats.get("vol_30min"), float)[Bs]
        inten = np.asarray(feats.get("intensity_5min"), float)[Bs]
        nlrg = np.asarray(feats.get("n_large_5min", np.full(len(ts), np.nan)), float)[Bs]
        q = np.nanquantile(dmac[~np.isnan(dmac)], [1 / 3, 2 / 3]); q1, q2 = q[0], q[1]
        bm = ~np.isnan(dmac) & (dmac >= q2) & ~np.isnan(d1)
        m1 = np.median(d1[bm]) if bm.sum() else 0.0
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
            volr = v5[k] / v30[k] if v30[k] > 0 else np.nan
            ent.append(dict(t=int(ts[Bs[k]]), d=d, net=-d * fwd[k] - FEE_MK, cont=d * fwd[k],
                            tfi=tfi[k], volr=volr, inten=inten[k], nlrg=nlrg[k]))
    if not ent:
        print("pas d'entrees"); return
    A = {kk: np.array([e[kk] for e in ent], float) for kk in ("tfi", "volr", "inten", "nlrg")}
    qtfi = np.nanquantile(np.abs(A["tfi"]), 0.70)

    def pct(arr, val):
        a = arr[~np.isnan(arr)]
        return 100.0 * np.mean(a < val) if len(a) and not np.isnan(val) else float('nan')

    ent.sort(key=lambda e: e["net"])
    print(f"  seuil FLOW |tfi_5min|>=Q70 = {qtfi:.3f}\n")
    print("  --- 6 PIRES trades fade ETH (a l'entree, causal) ---")
    for e in ent[:6]:
        dt = time.strftime("%Y-%m-%d %H:%M", time.gmtime(e["t"] / 1000))
        flow = (not np.isnan(e["tfi"])) and (np.sign(e["tfi"]) == e["d"]) and (abs(e["tfi"]) >= qtfi)
        print(f"  {dt} net{e['net']:>+7.1f} | jambe d={e['d']:+.0f} continuation{e['cont']:>+7.0f}bps en 30min")
        print(f"     |tfi|={abs(e['tfi']):.2f}(p{pct(np.abs(A['tfi']),abs(e['tfi'])):.0f}, sign{'==' if np.sign(e['tfi'])==e['d'] else '!='}jambe) "
              f"volr={e['volr']:.2f}(p{pct(A['volr'],e['volr']):.0f}) inten=p{pct(A['inten'],e['inten']):.0f} nlrg=p{pct(A['nlrg'],e['nlrg']):.0f} "
              f"-> FLOW {'COUPE' if flow else 'rate'}")
        # cross-asset : les autres ont-ils bouge dans [t, t+30min] ?
        co = []
        for p in PAIRS:
            mv = np.nan
            for wk, (ts, mid, _) in D.get(p, {}).items():
                if ts[0] <= e["t"] <= ts[-1]:
                    mv = move_at(ts, mid, e["t"]); break
            co.append(f"{p[:3]}{mv:>+5.0f}" if not np.isnan(mv) else f"{p[:3]} na")
        print(f"     cross-asset move 30min: {' '.join(co)}")
    # synthese : les pires sont-ils elevees en signatures ?
    worst = ent[:6]
    print("\n  --- synthese : signatures aux 6 pires vs mediane des entrees ---")
    for kk, lab in (("volr", "vol-expansion v5/v30"), ("inten", "intensite"), ("nlrg", "gros trades")):
        wpix = np.nanmedian([pct(A[kk], e[kk]) for e in worst])
        print(f"     {lab:>22} : percentile median aux pires = {wpix:.0f}  (50=normal, >70=eleve=detectable)")
    wtfi = np.nanmedian([pct(np.abs(A["tfi"]), abs(e["tfi"])) for e in worst])
    print(f"     {'|flux taker|':>22} : percentile median aux pires = {wtfi:.0f}")
    print("\n  LECTURE : si les pires trades ont des signatures ELEVEES (percentile >70) + cross-asset co-move")
    print("    -> la toxicite/l'info etait QUANTIFIABLE et detectable. Si signatures ~normales -> furtif (flux seul insuffisant).")


if __name__ == "__main__":
    main()
