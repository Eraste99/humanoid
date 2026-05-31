"""TEMPS DE CAPTURE du fade-extension — combien de temps pour atteindre +7 bps, et quelle chaleur (MAE) avant ?

User (juste) : ce n'est PAS une strat a la seconde pres. Si le hold se compte en minutes, l'adverse-selection
au fill compte peu. La vraie question = temps moyen pour capturer +7 bps + creux adverse (MAE) avant le rebond.

Setup = celui validE : a chaque fin de jambe 50bps en REGIME TENDANCIEL (bear OU bull, terciles drift_4h, on
exclut chop), on FADE (position -d). On marche tick-par-tick (gap-aware, cap 2h) et on mesure, pour la jambe
qui FADE :
  - % qui atteignent +5/+7/+10 bps favorables dans 2h ; temps median & moyen pour atteindre +7 ;
  - MAE = pire excursion adverse AVANT d'atteindre +7 (= la chaleur a tenir ; stat d'execution clE) ;
  - split bucket MEAN-REV (jambe AVEC macro = extension, le cœur de l'edge) vs CONTIN.
HL vrai mid L2, in-sample, tick-level, ZERO sampling. (Binance OOS dans le script jumeau.)
"""
import os, sys, time
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import leg_chain, drift_over, PAIRS, WEEKS, MACRO, MIN, GAP

CAP = 120 * MIN
TARGETS = [5.0, 7.0, 10.0]


def walk_fade(ts, mid, ib, d, cap=CAP):
    entry = mid[ib]; t0 = ts[ib]
    jend = np.searchsorted(ts, t0 + cap, side="right")
    if jend <= ib + 1:
        return None
    seg_ts = ts[ib + 1:jend]; seg = mid[ib + 1:jend]
    dts = np.diff(np.concatenate([[t0], seg_ts]))
    g = np.where(dts > GAP)[0]
    if len(g):
        seg_ts = seg_ts[:g[0]]; seg = seg[:g[0]]
    if len(seg) < 1:
        return None
    fav = -d * (seg - entry) / entry * 1e4
    rel = (seg_ts - t0) / float(MIN)            # minutes
    out = {}
    for tg in TARGETS:
        h = np.where(fav >= tg)[0]
        out[tg] = float(rel[h[0]]) if len(h) else -1.0
    h7 = np.where(fav >= 7.0)[0]
    end = h7[0] if len(h7) else len(fav) - 1
    out["mae"] = float(np.min(fav[:end + 1]))
    out["mfe"] = float(np.max(fav))
    out["final"] = float(fav[-1])
    return out


def agg(rows, label):
    n = len(rows)
    if n == 0:
        print(f"  {label:>22} | (aucun)")
        return
    r7 = [x[7.0] for x in rows if x[7.0] >= 0]
    r5 = [x[5.0] for x in rows if x[5.0] >= 0]
    r10 = [x[10.0] for x in rows if x[10.0] >= 0]
    p7 = 100.0 * len(r7) / n; p5 = 100.0 * len(r5) / n; p10 = 100.0 * len(r10) / n
    med7 = np.median(r7) if r7 else float('nan'); mean7 = np.mean(r7) if r7 else float('nan')
    mae = np.median([x["mae"] for x in rows]); mfe = np.median([x["mfe"] for x in rows])
    fin = np.mean([x["final"] for x in rows])
    print(f"  {label:>22} | n={n:>5} | +5:{p5:>4.0f}% +7:{p7:>4.0f}% +10:{p10:>4.0f}% | "
          f"t+7 med={med7:>5.1f}m mean={mean7:>5.1f}m | MAEmed={mae:>+6.1f} MFEmed={mfe:>+5.1f} | final~{fin:>+5.1f}")


def main():
    t0 = time.time()
    print("=== TEMPS DE CAPTURE fade-extension (regimes tendanciels) — HL vrai mid L2, in-sample, cap 2h ===")
    print("question user : temps moyen pour capturer +7 bps + MAE (chaleur) avant rebond.\n")
    POOL = {"all": [], "MEAN-REV": [], "CONTIN": []}
    for pair in PAIRS:
        wk_data = []; dmacs = []
        for wk, ddir in WEEKS.items():
            try:
                ticks, trades, l2 = load_all(pair, ddir)
                ts, mid, _ = vectorized_features(ticks, trades, l2)
            except Exception as e:
                print(f"  [skip] {pair}/{wk}: {e}"); continue
            ts = np.asarray(ts, np.int64); mid = np.asarray(mid, np.float64)
            Bs, ds = leg_chain(ts, mid)
            if len(Bs) < 10:
                continue
            dmac = drift_over(ts, mid, MACRO)[Bs]
            wk_data.append((ts, mid, Bs, ds, dmac)); dmacs.append(dmac)
        if not wk_data:
            continue
        alld = np.concatenate(dmacs); v = ~np.isnan(alld)
        if v.sum() < 30:
            continue
        q1, q2 = np.quantile(alld[v], [1 / 3, 2 / 3])
        prow = {"all": [], "MEAN-REV": [], "CONTIN": []}
        for ts, mid, Bs, ds, dmac in wk_data:
            for k in range(len(Bs)):
                dm = dmac[k]
                if np.isnan(dm):
                    continue
                if dm <= q1:
                    pass
                elif dm >= q2:
                    pass
                else:
                    continue                      # chop exclu
                m = 1.0 if dm > 0 else -1.0
                bucket = "MEAN-REV" if ds[k] == m else "CONTIN"
                w = walk_fade(ts, mid, int(Bs[k]), float(ds[k]))
                if w is None:
                    continue
                prow["all"].append(w); prow[bucket].append(w)
                POOL["all"].append(w); POOL[bucket].append(w)
        print(f"--- {pair} (regimes tendanciels, fade) ---")
        for lab in ("all", "MEAN-REV", "CONTIN"):
            agg(prow[lab], lab)
    print("\n=== POOL 5 actifs ===")
    for lab in ("all", "MEAN-REV", "CONTIN"):
        agg(POOL[lab], lab)
    print("\n  LECTURE : t+7 med/mean = combien de minutes pour capturer 7 bps. MAEmed = creux adverse median a tenir")
    print("            avant le rebond (si MAE ~ -spread c'est doux ; si MAE tres negatif = il faut encaisser une plonge).")
    print(f"\n[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
