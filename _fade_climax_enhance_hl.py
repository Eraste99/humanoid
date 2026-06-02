"""ENHANCEMENT 'CLIMAX-FADE' : sur les MAJORS, les jambes à FORT |drift_8h| (climax d'un move) sont-elles les MEILLEURS fades ?
Hypothèse (inverse du vieux gate) : FAVORISER ces fades (au lieu de les skipper) augmente l'EV. À valider proprement :
  A. effet : EV-fade par tercile de |drift_8h| (par actif + POOL) — monotone croissant (climax = mieux) ?
  B. persistance : high vs low |d8| en période E (début mai) vs M (mi-mai) — l'effet tient-il dans les 2 ?
  C. CAUSAL : flag climax via percentile roulant PAR ACTIF (trailing K jambes, leg-based, ZÉRO grille) — survit-il causalement ?
  D. gain de sizing : EV/trade du livre à plat vs tilt-climax (2× sur high |d8|) + queue.
Sur le stream DÉPLOYABLE (fade + FLOW + Binance-lead, majors). HL true-mid, tick-level/leg-based, ZÉRO sampling.
"""
import os, sys, datetime
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import leg_chain, drift_over, WEEKS, MACRO, MIN, GAP
from _fade_binlead_fullbook_hl import load_bin, bdrift, BIN

HOUR = 60 * MIN; W8 = 8 * HOUR; FEE_MK = 3.0; HOLD = 30 * MIN; THR = 30.0
MAJ = ("BTCUSDC", "ETHUSDC", "SOLUSDC", "XRPUSDC")
KTRAIL = 40; MINK = 15   # fenêtre roulante causale (en jambes) pour le seuil climax par actif


def fwd_at(ts, mid, Bs, H):
    n = len(ts); jf = np.searchsorted(ts, ts[Bs] + H, side="left"); ok = jf < n
    f = np.full(len(Bs), np.nan); f[ok] = (mid[jf[ok]] - mid[Bs[ok]]) / mid[Bs[ok]] * 1e4
    tf = np.zeros(len(Bs), bool); tf[ok] = (ts[jf[ok]] - ts[Bs[ok]]) > (H + GAP); f[tf] = np.nan
    return f


def per(t):
    return "E" if datetime.datetime.utcfromtimestamp(t / 1000).day <= 8 else "M"


def collect(pair, binD):
    """fades DÉPLOYABLES (fade+FLOW+Binance-lead) ; garde |drift_8h| à l'entrée. retourne [(t, absd8, net, periode)]."""
    wkA = {}; poolD = []; poolD1 = []
    for wk, ddir in WEEKS.items():
        try:
            ticks, trades, l2 = load_all(pair, ddir); ts, mid, feats = vectorized_features(ticks, trades, l2)
        except Exception:
            continue
        ts = np.asarray(ts, np.int64); mid = np.asarray(mid, np.float64)
        Bs, ds = leg_chain(ts, mid, 50.0)
        if len(Bs) < 5:
            continue
        dmac = drift_over(ts, mid, MACRO)[Bs]; d1 = drift_over(ts, mid, HOUR)[Bs]
        d8 = drift_over(ts, mid, W8)[Bs]
        tfi = np.asarray(feats.get("tfi_5min", np.full(len(ts), np.nan)), np.float64)[Bs]
        fwd = fwd_at(ts, mid, Bs, HOLD)
        wkA[wk] = (ts[Bs], ds, dmac, d1, d8, tfi, fwd); poolD.append(dmac); poolD1.append(d1)
    if not wkA:
        return []
    dd = np.concatenate(poolD); v = ~np.isnan(dd)
    if v.sum() < 30:
        return []
    q1, q2 = np.quantile(dd[v], [1 / 3, 2 / 3])
    d1p = np.concatenate(poolD1); bp = v & (dd >= q2) & ~np.isnan(d1p); m1 = np.median(d1p[bp]) if bp.sum() else 0.0
    ent = []
    for wk, (tB, ds, dmac, d1, d8, tfi, fwd) in wkA.items():
        for k in range(len(ds)):
            dm = dmac[k]
            if np.isnan(dm) or np.isnan(fwd[k]) or np.isnan(d8[k]):
                continue
            d = float(ds[k])
            if dm <= q1:
                if d != -1.0: continue
            elif dm >= q2:
                if d != 1.0 or np.isnan(d1[k]) or d1[k] < m1: continue
            else:
                continue
            ent.append((int(tB[k]), d, abs(float(d8[k])), tfi[k], -d * fwd[k] - FEE_MK))
    if len(ent) < 12:
        return []
    qtfi = np.nanquantile(np.abs(np.array([e[3] for e in ent])), 0.70)
    rows = []
    for tB, d, ad8, tfival, net in ent:
        if (not np.isnan(tfival)) and (np.sign(tfival) == d) and (abs(tfival) >= qtfi):
            continue
        bs = bdrift(binD.get(BIN[pair]), tB)
        if (not np.isnan(bs)) and (np.sign(bs) == d) and (abs(bs) >= THR):
            continue
        rows.append((tB, ad8, net, per(tB)))
    return sorted(rows)


def main():
    print("=== ENHANCEMENT CLIMAX-FADE : favoriser les fades à fort |drift_8h| sur majors augmente-t-il l'EV ? (tick-level, causal, persistance) ===\n")
    binD = {s: load_bin(s) for s in set(BIN.values())}
    D = {p: collect(p, binD) for p in MAJ}

    # A. EV par tercile de |d8| (par actif + POOL), terciles PROPRES à chaque actif
    print("--- A. EV-fade par tercile de |drift_8h| (climax = tercile HAUT). monotone croissant = climax meilleur ? ---")
    print(f"  {'actif':>9} | {'n':>4} | {'LOW |d8|':>9} {'MID':>7} {'HIGH(climax)':>13} | {'win HIGH':>8}")
    poolL = {"LOW": [], "MID": [], "HIGH": []}; poolP = {"E": {"LOW": [], "HIGH": []}, "M": {"LOW": [], "HIGH": []}}
    for p in MAJ:
        r = D[p]
        if len(r) < 15:
            continue
        ad = np.array([x[1] for x in r]); nt = np.array([x[2] for x in r]); pr = np.array([x[3] for x in r])
        a1, a2 = np.quantile(ad, [1/3, 2/3])
        lo = nt[ad <= a1]; mi = nt[(ad > a1) & (ad < a2)]; hi = nt[ad >= a2]
        poolL["LOW"] += list(lo); poolL["MID"] += list(mi); poolL["HIGH"] += list(hi)
        for prd in ("E", "M"):
            poolP[prd]["LOW"] += list(nt[(ad <= a1) & (pr == prd)]); poolP[prd]["HIGH"] += list(nt[(ad >= a2) & (pr == prd)])
        print(f"  {p:>9} | {len(r):>4} | {np.mean(lo):>+9.1f} {np.mean(mi):>+7.1f} {np.mean(hi):>+13.1f} | {100*np.mean(hi>0):>7.0f}%")
    print(f"  {'POOL':>9} | {sum(len(poolL[k]) for k in poolL):>4} | {np.mean(poolL['LOW']):>+9.1f} {np.mean(poolL['MID']):>+7.1f} {np.mean(poolL['HIGH']):>+13.1f} | {100*np.mean(np.array(poolL['HIGH'])>0):>7.0f}%")

    # B. persistance E vs M (POOL)
    print(f"\n--- B. PERSISTANCE (POOL majors) : LOW vs HIGH |d8| en E (début mai) vs M (mi-mai) — l'effet tient-il dans les 2 ? ---")
    for prd in ("E", "M"):
        lo = np.array(poolP[prd]["LOW"]); hi = np.array(poolP[prd]["HIGH"])
        if len(lo) >= 5 and len(hi) >= 5:
            print(f"  {prd} | LOW {np.mean(lo):>+6.1f}(n{len(lo):>3}) | HIGH {np.mean(hi):>+6.1f}(n{len(hi):>3}) | climax-low {np.mean(hi)-np.mean(lo):>+6.1f}")

    # C. CAUSAL : flag climax via percentile roulant par actif (trailing K jambes, leg-based)
    print(f"\n--- C. CAUSAL : climax = |d8| >= Q70 roulant des {KTRAIL} dernières jambes de l'actif (leg-based, ZÉRO grille) ---")
    cC = {"climax": [], "rest": []}; cP = {"E": {"climax": [], "rest": []}, "M": {"climax": [], "rest": []}}
    for p in MAJ:
        r = D[p]
        if len(r) < MINK + 10:
            continue
        ad = np.array([x[1] for x in r]); nt = np.array([x[2] for x in r]); pr = np.array([x[3] for x in r])
        for i in range(len(r)):
            if i < MINK:
                continue
            q70 = np.quantile(ad[max(0, i - KTRAIL):i], 0.70)   # passé strict = causal
            key = "climax" if ad[i] >= q70 else "rest"
            cC[key].append(nt[i]); cP[pr[i]][key].append(nt[i])
    for k in ("climax", "rest"):
        a = np.array(cC[k])
        print(f"  POOL {k:>6} | n={len(a):>4} | EV {np.mean(a):>+6.1f} | win {100*np.mean(a>0):>3.0f}% | worst {np.min(a):>+6.0f}")
    for prd in ("E", "M"):
        cl = np.array(cP[prd]["climax"]); rs = np.array(cP[prd]["rest"])
        if len(cl) >= 5 and len(rs) >= 5:
            print(f"  {prd}: climax {np.mean(cl):>+6.1f}(n{len(cl):>3}) vs rest {np.mean(rs):>+6.1f}(n{len(rs):>3}) | écart {np.mean(cl)-np.mean(rs):>+6.1f}")

    # D. gain de sizing : à plat vs tilt-climax (causal 2× sur climax)
    print(f"\n--- D. GAIN DE SIZING : EV/trade pondéré — à plat vs tilt-climax (2× sur climax causal) ---")
    allnet = np.concatenate([np.array([x[2] for x in D[p]]) for p in MAJ if len(D[p]) >= 15])
    flat = np.mean(allnet)
    cl = np.array(cC["climax"]); rs = np.array(cC["rest"])
    if len(cl) and len(rs):
        w_ev = (2 * np.sum(cl) + np.sum(rs)) / (2 * len(cl) + len(rs))   # EV par unité de risque, tilt 2× climax
        print(f"  à plat : EV/trade {flat:>+6.1f} (worst {np.min(allnet):>+5.0f})")
        print(f"  tilt-climax 2× : EV/unité-risque {w_ev:>+6.1f}  (climax EV {np.mean(cl):>+5.1f} vs rest {np.mean(rs):>+5.1f})")
        print(f"  -> si EV/unité-risque (tilt) >> à plat ET la queue ne gonfle pas -> favoriser le climax = enhancement réel.")

    print("\n  LECTURE : A monotone (LOW<MID<HIGH) + B effet présent en E ET M + C causal climax>rest -> enhancement ROBUSTE & déployable.")
    print("   Si l'effet disparaît en causal (C) ou flippe E/M (B) -> c'était in-sample/non-causal -> NE PAS l'ajouter (comme les autres embellissements).")


if __name__ == "__main__":
    main()
