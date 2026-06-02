"""TEST DÉDIÉ de (A) le détecteur |drift_8h| MAGNITUDE (l'original "HYPE 382 vs majors") — TICK-LEVEL / LEG-BASED, ZÉRO grille.
Fermeture propre (pas d'inférence) sur 2 axes, avec la MÊME loupe de persistance qu'ETH/réversion/trendabilité-continuation :
  1. NIVEAU |drift_8h| par actif (aux jambes) + PERSISTANCE période E (début mai) vs M (mi-mai) : HYPE reste-t-il le + haut ?
  2. PRÉMISSE DU GATE : "fader une jambe qui ÉTEND un trend_8h FORT (sign(d8)==d & |d8|>=Q70 propre à l'actif) = mauvais fade" —
     est-ce réel (fade EV extend-strong < reste) ET PERSISTANT (présent en E ET en M) ? (sur majors = là où on fade).
|drift_8h| via drift_over sur série COMPLÈTE, indexé aux fins de jambes (events tick-level) ; fwd via searchsorted. ZÉRO sampling.
"""
import os, sys, datetime
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import leg_chain, drift_over, PAIRS, WEEKS, MIN, GAP

HOUR = 60 * MIN; W8 = 8 * HOUR; H30 = 30 * MIN
MAJ = ("BTCUSDC", "ETHUSDC", "SOLUSDC", "XRPUSDC")


def fwd_at(ts, mid, Bs, H):
    n = len(ts); jf = np.searchsorted(ts, ts[Bs] + H, side="left"); ok = jf < n
    f = np.full(len(Bs), np.nan); f[ok] = (mid[jf[ok]] - mid[Bs[ok]]) / mid[Bs[ok]] * 1e4
    tf = np.zeros(len(Bs), bool); tf[ok] = (ts[jf[ok]] - ts[Bs[ok]]) > (H + GAP); f[tf] = np.nan
    return f


def per(t):
    return "E" if datetime.datetime.utcfromtimestamp(t / 1000).day <= 8 else "M"


def collect(pair):
    """par jambe (tick-level) : (t, |d8|, extends(sign d8==jambe), fade=-d·fwd30, periode)."""
    rows = []
    for wk, ddir in WEEKS.items():
        try:
            ti, tr, l2 = load_all(pair, ddir); ts, mid, _ = vectorized_features(ti, tr, l2)
        except Exception:
            continue
        ts = np.asarray(ts, np.int64); mid = np.asarray(mid, np.float64)
        Bs, ds = leg_chain(ts, mid, 50.0)
        if len(Bs) < 6:
            continue
        d8 = drift_over(ts, mid, W8)[Bs]; f30 = fwd_at(ts, mid, Bs, H30)
        for k in range(len(Bs)):
            if np.isnan(d8[k]) or np.isnan(f30[k]):
                continue
            d = float(ds[k])
            rows.append((int(ts[Bs[k]]), abs(float(d8[k])), np.sign(d8[k]) == d, -d * f30[k], per(int(ts[Bs[k]]))))
    return rows


def main():
    print("=== TEST DÉDIÉ (A) |drift_8h| MAGNITUDE — tick-level/leg-based, ZÉRO grille — séparation + persistance + gate ===\n")
    D = {p: collect(p) for p in PAIRS}

    # 1. NIVEAU |drift_8h| par actif + persistance E vs M
    print("--- 1. NIVEAU |drift_8h| par actif (aux jambes, tick-level) + persistance E vs M : HYPE reste-t-il le + haut ? ---")
    print(f"  {'actif':>9} | {'n':>4} {'med|d8| all':>11} | {'med|d8| E':>10} {'med|d8| M':>10} | rang stable ?")
    medE = {}; medM = {}
    for p in PAIRS:
        r = D[p]
        if len(r) < 12:
            continue
        ad = np.array([x[1] for x in r]); pr = np.array([x[4] for x in r])
        aE = ad[pr == "E"]; aM = ad[pr == "M"]
        medE[p] = np.median(aE) if len(aE) >= 6 else np.nan; medM[p] = np.median(aM) if len(aM) >= 6 else np.nan
        print(f"  {p:>9} | {len(r):>4} {np.median(ad):>11.0f} | {medE[p]:>10.0f} {medM[p]:>10.0f}")
    okp = [p for p in PAIRS if p in medE and not np.isnan(medE[p]) and not np.isnan(medM[p])]
    if okp:
        topE = max(okp, key=lambda p: medE[p]); topM = max(okp, key=lambda p: medM[p])
        print(f"\n  + haut |d8| en E = {topE} ; en M = {topM}  -> {'HYPE le + haut dans les 2 = séparation PERSISTE' if topE=='HYPEUSDC' and topM=='HYPEUSDC' else 'rang change'}")
        rkE = " > ".join(p[:3] for p in sorted(okp, key=lambda p: -medE[p]))
        rkM = " > ".join(p[:3] for p in sorted(okp, key=lambda p: -medM[p]))
        print(f"  rang |d8| E (haut->bas) : {rkE}\n  rang |d8| M (haut->bas) : {rkM}")

    # 2. PRÉMISSE DU GATE (sur majors) : fader une jambe qui ÉTEND un trend_8h FORT = mauvais ? persistant E & M ?
    print(f"\n--- 2. GATE |drift_8h| (sur MAJORS) : fade EV 'étend trend_8h FORT (sign==d & |d8|>=Q70/actif)' vs 'reste' — réel ? persistant ? ---")
    print(f"  {'actif':>9} | {'période':>7} | {'extend-strong':>14} {'reste':>14} | {'gain du gate':>12}")
    for p in MAJ:
        r = D[p]
        if len(r) < 15:
            continue
        ad = np.array([x[1] for x in r]); ext = np.array([x[2] for x in r]); fade = np.array([x[3] for x in r]); pr = np.array([x[4] for x in r])
        q70 = np.quantile(ad, 0.70)
        strong = ext & (ad >= q70)
        for lab, mask in (("ALL", np.ones(len(r), bool)), ("E", pr == "E"), ("M", pr == "M")):
            es = fade[mask & strong]; rest = fade[mask & ~strong]
            if len(es) >= 4 and len(rest) >= 4:
                gain = np.mean(rest) - np.mean(es)
                print(f"  {p:>9} | {lab:>7} | {np.mean(es):>+8.1f}(n{len(es):>3}) {np.mean(rest):>+8.1f}(n{len(rest):>3}) | {gain:>+12.1f}")
            else:
                print(f"  {p:>9} | {lab:>7} | (n trop faible)")
        print()

    print("  LECTURE :")
    print("   (1) si HYPE reste le + haut |d8| en E ET M -> la separation de niveau persiste (pas comme la continuation/reversion qui reshufflaient).")
    print("       si le rang change -> (A) aussi period-specific.")
    print("   (2) 'gain du gate' = fade(reste) − fade(extend-strong). >0 = le gate aide (extend-strong est bien un + mauvais fade).")
    print("       si >0 en ALL mais FLIP de signe entre E et M -> l'effet du gate n'est PAS persistant (comme le reste) -> (A) mort.")
    print("       si >0 ET stable en E ET M sur les majors -> (A) SURVIT comme garde-fou (contrairement à B).")


if __name__ == "__main__":
    main()
