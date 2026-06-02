"""TRENDABILITÉ — refait TICK-LEVEL / LEG-BASED (ZÉRO grille 5min) + test de PERSISTANCE (même loupe que la réversion).
Le score grille 5min disait HYPE +91.7 vs majors ~0. On (a) le refait sur les JAMBES (events tick-level, pas de grille temporelle)
et (b) on teste si HYPE reste 'trend' d'une PÉRIODE à l'autre (début mai vs mi-mai), comme on l'a fait pour la réversion.
Trendabilité d'un actif = à chaque fin de jambe, est-ce que la DIRECTION MACRO (sign drift_8h) CONTINUE sur l'horizon (2h/8h) ?
  trend_outcome = sign(drift_8h)·fwd_H  ( >0 = le trend macro continue = ride-able ; <0 = reverte = fade-able ). E[.] par actif.
INCLUT HYPE. drift_8h via drift_over sur la série COMPLÈTE indexée aux jambes ; fwd via searchsorted -> tick-level, ZÉRO sampling.
"""
import os, sys, datetime
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import leg_chain, drift_over, PAIRS, WEEKS, MIN, GAP

HOUR = 60 * MIN; W8 = 8 * HOUR; H2 = 2 * HOUR


def fwd_at(ts, mid, Bs, H):
    n = len(ts); jf = np.searchsorted(ts, ts[Bs] + H, side="left"); ok = jf < n
    f = np.full(len(Bs), np.nan); f[ok] = (mid[jf[ok]] - mid[Bs[ok]]) / mid[Bs[ok]] * 1e4
    tf = np.zeros(len(Bs), bool); tf[ok] = (ts[jf[ok]] - ts[Bs[ok]]) > (H + GAP); f[tf] = np.nan
    return f


def collect(pair):
    """par jambe (tick-level) : (t, macro_dir=sign(drift_8h), fwd8, fwd2). ZÉRO grille."""
    T = []; MD = []; F8 = []; F2 = []
    for wk, ddir in WEEKS.items():
        try:
            ti, tr, l2 = load_all(pair, ddir); ts, mid, _ = vectorized_features(ti, tr, l2)
        except Exception:
            continue
        ts = np.asarray(ts, np.int64); mid = np.asarray(mid, np.float64)
        Bs, ds = leg_chain(ts, mid, 50.0)
        if len(Bs) < 6:
            continue
        d8 = drift_over(ts, mid, W8)[Bs]; f8 = fwd_at(ts, mid, Bs, W8); f2 = fwd_at(ts, mid, Bs, H2)
        for k in range(len(Bs)):
            md = float(np.sign(d8[k])) if not np.isnan(d8[k]) else 0.0
            T.append(int(ts[Bs[k]])); MD.append(md); F8.append(f8[k]); F2.append(f2[k])
    o = np.argsort(T)
    return np.array(T)[o], np.array(MD)[o], np.array(F8)[o], np.array(F2)[o]


def trend_of(md, f, mask):
    ride = md * f; m = mask & ~np.isnan(ride) & (md != 0)
    if m.sum() < 6:
        return np.nan, np.nan, int(m.sum())
    return float(np.mean(ride[m])), float(np.mean(ride[m] > 0)), int(m.sum())


def blk(t):
    d = datetime.datetime.utcfromtimestamp(t / 1000).day
    if d <= 4: return "B1 05-01/04"
    if d <= 8: return "B2 05-05/08"
    if 15 <= d <= 18: return "B3 05-16/18"
    if d >= 19: return "B4 05-19/22"
    return None


def per(t):
    return "E" if datetime.datetime.utcfromtimestamp(t / 1000).day <= 8 else "M"


def main():
    print("=== TRENDABILITÉ tick-level/leg-based (ZÉRO grille) + PERSISTANCE — l'aiguilleur HYPE tient-il la même loupe qu'ETH ? ===")
    print("    trend = sign(drift_8h)·fwd_H ( >0 = le trend macro continue ; <0 = reverte ). INCLUT HYPE. tick-level, zéro sampling.\n")
    L = {p: collect(p) for p in PAIRS}

    print("--- 1. TRENDABILITÉ par actif (toutes jambes) : HYPE >> majors ? (refait tick-level, pas de grille) ---")
    print(f"  {'actif':>9} | {'n':>4} | {'trend_8h':>9} {'%cont8':>6} | {'trend_2h':>9} {'%cont2':>6} | verdict")
    for p in PAIRS:
        t, md, f8, f2 = L[p]
        r8, c8, n8 = trend_of(md, f8, np.ones(len(t), bool))
        r2, c2, _ = trend_of(md, f2, np.ones(len(t), bool))
        v = "TREND (ride)" if r8 > 20 else ("~neutre" if r8 > -10 else "REVERTE (fade)")
        print(f"  {p:>9} | {n8:>4} | {r8:>+9.1f} {100*c8:>5.0f}% | {r2:>+9.1f} {100*c2:>5.0f}% | {v}")

    print(f"\n--- 2. PERSISTANCE période E (début mai) vs M (mi-mai, ~10j) : HYPE reste-t-il TREND dans les 2 ? ---")
    print(f"  {'actif':>9} | {'trend_8h E':>11} {'trend_8h M':>11} | stable ?")
    rE = {}; rM = {}
    for p in PAIRS:
        t, md, f8, f2 = L[p]; pr = np.array([per(x) for x in t])
        re, _, ne = trend_of(md, f8, pr == "E"); rm, _, nm = trend_of(md, f8, pr == "M")
        rE[p] = re; rM[p] = rm
        if np.isnan(re) or np.isnan(rm):
            s = "n/a"
        elif np.sign(re) == np.sign(rm):
            s = "OUI (même signe)"
        else:
            s = "FLIP"
        print(f"  {p:>9} | {re:>+8.1f}(n{ne:>3}) {rm:>+8.1f}(n{nm:>3}) | {s}")
    ok = [p for p in PAIRS if not np.isnan(rE[p]) and not np.isnan(rM[p])]
    if len(ok) >= 3:
        ve = np.array([rE[p] for p in ok]); vm = np.array([rM[p] for p in ok])
        print(f"\n  corrélation cross-pair (trend_E vs trend_M) = {np.corrcoef(ve, vm)[0,1]:>+.2f}  (>0 = le profil de trendabilité PERSISTE)")
        topE = max(ok, key=lambda p: rE[p]); topM = max(ok, key=lambda p: rM[p])
        print(f"  + trendant en E = {topE} ; + trendant en M = {topM}  -> {'HYPE domine les 2 = PERSISTANT' if topE=='HYPEUSDC' and topM=='HYPEUSDC' else 'rang change'}")

    print(f"\n--- 3. TRENDABILITÉ par actif × bloc ~3-4j (HYPE reste-t-il en tête partout ?) ---")
    blocks = ["B1 05-01/04", "B2 05-05/08", "B3 05-16/18", "B4 05-19/22"]
    print(f"  {'actif':>9} | " + " ".join(f"{b:>13}" for b in blocks))
    for p in PAIRS:
        t, md, f8, f2 = L[p]; bl = np.array([blk(x) for x in t])
        cells = []
        for b in blocks:
            r, _, n = trend_of(md, f8, bl == b)
            cells.append(f"{r:>+6.0f}(n{n:>3})" if not np.isnan(r) else f"{'--':>11}")
        print(f"  {p:>9} | " + " ".join(f"{c:>13}" for c in cells))

    print("\n  LECTURE :")
    print("   (1) si HYPE trend_8h >> 0 et majors ~0/negatif (tick-level) -> l'aiguilleur tient SANS la grille (sampling éliminé).")
    print("   (2)/(3) si HYPE reste le + trendant dans LES 2 periodes / TOUS les blocs + corr>0 -> trendabilite PERSISTE (pas comme la reversion).")
    print("       Gap HYPE vs majors énorme attendu -> bien + robuste que les ~5bps de réversion qui, eux, reshufflaient.")


if __name__ == "__main__":
    main()
