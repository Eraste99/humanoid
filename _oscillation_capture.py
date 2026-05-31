"""CAPTURER LES OSCILLATIONS sur TOUS LES TERRAINS — RIDE vs FADE vs NULL, par regime (bull/bear/chop).
HYPE + majors, HL vrai mid L2, tick-level, ZERO sampling.

Vision user : operer partout. Bull -> short les mini-pullbacks + long les reprises (= RIDE chaque jambe).
Bear -> miroir. Chop -> fader. Question : timer les wiggles (jambes du zig-zag) bat-il le NULL, dans CHAQUE regime ?

Jambes = zig-zag first-passage 50 bps (les oscillations). A chaque fin de jambe B_i (un retournement) :
  recent dir = d_i (la jambe qui vient de finir).
  RIDE  = parier d_i (la jambe continue / on suit le wiggle).
  FADE  = parier -d_i (ca reverse).
  regime au point B_i = tercile du drift_macro (4h) de l'actif : bear / chop / bull.
PnL = position * retour_fwd(H) - 3bps, H in {30min, 2h}. NULL = signe aleatoire (200 seeds).
Matrice par actif x regime : RIDE net / FADE net / NULL net. Si RIDE ou FADE >> NULL dans un regime -> timing utile.
"""
import os, sys, time
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features

PAIRS = ["HYPEUSDC", "BTCUSDC", "ETHUSDC", "SOLUSDC", "XRPUSDC"]
WEEKS = {"W507": r"C:\Users\erast\hl_data\week_20260507",
         "W515": r"C:\Users\erast\hl_data\week_20260515",
         "W516": r"C:\Users\erast\hl_data\week_20260516"}
MIN = 60 * 1000
GAP = 10 * MIN
FEE = 3.0
MACRO = 4 * 60 * MIN
HS = {"30m": 30 * MIN, "2h": 120 * MIN}
RNG = np.random.default_rng(21)
NSEED = 200


def drift_over(ts, mid, W):
    p = np.searchsorted(ts, ts - W, side="left")
    d = (mid - mid[p]) / mid[p] * 1e4
    return np.where((ts - ts[p]) >= 0.5 * W, d, np.nan)


def leg_chain(ts, mid, thr_bps=50.0):
    thr = thr_bps / 1e4; n = len(ts); Bs = []; ds = []
    i = 0
    while i < n - 1:
        p = mid[i]; up = p * (1 + thr); dn = p * (1 - thr)
        j = i + 1; found = -1; fdir = 0; win = 4096
        while j < n:
            end = min(n, j + win); seg = mid[j:end]
            hit = (seg >= up) | (seg <= dn)
            if hit.any():
                k = int(np.argmax(hit)); found = j + k
                fdir = 1 if mid[found] >= up else -1; break
            j = end; win = min(win * 4, 4_000_000)
        if found < 0:
            break
        Bs.append(found); ds.append(fdir); i = found
    return np.array(Bs, dtype=int), np.array(ds, dtype=float)


def collect(pair, ddir):
    ticks, trades, l2 = load_all(pair, ddir)
    ts, mid, feats = vectorized_features(ticks, trades, l2)
    ts = np.asarray(ts, np.int64); mid = np.asarray(mid, np.float64)
    Bs, ds = leg_chain(ts, mid)
    if len(Bs) < 10:
        return None
    dmac = drift_over(ts, mid, MACRO)[Bs]
    fwd = {}
    n = len(ts)
    for hk, H in HS.items():
        jf = np.searchsorted(ts, ts[Bs] + H, side="left")
        ok = jf < n
        f = np.full(len(Bs), np.nan)
        f[ok] = (mid[jf[ok]] - mid[Bs[ok]]) / mid[Bs[ok]] * 1e4
        tf = np.zeros(len(Bs), bool); tf[ok] = (ts[jf[ok]] - ts[Bs[ok]]) > (H + GAP)
        f[tf] = np.nan
        fwd[hk] = f
    return dict(d=ds, dmac=dmac, fwd=fwd)


def main():
    t0 = time.time()
    print("=== CAPTURER LES OSCILLATIONS — RIDE vs FADE vs NULL par regime (bull/bear/chop) — HYPE+majors HL ===")
    print("jambes zig-zag 50bps ; RIDE=suivre la jambe / FADE=contre ; regime=tercile drift_4h ; net 3bps. H=30m,2h.\n")
    A = {p: {"d": [], "dmac": [], "fwd": {hk: [] for hk in HS}} for p in PAIRS}
    for pair in PAIRS:
        for wk, ddir in WEEKS.items():
            try:
                r = collect(pair, ddir)
            except Exception as e:
                print(f"  [skip] {pair}/{wk}: {e}"); continue
            if r is None:
                continue
            A[pair]["d"].append(r["d"]); A[pair]["dmac"].append(r["dmac"])
            for hk in HS:
                A[pair]["fwd"][hk].append(r["fwd"][hk])
            print(f"  {pair}/{wk}: {len(r['d'])} jambes")

    for hk in HS:
        print("\n" + "=" * 104)
        print(f"HORIZON H = {hk}  | net bps/jambe par actif x regime : RIDE / FADE / NULL  (n)")
        print("=" * 104)
        print(f"  {'actif':>9} {'regime':>6} | {'RIDE':>7} {'FADE':>7} {'NULL':>7} | {'n':>5} {'meanDrift4h':>11}")
        print("  " + "-" * 70)
        for p in PAIRS:
            if not A[p]["d"]:
                continue
            d = np.concatenate(A[p]["d"]); dmac = np.concatenate(A[p]["dmac"]); fwd = np.concatenate(A[p]["fwd"][hk])
            ok = ~(np.isnan(fwd) | np.isnan(dmac))
            d = d[ok]; dmac = dmac[ok]; fwd = fwd[ok]
            if len(d) < 30:
                continue
            q1, q2 = np.quantile(dmac, [1 / 3, 2 / 3])
            regimes = [("bear", dmac <= q1), ("chop", (dmac > q1) & (dmac < q2)), ("bull", dmac >= q2)]
            for rname, rmask in regimes:
                if rmask.sum() < 10:
                    continue
                dd = d[rmask]; ff = fwd[rmask]
                ride = np.mean(dd * ff - FEE)
                fade = np.mean(-dd * ff - FEE)
                nullv = 0.0
                for s in range(NSEED):
                    sg = np.where(RNG.random(len(dd)) < 0.5, 1.0, -1.0)
                    nullv += np.mean(sg * ff - FEE)
                nullv /= NSEED
                print(f"  {p:>9} {rname:>6} | {ride:>+7.1f} {fade:>+7.1f} {nullv:>+7.1f} | {int(rmask.sum()):>5} {np.mean(dmac[rmask]):>+11.0f}")

    print("\n  LECTURE : dans CHAQUE regime, RIDE ou FADE bat-il NULL ? si oui qq part -> timer les oscillations sert (vrai edge).")
    print("            si RIDE~FADE~NULL partout -> jambes pile/face, capturer les wiggles ne paie pas (mur frais).")
    print(f"\n[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
