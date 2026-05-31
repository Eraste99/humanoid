"""DECOMPOSER le FADE-en-regime-extreme : MEAN-REVERSION (contre macro) vs CONTINUATION (avec macro).

Question ouverte : "FADE gagne en bear/bull" — est-ce :
  (A) du TREND-FOLLOWING du macro deguise  -> fader un PULLBACK = re-suivre la tendance (pas un nouvel edge), ou
  (B) de la VRAIE MEAN-REVERSION           -> fader une EXTENSION = parier le retournement (nouvel edge) ?

A chaque fin de jambe B_i (zig-zag 50bps), d_i = direction de la jambe qui vient de finir ;
m = signe(drift_4h macro) au point B_i. On split CHAQUE jambe en deux buckets :

  - jambe AVEC macro  (d_i == m)  : EXTENSION dans le sens du trend.
        FADE = position -d_i = CONTRE macro  => pari de RETOURNEMENT = MEAN-REVERSION.
  - jambe CONTRE macro (d_i == -m): PULLBACK contre le trend.
        FADE = position -d_i = AVEC macro    => pari de CONTINUATION = TREND-FOLLOWING.

Pour chaque actif x regime (terciles drift_4h) x bucket : n, part%, RIDE net, FADE net, fwd moyen.
Lecture : si le FADE positif vient du bucket CONTINUATION -> trend-following macro (deja connu, fragile OOS).
          si le FADE positif vient du bucket MEAN-REVERSION -> vrai edge de retournement (nouveau).
net 3bps, H = 30m & 2h. HYPE + majors, HL vrai mid L2, tick-level, ZERO sampling.
"""
import os, sys, time
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# reuse the EXACT construction (legs/regime/fwd) so les chiffres sont comparables a _oscillation_capture
from _oscillation_capture import collect, PAIRS, WEEKS, HS, FEE


def main():
    t0 = time.time()
    print("=== DECOMPOSER LE FADE : MEAN-REVERSION (contre macro) vs CONTINUATION (avec macro) — HYPE+majors HL ===")
    print("jambe AVEC macro -> FADE = pari retournement = MEAN-REV ; jambe CONTRE macro -> FADE = pari continuation = TREND.\n")
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
        print("\n" + "=" * 118)
        print(f"HORIZON H = {hk}  | FADE decompose : bucket MEAN-REV (jambe AVEC macro) vs CONTINUATION (jambe CONTRE macro)")
        print("=" * 118)
        print(f"  {'actif':>9} {'regime':>6} {'bucket':>12} | {'n':>5} {'part%':>5} | {'RIDE':>7} {'FADE':>7} | {'fwd':>7} {'meanDrift4h':>11}")
        print("  " + "-" * 88)
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
                m = np.sign(dmac)  # signe macro par jambe
                ntot = int(rmask.sum())
                buckets = [
                    ("MEAN-REV", rmask & (d == m)),     # jambe AVEC macro -> FADE contre macro = retournement
                    ("CONTIN",   rmask & (d == -m)),    # jambe CONTRE macro -> FADE avec macro = continuation
                ]
                for bname, bmask in buckets:
                    nb = int(bmask.sum())
                    if nb < 5:
                        print(f"  {p:>9} {rname:>6} {bname:>12} | {nb:>5} {'--':>5} | {'--':>7} {'--':>7} | {'--':>7} {'':>11}")
                        continue
                    dd = d[bmask]; ff = fwd[bmask]
                    ride = float(np.mean(dd * ff - FEE))
                    fade = float(np.mean(-dd * ff - FEE))
                    part = 100.0 * nb / ntot
                    print(f"  {p:>9} {rname:>6} {bname:>12} | {nb:>5} {part:>5.0f} | {ride:>+7.1f} {fade:>+7.1f} | "
                          f"{np.mean(ff):>+7.1f} {np.mean(dmac[bmask]):>+11.0f}")
                print("  " + "." * 88)

    print("\n  LECTURE : pour CHAQUE regime, regarde quel bucket porte le FADE positif.")
    print("    bucket CONTINUATION (jambe CONTRE macro) gagne  -> FADE = trend-following macro deguise (fragile OOS).")
    print("    bucket MEAN-REV (jambe AVEC macro) gagne         -> vrai edge de retournement local (nouveau).")
    print(f"\n[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
