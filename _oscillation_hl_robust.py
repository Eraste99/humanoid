"""HL in-sample ROBUSTE — meme construction oscillation, stats robustes (median, hit-rate, winsor).

Controle symetrique du test Binance robuste : le FADE-en-bear sur HL (vrai mid L2, in-sample mai) est-il
porte par une TENDANCE CENTRALE (median>0, hit>50%) ou par quelques outliers (n=40-63/bucket = fragile) ?
Si HL median/hit confirment ET Binance robuste confirme -> mean-reversion bear = solide.
net 3bps, H=30m & 2h. HYPE + majors HL, tick-level, ZERO sampling.
"""
import os, sys, time
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _oscillation_capture import collect, PAIRS, WEEKS, HS, FEE


def winmean(x, lo=1, hi=99):
    if len(x) < 10:
        return float(np.mean(x)) if len(x) else float('nan')
    a, b = np.percentile(x, [lo, hi])
    return float(np.mean(np.clip(x, a, b)))


def main():
    t0 = time.time()
    print("=== HL IN-SAMPLE ROBUSTE — fade par regime : mean vs WINSOR / MEDIAN / HIT% (anti-outlier) — HYPE+majors ===")
    print("controle : le FADE-bear HL est-il une tendance centrale (median>0,hit>50%) ou des outliers (n petit) ?\n")
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
    for hk in HS:
        print("\n" + "=" * 116)
        print(f"HORIZON H = {hk}  | FADE robuste par regime (net 3bps) : mean vs WINSOR / MEDIAN / HIT%   [HL in-sample]")
        print("=" * 116)
        print(f"  {'actif':>9} {'regime':>6} | {'n':>4} | {'FADEmean':>8} {'FADEwins':>8} {'FADEmed':>8} | {'hit>0%':>7} {'hit>fee%':>8} | {'RIDEmed':>8}")
        print("  " + "-" * 90)
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
                fade_raw = -dd * ff; ride_raw = dd * ff
                fmean = float(np.mean(fade_raw)) - FEE
                fwins = winmean(fade_raw) - FEE
                fmed = float(np.median(fade_raw)) - FEE
                hit0 = 100.0 * np.mean(fade_raw > 0)
                hitf = 100.0 * np.mean(fade_raw > FEE)
                rmed = float(np.median(ride_raw)) - FEE
                print(f"  {p:>9} {rname:>6} | {int(rmask.sum()):>4} | {fmean:>+8.1f} {fwins:>+8.1f} {fmed:>+8.1f} | "
                      f"{hit0:>7.1f} {hitf:>8.1f} | {rmed:>+8.1f}")
    print("\n  LECTURE : compare a Binance robuste. Si HL bear FADEmed>0 ET hit>50% sur BTC/SOL/XRP -> in-sample SOLIDE")
    print("            (pas porte par outliers malgre n petit). HYPE inverse attendu (momentum).")
    print(f"\n[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
