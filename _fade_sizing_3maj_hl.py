"""SIZING du LIVRE PROPRE = BTC/SOL/XRP (sans ETH, le driver de queue). Re-joue l'etude sizing sur le bon livre.
Reutilise collect() (fade+FLOW, no stop). HL in-sample."""
import os, sys, time
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _fade_sizing_book_hl import collect

MAJ3 = ("BTCUSDC", "SOLUSDC", "XRPUSDC")
HOLD = 30 * 60 * 1000; DAYMS = 86400000


def main():
    print("=== SIZING LIVRE PROPRE (BTC/SOL/XRP, sans ETH) — fade+FLOW, no stop, HL in-sample ===\n")
    book = {}
    for p in MAJ3:
        r = collect(p)
        if r:
            book[p] = sorted(r)
    alltr = sorted([x for p in book for x in book[p]])
    net = np.array([x[1] for x in alltr]); ts = np.array([x[0] for x in alltr])
    worst = float(np.min(net)); ndays = len(np.unique(ts // DAYMS))
    eq = np.cumsum(net); maxdd = float(np.max(np.maximum.accumulate(eq) - eq)); bpsday = float(eq[-1]) / ndays
    # concurrence
    iv = []
    for p in book:
        for tB, _ in book[p]:
            iv.append((tB, 1)); iv.append((tB + HOLD, -1))
    iv.sort(); cur = mx = 0
    for _, d in iv:
        cur += d; mx = max(mx, cur)
    print(f"  trades={len(net)} sur {ndays}j -> {len(net)/ndays:.1f}/j | mean{np.mean(net):+.1f} worst{worst:+.0f} p1{np.percentile(net,1):+.0f} | maxDD {maxdd:.0f}bps | concurrence max {mx}")
    WC = 1.5 * abs(worst)
    print(f"\n  TABLE SIZING (pire-cas suppose=1.5x worst={WC:.0f}bps) :")
    print(f"  {'risk/trade':>10} | {'notionnel/trade':>15} | {'expo max':>9} | {'rdt/jour':>9} | {'rdt/mois(20j)':>13} | {'maxDD capital':>13}")
    for R in (0.25, 0.5, 1.0):
        f = (R / 100.0) / (WC / 1e4)
        print(f"  {R:>9.2f}% | {100*f:>13.1f}% | {100*f*mx:>7.0f}% | {bpsday*f/1e4*100:>+8.3f}% | {bpsday*f/1e4*100*20:>+12.2f}% | {maxdd*f/1e4*100:>12.1f}%")
    print("\n  vs livre 4-majors (avec ETH) : notionnel ~7% / +0.12%j a 0.25%risk. Ici sans ETH, worst plus doux -> sizing plus gros.")
    print("  in-sample/mai ; expo>100% = levier (a borner) ; 11 juin = juge.")


if __name__ == "__main__":
    main()
