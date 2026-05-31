"""FREQUENCE des trades + FREQUENCE des worst-cases — sur le livre fade deployable (fade+FLOW, no stop).
Repond a : combien de trades/jour ? a quelle frequence arrivent les grosses pertes (-100/-150/-200) ?
Reutilise collect() de l'etude sizing. 4 majors ET 3 majors (sans ETH = le driver de queue). HL in-sample.
"""
import os, sys, time
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _fade_sizing_book_hl import collect, MAJ

DAYMS = 86400000


def analyze(label, pairs, trades):
    sub = [t for t in trades if t[2] in pairs]
    if not sub:
        return
    ts = np.array([t[0] for t in sub]); net = np.array([t[1] for t in sub])
    days = ts // DAYMS
    uniq = np.unique(days); nd = len(uniq)
    perday = np.array([np.sum(days == d) for d in uniq])
    N = len(sub); tpd = N / nd
    print(f"\n=== {label} ===")
    print(f"  trades total = {N} sur {nd} jours actifs -> {tpd:.1f} trades/jour (median {np.median(perday):.0f}, min {perday.min()}, max {perday.max()})")
    print(f"  pertes : mean {np.mean(net):+.1f} | worst {np.min(net):+.0f} | p1 {np.percentile(net,1):+.0f} | p5 {np.percentile(net,5):+.0f} bps")
    print(f"  {'seuil':>8} | {'#trades':>7} | {'1 tous les N trades':>20} | {'1 tous les N jours':>19}")
    for thr in (50, 100, 150, 200):
        c = int(np.sum(net < -thr))
        if c == 0:
            print(f"  < -{thr:>3} | {c:>7} | {'(jamais)':>20} | {'(jamais)':>19}")
        else:
            print(f"  < -{thr:>3} | {c:>7} | {('1 / %.0f' % (N / c)):>20} | {('1 / %.1f j' % (nd / c)):>19}")
    # worst trades datés
    order = np.argsort(net)[:8]
    print("  worst 8 trades :")
    for i in order:
        d = time.strftime("%Y-%m-%d %H:%M", time.gmtime(sub[i][0] / 1000))
        print(f"     {net[i]:>+7.0f} bps  {sub[i][2]:>9}  {d}")


def main():
    print("=== FREQUENCE trades + worst-cases — livre fade deployable (fade+FLOW, no stop) — HL in-sample ===")
    trades = []
    for p in MAJ:
        for (ts, net) in collect(p):
            trades.append((ts, net, p))
    analyze("4 MAJORS (BTC/ETH/SOL/XRP)", set(MAJ), trades)
    analyze("3 MAJORS (sans ETH = le driver de queue)", {"BTCUSDC", "SOLUSDC", "XRPUSDC"}, trades)
    print("\n  LECTURE : trades/jour = frequence d'activite ; '1 tous les N jours' = a quel rythme une grosse perte tombe.")
    print("    sans ETH la queue est bien plus douce (worst ~-90 vs -237). in-sample/mai, le 11 juin juge.")


if __name__ == "__main__":
    main()
