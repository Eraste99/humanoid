"""DETAIL gagnants/perdants du livre fade DEPLOYABLE (BTC/SOL/XRP, fade+FLOW+Binance-lead) : combien de trades, combien perdent,
quelle taille de gain vs perte, par jour. Repond a "9-13/jour c'est peu ?" et "parmi les 13, combien de pertes ?". Descriptif, tick-level.
"""
import os, sys, datetime
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _fade_climax_causal_robust_hl import collect, MAJ
from _fade_binlead_fullbook_hl import load_bin, BIN

DAY = 86400000


def main():
    print("=== DETAIL GAGNANTS/PERDANTS — livre fade deployable BTC/SOL/XRP (fade+FLOW+Binance-lead), tick-level ===\n")
    binD = {s: load_bin(s) for s in set(BIN.values())}
    net = []; ts = []
    for p in MAJ:
        _, fades = collect(p, binD)
        for (t, ad8, n, prd) in fades:
            net.append(n); ts.append(t)
    net = np.array(net); ts = np.array(ts)
    days = np.unique(ts // DAY); nd = len(days)
    win = net > 0; los = net <= 0
    aw = net[win]; al = net[los]

    print(f"--- VOLUME ---")
    print(f"  trades total = {len(net)} sur {nd} jours actifs -> {len(net)/nd:.1f}/jour (sur 3 paires = ~{len(net)/nd/3:.1f}/paire/jour)")
    print(f"\n--- GAGNANTS vs PERDANTS ---")
    print(f"  win rate = {100*win.mean():.0f}%  ->  par jour : ~{win.mean()*len(net)/nd:.0f} gagnants, ~{los.mean()*len(net)/nd:.0f} perdants (sur ~{len(net)/nd:.0f})")
    print(f"  gain moyen   (gagnant) = {np.mean(aw):>+6.1f} bps  (median {np.median(aw):>+5.1f})")
    print(f"  perte moyenne(perdant) = {np.mean(al):>+6.1f} bps  (median {np.median(al):>+5.1f})")
    print(f"  EV/trade = {np.mean(net):>+5.1f} bps = {100*win.mean():.0f}%*{np.mean(aw):+.0f} + {100*los.mean():.0f}%*{np.mean(al):+.0f}")
    print(f"  ratio gain/perte (|moyennes|) = {abs(np.mean(aw)/np.mean(al)):.2f}")

    print(f"\n--- TAILLE DES PERTES (la queue short-gamma) ---")
    for lo, hi, lab in [(-1e9, -50, "< -50 (grosses)"), (-50, -30, "-50..-30"), (-30, -10, "-30..-10"), (-10, 0, "-10..0 (petites)")]:
        m = (al > lo) & (al <= hi)
        print(f"  {lab:>18} | n={m.sum():>3} ({100*m.sum()/len(net):>3.0f}% des trades) | ~{m.sum()/nd:.2f}/jour")
    print(f"  pire perte = {np.min(net):>+.0f} bps ; perte > -50 ~ {((net< -50).sum())/nd:.2f}/jour")

    print(f"\n--- DISTRIBUTION par jour (combien de trades les jours actifs) ---")
    cnt = np.array([np.sum(ts // DAY == d) for d in days])
    print(f"  trades/jour : min {cnt.min()}, median {int(np.median(cnt))}, max {cnt.max()}  (tire EN GRAPPES : choppy=mitraille, trend calme=peu)")

    print("\n  LECTURE : ~9-13/jour sur 3 paires = volontairement SOBRE (jambe 50bps + 2 filtres qui coupent ~moitie). Pour PLUS de trades :")
    print("   baisser le seuil de jambe (20-30bps -> bcp plus, mais + minces) OU elargir l'univers (plus de paires). C'est un LEVIER, pas une fatalite.")
    print("   La majorite des trades gagnent peu, une minorite perd ; les rares grosses pertes = la queue (geree par sizing petit + Binance-lead, PAS par stop).")


if __name__ == "__main__":
    main()
