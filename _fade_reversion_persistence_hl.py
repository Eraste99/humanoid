"""PERSISTANCE du caractère RÉVERSION-30min : une paire fade-able (ou ETH le laggard) le RESTE-t-elle sur 1-2 semaines ?
= la condition de validité du sélecteur LENT (recalib bi-hebdo). Si stable sur ~2 semaines -> bi-hebdo OK ; si ça flippe en jours -> recalib + vite.
On découpe la data (mai) en blocs ~3-4 jours + 2 PÉRIODES (début mai 01-08 vs mi-mai 15-22, ~10j d'écart) et on regarde la réversion-30min PAR ACTIF par bloc :
  - le RANG tient-il (ETH reste le plus bas, BTC/SOL/XRP restent hauts) ? - le NIVEAU est-il stable bloc-à-bloc ?
  - corrélation cross-pair (période E vs période M) = le profil de réversion persiste-t-il sur ~1-2 semaines ?
réversion = E[-d·fwd30] sur TOUTES jambes (= caractère de la paire). leg-level, tick, ZÉRO grille. Caveat : 3 sem in-sample (2 périodes).
"""
import os, sys, datetime
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _fade_reversion30_selector_hl import collect_legs, MAJ


def blk(t):
    d = datetime.datetime.utcfromtimestamp(t / 1000).day
    if d <= 4:
        return "B1 05-01/04"
    if d <= 8:
        return "B2 05-05/08"
    if 15 <= d <= 18:
        return "B3 05-16/18"
    if d >= 19:
        return "B4 05-19/22"
    return None


def period(t):
    return "E (debut mai)" if datetime.datetime.utcfromtimestamp(t / 1000).day <= 8 else "M (mi-mai)"


def rev_of(t, d, f, mask):
    fade = -d * f; m = mask & ~np.isnan(fade)
    return (float(np.mean(fade[m])), int(m.sum())) if m.sum() >= 6 else (np.nan, int(m.sum()))


def main():
    print("=== PERSISTANCE du caractère RÉVERSION-30min : une paire reste-t-elle choppy/fade-able sur ~1-2 semaines ? ===")
    print("    leg-level, tick, ZÉRO grille. réversion = E[-d·fwd30] toutes jambes. Caveat : mai in-sample, 2 périodes (~10j d'écart).\n")
    L = {p: collect_legs(p) for p in MAJ}
    blocks = ["B1 05-01/04", "B2 05-05/08", "B3 05-16/18", "B4 05-19/22"]

    print("--- 1. RÉVERSION-30min par actif × bloc ~3-4j (le rang/niveau tient-il dans le temps ?) ---")
    print(f"  {'actif':>9} | " + " ".join(f"{b:>13}" for b in blocks))
    REV = {p: {} for p in MAJ}
    for p in MAJ:
        t, d, f, g = L[p]; bl = np.array([blk(x) for x in t])
        cells = []
        for b in blocks:
            r, n = rev_of(t, d, f, bl == b); REV[p][b] = r
            cells.append(f"{r:>+6.1f}(n{n:>3})" if not np.isnan(r) else f"{'--':>11}")
        print(f"  {p:>9} | " + " ".join(f"{c:>13}" for c in cells))

    print(f"\n--- 2. PÉRIODE E (début mai) vs M (mi-mai, ~10j après) — le profil persiste-t-il sur 1-2 semaines ? ---")
    print(f"  {'actif':>9} | {'rev E':>10} {'rev M':>10} | stable ?")
    rE = {}; rM = {}
    for p in MAJ:
        t, d, f, g = L[p]; pr = np.array([period(x) for x in t])
        re, ne = rev_of(t, d, f, pr == "E (debut mai)"); rm, nm = rev_of(t, d, f, pr == "M (mi-mai)")
        rE[p] = re; rM[p] = rm
        same = (not np.isnan(re) and not np.isnan(rm)) and (np.sign(re) == np.sign(rm))
        print(f"  {p:>9} | {re:>+7.1f}(n{ne:>3}) {rm:>+7.1f}(n{nm:>3}) | {'OUI (même signe)' if same else 'flip/na'}")
    # corrélation cross-pair E vs M (le RANG des paires persiste-t-il ?)
    ps = [p for p in MAJ if not np.isnan(rE[p]) and not np.isnan(rM[p])]
    if len(ps) >= 3:
        ve = np.array([rE[p] for p in ps]); vm = np.array([rM[p] for p in ps])
        print(f"\n  corrélation cross-pair (rev_E vs rev_M) = {np.corrcoef(ve, vm)[0,1]:>+.2f}  (>0 = le profil de réversion des paires PERSISTE E->M)")
        # rang d'ETH dans chaque période
        rkE = sorted(ps, key=lambda p: rE[p]); rkM = sorted(ps, key=lambda p: rM[p])
        print(f"  rang réversion E (bas->haut) : {' < '.join(p[:3] for p in rkE)}")
        print(f"  rang réversion M (bas->haut) : {' < '.join(p[:3] for p in rkM)}")
        print(f"  ETH est-il le + BAS dans les 2 périodes ? {'OUI' if rkE[0]=='ETHUSDC' and rkM[0]=='ETHUSDC' else 'NON'}")

    # 3. variabilité intra-pair (bloc-à-bloc) vs spread cross-pair = le signal domine-t-il le bruit ?
    print(f"\n--- 3. SIGNAL vs BRUIT : variabilité bloc-à-bloc d'une paire vs écart entre paires ---")
    within = []
    for p in MAJ:
        vals = [REV[p][b] for b in blocks if not np.isnan(REV[p][b])]
        if len(vals) >= 2:
            within.append(np.std(vals))
    blockmeans = {b: np.nanmean([REV[p][b] for p in MAJ]) for b in blocks}
    crossvals = [v for p in MAJ for v in [np.nanmean([REV[p][b] for b in blocks])]]
    print(f"  std intra-pair (bloc-à-bloc), moyenne = {np.mean(within):>4.1f} bps   |   spread cross-pair (max-min des moyennes) = {np.nanmax(crossvals)-np.nanmin(crossvals):>4.1f} bps")
    print(f"  -> si le spread cross-pair >> la variabilité intra-pair, le caractère de la paire DOMINE le bruit temporel = sélecteur lent fiable.")

    print("\n  LECTURE :")
    print("   - si ETH reste le + bas (et BTC/SOL/XRP hauts) dans LES 2 périodes + corr E-M >0 -> le caractère réversion PERSISTE ~1-2 semaines -> recalib bi-hebdo OK.")
    print("   - si les rangs flippent E->M -> le caractère tourne plus vite -> recalibrer + souvent (mais PAS intraday, déjà réfuté).")
    print("   ATTENTION : 3 semaines / 2 periodes in-sample : indicatif, pas definitif. La vraie persistance se confirmera sur l'OOS 11 juin + les mois a venir.")


if __name__ == "__main__":
    main()
