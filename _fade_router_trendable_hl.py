"""AIGUILLEUR (router) : le detecteur rolling per-asset classe chaque paire trend-UP / trend-DOWN / CHOPPY (CAUSAL).
trendable = |drift_8h| fort POUR LA PAIRE (percentile roulant) ET directionnellement PERSISTANT (sign-consistency) => PAS choppy.
Archi user : paire trendable -> RIDE (long&hold si up, short&hold si down = on TIENT le trend, capture beta) ; choppy -> FADE (edge MR, sleeve separe).
Ce script teste la PREMISSE de l'aiguillage (PAS un filtre de fade) :
  (1) quand le detecteur dit 'trendable', ca CONTINUE-t-il (le ride paie) ? (2) reste-t-il a l'ecart des chops (precision 'PAS choppy') ?
SORTIE par paire : duty-cycle des etats, PnL realise du sleeve RIDE (state-machine : tient tant que trendable), vs buy&hold naif, + continuation par etat.
Le 'persistance' (sign-consistency du drift recent) est le bouton qui exclut le choppy. HL true-mid, causal, zero lookahead.
"""
import os, sys
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import drift_over, PAIRS, WEEKS, MIN, GAP

W8 = 8 * 60 * MIN; DAY = 24 * 60 * MIN
SAMPLE_DT = 5 * MIN
WIN = 3 * DAY                 # fenetre du percentile roulant (magnitude)
PERS_WIN = 1 * DAY            # fenetre de la persistance (sign-consistency)
MIN_BUF = int(1 * DAY / SAMPLE_DT)
RT = 6.0                      # cout round-trip ride (taker, amorti sur des heures de hold)
CONFIGS = [(60.0, 0.5), (60.0, 0.3), (70.0, 0.5)]   # (percentile magnitude P, seuil persistance taup)
HCONT = {"2h": 2 * 60 * MIN, "8h": W8}


def grid_series(ts, mid):
    """grille 5min : temps, mid, drift_8h signe (causal)."""
    d8full = drift_over(ts, mid, W8)
    G = np.arange(ts[0], ts[-1], SAMPLE_DT)
    gi = np.clip(np.searchsorted(ts, G, side="left"), 0, len(ts) - 1)
    gd8 = d8full[gi]; gmid = mid[gi]; gok = ~np.isnan(gd8)
    return G[gok], gmid[gok], gd8[gok]


def classify(G, gd8, P, taup):
    """etat causal par point : +1 trend-up / -1 trend-down / 0 choppy. magnitude(percentile roulant) ET persistance."""
    gabs = np.abs(gd8); n = len(G); st = np.zeros(n, np.int8); warm = np.zeros(n, bool)
    loM = np.searchsorted(G, G - WIN, side="left"); loP = np.searchsorted(G, G - PERS_WIN, side="left")
    sgn = np.sign(gd8)
    for i in range(n):
        if i - loM[i] < MIN_BUF:
            continue
        warm[i] = True
        thr = np.quantile(gabs[loM[i]:i], P / 100.0)      # |d8| typique de la paire (son propre historique)
        pers = abs(np.mean(sgn[loP[i]:i + 1])) if i + 1 - loP[i] > 0 else 0.0   # 1=mono-directionnel, 0=flip/choppy
        if gabs[i] >= thr and pers >= taup:
            st[i] = 1 if gd8[i] > 0 else -1
    return st, warm


def ride_pnl(G, gmid, st):
    """state-machine : tient long en trend-up / short en trend-down / flat en choppy. retourne (somme bps, n episodes, win%, duree moy h)."""
    pos = 0; entry = 0.0; eps = []
    for i in range(len(G)):
        d = int(st[i])
        if d != pos:
            if pos != 0:
                eps.append(pos * (gmid[i] - entry) / entry * 1e4 - RT)
            if d != 0:
                entry = gmid[i]; tentry = G[i]
            pos = d
    if pos != 0:
        eps.append(pos * (gmid[-1] - entry) / entry * 1e4 - RT)
    if not eps:
        return 0.0, 0, 0.0
    eps = np.array(eps)
    return float(eps.sum()), len(eps), float(100 * np.mean(eps > 0))


def main():
    print("=== AIGUILLEUR : detecteur rolling per-asset -> RIDE (trendable) / FADE (choppy). Premisse : trendable continue-t-il ? ===")
    print(f"    magnitude=percentile roulant {WIN//DAY}j ; persistance=|sign-consistency| sur {PERS_WIN//DAY}j ; cout ride RT={RT:.0f}bps. HL true-mid causal.\n")
    # charge une fois par (paire, semaine)
    SESS = {}
    for p in PAIRS:
        rows = []
        for wk, dd in WEEKS.items():
            try:
                ti, tr, l2 = load_all(p, dd); ts, mid, _ = vectorized_features(ti, tr, l2)
            except Exception:
                continue
            ts = np.asarray(ts, np.int64); mid = np.asarray(mid, np.float64)
            if len(ts) < 100:
                continue
            G, gmid, gd8 = grid_series(ts, mid)
            if len(G) < MIN_BUF + 10:
                continue
            rows.append((ts, mid, G, gmid, gd8))
        SESS[p] = rows

    for P, taup in CONFIGS:
        print(f"==================  CONFIG : P_magnitude={P:.0f}  taup_persistance={taup:.2f}  ==================")
        print(f"  {'paire':>9} | {'%up':>4} {'%dn':>4} {'%chop':>5} | {'RIDE bps':>9} {'nep':>4} {'win%':>4} | {'buy&hold':>9} | ride vs b&h")
        for p in PAIRS:
            rows = SESS.get(p, [])
            if not rows:
                continue
            tot_ride = 0.0; tot_ep = 0; wins = []; bh = 0.0
            up = dn = ch = wsum = 0
            for ts, mid, G, gmid, gd8 in rows:
                st, warm = classify(G, gd8, P, taup)
                rp, nep, winp = ride_pnl(G, gmid, st)
                tot_ride += rp; tot_ep += nep
                if nep:
                    wins.append(winp * nep)
                bh += (gmid[-1] - gmid[0]) / gmid[0] * 1e4
                w = warm.sum()
                if w:
                    up += int((st[warm] == 1).sum()); dn += int((st[warm] == -1).sum()); ch += int((st[warm] == 0).sum()); wsum += w
            if wsum == 0:
                continue
            wintot = (sum(wins) / tot_ep) if tot_ep else 0.0
            cmp = "RIDE>b&h" if tot_ride > bh else ("~" if abs(tot_ride - bh) < 50 else "b&h>RIDE")
            print(f"  {p:>9} | {100*up/wsum:>3.0f}% {100*dn/wsum:>3.0f}% {100*ch/wsum:>4.0f}% | {tot_ride:>+9.0f} {tot_ep:>4} {wintot:>3.0f}% | {bh:>+9.0f} | {cmp}")
        print()

    # ---- continuation par etat (config principale) : 'trendable' predit-il la suite ? PAS choppy ? ----
    P0, tp0 = CONFIGS[0]
    print(f"--- CONTINUATION par etat (P={P0:.0f}, taup={tp0:.2f}) : E[ retour fwd DANS le sens du trend ] par etat & horizon ---")
    print(f"    (trend doit etre POSITIF=continue ; choppy doit etre ~0 ou negatif=reverte). hit = P(continue).")
    print(f"  {'paire':>9} | " + " ".join(f"{'trend@'+h:>13} {'chop@'+h:>12}" for h in HCONT))
    for p in PAIRS:
        rows = SESS.get(p, [])
        if not rows:
            continue
        acc = {h: {"tr": [], "ch": []} for h in HCONT}
        for ts, mid, G, gmid, gd8 in rows:
            st, warm = classify(G, gd8, P0, tp0)
            for h, H in HCONT.items():
                jf = np.clip(np.searchsorted(G, G + H, side="left"), 0, len(G) - 1)
                okf = (G[jf] - G) <= (H + GAP)
                fwd = np.where(okf, (gmid[jf] - gmid) / gmid * 1e4, np.nan)
                for i in range(len(G)):
                    if not warm[i] or np.isnan(fwd[i]):
                        continue
                    if st[i] != 0:
                        acc[h]["tr"].append(st[i] * fwd[i])          # capture dans le sens du trend
                    else:
                        acc[h]["ch"].append(fwd[i])
        cells = []
        for h in HCONT:
            tr = np.array(acc[h]["tr"]); chk = np.array(acc[h]["ch"])
            ts_ = f"{np.mean(tr):>+6.0f}({100*np.mean(tr>0):>2.0f}%)" if len(tr) >= 5 else f"{'n<5':>9}"
            cs_ = f"{np.mean(chk):>+5.0f}" if len(chk) >= 5 else f"{'n<5':>6}"
            cells.append(f"{ts_:>13} {cs_:>12}")
        print(f"  {p:>9} | " + " ".join(cells))

    print("\n  LECTURE :")
    print("   - RIDE bps >> 0 sur HYPE (et >= buy&hold) = le ride capte le trend ; sur majors ~0 + peu d'episodes = ils restent en FADE (correct).")
    print("   - continuation 'trend@' POSITIF + hit>50% = trendable predit la suite (le ride paie) ; 'chop@' ~0/negatif = choppy reverte (le fade paie).")
    print("   - si un major a un %up/%dn eleve MAIS ride<0/continuation faible = faux-positif 'trendable' -> monter taup (persistance) pour rester PAS-choppy.")


if __name__ == "__main__":
    main()
