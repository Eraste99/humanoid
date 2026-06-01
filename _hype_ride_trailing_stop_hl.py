"""EXIT du sleeve HYPE-RIDE = TRAILING STOP (ratchet : monte avec le prix). On NE FIXE PAS de sortie temporelle.
Stop CONTRE-PRODUCTIF pour le fade (l'excursion adverse=l'edge) MAIS BON pour le ride (couper un trend qui se retourne).
Le payoff (protection crash) est NON-testable sur mai (HYPE up-only) ; on mesure ce qui EST testable :
  (1) PROFONDEUR DES PULLBACKS de HYPE en uptrend -> largeur MINIMALE du trailing (sinon whipsaw sur replis normaux).
  (2) CAPTURE vs buy&hold par largeur de trailing -> la "prime d'assurance" (combien le stop coute en uptrend).
Sim sur grille 5min (lisse les meches). 2 variantes : sans re-entree (borne pessimiste) / avec re-entree au nouveau plus-haut.
HL true-mid, causal. Le bon choix final de largeur exigera un BEAR HYPE (data fraiche) — ici on borne le cout + la largeur saine.
"""
import os, sys
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import PAIRS, WEEKS, MIN

DAY = 24 * 60 * MIN; SAMPLE_DT = 5 * MIN
WIDTHS = [5.0, 8.0, 10.0, 12.0, 15.0, 20.0]   # largeurs de trailing testees (% sous le pic)
COSTRT = 0.0006                                # 6 bps RT taker par stop-out (negligeable sur des jours de hold)


def grid_mid(ts, mid):
    G = np.arange(ts[0], ts[-1], SAMPLE_DT)
    gi = np.clip(np.searchsorted(ts, G, side="left"), 0, len(ts) - 1)
    return G, mid[gi]


def pullback_depths(gm):
    """profondeurs (%) des replis entre plus-hauts successifs (un repli 'ferme' = recovery a un nouveau plus-haut)."""
    peak = gm[0]; trough = gm[0]; deps = []
    for p in gm[1:]:
        if p > peak:
            if trough < peak:
                deps.append((peak - trough) / peak * 100.0)
            peak = p; trough = p
        else:
            trough = min(trough, p)
    final_dd = (peak - trough) / peak * 100.0 if trough < peak else 0.0   # repli ouvert en fin (peut etre un vrai retournement)
    return np.array(deps), final_dd


def ride_trailing(gm, W, reenter):
    """buy&hold avec trailing stop a W% sous le pic. reenter=True -> re-entre au franchissement d'un nouveau plus-haut.
    retourne (rendement total %, n stop-outs)."""
    eq = 1.0; inpos = True; entry = gm[0]; peak = gm[0]; last_exit_peak = None; nstop = 0
    for p in gm[1:]:
        if inpos:
            if p > peak:
                peak = p
            if p <= peak * (1 - W / 100.0):
                eq *= (p / entry) * (1 - COSTRT); inpos = False; last_exit_peak = peak; nstop += 1
        else:
            if reenter and last_exit_peak is not None and p > last_exit_peak:
                inpos = True; entry = p; peak = p
    if inpos:
        eq *= (gm[-1] / entry)
    return (eq - 1.0) * 100.0, nstop


def main():
    print("=== HYPE-RIDE : TRAILING STOP (ratchet). (1) profondeur pullbacks -> largeur min ; (2) capture vs buy&hold -> prime ===")
    print(f"    grille {SAMPLE_DT//MIN}min (lisse meches) ; largeurs {WIDTHS} % ; cout {COSTRT*1e4:.0f}bps/stop-out. mai = up-only (payoff crash NON testable).\n")
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
            G, gm = grid_mid(ts, mid)
            rows.append((wk, gm))
        SESS[p] = rows

    # ---- 1. PROFONDEUR DES PULLBACKS (tous actifs pour contraste ; HYPE doit etre le + profond -> stop + large) ----
    print("--- 1. PROFONDEUR DES PULLBACKS en uptrend (%). HYPE = replis profonds -> trailing DOIT etre plus large que ~p90 ---")
    print(f"  {'actif':>9} | {'n repl':>6} {'med':>5} {'p75':>5} {'p90':>5} {'p95':>5} {'max':>5} | {'repli final (poss. retournmt)':>30}")
    for p in PAIRS:
        alld = []; finals = []
        for wk, gm in SESS.get(p, []):
            d, fdd = pullback_depths(gm); alld.append(d); finals.append(fdd)
        if not alld:
            continue
        d = np.concatenate(alld)
        if len(d) < 3:
            continue
        fmax = max(finals) if finals else 0.0
        print(f"  {p:>9} | {len(d):>6} {np.median(d):>5.1f} {np.percentile(d,75):>5.1f} {np.percentile(d,90):>5.1f} {np.percentile(d,95):>5.1f} {np.max(d):>5.1f} | {fmax:>29.1f}%")

    # ---- 2. CAPTURE HYPE : trailing stop vs buy&hold, par largeur (la prime d'assurance en uptrend) ----
    print(f"\n--- 2. HYPE — CAPTURE du trailing stop vs buy&hold, par largeur (re-entree au nouveau plus-haut). 100% = aussi bon que hold ---")
    rows = SESS.get("HYPEUSDC", [])
    if rows:
        bh_tot = 0.0
        print(f"  {'session':>8} | {'buy&hold%':>9} | " + " ".join(f"{'W'+str(int(w)):>13}" for w in WIDTHS))
        per_sess = []
        for wk, gm in rows:
            bh = (gm[-1] / gm[0] - 1) * 100.0; bh_tot += bh
            cells = []
            sess_caps = []
            for W in WIDTHS:
                r, ns = ride_trailing(gm, W, reenter=True)
                cap = (r / bh * 100.0) if abs(bh) > 1e-9 else float('nan')
                sess_caps.append((W, r, ns, cap))
                cells.append(f"{r:>+6.0f}({cap:>3.0f}%,{ns})")
            per_sess.append((wk, bh, sess_caps))
            print(f"  {wk:>8} | {bh:>+9.0f} | " + " ".join(f"{c:>13}" for c in cells))
        # agrege : somme des rendements (approx d'un capital remis a flot par session) + capture moyenne ponderee
        print(f"  {'TOTAL':>8} | {bh_tot:>+9.0f} |", end=" ")
        agg = []
        for j, W in enumerate(WIDTHS):
            rtot = sum(s[2][j][1] for s in per_sess); cap = (rtot / bh_tot * 100.0) if abs(bh_tot) > 1e-9 else float('nan')
            nstot = sum(s[2][j][2] for s in per_sess)
            agg.append((W, rtot, cap, nstot))
            print(f"{rtot:>+6.0f}({cap:>3.0f}%,{nstot})".rjust(13), end=" ")
        print()

        # variante pessimiste : SANS re-entree (borne basse de capture)
        print(f"\n  (borne pessimiste — SANS re-entree apres le 1er stop) :")
        print(f"  {'session':>8} | {'buy&hold%':>9} | " + " ".join(f"{'W'+str(int(w)):>13}" for w in WIDTHS))
        for wk, gm in rows:
            bh = (gm[-1] / gm[0] - 1) * 100.0
            cells = []
            for W in WIDTHS:
                r, ns = ride_trailing(gm, W, reenter=False)
                cap = (r / bh * 100.0) if abs(bh) > 1e-9 else float('nan')
                cells.append(f"{r:>+6.0f}({cap:>3.0f}%)")
            print(f"  {wk:>8} | {bh:>+9.0f} | " + " ".join(f"{c:>13}" for c in cells))

    print("\n  LECTURE :")
    print("   (1) largeur min du trailing ~ p90 des pullbacks HYPE (sinon il saute sur des replis normaux). HYPE >> majors = stop large requis.")
    print("   (2) si une largeur >= p90 garde ~85-100% du buy&hold avec peu de stop-outs -> prime FAIBLE = approche saine.")
    print("       si meme une largeur large se fait hacher (capture << 100%, n stop-outs eleve) -> HYPE trop volatil pour un trailing serre.")
    print("   RAPPEL : le BENEFICE (couper un vrai crash) reste NON mesure ici (mai up-only) — il faut un bear HYPE pour valider le payoff.")


if __name__ == "__main__":
    main()
