"""SCORE DE TRENDABILITE ROULANT PAR ACTIF (le vrai classificateur/sentinelle de l'aiguilleur, demande user).
Au niveau ACTIF (pas par-moment) et base CONTINUATION (pas percentile relatif qui flag tout le monde pareil) :
  score(D) = sur une fenetre glissante PASSEE, est-ce que le drift_8h de CETTE paire a CONTINUE (trend) ou REVERTE (chop) ?
            = moyenne de [ sign(drift_8h(t)) * retour_forward_H(t) ] sur t dans [D-H-Wscore, D-H]  (forward DEJA observe = causal).
Regle d'aiguillage : score>0 (+ hit>50%) -> TREND -> HOLD/ride ; score<0 -> CHOP -> FADE. Time-varying => SENTINELLE (bascule un major en hold s'il se met a continuer).
On VALIDE : (1) separation HYPE(>0) vs majors(<0) nette ? (2) le score PASSE PREDIT-il la continuation FUTURE (sinon sentinelle inutile) ?
La fenetre de score [D-H-W, D-H] et l'outcome futur [D, D+H] NE SE CHEVAUCHENT PAS (gap H) => zero leakage. HL true-mid, causal, zero sampling.
En LIVE : un flag de regime par actif, recalcule en continu = ce que le user surveille pour savoir qui fader / qui tenir.
"""
import os, sys, datetime
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import drift_over, PAIRS, WEEKS, MIN, GAP

W8 = 8 * 60 * MIN; DAY = 24 * 60 * MIN
SAMPLE_DT = 5 * MIN
H = W8                      # horizon de continuation (= le drift_8h)
WSCORE = 2 * DAY            # fenetre glissante du score
MINW = 100                 # min d'echantillons valides dans la fenetre pour decider


def grid_series(ts, mid):
    d8full = drift_over(ts, mid, W8)
    G = np.arange(ts[0], ts[-1], SAMPLE_DT)
    gi = np.clip(np.searchsorted(ts, G, side="left"), 0, len(ts) - 1)
    gd8 = d8full[gi]; gmid = mid[gi]; gok = ~np.isnan(gd8)
    return G[gok], gmid[gok], gd8[gok]


def compute(ts, mid):
    """retourne par point de decision D : (temps, score, hit, ride_ret_futur). causal, fenetre/outcome disjoints."""
    G, gmid, gd8 = grid_series(ts, mid)
    n = len(G)
    if n < MINW + 20:
        return None
    jf = np.clip(np.searchsorted(G, G + H, side="left"), 0, n - 1)
    okf = (G[jf] - G) <= (H + GAP)
    fwd = np.where(okf, (gmid[jf] - gmid) / gmid * 1e4, np.nan)
    ride = np.sign(gd8) * fwd                       # PnL de rider la direction 8h courante sur H
    loW = np.searchsorted(G, G - H - WSCORE, side="left")   # debut fenetre passee
    hiW = np.searchsorted(G, G - H, side="left")            # fin fenetre passee (outcome deja connu)
    T = []; SC = []; HT = []; FR = []
    for D in range(n):
        a, b = loW[D], hiW[D]
        if b - a < MINW:
            continue
        w = ride[a:b]; w = w[~np.isnan(w)]
        if len(w) < MINW:
            continue
        T.append(G[D]); SC.append(float(np.mean(w))); HT.append(float(np.mean(w > 0))); FR.append(ride[D])
    if not T:
        return None
    return np.array(T), np.array(SC), np.array(HT), np.array(FR)


def main():
    print("=== SCORE DE TRENDABILITE ROULANT PAR ACTIF (continuation, causal) — classificateur/sentinelle de l'aiguilleur ===")
    print(f"    score = moyenne[ sign(d8)*fwd_{H//(60*MIN)}h ] sur fenetre glissante {WSCORE//DAY}j ; fenetre & outcome disjoints (gap {H//(60*MIN)}h). HL true-mid.\n")
    D = {}
    for p in PAIRS:
        chunks = []
        for wk, dd in WEEKS.items():
            try:
                ti, tr, l2 = load_all(p, dd); ts, mid, _ = vectorized_features(ti, tr, l2)
            except Exception:
                continue
            ts = np.asarray(ts, np.int64); mid = np.asarray(mid, np.float64)
            if len(ts) < 100:
                continue
            r = compute(ts, mid)
            if r is not None:
                chunks.append(r)
        D[p] = chunks

    # ---- 1. SEPARATION : HYPE doit etre score>0 (trend) ; majors score<0 (revert). Le GAP = la puissance du classificateur ----
    print("--- 1. SEPARATION par actif : score moyen, %temps score>0 (=verdict TREND), hit moyen. HYPE>0 / majors<0 attendu ---")
    print(f"  {'actif':>9} | {'score moy':>9} {'%temps>0':>8} {'hit moy':>7} {'n dec':>6} | verdict dominant")
    for p in PAIRS:
        ch = D.get(p, [])
        if not ch:
            continue
        sc = np.concatenate([c[1] for c in ch]); ht = np.concatenate([c[2] for c in ch])
        verdict = "TREND (hold)" if np.mean(sc) > 0 else "CHOP (fade)"
        print(f"  {p:>9} | {np.mean(sc):>+9.1f} {100*np.mean(sc>0):>7.0f}% {np.mean(ht):>7.2f} {len(sc):>6} | {verdict}")

    # ---- 2. OBSERVABILITE : evolution JOUR PAR JOUR du regime de chaque paire (le dashboard de surveillance) ----
    print(f"\n--- 2. EVOLUTION du regime PAR JOUR (ce que tu surveilles) : score moyen/jour/actif. 'T'=trend(>0) 'f'=fade(<0) ---")
    daymap = {}   # day -> {pair: score}
    for p in PAIRS:
        for c in D.get(p, []):
            t, sc = c[0], c[1]
            dk = (t // DAY)
            for d, s in zip(dk, sc):
                daymap.setdefault(int(d), {}).setdefault(p, []).append(s)
    days = sorted(daymap)
    print(f"    {'jour':>6} | " + " ".join(f"{p[:4]:>8}" for p in PAIRS))
    for d in days:
        cells = []
        for p in PAIRS:
            vals = daymap[d].get(p)
            if vals:
                m = np.mean(vals); cells.append(f"{m:>+6.0f}{'T' if m>0 else 'f'}")
            else:
                cells.append(f"{'.':>8}")
        dt = datetime.datetime.utcfromtimestamp(d * DAY / 1000).strftime('%m-%d')
        print(f"    {dt:>6} | " + " ".join(f"{c:>8}" for c in cells))

    # ---- 3. VALIDATION : le score PASSE predit-il la continuation FUTURE ? (sinon la sentinelle ne vaut rien) ----
    print(f"\n--- 3. VALIDATION sentinelle : le score PASSE predit-il l'outcome FUTUR (ride_ret sur [D, D+{H//(60*MIN)}h]) ? ---")
    print(f"    score>0 -> fwd ride doit etre POSITIF (continue, le hold paie) ; score<0 -> NEGATIF (reverte, donc FADER paie). disjoints=causal.")
    print(f"  {'actif':>9} | {'fwd|score>0':>12} {'n':>5} | {'fwd|score<0':>12} {'n':>5} | predictif ?")
    allp = {"pos": [], "neg": []}
    for p in PAIRS:
        ch = D.get(p, [])
        if not ch:
            continue
        sc = np.concatenate([c[1] for c in ch]); fr = np.concatenate([c[3] for c in ch])
        ok = ~np.isnan(fr); sc, fr = sc[ok], fr[ok]
        if len(sc) < 10:
            continue
        pos = fr[sc > 0]; neg = fr[sc < 0]
        allp["pos"].append(pos); allp["neg"].append(neg)
        ps = f"{np.mean(pos):>+12.0f} {len(pos):>5}" if len(pos) >= 5 else f"{'n<5':>12} {len(pos):>5}"
        ns = f"{np.mean(neg):>+12.0f} {len(neg):>5}" if len(neg) >= 5 else f"{'n<5':>12} {len(neg):>5}"
        good = (len(pos) >= 5 and np.mean(pos) > 0) and (len(neg) >= 5 and np.mean(neg) < 0)
        flag = "OUI" if good else ("partiel" if (len(pos) >= 5 and np.mean(pos) > 0) or (len(neg) >= 5 and np.mean(neg) < 0) else "non")
        print(f"  {p:>9} | {ps} | {ns} | {flag}")
    if allp["pos"] and allp["neg"]:
        P = np.concatenate(allp["pos"]); N = np.concatenate(allp["neg"])
        print(f"  {'POOL':>9} | {np.mean(P):>+12.0f} {len(P):>5} | {np.mean(N):>+12.0f} {len(N):>5} |")

    print("\n  LECTURE :")
    print("   (1) HYPE score>0 + majors score<0, avec un GAP net = le classificateur asset-level discrimine (ce que le percentile relatif ratait).")
    print("   (2) evolution : HYPE reste 'T' ; majors restent 'f'. Une bascule d'un major vers 'T' = la sentinelle qui dit 'celui-la se met a trender, tiens-le'.")
    print("   (3) si fwd|score>0 POSITIF & fwd|score<0 NEGATIF -> le score passe PREDIT la suite => sentinelle FIABLE (aiguillage causal valide).")


if __name__ == "__main__":
    main()
