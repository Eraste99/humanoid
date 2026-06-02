"""SÉLECTEUR DE PAIRES = score de RÉVERSION-30min roulant, PAR ACTIF, CAUSAL — calculé SUR LES JAMBES (tick-level, ZÉRO grille).
Idée user : le fade marche sur une paire SEULEMENT si elle reverte à SON horizon de hold (30min). ETH reverte à 8h, pas 30min -> écarté.
Au lieu d'exclure ETH en dur : on mesure la réversion-30min roulante de CHAQUE paire et on ne fade que celles qui revertent -> dynamique.
VALIDATION (intraday, PAS semaine-contre-semaine — pour la PUISSANCE) :
  1. SÉLECTION transversale : moyenne réversion-30min par actif (BTC/SOL/XRP haut vs ETH bas) -> sépare proprement ?
  2. PRÉDICTIVITÉ INTRADAY : le score sur une fenêtre glissante de QUELQUES HEURES prédit-il le fade des jambes SUIVANTES ?
     -> à chaque jambe i : score_i = E[-d·fwd30] sur les jambes des [WBACK..30min] AVANT i (forward déjà observé = causal) ;
        outcome_i = -d_i·fwd30_i (le fade de la jambe i, dans le FUTUR). On poole TOUTES les jambes -> centaines de points.
  3. SÉLECTEUR EN ACTION : si on ne fade que quand score_roulant>0, l'EV-fade s'améliore-t-elle ?
réversion = E[-d·fwd] : >0 = reverte (fade gagne), <0 = continue (fade perd). Fenêtre glissante < gap inter-semaine -> pas de contamination.
HL true-mid, tick-level, ALL jambes, causal, ZÉRO sampling.
"""
import os, sys
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import leg_chain, drift_over, WEEKS, MACRO, MIN, GAP

H30 = 30 * MIN; HOUR = 60 * MIN
MAJ = ("BTCUSDC", "ETHUSDC", "SOLUSDC", "XRPUSDC")
WBACKS = [6 * HOUR, 12 * HOUR, 24 * HOUR]   # fenêtres glissantes INTRADAY
MINLEG = 4                                   # min de jambes (forward observé) dans la fenêtre pour scorer


def fwd_at(ts, mid, Bs, H):
    n = len(ts); jf = np.searchsorted(ts, ts[Bs] + H, side="left"); ok = jf < n
    f = np.full(len(Bs), np.nan); f[ok] = (mid[jf[ok]] - mid[Bs[ok]]) / mid[Bs[ok]] * 1e4
    tf = np.zeros(len(Bs), bool); tf[ok] = (ts[jf[ok]] - ts[Bs[ok]]) > (H + GAP); f[tf] = np.nan
    return f


def collect_legs(pair):
    """TOUTES les jambes 50bps (tick-level) : (t, d, fwd30, gated). gated = entrée fade régime drift_4h."""
    T = []; D = []; F = []; G = []
    for wk, ddir in WEEKS.items():
        try:
            ticks, trades, l2 = load_all(pair, ddir); ts, mid, _ = vectorized_features(ticks, trades, l2)
        except Exception:
            continue
        ts = np.asarray(ts, np.int64); mid = np.asarray(mid, np.float64)
        Bs, ds = leg_chain(ts, mid, 50.0)
        if len(Bs) < 6:
            continue
        f30 = fwd_at(ts, mid, Bs, H30); dmac = drift_over(ts, mid, MACRO)[Bs]; d1 = drift_over(ts, mid, HOUR)[Bs]
        ok = ~np.isnan(dmac); q1, q2 = (np.quantile(dmac[ok], [1/3, 2/3]) if ok.sum() >= 15 else (np.nan, np.nan))
        bm = ok & (dmac >= q2) & ~np.isnan(d1); m1 = np.median(d1[bm]) if bm.sum() else 0.0
        for k in range(len(Bs)):
            d = float(ds[k]); g = False
            if not np.isnan(dmac[k]) and not np.isnan(q1):
                if dmac[k] <= q1 and d == -1.0:
                    g = True
                elif dmac[k] >= q2 and d == 1.0 and not np.isnan(d1[k]) and d1[k] >= m1:
                    g = True
            T.append(int(ts[Bs[k]])); D.append(d); F.append(f30[k]); G.append(g)
    o = np.argsort(T)
    return np.array(T)[o], np.array(D)[o], np.array(F)[o], np.array(G)[o]


def rolling_scores(t, d, f, wback):
    """score_i = E[-d·fwd30] sur jambes des [t_i-wback, t_i-30min] (forward observé = causal). nan si < MINLEG."""
    fade = -d * f                                   # PnL fade par jambe
    n = len(t); sc = np.full(n, np.nan)
    lo = np.searchsorted(t, t - wback, side="left")
    hi = np.searchsorted(t, t - H30, side="right")  # jambes dont le forward 30min est observé à t_i
    for i in range(n):
        w = fade[lo[i]:hi[i]]; w = w[~np.isnan(w)]
        if len(w) >= MINLEG:
            sc[i] = np.mean(w)
    return sc


def main():
    print("=== SÉLECTEUR RÉVERSION-30min roulant PAR ACTIF (leg-level, tick, causal, ZÉRO grille) — validation INTRADAY ===\n")
    L = {p: collect_legs(p) for p in MAJ}

    # 1. SÉLECTION transversale (tick-level, toutes jambes) : BTC/SOL/XRP haut vs ETH bas ?
    print("--- 1. SÉLECTION transversale : réversion-30min moyenne par actif (E[-d·fwd30]). >0=fade-friendly ---")
    print(f"  {'actif':>9} | {'n jambes':>8} | {'rev30 (all)':>11} {'%fade>0':>7} | {'rev30 (gated)':>13}")
    for p in MAJ:
        t, d, f, g = L[p]; fade = -d * f; m = ~np.isnan(fade)
        fg = fade[g & m]
        print(f"  {p:>9} | {int(m.sum()):>8} | {np.mean(fade[m]):>+11.1f} {100*np.mean(fade[m]>0):>6.0f}% | {np.mean(fg) if len(fg) else float('nan'):>+13.1f}")

    # 2. PRÉDICTIVITÉ INTRADAY : score roulant (passé) -> outcome fade (futur), pooled, par fenêtre
    print(f"\n--- 2. PRÉDICTIVITÉ INTRADAY : score réversion roulant (passé, causal) -> fade de la jambe SUIVANTE. pooled toutes paires ---")
    print(f"  (si tercile HAUT du score -> outcome futur > tercile BAS, le score roulant PRÉDIT -> sélecteur valide & adaptatif)")
    for wb in WBACKS:
        SC = []; OUT = []
        for p in MAJ:
            t, d, f, g = L[p]; sc = rolling_scores(t, d, f, wb); out = -d * f
            ok = ~np.isnan(sc) & ~np.isnan(out)
            SC.append(sc[ok]); OUT.append(out[ok])
        SC = np.concatenate(SC); OUT = np.concatenate(OUT)
        if len(SC) < 30:
            print(f"  WBACK={wb//HOUR:>2}h | n={len(SC)} (trop peu)")
            continue
        q1, q2 = np.quantile(SC, [1/3, 2/3])
        lo = OUT[SC <= q1]; mid = OUT[(SC > q1) & (SC < q2)]; hi = OUT[SC >= q2]
        cov = len(SC)
        print(f"  WBACK={wb//HOUR:>2}h | n={cov:>4} | tercile BAS {np.mean(lo):>+6.1f} | MID {np.mean(mid):>+6.1f} | HAUT {np.mean(hi):>+6.1f} | "
              f"spread(H-B) {np.mean(hi)-np.mean(lo):>+6.1f} | corr {np.corrcoef(SC, OUT)[0,1]:>+.2f}")

    # 3. SÉLECTEUR EN ACTION : ne fader que si score roulant > 0 (sur jambes GATÉES = vrai fade)
    print(f"\n--- 3. SÉLECTEUR EN ACTION (WBACK=12h) : EV-fade des jambes GATÉES selon le score roulant — gating améliore ? ---")
    print(f"  {'actif':>9} | {'gated all':>10} | {'score>0':>16} | {'score<=0':>16}")
    wb = 12 * HOUR
    for p in MAJ:
        t, d, f, g = L[p]; sc = rolling_scores(t, d, f, wb); out = -d * f
        m = g & ~np.isnan(out) & ~np.isnan(sc)
        if m.sum() < 8:
            print(f"  {p:>9} | (n<8)")
            continue
        base = out[m]; pos = out[m & (sc > 0)]; neg = out[m & (sc <= 0)]
        ps = f"{np.mean(pos):>+6.1f} (n={len(pos):>3})" if len(pos) >= 3 else f"{'n<3':>16}"
        ns = f"{np.mean(neg):>+6.1f} (n={len(neg):>3})" if len(neg) >= 3 else f"{'n<3':>16}"
        print(f"  {p:>9} | {np.mean(base):>+6.1f}(n={len(base):>3}) | {ps:>16} | {ns:>16}")

    print("\n  LECTURE :")
    print("   (1) BTC/SOL/XRP rev30>>0, ETH ~bas -> la sélection transversale tient (tick-level, confirme l'étude ETH).")
    print("   (2) si spread(HAUT-BAS)>0 et corr>0 sur des n de centaines -> le score roulant PRÉDIT le fade futur INTRADAY = sélecteur causal valide.")
    print("       si spread~0 -> la réversion-30min n'est pas auto-prédictive intraday (c'est un trait LENT de la paire, pas un état qui tourne vite).")
    print("   (3) si 'score>0' bat 'gated all' et 'score<=0' -> gater le fade par le score roulant améliore l'EV en live.")


if __name__ == "__main__":
    main()
