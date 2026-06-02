"""QU'A ETH DE SPÉCIAL ? — caractérisation CAUSALE par actif, À L'ÉCHELLE DU FADE (jambe 50bps, forward multi-horizon).
Question user : ETH TREND (le fade s'y fait écraser) ou il CHOPPE (bon fade) ? Réponse mesurée, pas descriptive.
On compare BTC/ETH/SOL/XRP sur ce qui DÉTERMINE le P&L du fade :
  1. PERSISTANCE des jambes : P(jambe i+1 == jambe i). Haut = zig-zag qui DÉRIVE (trend, fade-hostile) ; bas = alterne (choppe, fade-friendly).
  2. RÉVERSION post-jambe multi-horizon : E[-d·fwd_H] (>0 = reverte/fade-friendly ; <0 = continue/trend) + P(continuation) à 5m/30m/2h/8h.
     -> ETH reverte-t-il à 8h MAIS continue à 30min (l'échelle du fade) ? = la vraie réponse, scale-dependent.
  3. VOL / QUEUE du forward 30min : std, |move| médian, worst, p5 -> ETH est-il juste plus VOLATIL/jumpy (pertes plus grosses quand on a tort) ?
  4. idem sur la POPULATION FADE GATÉE (régime drift_4h) = pertinence déployable.
HL true-mid, tick-level, causal, ZERO sampling. Pur HL (pas de Binance ici).
"""
import os, sys
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import leg_chain, drift_over, WEEKS, MACRO, MIN, GAP

HOUR = 60 * MIN
HS = {"5m": 5 * MIN, "30m": 30 * MIN, "2h": 120 * MIN, "8h": 8 * HOUR}
MAJ = ("BTCUSDC", "ETHUSDC", "SOLUSDC", "XRPUSDC")


def fwd_at(ts, mid, Bs, H):
    n = len(ts); jf = np.searchsorted(ts, ts[Bs] + H, side="left"); ok = jf < n
    f = np.full(len(Bs), np.nan); f[ok] = (mid[jf[ok]] - mid[Bs[ok]]) / mid[Bs[ok]] * 1e4
    tf = np.zeros(len(Bs), bool); tf[ok] = (ts[jf[ok]] - ts[Bs[ok]]) > (H + GAP); f[tf] = np.nan
    return f


def collect(pair):
    """retourne dict : ds (toutes jambes), fwd par horizon, persistence, + masque gate fade (régime drift_4h)."""
    DS = []; FW = {h: [] for h in HS}; GATE = []; persist_num = 0; persist_den = 0
    for wk, ddir in WEEKS.items():
        try:
            ticks, trades, l2 = load_all(pair, ddir); ts, mid, _ = vectorized_features(ticks, trades, l2)
        except Exception:
            continue
        ts = np.asarray(ts, np.int64); mid = np.asarray(mid, np.float64)
        Bs, ds = leg_chain(ts, mid, 50.0)
        if len(Bs) < 6:
            continue
        # persistance des jambes (au sein de la semaine)
        persist_num += int(np.sum(ds[1:] == ds[:-1])); persist_den += len(ds) - 1
        dmac = drift_over(ts, mid, MACRO)[Bs]; d1 = drift_over(ts, mid, HOUR)[Bs]
        ok = ~np.isnan(dmac)
        q1, q2 = (np.quantile(dmac[ok], [1 / 3, 2 / 3]) if ok.sum() >= 15 else (np.nan, np.nan))
        bm = ok & (dmac >= q2) & ~np.isnan(d1); m1 = np.median(d1[bm]) if bm.sum() else 0.0
        fwd = {h: fwd_at(ts, mid, Bs, H) for h, H in HS.items()}
        for k in range(len(Bs)):
            d = float(ds[k]); DS.append(d)
            for h in HS:
                FW[h].append(fwd[h][k])
            # gate fade : bear down-leg (d==-1, dmac<=q1) OU bull up-leg (d==+1, dmac>=q2 & accel)
            g = False
            if not np.isnan(dmac[k]) and not np.isnan(q1):
                if dmac[k] <= q1 and d == -1.0:
                    g = True
                elif dmac[k] >= q2 and d == 1.0 and not np.isnan(d1[k]) and d1[k] >= m1:
                    g = True
            GATE.append(g)
    return dict(ds=np.array(DS), fw={h: np.array(FW[h]) for h in HS}, gate=np.array(GATE),
                persist=(persist_num / persist_den if persist_den else np.nan))


def revstats(d, fwd):
    """sur jambes valides : E[-d·fwd] (réversion >0 / continuation <0), P(continuation), std, worst fade, p5 fade."""
    m = ~np.isnan(fwd) & (d != 0)
    if m.sum() < 8:
        return None
    d = d[m]; fwd = fwd[m]; fade = -d * fwd          # PnL gross du fade (avant frais)
    return dict(n=int(m.sum()), rev=float(np.mean(fade)), cont=float(np.mean(d * fwd > 0)),
                std=float(np.std(fwd)), worst=float(np.min(fade)), p5=float(np.percentile(fade, 5)),
                absmed=float(np.median(np.abs(fwd))))


def main():
    print("=== QU'A ETH DE SPÉCIAL ? caractérisation par actif À L'ÉCHELLE DU FADE (jambe 50bps, multi-horizon). HL pur, causal ===\n")
    D = {p: collect(p) for p in MAJ}

    print("--- 1. PERSISTANCE des jambes : P(jambe suivante MÊME sens). haut=zig-zag dérive (TREND) ; bas=alterne (CHOPPE) ---")
    for p in MAJ:
        print(f"  {p:>9} | persistance jambes = {D[p]['persist']:>.2f}   ({'plutôt TREND' if D[p]['persist']>0.5 else 'plutôt CHOPPE (alterne)'})")

    print(f"\n--- 2. RÉVERSION post-jambe E[-d·fwd] (>0=reverte/fade-friendly, <0=continue/TREND) par horizon — TOUTES jambes ---")
    print(f"  {'actif':>9} | " + " ".join(f"{h:>16}" for h in HS) + "   (rev bps | %cont)")
    for p in MAJ:
        cells = []
        for h in HS:
            s = revstats(D[p]['ds'], D[p]['fw'][h])
            cells.append(f"{s['rev']:>+6.1f}|{100*s['cont']:>3.0f}%" if s else f"{'n<8':>10}")
        print(f"  {p:>9} | " + " ".join(f"{c:>16}" for c in cells))

    print(f"\n--- 3. VOL / QUEUE du forward 30min (TOUTES jambes) : ETH juste plus VOLATIL/jumpy ? ---")
    print(f"  {'actif':>9} | {'std fwd30':>9} {'|move|med':>9} | {'worst fade':>10} {'p5 fade':>8}")
    for p in MAJ:
        s = revstats(D[p]['ds'], D[p]['fw']['30m'])
        if s:
            print(f"  {p:>9} | {s['std']:>9.0f} {s['absmed']:>9.0f} | {s['worst']:>+10.0f} {s['p5']:>+8.0f}")

    print(f"\n--- 4. POPULATION FADE GATÉE (régime drift_4h) — réversion 30min réelle du fade déployable ---")
    print(f"  {'actif':>9} | {'n gated':>7} | {'rev bps (30m)':>13} {'%cont':>6} {'worst':>6} {'p5':>6} {'std':>5}")
    for p in MAJ:
        g = D[p]['gate']; d = D[p]['ds']; f30 = D[p]['fw']['30m']
        m = g & ~np.isnan(f30) & (d != 0)
        if m.sum() < 8:
            print(f"  {p:>9} | {int(m.sum()):>7} | (n<8)")
            continue
        fade = -d[m] * f30[m]
        print(f"  {p:>9} | {int(m.sum()):>7} | {np.mean(fade):>+13.1f} {100*np.mean(d[m]*f30[m]>0):>5.0f}% {np.min(fade):>+6.0f} {np.percentile(fade,5):>+6.0f} {np.std(f30[m]):>5.0f}")

    print("\n  LECTURE (réponse à 'ETH trend ou choppe ?') :")
    print("   - si ETH a persistance jambes PLUS HAUTE et/ou réversion 30m PLUS BASSE/négative que les autres -> il TREND à l'échelle du fade = cause structurelle.")
    print("   - si ETH reverte à 8h mais PAS à 30m -> scale-dependent (mean-revert lent, momentum court) = il faut un autre horizon de hold pour ETH.")
    print("   - si ETH a juste std/worst PLUS GROS à réversion ÉGALE -> ce n'est pas qu'il trend, c'est qu'il est plus VOLATIL/jumpy (pertes plus lourdes quand on a tort).")
    print("   - si ETH ressemble aux autres partout -> le -106 était bien du petit-échantillon, pas une spécificité ETH.")


if __name__ == "__main__":
    main()
