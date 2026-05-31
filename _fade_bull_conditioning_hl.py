"""COMMENT BIEN UTILISER LE FADE-BULL — short rips en uptrend : conditionner le regime bull pour trouver QUAND ca marche.

Hypothese (asymetrie crypto) : uptrend qui ACCELERE (melt-up) -> rip continue -> fade perd ;
uptrend qui S'ESSOUFFLE (decelere) -> rip retrace -> fade gagne. Et : uptrend MODERE vs FORT.
Regime bull = tercile haut de drift_4h. Sous-conditions (split median DANS le bull) :
  - force : dmac (drift_4h) bas/haut  -> 'mod' vs 'strong'
  - recent: d1 (drift_1h) bas/haut    -> 'decel' (steam qui tombe) vs 'accel' (melt-up)
fade = short l'up-leg, hold 30min, sortie maker RT3. net/trade + win% + n, par paire (+ majors pooles).
bear_all en reference (doit etre +). HL vrai mid, tick-level, ZERO sampling. mid-proxy (fill~mid valide).
"""
import os, sys, time
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import leg_chain, drift_over, PAIRS, WEEKS, MACRO, MIN, GAP

HOUR = 60 * MIN
FEE_MK = 3.0
HOLD = 30 * MIN


def fwd_at(ts, mid, Bs, H):
    n = len(ts)
    jf = np.searchsorted(ts, ts[Bs] + H, side="left")
    ok = jf < n
    f = np.full(len(Bs), np.nan)
    f[ok] = (mid[jf[ok]] - mid[Bs[ok]]) / mid[Bs[ok]] * 1e4
    tf = np.zeros(len(Bs), bool); tf[ok] = (ts[jf[ok]] - ts[Bs[ok]]) > (H + GAP)
    f[tf] = np.nan
    return f


def cond_report(label, fade_net, mask):
    m = mask & ~np.isnan(fade_net)
    n = int(m.sum())
    if n < 8:
        print(f"  {label:>26} | n={n:>4} | (insuffisant)")
        return
    a = fade_net[m]
    print(f"  {label:>26} | n={n:>4} | net {np.mean(a):>+6.1f} | win {100*np.mean(a>0):>4.0f}%")


def main():
    t0 = time.time()
    print("=== FADE-BULL CONDITIONNE — short rips : QUAND ca marche (force / acceleration de l'uptrend) — HL true-mid ===")
    print("fade=short up-leg, hold 30min, maker RT3. bull=tercile haut drift_4h ; splits median DANS le bull.\n")
    MAJ = {"fade": [], "dmac": [], "d1": []}
    for pair in PAIRS:
        FN = []; DM = []; D1 = []
        for wk, ddir in WEEKS.items():
            try:
                ticks, trades, l2 = load_all(pair, ddir)
                ts, mid, _ = vectorized_features(ticks, trades, l2)
            except Exception:
                continue
            ts = np.asarray(ts, np.int64); mid = np.asarray(mid, np.float64)
            Bs, ds = leg_chain(ts, mid, 50.0)
            if len(Bs) < 5:
                continue
            dmac = drift_over(ts, mid, MACRO)[Bs]
            d1 = drift_over(ts, mid, HOUR)[Bs]
            fwd = fwd_at(ts, mid, Bs, HOLD)
            fade = -ds * fwd - FEE_MK            # short the up-leg (fade), net maker
            FN.append(fade); DM.append(dmac); D1.append(d1)
        if not FN:
            continue
        fade = np.concatenate(FN); dmac = np.concatenate(DM); d1 = np.concatenate(D1)
        ok = ~(np.isnan(dmac) | np.isnan(fade))
        if ok.sum() < 30:
            continue
        q1, q2 = np.quantile(dmac[ok], [1 / 3, 2 / 3])
        bear = ok & (dmac <= q1)
        bull = ok & (dmac >= q2)
        if bull.sum() >= 16:
            md = np.median(dmac[bull]); m1 = np.median(d1[bull & ~np.isnan(d1)]) if (bull & ~np.isnan(d1)).sum() else 0.0
        else:
            md = m1 = 0.0
        print(f"--- {pair} ---")
        cond_report("bear_all (ref)", fade, bear)
        cond_report("bull_all", fade, bull)
        cond_report("bull_mod (4h faible)", fade, bull & (dmac < md))
        cond_report("bull_strong (4h fort)", fade, bull & (dmac >= md))
        cond_report("bull_decel (1h<mediane)", fade, bull & (d1 < m1))
        cond_report("bull_accel (1h>=mediane)", fade, bull & (d1 >= m1))
        if pair != "HYPEUSDC":
            MAJ["fade"].append(fade); MAJ["dmac"].append(dmac); MAJ["d1"].append(d1)
    # majors pooled
    if MAJ["fade"]:
        fade = np.concatenate(MAJ["fade"]); dmac = np.concatenate(MAJ["dmac"]); d1 = np.concatenate(MAJ["d1"])
        ok = ~(np.isnan(dmac) | np.isnan(fade))
        q1, q2 = np.quantile(dmac[ok], [1 / 3, 2 / 3])
        bear = ok & (dmac <= q1); bull = ok & (dmac >= q2)
        md = np.median(dmac[bull]); m1 = np.median(d1[bull & ~np.isnan(d1)])
        print("--- MAJORS POOLES (BTC+ETH+SOL+XRP) ---")
        cond_report("bear_all (ref)", fade, bear)
        cond_report("bull_all", fade, bull)
        cond_report("bull_mod (4h faible)", fade, bull & (dmac < md))
        cond_report("bull_strong (4h fort)", fade, bull & (dmac >= md))
        cond_report("bull_decel (1h<mediane)", fade, bull & (d1 < m1))
        cond_report("bull_accel (1h>=mediane)", fade, bull & (d1 >= m1))
    print("\n  LECTURE : si bull_decel >> bull_accel (et > 0) -> 'shorter le rip quand l'uptrend s'essouffle' = la bonne regle.")
    print("    si bull_mod > bull_strong -> fader seulement les uptrends MODERES. si tout ~0/negatif -> fade-bull pas sauvable.")
    print(f"\n[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
