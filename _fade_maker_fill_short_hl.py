"""SIM CARNET-FILL REEL au hold COURT (15 & 30 min) — la forme EFFICIENTE survit-elle aux vrais fills maker ?
bear ET bull, PAR SEMAINE (W507 vs W516), donnees propres HL true-mid + vrai carnet L2.

On poste un bid(long)/ask(short) au 'best' ; fill detecte quand l'ask descend / le bid monte a nous (timeout 10min=miss) ;
puis sortie MAKER (RT3) a t_fill+15min et t_fill+30min (hold court, on n'est pas presse). NET/SIGNAL = fill_rate x net.
But : confirmer que le +4-11 (15min) vu en mid-proxy tient avec l'adverse-selection reelle, sur les 2 semaines, bear&bull.
Reutilise le moteur fill de _fade_maker_fill_hl. tick-level, ZERO sampling.
"""
import os, sys, time
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _fade_maker_fill_hl import (leg_chain, drift_over, detect_fill, post_price,
                                 MIN, GAP, MACRO, FEE_MK, PAIRS, WEEKS)

HOLDS = [15, 30]


def exit_at(ts, mid, t_fill, P, pos, Tmin):
    i0 = np.searchsorted(ts, t_fill, side="left")
    if i0 >= len(ts):
        return None
    j = np.searchsorted(ts, t_fill + Tmin * MIN, side="left")
    if j >= len(ts):
        j = len(ts) - 1
    sub = ts[i0:j + 1]
    if len(sub) > 1:
        g = np.where(np.diff(sub) > GAP)[0]
        if len(g):
            j = i0 + g[0]
    if j < i0:
        return None
    return pos * (mid[j] - P) / P * 1e4 - FEE_MK     # close maker RT3


def main():
    t0 = time.time()
    print("=== CARNET-FILL REEL au hold COURT (15 & 30min) — bear & bull, par semaine — HL true-mid + vrai L2 ===")
    print("post 'best', fill via carnet (miss=adverse-sel), sortie maker RT3. NET/SIGNAL = fill_rate x net.\n")
    print(f"  {'actif':>9} {'reg':>4} {'wk':>5} | {'sig':>4} {'fill%':>5} | {'net15':>6} {'w15':>4} {'sig15':>6} | {'net30':>6} {'w30':>4} {'sig30':>6}")
    print("  " + "-" * 88)
    for pair in PAIRS:
        wk_data = {}; pool = []
        for wk, ddir in WEEKS.items():
            try:
                ticks, trades, l2 = load_all(pair, ddir)
                ts, mid, _ = vectorized_features(ticks, trades, l2)
            except Exception as e:
                print(f"  [skip] {pair}/{wk}: {e}"); continue
            ts = np.asarray(ts, np.int64); mid = np.asarray(mid, np.float64)
            l2_ts = l2["ts_ms"].values.astype(np.int64)
            b1p = l2["b1p"].values.astype(np.float64); a1p = l2["a1p"].values.astype(np.float64)
            okl = (b1p > 0) & (a1p > 0) & (a1p >= b1p)
            l2_ts = l2_ts[okl]; b1p = b1p[okl]; a1p = a1p[okl]
            if len(l2_ts) < 100:
                continue
            Bs, ds = leg_chain(ts, mid, 50.0)
            if len(Bs) < 5:
                continue
            dmac = drift_over(ts, mid, MACRO)[Bs]
            wk_data[wk] = (ts, mid, Bs, ds, dmac, l2_ts, b1p, a1p); pool.append(dmac)
        if not pool:
            continue
        alld = np.concatenate(pool); v = ~np.isnan(alld)
        if v.sum() < 30:
            continue
        q1, q2 = np.quantile(alld[v], [1 / 3, 2 / 3])
        for regime in ("bear", "bull"):
            for wk in WEEKS:
                if wk not in wk_data:
                    continue
                ts, mid, Bs, ds, dmac, l2_ts, b1p, a1p = wk_data[wk]
                sig = 0; fill = 0; H = {T: [] for T in HOLDS}
                for k in range(len(Bs)):
                    dm = dmac[k]
                    if np.isnan(dm):
                        continue
                    if regime == "bear" and dm > q1:
                        continue
                    if regime == "bull" and dm < q2:
                        continue
                    sig += 1
                    ib = int(Bs[k]); pos = -float(ds[k]); tB = ts[ib]
                    jl = np.searchsorted(l2_ts, tB, side="left")
                    if jl >= len(l2_ts):
                        continue
                    b1 = b1p[jl]; a1 = a1p[jl]; midl = 0.5 * (b1 + a1)
                    P, is_long = post_price(pos, b1, a1, midl, "best")
                    tf = detect_fill(l2_ts, b1p, a1p, tB, P, is_long)
                    if tf is None:
                        continue
                    fill += 1
                    for T in HOLDS:
                        net = exit_at(ts, mid, tf, P, pos, T)
                        if net is not None:
                            H[T].append(net)
                if sig < 8:
                    continue
                fr = 100.0 * fill / sig

                def cell(T):
                    lst = H[T]
                    if not lst:
                        return float('nan'), float('nan'), float('nan')
                    a = np.array(lst)
                    return float(np.mean(a)), 100.0 * np.mean(a > 0), float(np.sum(a) / sig)
                n15, w15, ps15 = cell(15); n30, w30, ps30 = cell(30)
                print(f"  {pair:>9} {regime:>4} {wk:>5} | {sig:>4} {fr:>5.0f} | {n15:>+6.1f} {w15:>4.0f} {ps15:>+6.1f} | {n30:>+6.1f} {w30:>4.0f} {ps30:>+6.1f}")
            print("  " + "." * 88)
    print("\n  LECTURE : net15/net30 = bps/trade rempli (maker RT3, fill carnet reel). sig15/sig30 = NET/SIGNAL (miss=0).")
    print("    si bear net15>0 sur W507 ET W516 avec fill eleve -> forme EFFICIENTE survit aux vrais fills = spec solide pour 11 juin.")
    print(f"\n[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
