"""MUR #2 — la QUEUE et le STOP du fade-extension bear (le setup deployable). HL in-sample, granularite minute.

hold-2h sans stop est +EV in-sample MAIS sans stop = un bear non-reverse = perte tenue jusqu'a 2h. On stresse la
QUEUE : worst trade, pire cluster de 10 trades consecutifs, max drawdown de l'equity, p5. Et on teste si un STOP
LARGE (SL -20/-30/-40 bps, coupe en TAKER car urgent) reduit la queue SANS tuer l'EV (un stop trop serre couperait
la mean-reversion au plus bas, juste avant le rebond). granularite minute (stop verifie sur le chemin mid, pas a la ms).
Setup = fade, post 'best' (fill via vrai carnet L2), regime bear, sortie maker RT3 (sauf stop=taker RT6). HYPE = controle.
"""
import os, sys, time
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _fade_maker_fill_hl import (leg_chain, drift_over, detect_fill, post_price,
                                 GAP, MACRO, CAP, FEE_MK, FEE_TK, PAIRS, WEEKS)

SLS = [None, 20.0, 30.0, 40.0]
POST = "best"


def exit_stop(ts, mid, t_fill, P, pos, sl):
    i0 = np.searchsorted(ts, t_fill, side="left")
    if i0 >= len(ts):
        return None
    t0 = ts[i0]; jend = np.searchsorted(ts, t0 + CAP, side="right")
    seg_ts = ts[i0:jend]; seg = mid[i0:jend]
    if len(seg) < 1:
        return None
    g = np.where(np.diff(np.concatenate([[t0], seg_ts])) > GAP)[0]
    if len(g):
        seg = seg[:g[0]] if g[0] > 0 else seg[:1]
    if len(seg) < 1:
        return None
    fav = pos * (seg - P) / P * 1e4
    if sl is not None:
        slh = np.where(fav <= -sl)[0]
        if len(slh):
            return float(-sl) - FEE_TK
    return float(fav[-1]) - FEE_MK


def cellstats(rows):
    rows = sorted(rows, key=lambda x: x[0])
    nets = np.array([r[1] for r in rows], dtype=float)
    n = len(nets)
    if n == 0:
        return None
    mean = float(np.mean(nets)); win = 100.0 * np.mean(nets > 0)
    worst1 = float(np.min(nets))
    if n >= 10:
        csum = np.convolve(nets, np.ones(10), "valid")
        worst10 = float(np.min(csum))
    else:
        worst10 = float('nan')
    eq = np.cumsum(nets); maxdd = float(np.max(np.maximum.accumulate(eq) - eq))
    p5 = float(np.percentile(nets, 5))
    total = float(eq[-1])
    return n, mean, win, worst1, worst10, maxdd, p5, total


def main():
    t0 = time.time()
    print("=== MUR #2 : QUEUE + STOP du fade-extension BEAR (deployable) — HL in-sample, fill carnet, granularite minute ===")
    print("hold-2h maker RT3 ; STOP en taker RT6 ; on cherche un SL qui coupe la queue SANS tuer l'EV.\n")
    acc = {}   # (pair, sl) -> list of (t_fill, net)
    for pair in PAIRS:
        wk_data = []; dmacs = []
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
            Bs, ds = leg_chain(ts, mid)
            if len(Bs) < 10:
                continue
            dmac = drift_over(ts, mid, MACRO)[Bs]
            wk_data.append((ts, mid, Bs, ds, dmac, l2_ts, b1p, a1p)); dmacs.append(dmac)
        if not wk_data:
            continue
        alld = np.concatenate(dmacs); v = ~np.isnan(alld)
        if v.sum() < 30:
            continue
        q1 = np.quantile(alld[v], 1 / 3)          # bear = tercile bas
        for ts, mid, Bs, ds, dmac, l2_ts, b1p, a1p in wk_data:
            for k in range(len(Bs)):
                dm = dmac[k]
                if np.isnan(dm) or dm > q1:
                    continue                      # bear only
                d = float(ds[k]); ib = int(Bs[k]); tB = ts[ib]
                jl = np.searchsorted(l2_ts, tB, side="left")
                if jl >= len(l2_ts):
                    continue
                b1 = b1p[jl]; a1 = a1p[jl]; midl = 0.5 * (b1 + a1); pos = -d
                P, is_long = post_price(pos, b1, a1, midl, POST)
                tf = detect_fill(l2_ts, b1p, a1p, tB, P, is_long)
                if tf is None:
                    continue
                for sl in SLS:
                    net = exit_stop(ts, mid, tf, P, pos, sl)
                    if net is not None:
                        acc.setdefault((pair, sl), []).append((tf, net))
        print(f"  [ok] {pair}")

    print("\n" + "=" * 110)
    print("BEAR fade (post 'best', fill carnet) : effet du STOP sur EV vs QUEUE  — net bps/trade, in-sample")
    print("=" * 110)
    print(f"  {'actif':>9} {'stop':>6} | {'n':>4} {'mean':>6} {'win%':>5} | {'worst1':>7} {'worst10':>8} {'maxDD':>7} {'p5':>6} | {'total':>7}")
    print("  " + "-" * 84)
    for pair in PAIRS:
        for sl in SLS:
            st = cellstats(acc.get((pair, sl), []))
            if st is None:
                continue
            n, mean, win, w1, w10, mdd, p5, tot = st
            sllab = "none" if sl is None else f"-{int(sl)}"
            print(f"  {pair:>9} {sllab:>6} | {n:>4} {mean:>+6.1f} {win:>5.0f} | {w1:>+7.1f} {w10:>+8.1f} {mdd:>7.0f} {p5:>+6.1f} | {tot:>+7.0f}")
        print("  " + "." * 84)
    print("\n  LECTURE : compare 'none' aux stops. Un bon stop : maxDD/worst10 baissent NETTEMENT, mean ~ inchangee.")
    print("    si le stop FAIT BAISSER mean -> il coupe des mean-reversions au plus bas (garder no-stop + sizing serre).")
    print("    worst1/worst10 = pire trade / pire serie de 10 (en bps cumules) ; maxDD = pire creux d'equity (bps).")
    print(f"\n[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
