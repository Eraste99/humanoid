"""ROLE de Binance-lead PAR ECHELLE DE JAMBE (20/30/50bps) : le filtre coupe-t-il la queue AUSSI BIEN aux petites jambes ?
Question user : "a cette echelle Binance n'intervient que tres peu ?". On compare, par seuil de jambe, le livre SANS vs AVEC
le filtre Binance-lead : n, %retire, worst, maxDD. Si a 50bps Binance coupe enormement la queue mais a 20bps a peine -> Binance
intervient surtout a grande echelle (le seuil 30bps/120s est calibre pour les gros moves ; les petites jambes ont une queue
NON-Binance, due au nombre + concurrence + qualite). HL true-mid, tick-level, ZERO sampling.
"""
import os, sys, time
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import leg_chain, drift_over, WEEKS, MACRO, MIN, GAP
from _fade_binlead_fullbook_hl import load_bin, bdrift, BIN
from _fade_sizing_binlead_hl import book_stats

HOUR = 60 * MIN; FEE_MK = 3.0; HOLD = 30 * MIN; THR = 30.0
MAJ3 = ("BTCUSDC", "SOLUSDC", "XRPUSDC"); LEGS = [20.0, 30.0, 50.0]


def fwd_at(ts, mid, Bs, H):
    n = len(ts); jf = np.searchsorted(ts, ts[Bs] + H, side="left"); ok = jf < n
    f = np.full(len(Bs), np.nan); f[ok] = (mid[jf[ok]] - mid[Bs[ok]]) / mid[Bs[ok]] * 1e4
    tf = np.zeros(len(Bs), bool); tf[ok] = (ts[jf[ok]] - ts[Bs[ok]]) > (H + GAP); f[tf] = np.nan
    return f


def stream(DATA, pair, binD, legbps, use_bin):
    wkA = {}; poolD = []; poolD1 = []
    for wk in WEEKS:
        if (pair, wk) not in DATA:
            continue
        ts, mid, feats = DATA[(pair, wk)]
        Bs, ds = leg_chain(ts, mid, legbps)
        if len(Bs) < 5:
            continue
        dmac = drift_over(ts, mid, MACRO)[Bs]; d1 = drift_over(ts, mid, HOUR)[Bs]
        tfi = np.asarray(feats.get("tfi_5min", np.full(len(ts), np.nan)), np.float64)[Bs]
        fwd = fwd_at(ts, mid, Bs, HOLD)
        wkA[wk] = (ts[Bs], ds, dmac, d1, tfi, fwd); poolD.append(dmac); poolD1.append(d1)
    if not wkA:
        return []
    dd = np.concatenate(poolD); v = ~np.isnan(dd)
    if v.sum() < 30:
        return []
    q1, q2 = np.quantile(dd[v], [1 / 3, 2 / 3])
    d1p = np.concatenate(poolD1); bp = v & (dd >= q2) & ~np.isnan(d1p); m1 = np.median(d1p[bp]) if bp.sum() else 0.0
    ent = []
    for wk, (tB, ds, dmac, d1, tfi, fwd) in wkA.items():
        for k in range(len(ds)):
            dm = dmac[k]
            if np.isnan(dm) or np.isnan(fwd[k]):
                continue
            d = float(ds[k])
            if dm <= q1:
                if d != -1.0: continue
            elif dm >= q2:
                if d != 1.0 or np.isnan(d1[k]) or d1[k] < m1: continue
            else:
                continue
            ent.append((int(tB[k]), d, tfi[k], -d * fwd[k] - FEE_MK))
    if len(ent) < 12:
        return []
    qtfi = np.nanquantile(np.abs(np.array([e[2] for e in ent])), 0.70)
    rows = []
    for tB, d, tfival, net in ent:
        if (not np.isnan(tfival)) and (np.sign(tfival) == d) and (abs(tfival) >= qtfi):
            continue
        if use_bin:
            bs = bdrift(binD.get(BIN[pair]), tB)
            if (not np.isnan(bs)) and (np.sign(bs) == d) and (abs(bs) >= THR):
                continue
        rows.append((tB, net))
    return rows


def main():
    t0 = time.time()
    print("=== ROLE de Binance-lead PAR ECHELLE (20/30/50bps) : coupe-t-il la queue aussi bien aux petites jambes ? ===\n")
    DATA = {}
    for pair in MAJ3:
        for wk, ddir in WEEKS.items():
            try:
                ti, tr, l2 = load_all(pair, ddir); ts, mid, feats = vectorized_features(ti, tr, l2)
                DATA[(pair, wk)] = (np.asarray(ts, np.int64), np.asarray(mid, np.float64), feats)
            except Exception:
                continue
    binD = {s: load_bin(s) for s in set(BIN.values())}
    print(f"  {'jambe':>6} | {'config':>10} | {'n':>4} {'%retire':>7} | {'worst':>6} {'maxDD':>6} | effet Binance sur la queue")
    for legbps in LEGS:
        bno = {p: sorted(stream(DATA, p, binD, legbps, False)) for p in MAJ3}
        byes = {p: sorted(stream(DATA, p, binD, legbps, True)) for p in MAJ3}
        sno = book_stats(bno); sy = book_stats(byes)
        if not sno or not sy:
            continue
        rem = 100 * (1 - sy['n'] / sno['n'])
        dW = sno['worst'] - sy['worst']; dDD = sno['maxdd'] - sy['maxdd']
        print(f"  {legbps:>5.0f}b | {'SANS bin':>10} | {sno['n']:>4} {'--':>7} | {sno['worst']:>+6.0f} {sno['maxdd']:>6.0f} |")
        print(f"  {legbps:>5.0f}b | {'+ Binance':>10} | {sy['n']:>4} {rem:>6.0f}% | {sy['worst']:>+6.0f} {sy['maxdd']:>6.0f} | worst {sno['worst']:>+.0f}->{sy['worst']:>+.0f} ({'+'if dW>=0 else ''}{dW:.0f}) ; maxDD -{dDD:.0f}")
        print()
    print("  LECTURE : si a 50bps Binance ameliore worst/maxDD ENORMEMENT mais a 20bps a peine -> Binance intervient surtout a")
    print("    GRANDE echelle (seuil 30bps/120s cale pour gros moves). La queue des petites jambes est NON-Binance (nombre+concurrence+qualite)")
    print("    -> on ne peut pas la filtrer par Binance ; raison de + de rester a 50bps.")
    print(f"\n[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
