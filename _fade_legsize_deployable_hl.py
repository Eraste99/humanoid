"""SEUIL DE JAMBE sur le STACK DÉPLOYABLE COMPLET (fade+FLOW+Binance-lead, BTC/SOL/XRP) à 20 / 30 / 50 bps.
On avait dit de viser 20-30bps (capture totale max) mais tout le reste était calculé à 50 par inertie. On rechiffre au bon seuil :
par seuil -> n, trades/jour, EV/trade (net), win%, bps/jour (capture), worst, maxDD, ET le rendement PRUDENT/mois (sizé no-levier).
Question : 20-30bps donne-t-il + de capture totale / + de rendement que 50, AVEC les filtres, malgré une EV/trade + mince (frais 3bps) ?
HL true-mid, tick-level, ZÉRO sampling. Data préchargée une fois.
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

HOUR = 60 * MIN; FEE_MK = 3.0; HOLD = 30 * MIN; THR = 30.0; MONTHD = 30
MAJ3 = ("BTCUSDC", "SOLUSDC", "XRPUSDC"); LEGS = [20.0, 30.0, 50.0]


def fwd_at(ts, mid, Bs, H):
    n = len(ts); jf = np.searchsorted(ts, ts[Bs] + H, side="left"); ok = jf < n
    f = np.full(len(Bs), np.nan); f[ok] = (mid[jf[ok]] - mid[Bs[ok]]) / mid[Bs[ok]] * 1e4
    tf = np.zeros(len(Bs), bool); tf[ok] = (ts[jf[ok]] - ts[Bs[ok]]) > (H + GAP); f[tf] = np.nan
    return f


def deploy_stream(DATA, pair, binD, legbps):
    """fade+FLOW+Binance-lead à seuil de jambe = legbps. retourne [(t, net)]."""
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
        bs = bdrift(binD.get(BIN[pair]), tB)
        if (not np.isnan(bs)) and (np.sign(bs) == d) and (abs(bs) >= THR):
            continue
        rows.append((tB, net))
    return rows


def main():
    t0 = time.time()
    print("=== SEUIL DE JAMBE 20/30/50 bps sur le STACK DÉPLOYABLE (fade+FLOW+Binance-lead, BTC/SOL/XRP) — rechiffrage au bon seuil ===\n")
    DATA = {}
    for pair in MAJ3:
        for wk, ddir in WEEKS.items():
            try:
                ti, tr, l2 = load_all(pair, ddir); ts, mid, feats = vectorized_features(ti, tr, l2)
                DATA[(pair, wk)] = (np.asarray(ts, np.int64), np.asarray(mid, np.float64), feats)
            except Exception:
                continue
    binD = {s: load_bin(s) for s in set(BIN.values())}

    print(f"  {'jambe':>6} | {'n':>4} {'/jour':>5} | {'EV/tr':>5} {'win%':>4} | {'bps/jour':>8} (capture) | {'worst':>6} {'maxDD':>6} {'conc':>4} | {'rdt prudent/mois':>16}")
    res = {}
    for legbps in LEGS:
        book = {p: sorted(deploy_stream(DATA, p, binD, legbps)) for p in MAJ3}
        s = book_stats(book)
        if not s:
            continue
        allnet = np.concatenate([np.array([x[1] for x in book[p]]) for p in MAJ3 if book[p]])
        win = 100 * np.mean(allnet > 0)
        f = min((0.25 / 100.0) / (1.5 * abs(s['worst']) / 1e4), 1.0 / s['conc'])   # prudent : 0.25%/tr ET expo<=100%
        mois = s['bpsday'] * f / 1e4 * 100 * MONTHD
        res[legbps] = (s, mois)
        print(f"  {legbps:>5.0f}b | {s['n']:>4} {s['nday']:>5.1f} | {s['mean']:>+5.1f} {win:>3.0f}% | {s['bpsday']:>+8.0f}           | {s['worst']:>+6.0f} {s['maxdd']:>6.0f} {s['conc']:>4} | {mois:>+15.2f}%")

    print(f"\n  LECTURE :")
    print("   - 'bps/jour' = capture totale du livre/jour (EV/trade × nb trades). Si 20-30bps > 50bps -> plus de trades compense la finesse.")
    print("   - 'EV/tr' baisse forcément à seuil + bas (move + mince, frais 3bps pèsent +). Le bon seuil = celui qui maximise rdt/mois prudent.")
    print("   - si worst/maxDD explosent à 20bps -> la queue se dégrade ; arbitrer capture vs queue.")
    if res:
        best = max(res, key=lambda L: res[L][1])
        print(f"   => MEILLEUR rendement prudent/mois @ seuil = {best:.0f}bps ({res[best][1]:+.1f}%/mois). (vs 50b = {res.get(50.0,(None,float('nan')))[1]:+.1f}%).")
    print(f"\n[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
