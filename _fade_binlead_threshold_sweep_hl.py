"""SWEEP du SEUIL Binance-lead (baseline / 30 / 25 / 20 / 15 bps). La décompo ETH a montré que ses grosses pertes étaient
des leads Binance à 19-27bps = JUSTE SOUS le seuil 30. Question : baisser le seuil (a) coupe-t-il ces near-miss sur TOUTES
les paires SANS tuer l'EV (skip trop de bons trades) ? (b) réhabilite-t-il ETH (4-major book aussi bon que 3-major) ?
Pour chaque seuil : stream déployable (fade+FLOW+Binance-lead SAME @ seuil), stats de risque + rendement PRUDENT (no-levier).
Books : BTC/SOL/XRP et +ETH. On suit aussi le worst d'ETH directement. HL in-sample/mai. Binance = SIGNAL only.
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

HOUR = 60 * MIN; FEE_MK = 3.0; HOLD = 30 * MIN; MONTHD = 30
MAJ3 = ("BTCUSDC", "SOLUSDC", "XRPUSDC"); MAJ4 = ("BTCUSDC", "ETHUSDC", "SOLUSDC", "XRPUSDC")
THRS = [999.0, 30.0, 25.0, 20.0, 15.0]   # 999 = baseline (filtre jamais déclenché)


def fwd_at(ts, mid, Bs, H):
    n = len(ts); jf = np.searchsorted(ts, ts[Bs] + H, side="left"); ok = jf < n
    f = np.full(len(Bs), np.nan); f[ok] = (mid[jf[ok]] - mid[Bs[ok]]) / mid[Bs[ok]] * 1e4
    tf = np.zeros(len(Bs), bool); tf[ok] = (ts[jf[ok]] - ts[Bs[ok]]) > (H + GAP); f[tf] = np.nan
    return f


def precompute(pair):
    """ent = [(t, d, tfi, net)] après gate régime, qtfi ; indépendant du seuil Binance (calculé une fois)."""
    wkA = {}; poolD = []; poolD1 = []
    for wk, ddir in WEEKS.items():
        try:
            ticks, trades, l2 = load_all(pair, ddir)
            ts, mid, feats = vectorized_features(ticks, trades, l2)
        except Exception:
            continue
        ts = np.asarray(ts, np.int64); mid = np.asarray(mid, np.float64)
        Bs, ds = leg_chain(ts, mid, 50.0)
        if len(Bs) < 5:
            continue
        dmac = drift_over(ts, mid, MACRO)[Bs]; d1 = drift_over(ts, mid, HOUR)[Bs]
        tfi = np.asarray(feats.get("tfi_5min", np.full(len(ts), np.nan)), np.float64)[Bs]
        fwd = fwd_at(ts, mid, Bs, HOLD)
        wkA[wk] = (ts[Bs], ds, dmac, d1, tfi, fwd); poolD.append(dmac); poolD1.append(d1)
    if not wkA:
        return [], 0.0
    dd = np.concatenate(poolD); v = ~np.isnan(dd)
    if v.sum() < 30:
        return [], 0.0
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
                if d != -1.0:
                    continue
            elif dm >= q2:
                if d != 1.0 or np.isnan(d1[k]) or d1[k] < m1:
                    continue
            else:
                continue
            ent.append((int(tB[k]), d, tfi[k], -d * fwd[k] - FEE_MK))
    if len(ent) < 12:
        return [], 0.0
    qtfi = np.nanquantile(np.abs(np.array([e[2] for e in ent])), 0.70)
    # FLOW filter une fois ; garde (t, d, net) pour appliquer le seuil Binance après
    kept = [(t, d, net) for (t, d, tfival, net) in ent
            if not ((not np.isnan(tfival)) and (np.sign(tfival) == d) and (abs(tfival) >= qtfi))]
    return kept, qtfi


def stream_at(kept, binsame, thr):
    """applique le filtre Binance-lead SAME au seuil thr ; retourne [(t, net)]."""
    out = []
    for t, d, net in kept:
        bs = bdrift(binsame, t)
        if (not np.isnan(bs)) and (np.sign(bs) == d) and (abs(bs) >= thr):
            continue
        out.append((t, net))
    return out


def prudent(s):
    if not s:
        return 0.0, 0.0
    f = min(0.0025 / (1.5 * abs(s['worst']) / 1e4), 1.0 / s['conc'])
    return f, s['bpsday'] * f / 1e4 * 100 * MONTHD


def main():
    t0 = time.time()
    print("=== SWEEP SEUIL Binance-lead (baseline/30/25/20/15) : coupe-t-il la queue SANS tuer l'EV ? réhabilite-t-il ETH ? ===\n")
    binD = {s: load_bin(s) for s in set(BIN.values())}
    KEPT = {p: precompute(p)[0] for p in MAJ4}
    nbase = {p: len(KEPT[p]) for p in MAJ4}

    def build(pairs, thr):
        return {p: sorted(stream_at(KEPT[p], binD.get(BIN[p]), thr)) for p in pairs}

    print("--- A. LIVRE BTC/SOL/XRP par seuil — coupe la queue sans tuer l'EV ? ---")
    print(f"  {'seuil':>7} | {'n':>4} {'/j':>4} {'%gardé':>6} | {'mean':>5} {'worst':>6} {'cluster':>7} {'maxDD':>6} | {'rdt/mois prudent':>16}")
    for thr in THRS:
        b3 = build(MAJ3, thr); s = book_stats(b3)
        if not s:
            continue
        n0 = sum(nbase[p] for p in MAJ3); _, mois = prudent(s)
        lab = "baseline" if thr > 100 else f"{thr:.0f}bps"
        print(f"  {lab:>7} | {s['n']:>4} {s['nday']:>4.1f} {100*s['n']/n0:>5.0f}% | {s['mean']:>+5.1f} {s['worst']:>+6.0f} {s['worst_cluster']:>+7.0f} {s['maxdd']:>6.0f} | {mois:>+15.2f}%")

    print(f"\n--- B. LIVRE +ETH (4 majors) par seuil — un seuil plus bas réhabilite-t-il ETH ? ---")
    print(f"  {'seuil':>7} | {'n':>4} {'/j':>4} | {'mean':>5} {'worst':>6} {'ETHworst':>8} {'cluster':>7} {'maxDD':>6} | {'rdt/mois':>9} | {'vs 3-maj':>8}")
    for thr in THRS:
        b4 = build(MAJ4, thr); s = book_stats(b4)
        s3 = book_stats(build(MAJ3, thr))
        if not s or not s3:
            continue
        ethnets = [net for (t, net) in b4["ETHUSDC"]]; ethw = min(ethnets) if ethnets else float('nan')
        _, mois4 = prudent(s); _, mois3 = prudent(s3)
        lab = "baseline" if thr > 100 else f"{thr:.0f}bps"
        flag = "ETH AIDE" if mois4 > mois3 + 0.3 else ("~égal" if mois4 > mois3 - 0.3 else "3-maj mieux")
        print(f"  {lab:>7} | {s['n']:>4} {s['nday']:>4.1f} | {s['mean']:>+5.1f} {s['worst']:>+6.0f} {ethw:>+8.0f} {s['worst_cluster']:>+7.0f} {s['maxdd']:>6.0f} | {mois4:>+8.2f}% | {flag:>8}")

    print("\n  LECTURE :")
    print("   A : si en baissant le seuil le worst/cluster baissent ET le mean monte (ou tient) ET rdt/mois ne s'effondre pas")
    print("       -> le seuil bas coupe les near-miss toxiques SANS tuer l'EV. Choisir le seuil au meilleur compromis tail/rdt.")
    print("   B : si à un seuil bas le worst d'ETH se resserre et +ETH atteint/dépasse le 3-major -> ETH RÉHABILITÉ (livre 4 majors).")
    print("       sinon (rdt 4-maj < 3-maj à tous seuils) -> garder ETH dehors pour l'instant, ré-includable via moniteur dynamique.")
    print(f"\n[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
