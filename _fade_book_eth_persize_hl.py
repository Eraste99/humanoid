"""OU EST ETH ? — on l'avait exclu par SIZING BOOK-WIDE (son worst -106 penalisait tout le livre). Test du bon reflexe :
INCLURE ETH (4 majors) et sizer PAR PAIRE (chaque paire a son propre worst -> ETH = notionnel + petit, sans plomber les autres).
Compare 3-majors vs 4-majors, en sizing BOOK-WIDE (ancien) vs PER-PAIRE (chaque paire sizee a 0.25%/trade de SON worst), expo<=100%.
Question : inclure ETH (sizee pour sa propre queue) AJOUTE-t-il au portefeuille, ou plombe-t-il ? Coherent avec flexibilite + portefeuille.
HL true-mid, tick-level, fade+FLOW+Binance-lead @50bps. ZERO sampling.
"""
import os, sys, time
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import leg_chain, drift_over, WEEKS, MACRO, MIN, GAP
from _fade_binlead_fullbook_hl import load_bin, bdrift, BIN

HOUR = 60 * MIN; FEE_MK = 3.0; HOLD = 30 * MIN; THR = 30.0; DAY = 86400000; MONTHD = 30
MAJ4 = ("BTCUSDC", "ETHUSDC", "SOLUSDC", "XRPUSDC")


def fwd_at(ts, mid, Bs, H):
    n = len(ts); jf = np.searchsorted(ts, ts[Bs] + H, side="left"); ok = jf < n
    f = np.full(len(Bs), np.nan); f[ok] = (mid[jf[ok]] - mid[Bs[ok]]) / mid[Bs[ok]] * 1e4
    tf = np.zeros(len(Bs), bool); tf[ok] = (ts[jf[ok]] - ts[Bs[ok]]) > (H + GAP); f[tf] = np.nan
    return f


def stream(DATA, pair, binD):
    wkA = {}; poolD = []; poolD1 = []
    for wk in WEEKS:
        if (pair, wk) not in DATA:
            continue
        ts, mid, feats = DATA[(pair, wk)]
        Bs, ds = leg_chain(ts, mid, 50.0)
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


def peak(streams, fmap):
    iv = []
    for p in streams:
        for t, _ in streams[p]:
            iv.append((t, fmap[p])); iv.append((t + HOLD, -fmap[p]))
    iv.sort(); cur = mx = 0.0
    for _, dlt in iv:
        cur += dlt; mx = max(mx, cur)
    return mx


def evalbook(streams):
    """retourne dict de sizing book-wide et per-paire."""
    pairs = [p for p in streams if streams[p]]
    allt = np.concatenate([np.array([x[0] for x in streams[p]]) for p in pairs])
    ndays = len(np.unique(allt // DAY))
    worsts = {p: min(x[1] for x in streams[p]) for p in pairs}
    totals = {p: sum(x[1] for x in streams[p]) for p in pairs}
    bpsday = {p: totals[p] / ndays for p in pairs}
    # book-wide : f = min(0.25%/(1.5*worst_book), 1/conc_book)
    worst_book = min(worsts.values()); conc_book = peak(streams, {p: 1.0 for p in pairs})
    f_bw = min((0.25 / 100) / (1.5 * abs(worst_book) / 1e4), 1.0 / conc_book)
    mois_bw = sum(bpsday.values()) * f_bw / 1e4 * 100 * MONTHD
    # per-paire : f_p = 0.25%/(1.5*worst_p), puis cap expo<=100%
    fp = {p: (0.25 / 100) / (1.5 * abs(worsts[p]) / 1e4) for p in pairs}
    pe = peak(streams, fp)
    if pe > 1.0:
        fp = {p: fp[p] / pe for p in pairs}
    mois_pp = sum(bpsday[p] * fp[p] / 1e4 * 100 * MONTHD for p in pairs)
    return dict(pairs=pairs, ndays=ndays, worsts=worsts, bpsday=bpsday, conc=conc_book,
                f_bw=f_bw, mois_bw=mois_bw, fp=fp, expo_pp=min(pe, 1.0) if pe > 1 else pe, mois_pp=mois_pp)


def main():
    t0 = time.time()
    print("=== OU EST ETH ? — 3-majors vs 4-majors (avec ETH), sizing BOOK-WIDE vs PER-PAIRE @50bps (fade+FLOW+Binance-lead) ===\n")
    DATA = {}
    for pair in MAJ4:
        for wk, ddir in WEEKS.items():
            try:
                ti, tr, l2 = load_all(pair, ddir); ts, mid, feats = vectorized_features(ti, tr, l2)
                DATA[(pair, wk)] = (np.asarray(ts, np.int64), np.asarray(mid, np.float64), feats)
            except Exception:
                continue
    binD = {s: load_bin(s) for s in set(BIN.values())}
    S = {p: sorted(stream(DATA, p, binD)) for p in MAJ4}

    print("--- worst & EV par paire (le pourquoi du sizing) ---")
    for p in MAJ4:
        if S[p]:
            nt = np.array([x[1] for x in S[p]])
            print(f"  {p:>9} | n={len(nt):>3} | EV/tr {np.mean(nt):>+5.1f} | worst {np.min(nt):>+5.0f}")

    for lab, book in (("3-majors (BTC/SOL/XRP)", {p: S[p] for p in ("BTCUSDC", "SOLUSDC", "XRPUSDC")}),
                      ("4-majors (+ETH)", {p: S[p] for p in MAJ4})):
        r = evalbook(book)
        print(f"\n=== {lab} ===")
        print(f"  BOOK-WIDE (taille tout au pire du livre, worst={min(r['worsts'].values()):.0f}, conc={r['conc']}) -> notionnel {100*r['f_bw']:.1f}%/trade -> {r['mois_bw']:+.1f}%/mois")
        fps = " ".join(f"{p[:3]}={100*r['fp'][p]:.0f}%" for p in r['pairs'])
        print(f"  PER-PAIRE (chaque paire a son worst ; expo pic {100*r['expo_pp']:.0f}%) -> notionnels [{fps}] -> {r['mois_pp']:+.1f}%/mois")

    print("\n  LECTURE : si 4-majors PER-PAIRE >= 3-majors -> inclure ETH (sizee petit pour sa queue) AJOUTE de la diversification/rdt")
    print("    sans plomber les autres -> garder ETH dans l'univers (coherent flexibilite). Si < -> ETH dilue meme bien sizee.")
    print(f"\n[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
