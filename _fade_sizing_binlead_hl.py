"""SIZING du LIVRE FADE-MAJORS avec la QUEUE RÉDUITE (filtre Binance-lead) -> le % déployable RÉEL.
Les anciens scripts sizaient sur worst −238 (SANS Binance-lead). Ici on size sur le stream DÉPLOYABLE :
  fade unifié + filtre FLOW + filtre BINANCE-LEAD (skip si |drift Binance 120s|>=30 & sign==jambe), no stop.
-> worst ~−71/−101, maxDD ÷2 => on peut sizer plus gros à risque égal => rendement plus haut. On chiffre de combien.
Sizing PRINCIPAL = basé sur le maxDD RÉEL du livre (inclut le clustering corrélé, le vrai risque compte) pour une tolérance
de drawdown capital donnée. + sizing single-trade (continuité). Compare baseline vs binSAME vs binEITHER. HL in-sample/mai.
"""
import os, sys, time
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import leg_chain, drift_over, WEEKS, MACRO, MIN, GAP
from _fade_binlead_fullbook_hl import load_bin, bdrift, BIN

HOUR = 60 * MIN; FEE_MK = 3.0; HOLD = 30 * MIN; DAYMS = 86400000; THR = 30.0
MAJ3 = ("BTCUSDC", "SOLUSDC", "XRPUSDC"); MAJ4 = ("BTCUSDC", "ETHUSDC", "SOLUSDC", "XRPUSDC")
MONTHD = 30   # crypto 24/7


def fwd_at(ts, mid, Bs, H):
    n = len(ts); jf = np.searchsorted(ts, ts[Bs] + H, side="left"); ok = jf < n
    f = np.full(len(Bs), np.nan); f[ok] = (mid[jf[ok]] - mid[Bs[ok]]) / mid[Bs[ok]] * 1e4
    tf = np.zeros(len(Bs), bool); tf[ok] = (ts[jf[ok]] - ts[Bs[ok]]) > (H + GAP); f[tf] = np.nan
    return f


def collect_deploy(pair, binD, binmode):
    """fade unifié + FLOW + Binance-lead(binmode in none/same/btc/either). retourne [(tB, net)]."""
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
                if d != -1.0:
                    continue
            elif dm >= q2:
                if d != 1.0 or np.isnan(d1[k]) or d1[k] < m1:
                    continue
            else:
                continue
            ent.append((int(tB[k]), d, tfi[k], -d * fwd[k] - FEE_MK))
    if len(ent) < 12:
        return []
    qtfi = np.nanquantile(np.abs(np.array([e[2] for e in ent])), 0.70)
    rows = []
    for tB, d, tfival, net in ent:
        if (not np.isnan(tfival)) and (np.sign(tfival) == d) and (abs(tfival) >= qtfi):
            continue                                       # FLOW skip
        if binmode != "none":
            bs = bdrift(binD.get(BIN[pair]), tB); bb = bdrift(binD.get("BTCUSDT"), tB)
            fire_same = (not np.isnan(bs)) and (np.sign(bs) == d) and (abs(bs) >= THR)
            fire_btc = (not np.isnan(bb)) and (np.sign(bb) == d) and (abs(bb) >= THR)
            if binmode == "same" and fire_same:
                continue
            if binmode == "btc" and fire_btc:
                continue
            if binmode == "either" and (fire_same or fire_btc):
                continue
        rows.append((tB, net))
    return rows


def book_stats(book):
    """book = {pair:[(tB,net)]}. retourne stats de risque incluant le clustering corrélé."""
    alltr = sorted([x for p in book for x in book[p]])
    if len(alltr) < 10:
        return None
    net = np.array([x[1] for x in alltr]); ts = np.array([x[0] for x in alltr])
    ndays = len(np.unique(ts // DAYMS)); eq = np.cumsum(net)
    maxdd = float(np.max(np.maximum.accumulate(eq) - eq)); bpsday = float(eq[-1]) / ndays
    # cluster corrélé : pire somme de net sur les trades entrés dans la même fenêtre 30min (positions ouvertes ensemble)
    buckets = {}
    for t, nt in zip(ts, net):
        buckets.setdefault(int(t // HOLD), 0.0)
        buckets[int(t // HOLD)] += nt
    worst_cluster = min(buckets.values())
    # concurrence
    iv = []
    for p in book:
        for tB, _ in book[p]:
            iv.append((tB, 1)); iv.append((tB + HOLD, -1))
    iv.sort(); cur = mx = 0
    for _, dlt in iv:
        cur += dlt; mx = max(mx, cur)
    return dict(n=len(net), nday=len(net) / ndays, mean=float(np.mean(net)), worst=float(np.min(net)),
               p1=float(np.percentile(net, 1)), worst_cluster=float(worst_cluster), maxdd=maxdd,
               bpsday=bpsday, conc=mx, ndays=ndays)


def main():
    t0 = time.time()
    print("=== SIZING LIVRE FADE-MAJORS (BTC/SOL/XRP) avec filtre BINANCE-LEAD — queue réduite -> % déployable réel ===\n")
    binD = {s: load_bin(s) for s in set(BIN.values())}
    modes = ("none", "same", "either")
    lab = {"none": "baseline", "same": "binSAME", "either": "binEITHER"}
    books3 = {m: {p: sorted(collect_deploy(p, binD, m)) for p in MAJ3} for m in modes}
    S3 = {m: book_stats(books3[m]) for m in modes}

    print("--- 1. RISQUE du stream par config (BTC/SOL/XRP, fade+FLOW, no stop) ---")
    print(f"  {'config':>10} | {'n':>4} {'/jour':>5} | {'mean':>5} | {'worst':>6} {'p1':>6} {'worst30min':>10} | {'maxDD livre':>11} {'conc':>4}")
    for m in modes:
        s = S3[m]
        if not s:
            continue
        print(f"  {lab[m]:>10} | {s['n']:>4} {s['nday']:>5.1f} | {s['mean']:>+5.1f} | {s['worst']:>+6.0f} {s['p1']:>+6.0f} {s['worst_cluster']:>+10.0f} | {s['maxdd']:>11.0f} {s['conc']:>4}")

    POLS = [("risk", 0.25, "risk 0.25%/tr"), ("risk", 0.50, "risk 0.50%/tr"), ("expo", 100.0, "expo<=100%")]

    def fnotional(s, kind, val):
        return (val / 100.0) / (1.5 * abs(s['worst']) / 1e4) if kind == "risk" else (val / 100.0) / s['conc']

    print("--- 2. SIZING PRUDENT par config — tout le risque visible (rdt in-sample/active-day x30) ---")
    print("    NB : on NE size PAS sur le maxDD in-sample (148-225bps=1.5-2.2% du capital -> impliquerait ~6x de levier = ABSURDE")
    print("    et dangereux OOS). On ancre sur (a) worst single-trade x1.5 = risk/trade, et (b) expo simultanée plafonnée. Le + petit gagne.")
    for m in modes:
        s = S3[m]
        if not s:
            continue
        print(f"\n  [{lab[m]}]  bps/jour={s['bpsday']:+.0f}  worst={s['worst']:+.0f}  worst-cluster={s['worst_cluster']:+.0f}  conc={s['conc']}")
        print(f"     {'politique':>14} | {'notion/tr':>9} {'expo max':>8} | {'rdt/mois':>9} | {'perte@1.5worst':>14} {'maxDD cap':>9}")
        for kind, val, plabel in POLS:
            f = fnotional(s, kind, val)
            expo = f * s['conc']; mois = s['bpsday'] * f / 1e4 * 100 * MONTHD
            wimp = 1.5 * abs(s['worst']) * f / 1e4 * 100; ddc = s['maxdd'] * f / 1e4 * 100
            print(f"     {plabel:>14} | {100*f:>8.1f}% {100*expo:>7.0f}% | {mois:>+8.2f}% | {wimp:>13.2f}% {ddc:>8.2f}%")

    def prudent(s):     # le + contraignant entre risk 0.25%/tr et expo<=100%
        f = min(fnotional(s, "risk", 0.25), fnotional(s, "expo", 100.0))
        return f, s['bpsday'] * f / 1e4 * 100 * MONTHD, f * s['conc']

    if S3["none"] and S3["same"]:
        b, e = S3["none"], S3["same"]
        fb, rb, xb = prudent(b); fe, re, xe = prudent(e)
        print(f"\n--- 3. DÉPLOYABLE PRUDENT (le + contraignant de : risk 0.25%/tr ET expo<=100%) ---")
        print(f"  baseline : notionnel {100*fb:>4.0f}%/tr, expo {100*xb:>3.0f}% -> {rb:>+5.1f}%/mois | risque : worst-cluster {b['worst_cluster']:+.0f}b, conc {b['conc']}")
        print(f"  binSAME  : notionnel {100*fe:>4.0f}%/tr, expo {100*xe:>3.0f}% -> {re:>+5.1f}%/mois | risque : worst-cluster {e['worst_cluster']:+.0f}b, conc {e['conc']}")
        print(f"  => le Binance-lead n'augmente pas tant le notionnel que la SÉCURITÉ : cluster {b['worst_cluster']:+.0f}->{e['worst_cluster']:+.0f}b, conc {b['conc']}->{e['conc']}, mean/tr {b['mean']:+.1f}->{e['mean']:+.1f}.")
        print(f"  vs HLP ~11%/an (~0.9%/mois) : ~{re:.0f}%/mois in-sample = ~{re/0.9:.0f}x — MAIS in-sample, le 11 juin juge.")

    # 4. ETH réhabilité par le Binance-lead ? (l'exclusion ETH datait d'AVANT le filtre)
    book4 = {p: sorted(collect_deploy(p, binD, "same")) for p in MAJ4}
    s4 = book_stats(book4)
    if s4 and S3["same"]:
        s3 = S3["same"]; f3, r3, _ = prudent(s3); f4, r4, _ = prudent(s4)
        print(f"\n--- 4. +ETH réhabilité par le Binance-lead ? (sizing prudent ; l'exclusion ETH datait d'AVANT le filtre) ---")
        print(f"  BTC/SOL/XRP (binSAME) : worst {s3['worst']:+.0f} cluster {s3['worst_cluster']:+.0f} conc {s3['conc']} -> {r3:+.1f}%/mois")
        print(f"  +ETH (binSAME)        : worst {s4['worst']:+.0f} cluster {s4['worst_cluster']:+.0f} conc {s4['conc']} -> {r4:+.1f}%/mois  ({s4['n']} tr, {s4['nday']:.1f}/j)")
        verdict = "ETH AIDE (rdt monte, queue maitrisee)" if r4 > r3 * 1.05 else "ETH re-introduit de la queue sans gain net clair -> garder exclu"
        print(f"  => {verdict}.")

    print("\n  LECTURE : (2)/(3) = le % deployable HONNETE : ancre sur le worst single-trade et l'expo, PAS sur le maxDD in-sample (trop")
    print("    optimiste). Le Binance-lead aide surtout en COUPANT le cluster correle (le vrai risque de ruine) et la concurrence.")
    print("    ATTENTION : in-sample/mai ; le 11 juin juge l'edge ; sizer petit au depart (la vraie queue OOS peut etre pire que mai).")
    print(f"\n[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
