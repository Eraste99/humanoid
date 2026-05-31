"""ETUDE SIZING / CORRELATION du LIVRE FADE — la derniere brique de deployabilite (sur donnees in-sample).

Config = fade unifie + filtre FLOW + AUCUN stop (la config validee). Sans stop -> chaque trade porte sa queue.
On chiffre "sizing petit pour encaisser la queue" :
  - distribution pertes single-trade (worst/p1/p5/mean/std), par paire + poole majors ;
  - CONCURRENCE : combien de positions ouvertes simultanement (holds 30min qui se chevauchent cross-paires) ;
  - CORRELATION cross-majors : les fades perdent-ils ENSEMBLE ? (PnL journalier par paire -> matrice) + ratio diversification ;
  - maxDD du LIVRE (1x notionnel/trade, time-ordered) + pire journee ;
  - TABLE DE SIZING : pour risk/trade in {0.25,0.5,1.0}% capital (pire-cas = 1.5x worst in-sample) -> notionnel/trade %,
    rendement %/jour, maxDD %/capital, exposition max simultanee %.
HL true-mid + feats causaux, tick-level, ZERO sampling.
"""
import os, sys, time
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import leg_chain, drift_over, PAIRS, WEEKS, MACRO, MIN, GAP

HOUR = 60 * MIN; FEE_MK = 3.0; HOLD = 30 * MIN; DAYMS = 86400000
MAJ = ("BTCUSDC", "ETHUSDC", "SOLUSDC", "XRPUSDC")


def fwd_at(ts, mid, Bs, H):
    n = len(ts); jf = np.searchsorted(ts, ts[Bs] + H, side="left"); ok = jf < n
    f = np.full(len(Bs), np.nan); f[ok] = (mid[jf[ok]] - mid[Bs[ok]]) / mid[Bs[ok]] * 1e4
    tf = np.zeros(len(Bs), bool); tf[ok] = (ts[jf[ok]] - ts[Bs[ok]]) > (H + GAP); f[tf] = np.nan
    return f


def collect(pair):
    rows = []  # (ts_entry, net_bps)
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
        return rows
    dd = np.concatenate(poolD); v = ~np.isnan(dd)
    if v.sum() < 30:
        return rows
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
        return rows
    qtfi = np.nanquantile(np.abs(np.array([e[2] for e in ent])), 0.70)
    for tB, d, tfival, net in ent:
        fire = (not np.isnan(tfival)) and (np.sign(tfival) == d) and (abs(tfival) >= qtfi)
        if not fire:                       # FLOW filter : on garde les non-toxiques
            rows.append((tB, net))
    return rows


def main():
    t0 = time.time()
    print("=== ETUDE SIZING / CORRELATION du LIVRE FADE (fade+FLOW, no stop) — HL in-sample ===\n")
    book = {}
    for p in MAJ:
        r = collect(p)
        if r:
            book[p] = sorted(r)
    # 1. pertes single-trade
    print("--- 1. PERTES single-trade (bps, notionnel) ---")
    allnet = []
    for p in MAJ:
        if p not in book:
            continue
        n = np.array([x[1] for x in book[p]]); allnet.append(n)
        print(f"  {p:>9} | n={len(n):>3} mean{np.mean(n):>+6.1f} std{np.std(n):>5.0f} | worst{np.min(n):>+7.1f} p1{np.percentile(n,1):>+7.1f} p5{np.percentile(n,5):>+6.1f}")
    allnet = np.concatenate(allnet)
    worst = float(np.min(allnet)); p1 = float(np.percentile(allnet, 1))
    print(f"  {'POOL':>9} | n={len(allnet):>3} mean{np.mean(allnet):>+6.1f} std{np.std(allnet):>5.0f} | worst{worst:>+7.1f} p1{p1:>+7.1f} p5{np.percentile(allnet,5):>+6.1f}")

    # 2. concurrence (positions ouvertes simultanement, holds 30min)
    iv = []
    for p in book:
        for tB, _ in book[p]:
            iv.append((tB, 1)); iv.append((tB + HOLD, -1))
    iv.sort()
    cur = 0; mx = 0
    for _, dlt in iv:
        cur += dlt; mx = max(mx, cur)
    print(f"\n--- 2. CONCURRENCE ---\n  positions simultanees max sur le livre = {mx}  (holds 30min, {len(MAJ)} majors)")

    # 3. correlation PnL journalier cross-majors
    days_all = sorted(set(x[0] // DAYMS for p in book for x in book[p]))
    didx = {d: i for i, d in enumerate(days_all)}
    M = np.zeros((len(MAJ), len(days_all)))
    for pi, p in enumerate(MAJ):
        if p not in book:
            continue
        for tB, net in book[p]:
            M[pi, didx[tB // DAYMS]] += net
    print("\n--- 3. CORRELATION PnL journalier cross-majors ---")
    valid = [i for i in range(len(MAJ)) if M[i].any()]
    if len(valid) >= 2:
        C = np.corrcoef(M[valid])
        labs = [MAJ[i][:3] for i in valid]
        print("        " + "  ".join(f"{l:>5}" for l in labs))
        for ii, i in enumerate(valid):
            print(f"  {labs[ii]:>5} " + "  ".join(f"{C[ii,jj]:>+5.2f}" for jj in range(len(valid))))
        bookday = M[valid].sum(0); sumvol = np.sum([np.std(M[i]) for i in valid]); bookvol = np.std(bookday)
        print(f"  ratio diversification (vol_livre / somme_vols) = {bookvol/sumvol:>.2f}  (1=aucune diversif, 0=parfaite)")

    # 4. maxDD du livre (1x notionnel/trade)
    alltr = sorted([x for p in book for x in book[p]])
    net = np.array([x[1] for x in alltr]); eq = np.cumsum(net)
    maxdd = float(np.max(np.maximum.accumulate(eq) - eq))
    ndays = len(days_all)
    bpsday = float(eq[-1]) / ndays
    print(f"\n--- 4. LIVRE (1x notionnel/trade, {len(net)} trades, {ndays} jours actifs) ---")
    print(f"  total {eq[-1]:>+6.0f} bps | bps/jour {bpsday:>+5.1f} | maxDD {maxdd:>5.0f} bps | worst trade {worst:>+6.0f} bps")

    # 5. table de sizing
    WC = 1.5 * abs(worst)         # pire-cas suppose = 1.5x worst in-sample
    print(f"\n--- 5. TABLE DE SIZING (pire-cas single-trade suppose = 1.5x worst = {WC:.0f} bps) ---")
    print(f"  {'risk/trade':>10} | {'notionnel/trade':>15} | {'expo max simult':>15} | {'rdt/jour':>9} | {'maxDD capital':>13}")
    for R in (0.25, 0.5, 1.0):
        f = (R / 100.0) / (WC / 1e4)          # notionnel en fraction du capital
        expo = f * mx                          # exposition simultanee max (fraction capital)
        rday = bpsday * f / 1e4 * 100          # rendement %/jour
        ddcap = maxdd * f / 1e4 * 100          # maxDD en % du capital
        print(f"  {R:>9.2f}% | {100*f:>13.1f}% | {100*expo:>13.0f}% | {rday:>+8.3f}% | {ddcap:>12.1f}%")
    print("\n  LECTURE : 'notionnel/trade' = % du capital par position pour que le pire-cas = risk/trade. 'expo max' peut depasser")
    print("    100% si concurrence>1 (=levier implicite) -> a borner. rdt/jour modeste = prix du short-gamma sans stop.")
    print("    rappel : in-sample/mai, le 11 juin juge. Ceci dimensionne, ne valide pas l'edge.")
    print(f"\n[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
