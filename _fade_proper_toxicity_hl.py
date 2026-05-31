"""VRAI calculateur de toxicite (recherche) vs mon raccourci tfi_5min — l'OFI depth-normalise + amincissement carnet
voient-ils venir les catastrophes que le flux TAKER (tfi) a ratees (le -237, calme cote trades) ?

Signaux a l'entree (causal, fenetre 5min de L2) :
  - OFI (Cont-Kukanov-Stoikov, evenements carnet) cumule, DEPTH-NORMALISE (/ profondeur liq_b5+liq_a5).
  - amincissement carnet : (depth now - depth 5min avant)/depth (negatif = makers retirent = precurseur).
  - spread now / spread median (elargissement).
  - vs le crude tfi_5min (mon filtre actuel).
Diagnostic : aux 6 pires trades, ces signaux sont-ils ELEVES (percentile) la ou tfi etait BAS ? + compare filtre tfi vs OFI.
HL true-mid + L2, causal, ZERO sampling.
"""
import os, sys, time
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import leg_chain, drift_over, WEEKS, MACRO, MIN, GAP

HOUR = 60 * MIN; W5 = 5 * MIN; FEE_MK = 3.0; HOLD = 30 * MIN
MAJ = ["BTCUSDC", "ETHUSDC", "SOLUSDC", "XRPUSDC"]


def ofi_series(b1p, b1s, a1p, a1s):
    db = np.diff(b1p, prepend=b1p[0]); da = np.diff(a1p, prepend=a1p[0])
    bsp = np.roll(b1s, 1); bsp[0] = b1s[0]; asp = np.roll(a1s, 1); asp[0] = a1s[0]
    dVb = np.where(db >= 0, b1s, 0) - np.where(db <= 0, bsp, 0)
    dVa = np.where(da <= 0, a1s, 0) - np.where(da >= 0, asp, 0)
    return (dVb - dVa).astype(np.float64)


def fwd_at(ts, mid, Bs, H):
    n = len(ts); jf = np.searchsorted(ts, ts[Bs] + H, side="left"); ok = jf < n
    f = np.full(len(Bs), np.nan); f[ok] = (mid[jf[ok]] - mid[Bs[ok]]) / mid[Bs[ok]] * 1e4
    tf = np.zeros(len(Bs), bool); tf[ok] = (ts[jf[ok]] - ts[Bs[ok]]) > (H + GAP); f[tf] = np.nan
    return f


def collect(pair):
    rows = []
    for wk, ddir in WEEKS.items():
        try:
            ticks, trades, l2 = load_all(pair, ddir)
            ts, mid, feats = vectorized_features(ticks, trades, l2)
        except Exception:
            continue
        ts = np.asarray(ts, np.int64); mid = np.asarray(mid, np.float64)
        lts = l2["ts_ms"].values.astype(np.int64)
        b1p = l2["b1p"].values.astype(np.float64); b1s = l2["b1s"].values.astype(np.float64)
        a1p = l2["a1p"].values.astype(np.float64); a1s = l2["a1s"].values.astype(np.float64)
        liq = (l2["liq_b5"].values + l2["liq_a5"].values).astype(np.float64)
        spr = l2["spread_bps"].values.astype(np.float64)
        okl = (b1p > 0) & (a1p > 0) & (liq > 0)
        lts, b1p, b1s, a1p, a1s, liq, spr = [x[okl] for x in (lts, b1p, b1s, a1p, a1s, liq, spr)]
        if len(lts) < 200:
            continue
        ofi = ofi_series(b1p, b1s, a1p, a1s)
        cofi = np.concatenate([[0.0], np.cumsum(ofi)])     # prefix-sum pour somme par fenetre
        Bs, ds = leg_chain(ts, mid, 50.0)
        if len(Bs) < 5:
            continue
        dmac = drift_over(ts, mid, MACRO)[Bs]; d1 = drift_over(ts, mid, HOUR)[Bs]
        tfi = np.asarray(feats.get("tfi_5min"), float)[Bs]
        fwd = fwd_at(ts, mid, Bs, HOLD)
        ok = ~np.isnan(dmac)
        if ok.sum() < 15:
            continue
        q1, q2 = np.quantile(dmac[ok], [1 / 3, 2 / 3])
        bm = ok & (dmac >= q2) & ~np.isnan(d1); m1 = np.median(d1[bm]) if bm.sum() else 0.0
        for k in range(len(Bs)):
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
            T = ts[Bs[k]]
            j0 = np.searchsorted(lts, T - W5); j1 = np.searchsorted(lts, T)
            if j1 - j0 < 5:
                continue
            ofi5 = cofi[j1] - cofi[j0]
            depth = np.median(liq[j0:j1])
            ofin = ofi5 / depth if depth > 0 else np.nan        # OFI depth-normalise
            # amincissement : depth fin de fenetre vs debut
            d_now = np.median(liq[max(j0, j1 - 10):j1]); d_old = np.median(liq[j0:j0 + 10])
            thin = (d_now - d_old) / d_old if d_old > 0 else np.nan   # <0 = carnet s'amincit
            sprn = np.median(spr[j0:j1])
            rows.append(dict(pair=pair, t=int(T), d=d, net=-d * fwd[k] - FEE_MK,
                             tfi=tfi[k], ofin=ofin, thin=thin, spr=sprn))
    return rows


def pctl(arr, v):
    a = arr[~np.isnan(arr)]
    return 100.0 * np.mean(a < v) if len(a) and not np.isnan(v) else float('nan')


def main():
    print("=== VRAI calculateur toxicite (OFI depth-norm + amincissement carnet) vs tfi — voit-il le -237 ? — HL ===\n")
    allrows = []
    for p in MAJ:
        allrows += collect(p)
    if not allrows:
        print("rien"); return
    TFI = np.array([r["tfi"] for r in allrows]); OFIN = np.array([r["ofin"] for r in allrows])
    THIN = np.array([r["thin"] for r in allrows]); SPR = np.array([r["spr"] for r in allrows])
    allrows.sort(key=lambda r: r["net"])
    print("  --- 6 PIRES trades (les signaux carnet s'allument-ils la ou tfi est BAS ?) ---")
    print(f"  {'date':>16} {'pair':>4} {'net':>7} {'d':>3} | {'tfi(p)':>8} | {'OFInorm(p,sgn)':>16} | {'thin(p)':>9} | {'spread(p)':>10}")
    for r in allrows[:6]:
        dt = time.strftime("%m-%d %H:%M", time.gmtime(r["t"] / 1000))
        sgn = "==d" if (not np.isnan(r["ofin"]) and np.sign(r["ofin"]) == r["d"]) else "!=d"
        print(f"  {dt:>16} {r['pair'][:3]:>4} {r['net']:>+7.0f} {r['d']:>+3.0f} | "
              f"p{pctl(np.abs(TFI),abs(r['tfi'])):>3.0f} | p{pctl(np.abs(OFIN),abs(r['ofin'])):>3.0f} {sgn:>4} | "
              f"p{pctl(-THIN,-r['thin']):>3.0f} | p{pctl(SPR,r['spr']):>3.0f}")
    print("  (thin percentile : haut = carnet s'amincit fort ; spread percentile : haut = spread large)")

    # compare filtres : tfi vs OFI-depth-norm (skip si signe==d & |.|>=Q70)
    net = np.array([r["net"] for r in allrows]); d = np.array([r["d"] for r in allrows])
    qtfi = np.nanquantile(np.abs(TFI), 0.70); qofi = np.nanquantile(np.abs(OFIN), 0.70)
    fire_tfi = (np.sign(TFI) == d) & (np.abs(TFI) >= qtfi)
    fire_ofi = (np.sign(OFIN) == d) & (np.abs(OFIN) >= qofi)
    fire_thin = THIN <= np.nanquantile(THIN, 0.30)        # carnet s'amincit (bottom 30%)

    def line(lab, keep):
        a = net[keep]
        if len(a) < 6:
            return
        eq = np.cumsum(a); mdd = np.max(np.maximum.accumulate(eq) - eq)
        print(f"  {lab:>16} | gardé {100*keep.mean():>3.0f}% | net {np.mean(a):>+5.1f} | worst {np.min(a):>+7.0f} | maxDD {mdd:>5.0f}")
    print("\n  --- compare filtres (POOL 4 majors) ---")
    line("baseline", np.ones(len(net), bool))
    line("tfi (actuel)", ~fire_tfi)
    line("OFI depth-norm", ~fire_ofi)
    line("OFI ou thinning", ~(fire_ofi | fire_thin))
    line("tfi ou OFI", ~(fire_tfi | fire_ofi))
    print("\n  LECTURE : si OFI/thinning ont percentile ELEVE aux pires (surtout le -237 ou tfi=p54) -> le vrai calculateur")
    print("    voit ce que tfi rate. Si le filtre OFI coupe worst SANS tuer net mieux que tfi -> on remplace.")


if __name__ == "__main__":
    main()
