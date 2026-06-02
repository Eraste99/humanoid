"""SIZING FADE CAUSAL = VOL-SCALING (taille ∝ 1/vol recente, estimee STRICTEMENT sur le passe) — ferme le trou lookahead.
Le sizing per-paire precedent utilisait le worst IN-SAMPLE (lookahead). Ici : a chaque trade, vol_recente = std des moves 30min
des jambes des 3 derniers jours (causal) ; f = C/vol. Compare 3 sizings, TOUS normalises a expo-pic=100% (sans levier) :
  (a) FLAT (taille egale)  (b) IN-SAMPLE worst per-paire (LOOKAHEAD, le +8% d'avant)  (c) VOL-SCALE CAUSAL (deployable).
On regarde : rdt/mois, PIRE trade en %capital REALISE, maxDD %capital REALISE. Si (c) ~= (b) en rdt ET maitrise la queue aussi
bien -> le ~8% TIENT sans lookahead. Livre fade+FLOW+Binance-lead, BTC/SOL/XRP @50bps. HL true-mid, tick-level, ZERO grille.
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
MAJ3 = ("BTCUSDC", "SOLUSDC", "XRPUSDC"); VOLWIN = 3 * DAY; MINLEG = 15


def fwd_at(ts, mid, Bs, H):
    n = len(ts); jf = np.searchsorted(ts, ts[Bs] + H, side="left"); ok = jf < n
    f = np.full(len(Bs), np.nan); f[ok] = (mid[jf[ok]] - mid[Bs[ok]]) / mid[Bs[ok]] * 1e4
    tf = np.zeros(len(Bs), bool); tf[ok] = (ts[jf[ok]] - ts[Bs[ok]]) > (H + GAP); f[tf] = np.nan
    return f


def collect(pair, binD):
    """retourne alllegs=(t,fwd30) de TOUTES jambes (pour la vol causale) ; fades=[(t,net)] deployables."""
    AT = []; AF = []; wkA = {}; poolD = []; poolD1 = []
    for wk in WEEKS:
        try:
            ti, tr, l2 = load_all(pair, WEEKS[wk]); ts, mid, feats = vectorized_features(ti, tr, l2)
        except Exception:
            continue
        ts = np.asarray(ts, np.int64); mid = np.asarray(mid, np.float64)
        Bs, ds = leg_chain(ts, mid, 50.0)
        if len(Bs) < 6:
            continue
        dmac = drift_over(ts, mid, MACRO)[Bs]; d1 = drift_over(ts, mid, HOUR)[Bs]
        tfi = np.asarray(feats.get("tfi_5min", np.full(len(ts), np.nan)), np.float64)[Bs]
        fwd = fwd_at(ts, mid, Bs, HOLD)
        for k in range(len(Bs)):
            if not np.isnan(fwd[k]):
                AT.append(int(ts[Bs[k]])); AF.append(abs(float(fwd[k])))   # |move 30min| pour la vol
        wkA[wk] = (ts[Bs], ds, dmac, d1, tfi, fwd); poolD.append(dmac); poolD1.append(d1)
    if not wkA:
        return (np.array([]), np.array([])), []
    dd = np.concatenate(poolD); v = ~np.isnan(dd)
    if v.sum() < 30:
        return (np.array([]), np.array([])), []
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
        return (np.array([]), np.array([])), []
    qtfi = np.nanquantile(np.abs(np.array([e[2] for e in ent])), 0.70)
    fades = []
    for tB, d, tfival, net in ent:
        if (not np.isnan(tfival)) and (np.sign(tfival) == d) and (abs(tfival) >= qtfi):
            continue
        bs = bdrift(binD.get(BIN[pair]), tB)
        if (not np.isnan(bs)) and (np.sign(bs) == d) and (abs(bs) >= THR):
            continue
        fades.append((tB, net))
    o = np.argsort(AT)
    return (np.array(AT)[o], np.array(AF)[o]), sorted(fades)


def peak_expo(trades):  # trades: list of (t, f)
    iv = []
    for t, f in trades:
        iv.append((t, f)); iv.append((t + HOLD, -f))
    iv.sort(); cur = mx = 0.0
    for _, d in iv:
        cur += d; mx = max(mx, cur)
    return mx


def report(name, trades, ndays):
    """trades: list (t, net, f_raw). normalise a expo-pic=100%, puis rdt/mois + pire trade + maxDD en %capital."""
    pe = peak_expo([(t, f) for t, _, f in trades])
    sc = 1.0 / pe if pe > 0 else 1.0
    trades = sorted(trades)
    pnl = np.array([net * (f * sc) / 1e4 for _, net, f in trades])   # rendement capital par trade
    eq = np.cumsum(pnl); maxdd = float(np.max(np.maximum.accumulate(eq) - eq))
    mois = float(np.sum(pnl)) / ndays * MONTHD * 100
    worst = float(np.min(pnl)) * 100
    avgnot = np.mean([f * sc for _, _, f in trades]) * 100
    print(f"  {name:>22} | notion moy {avgnot:>4.0f}% | rdt/mois {mois:>+6.2f}% | pire trade {worst:>+6.3f}% | maxDD {maxdd*100:>5.2f}%")


def main():
    t0 = time.time()
    print("=== SIZING FADE CAUSAL (vol-scaling) vs IN-SAMPLE-worst (lookahead) vs FLAT — tous normalises expo-pic=100% ===\n")
    binD = {s: load_bin(s) for s in set(BIN.values())}
    DATA = {p: collect(p, binD) for p in MAJ3}
    worst_p = {}
    for p in MAJ3:
        _, fades = DATA[p]
        worst_p[p] = min(n for _, n in fades) if fades else -100.0
    medvol = np.median(np.concatenate([DATA[p][0][1] for p in MAJ3 if len(DATA[p][0][1])]))

    allt = np.concatenate([np.array([t for t, _ in DATA[p][1]]) for p in MAJ3 if DATA[p][1]])
    ndays = len(np.unique(allt // DAY))
    warmed = 0; total = 0

    flat = []; ins = []; vol = []
    for p in MAJ3:
        (AT, AF), fades = DATA[p]
        if not fades:
            continue
        for t, net in fades:
            total += 1
            # vol causale = std des |move30| des jambes des 3 derniers jours, forward observe (t_leg+30min <= t)
            lo = np.searchsorted(AT, t - VOLWIN, side="left"); hi = np.searchsorted(AT, t - HOLD, side="left")
            if hi - lo >= MINLEG:
                vrec = float(np.std(AF[lo:hi])); warmed += 1
            else:
                vrec = float(medvol)   # cold-start : vol mediane cross-pair
            flat.append((t, net, 1.0))
            ins.append((t, net, 1.0 / (1.5 * abs(worst_p[p]))))   # ∝ 1/worst_paire (LOOKAHEAD)
            vol.append((t, net, 1.0 / vrec))                       # ∝ 1/vol_recente (CAUSAL)

    print(f"  livre BTC/SOL/XRP @50bps : {total} trades, {ndays} jours actifs, {100*warmed/total:.0f}% des trades avec vol causale chaude\n")
    report("FLAT (egal)", flat, ndays)
    report("IN-SAMPLE worst (LOOKAHEAD)", ins, ndays)
    report("VOL-SCALE CAUSAL (deploy.)", vol, ndays)

    print("\n  LECTURE : si VOL-SCALE CAUSAL a un rdt/mois ~= IN-SAMPLE-worst ET un pire-trade/maxDD <= (aussi bien maitrise) ->")
    print("    le ~8% TIENT SANS lookahead (vol-scaling = proxy causal du tail, deployable). Si bien pire -> le worst lookahead trichait.")
    print("    (tout normalise a expo-pic=100% sans levier -> comparaison juste a capital deploye egal.)")
    print(f"\n[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
