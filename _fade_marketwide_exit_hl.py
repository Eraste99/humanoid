"""KILL-SWITCH CROSS-ASSET (sortie marche-wide) — tester l'idee : sortir le fade quand le MARCHE (BTC) bouge
contre nous, PAS quand l'actif propre dip. Le stop par-actif a echoue (dip MR recuperable). Le stop MARCHE
ne se declenche que sur de vrais evenements (BTC trend = info qui continue) -> cape la queue SANS le probleme MR-stop ?

Setup : fade unifie + filtre FLOW (deployable), sur ETH/SOL/XRP (re-inclus ETH : le market-exit gere-t-il sa queue ?).
Marche = BTC (proxy leader). En cours de hold (grille 1min), si sign(BTC_ret_depuis_entree)==jambe ET |BTC_ret|>=THR
-> SORTIE taker immediate (le marche court contre nous). Sinon hold 30min maker.
Compare baseline (hold30) vs market-exit(THR). net, worst1, maxDD, %sorti-marche. HL true-mid, tick-level, ZERO sampling.
"""
import os, sys, time
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import leg_chain, drift_over, WEEKS, MACRO, MIN, GAP

HOUR = 60 * MIN; FEE_MK = 3.0; FEE_TK = 6.0; HOLD = 30 * MIN
ASSETS = ["ETHUSDC", "SOLUSDC", "XRPUSDC"]
THRS = [30.0, 50.0]


def fwd_at(ts, mid, Bs, H):
    n = len(ts); jf = np.searchsorted(ts, ts[Bs] + H, side="left"); ok = jf < n
    f = np.full(len(Bs), np.nan); f[ok] = (mid[jf[ok]] - mid[Bs[ok]]) / mid[Bs[ok]] * 1e4
    tf = np.zeros(len(Bs), bool); tf[ok] = (ts[jf[ok]] - ts[Bs[ok]]) > (H + GAP); f[tf] = np.nan
    return f


def xmid_at(ts, mid, t):
    i = np.searchsorted(ts, t, side="left")
    return mid[i] if i < len(ts) else np.nan


def sim_marketexit(ts, mid, ib, d, bts, bmid, THR):
    """hold 30min ; sortie si BTC bouge >=THR dans le sens de la jambe (marche court contre le fade). taker si exit-marche."""
    entry = mid[ib]; t0 = ts[ib]
    bi0 = np.searchsorted(bts, t0, side="left")
    if bi0 >= len(bts):
        return None
    b0 = bmid[bi0]
    for m in range(1, 31):
        tm = t0 + m * MIN
        bi = np.searchsorted(bts, tm, side="left")
        if bi >= len(bts) or (bts[bi] - t0) > (HOLD + GAP):
            break
        bret = (bmid[bi] - b0) / b0 * 1e4
        if np.sign(bret) == d and abs(bret) >= THR:        # le marche court dans le sens de la jambe
            xm = xmid_at(ts, mid, tm)
            if np.isnan(xm):
                break
            return (-d) * (xm - entry) / entry * 1e4 - FEE_TK, True
    # pas de market-exit : hold 30min maker
    j = np.searchsorted(ts, t0 + HOLD, side="left"); j = min(j, len(ts) - 1)
    return (-d) * (mid[j] - entry) / entry * 1e4 - FEE_MK, False


def tail(net):
    a = np.array(net)
    if len(a) < 6:
        return None
    eq = np.cumsum(a); mdd = float(np.max(np.maximum.accumulate(eq) - eq))
    return float(np.mean(a)), float(np.min(a)), mdd, float(eq[-1])


def main():
    print("=== KILL-SWITCH CROSS-ASSET (sortie quand BTC court contre le fade) — ETH/SOL/XRP, HL true-mid ===\n")
    D = {}
    for p in ASSETS + ["BTCUSDC"]:
        for wk, ddir in WEEKS.items():
            try:
                ticks, trades, l2 = load_all(p, ddir)
                ts, mid, feats = vectorized_features(ticks, trades, l2)
            except Exception:
                continue
            D.setdefault(p, {})[wk] = (np.asarray(ts, np.int64), np.asarray(mid, np.float64), feats)
    res = {}  # (scope, cfg) -> list net
    for asset in ASSETS:
        for wk in WEEKS:
            if wk not in D.get(asset, {}) or wk not in D.get("BTCUSDC", {}):
                continue
            ts, mid, feats = D[asset][wk]; bts, bmid, _ = D["BTCUSDC"][wk]
            Bs, ds = leg_chain(ts, mid, 50.0)
            if len(Bs) < 5:
                continue
            dmac = drift_over(ts, mid, MACRO)[Bs]; d1 = drift_over(ts, mid, HOUR)[Bs]
            tfi = np.asarray(feats.get("tfi_5min"), float)[Bs]
            # terciles + accel + flow-Q70 sur les entrees de CETTE semaine (approx ; in-sample)
            ok = ~np.isnan(dmac)
            if ok.sum() < 15:
                continue
            q1, q2 = np.quantile(dmac[ok], [1 / 3, 2 / 3])
            bm = ok & (dmac >= q2) & ~np.isnan(d1); m1 = np.median(d1[bm]) if bm.sum() else 0.0
            ents = []
            for k in range(len(Bs)):
                dm = dmac[k]
                if np.isnan(dm):
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
                ents.append((int(Bs[k]), d, tfi[k]))
            if len(ents) < 8:
                continue
            qtfi = np.nanquantile(np.abs(np.array([e[2] for e in ents])), 0.70)
            for ib, d, tfival in ents:
                if (not np.isnan(tfival)) and (np.sign(tfival) == d) and (abs(tfival) >= qtfi):
                    continue  # FLOW filter : skip
                # baseline hold30
                j = np.searchsorted(ts, ts[ib] + HOLD, side="left"); j = min(j, len(ts) - 1)
                base = (-d) * (mid[j] - mid[ib]) / mid[ib] * 1e4 - FEE_MK
                res.setdefault(("POOL", "baseline"), []).append(base)
                res.setdefault((asset, "baseline"), []).append(base)
                for THR in THRS:
                    r = sim_marketexit(ts, mid, ib, d, bts, bmid, THR)
                    if r is None:
                        continue
                    res.setdefault(("POOL", f"mkt{int(THR)}"), []).append(r[0])
                    res.setdefault((asset, f"mkt{int(THR)}"), []).append(r[0])
                    if THR == 30:
                        res.setdefault(("POOL", "exited30"), []).append(1.0 if r[1] else 0.0)

    for scope in ["POOL"] + ASSETS:
        cfgs = [("baseline", None)] + [(f"mkt{int(t)}", t) for t in THRS]
        print(f"=== {scope} ===")
        print(f"  {'config':>10} | {'n':>4} {'net/tr':>6} | {'worst1':>7} {'maxDD':>7} | {'total':>7} {'%exit-mkt':>9}")
        for cfg, _ in cfgs:
            lst = res.get((scope, cfg))
            if not lst:
                continue
            st = tail(lst)
            if st is None:
                continue
            mn, w1, mdd, tot = st
            exr = ""
            if scope == "POOL" and cfg == "mkt30":
                ex = res.get(("POOL", "exited30"))
                exr = f"{100*np.mean(ex):.0f}%" if ex else ""
            print(f"  {cfg:>10} | {len(lst):>4} {mn:>+6.1f} | {w1:>+7.1f} {mdd:>7.0f} | {tot:>+7.0f} {exr:>9}")
        print()
    print("  LECTURE : si market-exit BAISSE worst1/maxDD SANS tuer net/tr (vs per-asset stop qui tuait le net)")
    print("    -> le stop MARCHE marche la ou le stop ACTIF echouait. %exit-mkt faible = ne se declenche que sur vrais events.")


if __name__ == "__main__":
    main()
