"""FILTRE DE REGIME sur le FADE — kill-switch "trend informe" pour tuer la queue short-gamma (recherche-informe).

Litterature (3 agents) : le seul predicteur robuste de continuation = FLUX SIGNE SOUTENU (Ahern : makers sous-reagissent
a l'imbalance persistante) ; VPIN = reactif (inutile en lead) ; regime = throttle de RISQUE pas d'alpha. Donc on teste
un KILL-SWITCH causal : a l'entree du fade, si le flux/vol signale un trend informe DANS le sens de la jambe -> SKIP le fade.

Setup = fade unifie valide (bear: fade down-leg ; bull: fade up-leg + accel-1h), hold 30min, maker RT3.
Filtres (SKIP si fire), seuils = quantiles de l'actif sur le set d'entrees (causal-ish, calib roulante en live) :
  FLOW  : sign(tfi_5min)==jambe ET |tfi_5min|>=Q70  (flux taker soutient la jambe = continuation informee)
  VOLX  : vol_5min/vol_30min >= Q70                 (expansion de vol = evenement)
  SURGE : intensity_5min >= Q70                      (surge d'activite)
  ANY   : FLOW ou VOLX ou SURGE
Question : couper en regime-info TUE-t-il la queue (worst trade / maxDD) SANS tuer l'EV ? (attente lit. : coupe la queue, cout EV modeste)
HL true-mid + feats causaux, tick-level, ZERO sampling. mid-proxy.
"""
import os, sys, time
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import leg_chain, drift_over, PAIRS, WEEKS, MACRO, MIN, GAP

HOUR = 60 * MIN
FEE_MK = 3.0
HOLD = 30 * MIN


def fwd_at(ts, mid, Bs, H):
    n = len(ts)
    jf = np.searchsorted(ts, ts[Bs] + H, side="left")
    ok = jf < n
    f = np.full(len(Bs), np.nan)
    f[ok] = (mid[jf[ok]] - mid[Bs[ok]]) / mid[Bs[ok]] * 1e4
    tf = np.zeros(len(Bs), bool); tf[ok] = (ts[jf[ok]] - ts[Bs[ok]]) > (H + GAP)
    f[tf] = np.nan
    return f


def feat_at(feats, key, Bs, n):
    a = feats.get(key)
    if a is None:
        return np.full(len(Bs), np.nan)
    a = np.asarray(a, np.float64)
    return a[Bs]


def collect_entries(pair):
    """retourne dict d'arrays alignes sur les ENTREES du fade unifie (bear+bull-accel)."""
    TS = []; NET = []; D = []; TFI = []; VOLR = []; INTEN = []
    perwk = []
    for wk, ddir in WEEKS.items():
        try:
            ticks, trades, l2 = load_all(pair, ddir)
            ts, mid, feats = vectorized_features(ticks, trades, l2)
        except Exception:
            continue
        ts = np.asarray(ts, np.int64); mid = np.asarray(mid, np.float64); n = len(ts)
        Bs, ds = leg_chain(ts, mid, 50.0)
        if len(Bs) < 5:
            continue
        dmac = drift_over(ts, mid, MACRO)[Bs]; d1 = drift_over(ts, mid, HOUR)[Bs]
        fwd = fwd_at(ts, mid, Bs, HOLD)
        tfi = feat_at(feats, "tfi_5min", Bs, n)
        v5 = feat_at(feats, "vol_5min", Bs, n); v30 = feat_at(feats, "vol_30min", Bs, n)
        inten = feat_at(feats, "intensity_5min", Bs, n)
        perwk.append((wk, ts[Bs], ds, dmac, d1, fwd, tfi, v5, v30, inten))
    if not perwk:
        return None
    dmac_all = np.concatenate([p[3] for p in perwk]); v = ~np.isnan(dmac_all)
    if v.sum() < 30:
        return None
    q1, q2 = np.quantile(dmac_all[v], [1 / 3, 2 / 3])
    d1_all = np.concatenate([p[4] for p in perwk])
    bullpool = v & (dmac_all >= q2) & ~np.isnan(d1_all)
    m1 = np.median(d1_all[bullpool]) if bullpool.sum() else 0.0
    out = {"ts": [], "net": [], "d": [], "tfi": [], "volr": [], "inten": [], "wk": []}
    for wk, tB, ds, dmac, d1, fwd, tfi, v5, v30, inten in perwk:
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
            out["ts"].append(int(tB[k])); out["net"].append(-d * fwd[k] - FEE_MK); out["d"].append(d)
            out["tfi"].append(tfi[k]); out["volr"].append(v5[k] / v30[k] if v30[k] > 0 else np.nan)
            out["inten"].append(inten[k]); out["wk"].append(wk)
    for kk in out:
        out[kk] = np.array(out[kk], dtype=object if kk == "wk" else float)
    return out


def tailstats(ts, net):
    o = np.argsort(ts); net = net[o]
    n = len(net)
    if n < 6:
        return None
    worst1 = float(np.min(net))
    worst10 = float(np.min(np.convolve(net, np.ones(10), "valid"))) if n >= 10 else float('nan')
    eq = np.cumsum(net); maxdd = float(np.max(np.maximum.accumulate(eq) - eq))
    return np.mean(net), 100 * np.mean(net > 0), worst1, worst10, maxdd, float(eq[-1])


def report(scope, E):
    ts = E["ts"].astype(np.int64); net = E["net"].astype(float); d = E["d"].astype(float)
    tfi = E["tfi"].astype(float); volr = E["volr"].astype(float); inten = E["inten"].astype(float)
    nE = len(net)
    if nE < 12:
        return
    qtfi = np.nanquantile(np.abs(tfi), 0.70); qvol = np.nanquantile(volr, 0.70); qint = np.nanquantile(inten, 0.70)
    fire_flow = (np.sign(tfi) == d) & (np.abs(tfi) >= qtfi)
    fire_volx = volr >= qvol
    fire_surge = inten >= qint
    fire_any = fire_flow | fire_volx | fire_surge
    print(f"\n=== {scope} (n_entrees={nE}) ===")
    print(f"  {'filtre':>10} | {'%gardé':>6} | {'net/tr':>6} {'win%':>5} | {'worst1':>7} {'worst10':>8} {'maxDD':>7} | {'total':>7}")
    print("  " + "-" * 74)
    for lab, fire in (("BASELINE", np.zeros(nE, bool)), ("noFLOW", fire_flow), ("noVOLX", fire_volx),
                      ("noSURGE", fire_surge), ("noANY", fire_any)):
        keep = ~fire
        st = tailstats(ts[keep], net[keep])
        if st is None:
            continue
        mn, win, w1, w10, mdd, tot = st
        print(f"  {lab:>10} | {100*keep.mean():>5.0f}% | {mn:>+6.1f} {win:>5.0f} | {w1:>+7.1f} {w10:>+8.1f} {mdd:>7.0f} | {tot:>+7.0f}")


def main():
    t0 = time.time()
    print("=== FILTRE DE REGIME sur le FADE (kill-switch trend-informe, recherche-informe) — HL true-mid ===")
    print("SKIP le fade si flux soutient la jambe / vol expand / surge. But : couper la QUEUE sans tuer l'EV.\n")
    data = {}
    for pair in PAIRS:
        E = collect_entries(pair)
        if E is not None and len(E["net"]) >= 12:
            data[pair] = E
            report(pair, E)
    maj = [p for p in ("BTCUSDC", "ETHUSDC", "SOLUSDC", "XRPUSDC") if p in data]
    if maj:
        M = {k: np.concatenate([data[p][k] for p in maj]) for k in ("ts", "net", "d", "tfi", "volr", "inten")}
        M["wk"] = np.concatenate([data[p]["wk"] for p in maj])
        report("MAJORS POOLES", M)
    print("\n  LECTURE : un bon filtre = maxDD & worst1/worst10 BAISSENT nettement, net/tr ~ tenu (ou baisse << que la queue).")
    print("    si net s'ecroule autant que la queue -> le filtre coupe surtout des bons fades (info!=noise pas separe).")
    print("    rappel lit. : attendre une COUPE de queue avec cout EV modeste = succes ; alpha-add improbable.")
    print(f"\n[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
