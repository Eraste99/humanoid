"""RIDE / CONTINUATION (la moitie 'trend qui continue' de la vision tout-terrain) — miroir du fade.

Macro = sign(drift_4h). En regime tendanciel, position AVEC le macro (= macro_sign), hold 30min, maker RT3 :
  - sur jambe EXTENSION (d==macro) : RIDE le thrust = MOMENTUM (cense perdre majors / gagner HYPE).
  - sur jambe PULLBACK (d==-macro) : acheter le creux = CONTINUATION (rester dans la tendance) -- LE morceau neuf.
On mesure le net with-macro par actif x leg-type x semaine (+ NULL signe alea). bull+bear pooles (with-macro normalise).
Split force du trend (|drift_4h| moderate vs strong) pour voir si la continuation paie dans les trends FORTS.
HL true-mid, tick-level, ZERO sampling. mid-proxy.
"""
import os, sys, time
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import leg_chain, drift_over, PAIRS, WEEKS, MACRO, MIN, GAP

FEE_MK = 3.0
HOLD = 30 * MIN
RNG = np.random.default_rng(11)


def fwd_at(ts, mid, Bs, H):
    n = len(ts)
    jf = np.searchsorted(ts, ts[Bs] + H, side="left")
    ok = jf < n
    f = np.full(len(Bs), np.nan)
    f[ok] = (mid[jf[ok]] - mid[Bs[ok]]) / mid[Bs[ok]] * 1e4
    tf = np.zeros(len(Bs), bool); tf[ok] = (ts[jf[ok]] - ts[Bs[ok]]) > (H + GAP)
    f[tf] = np.nan
    return f


def wkcell(net, mask, wk, w):
    m = mask & (wk == w) & ~np.isnan(net)
    n = int(m.sum())
    if n < 6:
        return f"{w}:n{n:>3}(insuf)"
    a = net[m]
    return f"{w}:n{n:>3} {np.mean(a):>+6.1f}/{100*np.mean(a>0):>2.0f}%"


def collect(pair):
    F = []; DM = []; D = []; WK = []
    for wk, ddir in WEEKS.items():
        try:
            ticks, trades, l2 = load_all(pair, ddir)
            ts, mid, _ = vectorized_features(ticks, trades, l2)
        except Exception:
            continue
        ts = np.asarray(ts, np.int64); mid = np.asarray(mid, np.float64)
        Bs, ds = leg_chain(ts, mid, 50.0)
        if len(Bs) < 5:
            continue
        F.append(fwd_at(ts, mid, Bs, HOLD)); DM.append(drift_over(ts, mid, MACRO)[Bs])
        D.append(ds); WK.append(np.full(len(Bs), wk, dtype=object))
    if not F:
        return None
    return np.concatenate(F), np.concatenate(DM), np.concatenate(D), np.concatenate(WK)


def analyze(name, fwd, dmac, ds, wk):
    ok = ~(np.isnan(fwd) | np.isnan(dmac))
    if ok.sum() < 30:
        print(f"--- {name} --- (insuffisant)"); return
    q1, q2 = np.quantile(dmac[ok], [1 / 3, 2 / 3])
    msign = np.sign(dmac)
    trending = ok & ((dmac <= q1) | (dmac >= q2))
    ext = trending & (ds == msign)            # extension -> ride = momentum
    pb = trending & (ds == -msign)            # pullback  -> with-macro = continuation
    withmacro = msign * fwd - FEE_MK          # position AVEC le macro
    # null (signe alea) sur le trending
    sm = trending & ~np.isnan(fwd)
    nv = 0.0
    for _ in range(200):
        sg = np.where(RNG.random(int(sm.sum())) < 0.5, 1.0, -1.0)
        nv += np.mean(sg * fwd[sm] - FEE_MK)
    nv /= 200
    print(f"--- {name} ---  (trending n={int(trending.sum())}, NULL={nv:>+5.1f})")
    for lab, mask in (("MOMENTUM (ride ext)", ext), ("CONTINUATION (buy pullback)", pb)):
        pooled = withmacro[mask & ~np.isnan(withmacro)]
        if len(pooled) < 6:
            print(f"    {lab:>28} | n={len(pooled):>3} (insuf)"); continue
        cells = "  ".join(wkcell(withmacro, mask, wk, w) for w in WEEKS if (wk == w).sum() > 0)
        print(f"    {lab:>28} | n={len(pooled):>3} POOL {np.mean(pooled):>+6.1f}/{100*np.mean(pooled>0):>2.0f}%  | {cells}")
    # split force du trend (continuation seulement)
    amag = np.abs(dmac)
    if pb.sum() >= 12:
        medm = np.median(amag[trending])
        for lab, mask in (("CONT trend MODERE", pb & (amag < medm)), ("CONT trend FORT", pb & (amag >= medm))):
            p = withmacro[mask & ~np.isnan(withmacro)]
            if len(p) >= 6:
                print(f"    {lab:>28} | n={len(p):>3} POOL {np.mean(p):>+6.1f}/{100*np.mean(p>0):>2.0f}%")


def main():
    t0 = time.time()
    print("=== RIDE/CONTINUATION (moitie 'trend qui continue') — with-macro, hold 30min maker — HL true-mid ===")
    print("MOMENTUM=ride l'extension ; CONTINUATION=acheter le pullback pour rester dans la tendance. vs NULL.\n")
    data = {}
    for pair in PAIRS:
        r = collect(pair)
        if r is None:
            continue
        data[pair] = r
        analyze(pair, *r)
    maj = [data[p] for p in ("BTCUSDC", "ETHUSDC", "SOLUSDC", "XRPUSDC") if p in data]
    if maj:
        fwd = np.concatenate([m[0] for m in maj]); dmac = np.concatenate([m[1] for m in maj])
        ds = np.concatenate([m[2] for m in maj]); wk = np.concatenate([m[3] for m in maj])
        analyze("MAJORS POOLES", fwd, dmac, ds, wk)
    print("\n  LECTURE : net with-macro. MOMENTUM>0 surtout HYPE (asset-momentum). CONTINUATION>0 = buy-dip-en-trend paie.")
    print("    si CONTINUATION>0 cross-week ET > NULL (surtout trend FORT) -> la moitie RIDE existe = bascule fade/ride possible.")
    print(f"\n[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
