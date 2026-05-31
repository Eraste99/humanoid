"""TEST FRONTAL du SIGNAL UNIFIE (vision user) : UNE strategie fade qui reconnait bull ET bear.

Hypothese (inferee, PAS encore mesuree) : le vrai trigger = SHARPNESS recente 1h. On la teste comme UN signal :
  - regime tendanciel : |drift_4h| extreme -> bear (dmac<=q1) OU bull (dmac>=q2) ; macro_sign=sign(dmac).
  - jambe d'EXTENSION : d == macro_sign (down-leg en downtrend / up-leg en uptrend = le bucket MEAN-REV).
  - SHARP : le 1h a bouge fort DANS le sens macro -> recext = macro_sign*drift_1h >= mediane (parmi extensions).
  - action : FADE = position -d (long le dip / short le rip). hold 30min, maker RT3.
On compare SHARP vs DULL (extension molle) vs NULL (signe aleatoire), PAR SEMAINE, + split bear/bull du SHARP
pour PROUVER que le meme signal marche dans les DEUX regimes. HL true-mid, tick-level, ZERO sampling. mid-proxy.
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
RNG = np.random.default_rng(7)


def fwd_at(ts, mid, Bs, H):
    n = len(ts)
    jf = np.searchsorted(ts, ts[Bs] + H, side="left")
    ok = jf < n
    f = np.full(len(Bs), np.nan)
    f[ok] = (mid[jf[ok]] - mid[Bs[ok]]) / mid[Bs[ok]] * 1e4
    tf = np.zeros(len(Bs), bool); tf[ok] = (ts[jf[ok]] - ts[Bs[ok]]) > (H + GAP)
    f[tf] = np.nan
    return f


def stat(fade, mask):
    m = mask & ~np.isnan(fade)
    n = int(m.sum())
    if n < 6:
        return f"n={n:>3}(insuf)"
    a = fade[m]
    return f"n={n:>3} net{np.mean(a):>+6.1f} win{100*np.mean(a>0):>3.0f}%"


def collect(pair):
    F = []; DM = []; D1 = []; D = []; WK = []
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
        D1.append(drift_over(ts, mid, HOUR)[Bs]); D.append(ds); WK.append(np.full(len(Bs), wk, dtype=object))
    if not F:
        return None
    return (np.concatenate(F), np.concatenate(DM), np.concatenate(D1), np.concatenate(D), np.concatenate(WK))


def analyze(name, fwd, dmac, d1, ds, wk):
    ok = ~(np.isnan(fwd) | np.isnan(dmac) | np.isnan(d1))
    if ok.sum() < 30:
        print(f"--- {name} --- (insuffisant)"); return
    q1, q2 = np.quantile(dmac[ok], [1 / 3, 2 / 3])
    msign = np.sign(dmac)
    trending = ok & ((dmac <= q1) | (dmac >= q2))
    ext = trending & (ds == msign)                  # jambe etend le macro
    recext = msign * d1                              # 1h dans le sens macro
    if (ext).sum() < 12:
        print(f"--- {name} --- (peu d'extensions)"); return
    med = np.median(recext[ext])
    sharp = ext & (recext >= med)
    dull = ext & (recext < med)
    fade = -ds * fwd - FEE_MK
    print(f"--- {name} ---  (extensions n={int(ext.sum())}, seuil recext_1h={med:>+.0f}bps)")
    for w in WEEKS:
        wm = (wk == w)
        if wm.sum() == 0:
            continue
        print(f"    {w}: SHARP {stat(fade, sharp & wm)}   |  DULL {stat(fade, dull & wm)}")
    # preuve d'unification : SHARP marche-t-il dans les DEUX regimes ?
    sb = sharp & (msign < 0); su = sharp & (msign > 0)
    print(f"    SHARP-BEAR {stat(fade, sb)}   |  SHARP-BULL {stat(fade, su)}")
    # null : signe aleatoire sur les memes legs SHARP
    sm = sharp & ~np.isnan(fwd)
    if sm.sum() >= 6:
        nv = 0.0
        for _ in range(200):
            sg = np.where(RNG.random(int(sm.sum())) < 0.5, 1.0, -1.0)
            nv += np.mean(sg * fwd[sm] - FEE_MK)
        print(f"    SHARP net poolE = {np.mean(fade[sm]):>+5.1f}  vs NULL(signe alea) = {nv/200:>+5.1f}  [n={int(sm.sum())}]")


def main():
    t0 = time.time()
    print("=== SIGNAL UNIFIE 'fade la sur-extension 1h sharp' (bull+bear ensemble) — HL true-mid, par semaine ===")
    print("SHARP = jambe etend le macro 4h + le 1h a bouge fort dans ce sens. FADE = long dip / short rip. hold 30min.\n")
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
        d1 = np.concatenate([m[2] for m in maj]); ds = np.concatenate([m[3] for m in maj]); wk = np.concatenate([m[4] for m in maj])
        analyze("MAJORS POOLES (BTC+ETH+SOL+XRP)", fwd, dmac, d1, ds, wk)
    print("\n  LECTURE : si SHARP net>0 sur W507 ET W516, ET SHARP-BEAR>0 ET SHARP-BULL>0, ET SHARP>>NULL")
    print("    -> UN SEUL signal 'fade sur-extension 1h sharp' marche dans les DEUX regimes = la vision user VALIDEE (in-sample).")
    print("    si SHARP~DULL ou ~NULL ou flippe -> l'unification etait une jolie phrase, pas un signal.")
    print(f"\n[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
