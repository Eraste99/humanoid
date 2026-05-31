"""OOS de la CONSTRUCTION OSCILLATION (pas l'ancien fade-extension) — Binance 4 majors Jan->Mai, tick-level.

But : le pattern HL in-sample (mai, 3 semaines) "FADE bat NULL dans les regimes extremes" est-il ROBUSTE
hors-echantillon, ou un mirage de periode ? On REPLIQUE A L'IDENTIQUE _oscillation_capture.py :
  - jambes zig-zag first-passage 50 bps (les oscillations) ;
  - a chaque fin de jambe B_i : d_i = direction de la jambe ; RIDE=parier d_i, FADE=parier -d_i ;
  - regime au point B_i = tercile du drift_macro 4h (bear/chop/bull) ;
  - PnL = position * retour_fwd(H) - 3bps, H in {30min, 2h} ; NULL = signe aleatoire (200 seeds).
On sort la MEME matrice RIDE/FADE/NULL par actif x regime + la MEME decompo MEAN-REV vs CONTINUATION.

DONNEES : Binance perp USDT-M aggTrades. mid RECONSTRUIT (buy-taker=ask / sell-taker=bid / ffill).
CAVEAT de-bounce : la reconstruction GONFLE la continuation et SOUS-ESTIME le fade. Donc un FADE
positif ici est CONSERVATEUR (le vrai fade book-mid serait >=). Un RIDE positif est a prendre avec prudence.
ZERO sampling : chaque aggTrade = un tick. Blocs contigus (pas de leg a cheval sur un gap) ; pool par actif.
"""
import os, time, zipfile
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

BINANCE_DIR = r"C:\Users\erast\hl_data\binance"
DAILY_DIR = os.path.join(BINANCE_DIR, "daily")
MIN = 60 * 1000
GAP = 10 * MIN
FEE = 3.0
MACRO = 4 * 60 * MIN
HS = {"30m": 30 * MIN, "2h": 120 * MIN}
RNG = np.random.default_rng(21)
NSEED = 200
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]
MONTHS = ["2026-01", "2026-02", "2026-03", "2026-04"]
MAY_A = [f"2026-05-{d:02d}" for d in range(1, 8)]       # 05-01..05-07 (contigu)
MAY_B = [f"2026-05-{d:02d}" for d in range(15, 23)]     # 05-15..05-22 (contigu)


def load_zip(zp):
    """aggTrade -> (ts, mid reconstruit de-bounce). buy-taker=ask / sell-taker=bid / ffill."""
    with zipfile.ZipFile(zp) as z:
        name = z.namelist()[0]
        with z.open(name) as f:
            first = f.readline().decode("utf-8", "ignore")
        hh = "price" in first.lower()
        with z.open(name) as f:
            df = pd.read_csv(f, header=0 if hh else None)
    price = df.iloc[:, 1].values.astype(np.float64); T = df.iloc[:, 5].values.astype(np.int64)
    m = df.iloc[:, 6].astype(str).str.lower().values
    o = np.argsort(T, kind="stable"); T = T[o]; price = price[o]; m = m[o]
    ibt = (m == "false")                                 # buyer is taker -> aggressive BUY -> trade at ASK
    ask = pd.Series(np.where(ibt, price, np.nan)).ffill().values
    bid = pd.Series(np.where(~ibt, price, np.nan)).ffill().values
    mid = (ask + bid) / 2.0; ok = ~np.isnan(mid)
    return T[ok], mid[ok]


def load_block(paths):
    A = []; B = []
    for zp in paths:
        if os.path.exists(zp):
            t, m = load_zip(zp); A.append(t); B.append(m)
    if not A:
        return None
    ts = np.concatenate(A); mid = np.concatenate(B)
    o = np.argsort(ts, kind="stable")
    return ts[o], mid[o]


def drift_over(ts, mid, W):
    p = np.searchsorted(ts, ts - W, side="left")
    d = (mid - mid[p]) / mid[p] * 1e4
    return np.where((ts - ts[p]) >= 0.5 * W, d, np.nan)


def leg_chain(ts, mid, thr_bps=50.0):
    thr = thr_bps / 1e4; n = len(ts); Bs = []; ds = []
    i = 0
    while i < n - 1:
        p = mid[i]; up = p * (1 + thr); dn = p * (1 - thr)
        j = i + 1; found = -1; fdir = 0; win = 4096
        while j < n:
            end = min(n, j + win); seg = mid[j:end]
            hit = (seg >= up) | (seg <= dn)
            if hit.any():
                k = int(np.argmax(hit)); found = j + k
                fdir = 1 if mid[found] >= up else -1; break
            j = end; win = min(win * 4, 4_000_000)
        if found < 0:
            break
        Bs.append(found); ds.append(fdir); i = found
    return np.array(Bs, dtype=int), np.array(ds, dtype=float)


def block_collect(ts, mid):
    Bs, ds = leg_chain(ts, mid)
    if len(Bs) < 10:
        return None, 0
    dmac = drift_over(ts, mid, MACRO)[Bs]
    fwd = {}
    n = len(ts)
    for hk, H in HS.items():
        jf = np.searchsorted(ts, ts[Bs] + H, side="left")
        ok = jf < n
        f = np.full(len(Bs), np.nan)
        f[ok] = (mid[jf[ok]] - mid[Bs[ok]]) / mid[Bs[ok]] * 1e4
        tf = np.zeros(len(Bs), bool); tf[ok] = (ts[jf[ok]] - ts[Bs[ok]]) > (H + GAP)
        f[tf] = np.nan
        fwd[hk] = f
    return dict(d=ds, dmac=dmac, fwd=fwd), len(Bs)


def main():
    t0 = time.time()
    print("=== OOS BINANCE — construction OSCILLATION (RIDE/FADE/NULL par regime) — 4 majors Jan->Mai, tick-level ===")
    print("replique EXACTE de _oscillation_capture (HL in-sample mai). mid reconstruit de-bounce (fade = conservateur).\n")
    A = {s: {"d": [], "dmac": [], "fwd": {hk: [] for hk in HS}} for s in SYMBOLS}
    for sym in SYMBOLS:
        blocks = [(mo, [os.path.join(BINANCE_DIR, f"{sym}-aggTrades-{mo}.zip")]) for mo in MONTHS]
        blocks.append(("MAY-A", [os.path.join(DAILY_DIR, f"{sym}-aggTrades-{d}.zip") for d in MAY_A]))
        blocks.append(("MAY-B", [os.path.join(DAILY_DIR, f"{sym}-aggTrades-{d}.zip") for d in MAY_B]))
        for label, paths in blocks:
            lb = load_block(paths)
            if lb is None:
                print(f"  [skip] {sym}/{label}: pas de fichier"); continue
            ts, mid = lb
            if len(ts) < 5000:
                print(f"  [skip] {sym}/{label}: {len(ts)} ticks"); continue
            r, nlegs = block_collect(ts, mid)
            if r is None:
                print(f"  [skip] {sym}/{label}: {nlegs} jambes (<10)"); continue
            A[sym]["d"].append(r["d"]); A[sym]["dmac"].append(r["dmac"])
            for hk in HS:
                A[sym]["fwd"][hk].append(r["fwd"][hk])
            print(f"  {sym}/{label}: {nlegs} jambes  ({len(ts):,} ticks)")
            del ts, mid

    # ---- MATRICE RIDE / FADE / NULL (replique _oscillation_capture) ----
    for hk in HS:
        print("\n" + "=" * 104)
        print(f"HORIZON H = {hk}  | net bps/jambe par actif x regime : RIDE / FADE / NULL  (n)   [OOS Binance]")
        print("=" * 104)
        print(f"  {'actif':>9} {'regime':>6} | {'RIDE':>7} {'FADE':>7} {'NULL':>7} | {'n':>6} {'meanDrift4h':>11}")
        print("  " + "-" * 72)
        for s in SYMBOLS:
            if not A[s]["d"]:
                continue
            d = np.concatenate(A[s]["d"]); dmac = np.concatenate(A[s]["dmac"]); fwd = np.concatenate(A[s]["fwd"][hk])
            ok = ~(np.isnan(fwd) | np.isnan(dmac))
            d = d[ok]; dmac = dmac[ok]; fwd = fwd[ok]
            if len(d) < 30:
                continue
            q1, q2 = np.quantile(dmac, [1 / 3, 2 / 3])
            regimes = [("bear", dmac <= q1), ("chop", (dmac > q1) & (dmac < q2)), ("bull", dmac >= q2)]
            for rname, rmask in regimes:
                if rmask.sum() < 10:
                    continue
                dd = d[rmask]; ff = fwd[rmask]
                ride = float(np.mean(dd * ff - FEE))
                fade = float(np.mean(-dd * ff - FEE))
                nullv = 0.0
                for _ in range(NSEED):
                    sg = np.where(RNG.random(len(dd)) < 0.5, 1.0, -1.0)
                    nullv += np.mean(sg * ff - FEE)
                nullv /= NSEED
                print(f"  {s:>9} {rname:>6} | {ride:>+7.1f} {fade:>+7.1f} {nullv:>+7.1f} | {int(rmask.sum()):>6} {np.mean(dmac[rmask]):>+11.0f}")

    # ---- DECOMPO MEAN-REV vs CONTINUATION (replique _oscillation_decompose) ----
    for hk in HS:
        print("\n" + "=" * 118)
        print(f"HORIZON H = {hk}  | DECOMPO FADE : MEAN-REV (jambe AVEC macro) vs CONTINUATION (jambe CONTRE macro)  [OOS Binance]")
        print("=" * 118)
        print(f"  {'actif':>9} {'regime':>6} {'bucket':>12} | {'n':>6} {'part%':>5} | {'RIDE':>7} {'FADE':>7} | {'fwd':>7} {'meanDrift4h':>11}")
        print("  " + "-" * 90)
        for s in SYMBOLS:
            if not A[s]["d"]:
                continue
            d = np.concatenate(A[s]["d"]); dmac = np.concatenate(A[s]["dmac"]); fwd = np.concatenate(A[s]["fwd"][hk])
            ok = ~(np.isnan(fwd) | np.isnan(dmac))
            d = d[ok]; dmac = dmac[ok]; fwd = fwd[ok]
            if len(d) < 30:
                continue
            q1, q2 = np.quantile(dmac, [1 / 3, 2 / 3])
            regimes = [("bear", dmac <= q1), ("chop", (dmac > q1) & (dmac < q2)), ("bull", dmac >= q2)]
            for rname, rmask in regimes:
                if rmask.sum() < 10:
                    continue
                m = np.sign(dmac); ntot = int(rmask.sum())
                for bname, bmask in [("MEAN-REV", rmask & (d == m)), ("CONTIN", rmask & (d == -m))]:
                    nb = int(bmask.sum())
                    if nb < 5:
                        print(f"  {s:>9} {rname:>6} {bname:>12} | {nb:>6} {'--':>5} | {'--':>7} {'--':>7} | {'--':>7} {'':>11}")
                        continue
                    dd = d[bmask]; ff = fwd[bmask]
                    ride = float(np.mean(dd * ff - FEE)); fade = float(np.mean(-dd * ff - FEE))
                    print(f"  {s:>9} {rname:>6} {bname:>12} | {nb:>6} {100.0*nb/ntot:>5.0f} | {ride:>+7.1f} {fade:>+7.1f} | "
                          f"{np.mean(ff):>+7.1f} {np.mean(dmac[bmask]):>+11.0f}")
                print("  " + "." * 90)

    print("\n  LECTURE : compare a HL in-sample (mai). Si FADE bat NULL dans les memes regimes extremes ICI aussi")
    print("            -> pattern ROBUSTE (pas un mirage de periode). Si FADE~NULL partout -> c'etait la periode/le venue.")
    print("            rappel de-bounce : FADE positif = conservateur (vrai book-mid >=).")
    print(f"\n[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
