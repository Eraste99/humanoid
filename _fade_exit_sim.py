"""SIM DE SORTIE — fade-extension : l'EV NETTE d'une vraie regle TP/SL/timeout bat-elle ride & null ? OOS ?

Le temps-de-capture (script jumeau) a montre : toucher +7 = ~1min/95% = propriete de VOL, pas une preuve.
Ici on price la VRAIE strat : a chaque fin de jambe 50bps en REGIME TENDANCIEL (bear/bull, terciles drift_4h),
on simule plusieurs regles de sortie en marche tick-par-tick (first-passage TP-vs-SL, gap-aware, cap 2h) pour
3 positions : FADE (-d), RIDE (+d), NULL (= moyenne fade/ride, identite exacte du signe aleatoire).
Frais REALISTES : entree maker ; sortie TP = limit (maker) -> RT 3bps ; sortie SL/timeout = market (taker) -> RT 6bps.
Sortie : net bps/trade FADE vs RIDE vs NULL + win% FADE, par actif/regime, sur HL (in-sample) ET Binance (OOS 5 mois).

Verdict cherche : FADE net > 0 ET > NULL, en bear ET bull, sur les majors, ET OOS -> 1ere strat deployable.
HL vrai mid L2 ; Binance mid reconstruit de-bounce (fade conservateur). tick-level, ZERO sampling.
"""
import os, sys, time, zipfile
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features

MIN = 60 * 1000
GAP = 10 * MIN
MACRO = 4 * 60 * MIN
CAP = 120 * MIN
FEE_MK = 3.0      # RT maker-maker (sortie TP limit)
FEE_TK = 6.0      # RT maker-in + taker-out (sortie SL/timeout market)
CONFIGS = [("tp7sl10", 7.0, 10.0), ("tp10sl15", 10.0, 15.0), ("tp7sl20", 7.0, 20.0), ("hold2h", None, None)]

HL_PAIRS = ["HYPEUSDC", "BTCUSDC", "ETHUSDC", "SOLUSDC", "XRPUSDC"]
HL_WEEKS = {"W507": r"C:\Users\erast\hl_data\week_20260507",
            "W515": r"C:\Users\erast\hl_data\week_20260515",
            "W516": r"C:\Users\erast\hl_data\week_20260516"}
BIN_DIR = r"C:\Users\erast\hl_data\binance"
BIN_DAILY = os.path.join(BIN_DIR, "daily")
BIN_SYMS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]
MONTHS = ["2026-01", "2026-02", "2026-03", "2026-04"]
MAY_A = [f"2026-05-{d:02d}" for d in range(1, 8)]
MAY_B = [f"2026-05-{d:02d}" for d in range(15, 23)]


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


def sim(ts, mid, ib, pos, tp, sl):
    """marche TP-vs-SL first-passage (gap-aware, cap 2h). retourne net bps (frais selon type de sortie)."""
    entry = mid[ib]; t0 = ts[ib]
    jend = np.searchsorted(ts, t0 + CAP, side="right")
    if jend <= ib + 1:
        return None
    seg_ts = ts[ib + 1:jend]; seg = mid[ib + 1:jend]
    g = np.where(np.diff(np.concatenate([[t0], seg_ts])) > GAP)[0]
    if len(g):
        seg = seg[:g[0]]
    if len(seg) < 1:
        return None
    fav = pos * (seg - entry) / entry * 1e4
    if tp is None:
        return float(fav[-1]) - FEE_TK
    tph = np.where(fav >= tp)[0]; slh = np.where(fav <= -sl)[0]
    ti = tph[0] if len(tph) else 10**9; si = slh[0] if len(slh) else 10**9
    if ti == 10**9 and si == 10**9:
        return float(fav[-1]) - FEE_TK
    if ti <= si:
        return float(tp) - FEE_MK
    return float(-sl) - FEE_TK


def process(name, wk_data, acc):
    dmacs = [w[4] for w in wk_data]
    alld = np.concatenate(dmacs); v = ~np.isnan(alld)
    if v.sum() < 30:
        return
    q1, q2 = np.quantile(alld[v], [1 / 3, 2 / 3])
    for ts, mid, Bs, ds, dmac in wk_data:
        for k in range(len(Bs)):
            dm = dmac[k]
            if np.isnan(dm):
                continue
            reg = "bear" if dm <= q1 else ("bull" if dm >= q2 else None)
            if reg is None:
                continue
            d = float(ds[k]); ib = int(Bs[k])
            for cn, tp, sl in CONFIGS:
                fa = sim(ts, mid, ib, -d, tp, sl)
                ri = sim(ts, mid, ib, d, tp, sl)
                if fa is None or ri is None:
                    continue
                acc.setdefault((name, reg, cn), []).append((fa, ri))


def report(title, acc):
    print("\n" + "=" * 110)
    print(f"{title}  | net bps/trade (frais reels) : FADE vs RIDE vs NULL ; win% du FADE")
    print("=" * 110)
    print(f"  {'actif':>9} {'regime':>5} {'exit':>9} | {'n':>5} | {'FADE':>7} {'RIDE':>7} {'NULL':>7} | {'FADEwin%':>8}")
    print("  " + "-" * 70)
    names = sorted(set(k[0] for k in acc))
    for nm in names:
        for reg in ("bear", "bull"):
            for cn, _, _ in CONFIGS:
                key = (nm, reg, cn)
                if key not in acc or len(acc[key]) < 20:
                    continue
                arr = np.array(acc[key])
                fa = arr[:, 0]; ri = arr[:, 1]
                fmean = float(np.mean(fa)); rmean = float(np.mean(ri)); nmean = 0.5 * (fmean + rmean)
                win = 100.0 * np.mean(fa > 0)
                print(f"  {nm:>9} {reg:>5} {cn:>9} | {len(fa):>5} | {fmean:>+7.1f} {rmean:>+7.1f} {nmean:>+7.1f} | {win:>8.1f}")
            print("  " + "." * 70)


# ---------- HL ----------
def build_hl():
    acc = {}
    for pair in HL_PAIRS:
        wk_data = []
        for wk, ddir in HL_WEEKS.items():
            try:
                ticks, trades, l2 = load_all(pair, ddir)
                ts, mid, _ = vectorized_features(ticks, trades, l2)
            except Exception as e:
                print(f"  [skip] {pair}/{wk}: {e}"); continue
            ts = np.asarray(ts, np.int64); mid = np.asarray(mid, np.float64)
            Bs, ds = leg_chain(ts, mid)
            if len(Bs) < 10:
                continue
            dmac = drift_over(ts, mid, MACRO)[Bs]
            wk_data.append((ts, mid, Bs, ds, dmac))
        if wk_data:
            process(pair, wk_data, acc)
            print(f"  [HL] {pair} ok")
    return acc


# ---------- Binance ----------
def load_zip(zp):
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
    ibt = (m == "false")
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


def build_binance():
    acc = {}
    for sym in BIN_SYMS:
        blocks = [(mo, [os.path.join(BIN_DIR, f"{sym}-aggTrades-{mo}.zip")]) for mo in MONTHS]
        blocks.append(("MAY-A", [os.path.join(BIN_DAILY, f"{sym}-aggTrades-{d}.zip") for d in MAY_A]))
        blocks.append(("MAY-B", [os.path.join(BIN_DAILY, f"{sym}-aggTrades-{d}.zip") for d in MAY_B]))
        wk_data = []
        for label, paths in blocks:
            lb = load_block(paths)
            if lb is None:
                continue
            ts, mid = lb
            if len(ts) < 5000:
                continue
            Bs, ds = leg_chain(ts, mid)
            if len(Bs) < 10:
                continue
            dmac = drift_over(ts, mid, MACRO)[Bs]
            wk_data.append((ts, mid, Bs, ds, dmac))
            del ts, mid
        if wk_data:
            process(sym, wk_data, acc)
            print(f"  [BIN] {sym} ok")
    return acc


def main():
    t0 = time.time()
    print("=== SIM DE SORTIE fade-extension : EV nette TP/SL/timeout, FADE vs RIDE vs NULL, frais reels ===")
    print("HL in-sample (vrai mid) PUIS Binance OOS 5 mois (mid reconstruit, fade conservateur).\n")
    print("--- HL (in-sample) ---")
    hl = build_hl()
    report("HL IN-SAMPLE (vrai mid L2)", hl)
    print("\n--- Binance (OOS, chargement ~10min) ---")
    bn = build_binance()
    report("BINANCE OOS Jan->Mai (mid reconstruit de-bounce)", bn)
    print("\n  LECTURE : FADE>0 ET FADE>NULL en bear+bull sur majors, ET OOS -> edge net deployable.")
    print("            si FADE~NULL ou <0 net -> la regle de sortie mange l'edge (comme les autres edges fins du projet).")
    print("            hold2h = horizon valide (sortie market RT6) ; tp/sl = regles realistes.")
    print(f"\n[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
