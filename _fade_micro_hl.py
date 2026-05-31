"""FADE-EXTENSION + MICROSTRUCTURE (L2 imbalance + taker flow) — SUR HL, vrai mid L2, tick-level, ZERO sampling.

Le fade-extension original (_bt_fade_sustained) etait PRICE-ONLY (vol_30/drift_30/drift_8h). Il n'a JAMAIS
utilise le carnet L2 ni le taker-flow, pourtant dispo sur HL. Ici on BRANCHE la microstructure que Binance
ne peut pas voir :
  ENTREE  : fade-extension (vol30>=P75 & drift30 extreme & drift8h meme sens) FILTRE par l'imbalance du
            carnet qui SOUTIENT la reversion (liq_imb5 dans le sens du fade) et/ou le taker-flow qui se
            retourne en notre faveur (tfi_5min dans le sens du fade).
  SORTIE  : hold / TP-SL / trailing (comme l'original, pour comparer a iso-sortie).
Compare NONE vs +L2 vs +FLOW vs +BOTH. 5 actifs (BTC/ETH/SOL/XRP + HYPE), par semaine, queue. net 3bps.
fade_dir = +1 LONG (dip) / -1 SHORT (rip). liq_signed = liq_imb5*fade_dir (>0 = carnet soutient le fade).
"""
import os, sys, time
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features

PAIRS = ["BTCUSDC", "ETHUSDC", "SOLUSDC", "XRPUSDC", "HYPEUSDC"]
WEEKS = {"W507": r"C:\Users\erast\hl_data\week_20260507",
         "W515": r"C:\Users\erast\hl_data\week_20260515",
         "W516": r"C:\Users\erast\hl_data\week_20260516"}
MIN = 60 * 1000
GAP = 10 * MIN
FEE = 3.0
SUS = 480 * MIN


def drift_over(ts, mid, W):
    p = np.searchsorted(ts, ts - W, side="left")
    d = (mid - mid[p]) / mid[p] * 1e4
    return np.where((ts - ts[p]) >= 0.5 * W, d, np.nan)


def sim_exit(ts, mid, i, direction, mode, params):
    entry = mid[i]; dl = ts[i] + params["maxhold"] * MIN
    jend = np.searchsorted(ts, dl, side="right")
    seg_ts = ts[i:jend]
    if len(seg_ts) < 2:
        return None
    g = np.where(np.diff(seg_ts) > GAP)[0]
    if len(g):
        jend = i + g[0] + 1
    if jend - i < 2:
        return None
    seg = mid[i + 1:jend]
    dft = (seg - entry) / entry * 1e4
    fav = dft if direction == "LONG" else -dft
    if mode == "hold":
        return float(fav[-1])
    if mode == "tpsl":
        tp = params["tp"]; sl = params["sl"]
        tph = np.where(fav >= tp)[0]; slh = np.where(fav <= -sl)[0]
        ti = tph[0] if len(tph) else np.inf; si = slh[0] if len(slh) else np.inf
        if ti < si:
            return float(tp)
        if si < ti:
            return float(-sl)
        return float(fav[-1])
    if mode == "trail":
        arm = params["arm"]; tr = params["trail"]; peak = -1e9; armed = False
        for f in fav:
            if f > peak:
                peak = f
            if peak >= arm:
                armed = True
            if armed and f <= peak - tr:
                return float(f)
        return float(fav[-1])
    return None


def collect(pair, ddir):
    ticks, trades, l2 = load_all(pair, ddir)
    ts, mid, feats = vectorized_features(ticks, trades, l2)
    vol = feats["vol_30min"]; drift = feats["drift_30min"]
    liq = feats["liq_imb5"]; tfi = feats["tfi_5min"]
    valid = ~(np.isnan(vol) | np.isnan(drift))
    if valid.sum() < 200:
        return None
    vP = np.nanpercentile(vol[valid], 75)
    d25 = np.nanpercentile(drift[valid], 25); d75 = np.nanpercentile(drift[valid], 75)
    d8 = drift_over(ts, mid, SUS)
    short_sig = valid & (vol >= vP) & (drift >= d75) & (d8 > 0)
    long_sig = valid & (vol >= vP) & (drift <= d25) & (d8 < 0)
    sr = np.zeros(len(ts), bool); sr[1:] = short_sig[1:] & ~short_sig[:-1]
    lr = np.zeros(len(ts), bool); lr[1:] = long_sig[1:] & ~long_sig[:-1]
    ents = sorted([(int(i), "SHORT") for i in np.where(sr)[0]] + [(int(i), "LONG") for i in np.where(lr)[0]])
    out = []
    for i, d in ents:
        sgn = 1.0 if d == "LONG" else -1.0
        liq_s = liq[i] * sgn if not np.isnan(liq[i]) else np.nan       # >0 = carnet soutient le fade
        tfi_s = tfi[i] * sgn if not np.isnan(tfi[i]) else np.nan       # >0 = taker flow vers le fade
        out.append((i, d, liq_s, tfi_s))
    span = (ts[-1] - ts[0]) / (86400 * 1000)
    return ts, mid, out, span


FILTERS = {
    "NONE":  lambda liq_s, tfi_s: True,
    "+L2":   lambda liq_s, tfi_s: (not np.isnan(liq_s)) and liq_s > 0.10,
    "+FLOW": lambda liq_s, tfi_s: (not np.isnan(tfi_s)) and tfi_s > 0.0,
    "+BOTH": lambda liq_s, tfi_s: (not np.isnan(liq_s)) and (not np.isnan(tfi_s)) and liq_s > 0.10 and tfi_s > 0.0,
}
EXITS = [("hold", {"maxhold": 240}), ("tpsl", {"tp": 80, "sl": 120, "maxhold": 480}),
         ("trail", {"arm": 50, "trail": 25, "maxhold": 480})]


def main():
    t0 = time.time()
    print("=== FADE-EXTENSION + MICROSTRUCTURE (L2 imbalance + taker flow) sur HL — vrai mid, tick-level, AVEC HYPE ===")
    print("entry: fade-extension FILTRE par liq_imb5/tfi_5min dans le sens du fade. net 3bps. NONE/+L2/+FLOW/+BOTH.\n")
    data = {}
    for pair in PAIRS:
        for wk, ddir in WEEKS.items():
            try:
                r = collect(pair, ddir)
            except Exception as e:
                print(f"  [skip] {pair}/{wk}: {e}"); continue
            if r is None:
                continue
            data[(pair, wk)] = r
            ns = sum(1 for _, d, _, _ in r[2] if d == "SHORT")
            print(f"  {pair}/{wk}: {len(r[2])} signaux ({len(r[2])-ns}L/{ns}S)")

    for mode, params in EXITS:
        tag = mode + " " + ",".join(f"{k}{v}" for k, v in params.items() if k != "maxhold")
        print("\n" + "=" * 110)
        print(f"EXIT = {tag} h{params['maxhold']}  | net/trade, n, win%, par semaine, par actif, queue")
        print("=" * 110)
        print(f"  {'filtre':>7} | {'net':>6} {'n':>5} {'win%':>5} {'pire':>6} {'p5':>6} | {'W507':>6} {'W515':>6} {'W516':>6} | "
              + " ".join(f"{p[:3]:>7}" for p in PAIRS))
        for fname, ffun in FILTERS.items():
            allp = []; byw = {w: [] for w in WEEKS}; byp = {p: [] for p in PAIRS}; nd = 0.0
            seen_span = set()
            for (pair, wk), (ts, mid, ents, span) in data.items():
                if (pair, wk) not in seen_span:
                    nd += span; seen_span.add((pair, wk))
                last = -1
                for i, d, liq_s, tfi_s in ents:
                    if ts[i] <= last:
                        continue
                    if not ffun(liq_s, tfi_s):
                        continue
                    pnl = sim_exit(ts, mid, i, d, mode, params)
                    if pnl is None:
                        continue
                    net = pnl - FEE; last = ts[i] + params["maxhold"] * MIN
                    allp.append(net); byw[wk].append(net); byp[pair].append(net)
            if not allp:
                print(f"  {fname:>7} | (aucun)")
                continue
            a = np.array(allp)
            cells = []
            for p in PAIRS:
                cells.append(f"{np.mean(byp[p]):>6.1f}" if len(byp[p]) >= 3 else f"{'·':>7}")
            wkc = [f"{np.mean(byw[w]):>6.1f}" if byw[w] else f"{'·':>6}" for w in WEEKS]
            print(f"  {fname:>7} | {a.mean():>6.1f} {len(a):>5} {100*np.mean(a>0):>5.0f} {a.min():>6.0f} {np.percentile(a,5):>6.0f} | "
                  + " ".join(wkc) + " | " + " ".join(cells))

    print("\n  LECTURE : si +L2/+FLOW/+BOTH > NONE (net + queue + robustesse par actif/semaine), la microstructure HL")
    print("            time la reversion -> c'est l'edge que Binance ne pouvait pas voir. Sinon, le price-only suffisait.")
    print(f"\n[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
