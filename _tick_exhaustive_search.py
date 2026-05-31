"""EXHAUSTIVE SEARCH TICK LEVEL PUR (zero sampling) - 2026-05-28.

1. Features 16 vectorisees a CHAQUE tick (cumsum + searchsorted, pas de loop O(window))
2. Forward outcomes (LONG/SHORT pnl) pre-calcules par tick (TP+50/SL-25, hold 30min)
3. Exhaustive search 2-feature combos pour UP et DOWN
4. Test sur W507 + W515 + W516, trouver combos ROBUSTES (positif sur les 3)

Critere : combined maker_d > 30 (bat HLP) ET positif sur W507 ET W516 (les 2 full weeks).
"""
import os, sys, glob, time
from itertools import combinations, product
import numpy as np
import pandas as pd
from scipy.stats import fisher_exact
import warnings
warnings.filterwarnings('ignore')

PAIR = "BTCUSDC"
WEEKS = {
    "W507": r"C:\Users\erast\hl_data\week_20260507",
    "W515": r"C:\Users\erast\hl_data\week_20260515",
    "W516": r"C:\Users\erast\hl_data\week_20260516",
}
WIN_5M = 5*60*1000
WIN_15M = 15*60*1000
WIN_30M = 30*60*1000
WIN_1H = 60*60*1000
WIN_30S = 30*1000
TP_BPS = 50.0
SL_BPS = 25.0
TIME_MIN = 30


def load_all(pair, data_dir):
    ft = sorted(glob.glob(os.path.join(data_dir, f"ticks_{pair}_*.csv")))
    ticks = pd.concat([pd.read_csv(f, usecols=["ts_ms","mid"]) for f in ft], ignore_index=True)
    ticks = ticks.sort_values("ts_ms").drop_duplicates(subset=["ts_ms"]).reset_index(drop=True)
    ftr = sorted(glob.glob(os.path.join(data_dir, f"trades_{pair}_*.csv")))
    trades = pd.concat([pd.read_csv(f, usecols=["ts_ms","side","qty","px"]) for f in ftr], ignore_index=True)
    trades = trades.sort_values("ts_ms").reset_index(drop=True)
    trades = trades[trades["qty"]>0].dropna().reset_index(drop=True)
    trades["notional"] = trades["qty"]*trades["px"]
    trades["is_buy"] = (trades["side"].str.upper()=="BUY").astype(np.int8)
    fl2 = sorted(glob.glob(os.path.join(data_dir, f"l2_{pair}_*.csv")))
    l2 = pd.concat([pd.read_csv(f) for f in fl2], ignore_index=True)
    l2 = l2.sort_values("ts_ms").drop_duplicates(subset=["ts_ms"]).reset_index(drop=True)
    l2["liq_b5"] = sum(l2[f"b{i}p"]*l2[f"b{i}s"] for i in range(1,6))
    l2["liq_a5"] = sum(l2[f"a{i}p"]*l2[f"a{i}s"] for i in range(1,6))
    l2["spread_bps"] = np.where(l2["b1p"]>0, (l2["a1p"]-l2["b1p"])/l2["b1p"]*1e4, 0.0)
    return ticks, trades, l2


def vectorized_features(ticks, trades, l2):
    """Compute 16 features a CHAQUE tick, full vectorise."""
    ts = ticks["ts_ms"].values.astype(np.int64)
    mid = ticks["mid"].values.astype(np.float64)
    n = len(ts)

    feats = {}

    # --- DRIFT (searchsorted + indexing) ---
    for win, name in [(WIN_5M,"drift_5min"),(WIN_15M,"drift_15min"),(WIN_30M,"drift_30min"),(WIN_1H,"drift_1h")]:
        j = np.searchsorted(ts, ts - win, side="left")
        d = (mid - mid[j]) / mid[j] * 1e4
        d[j >= np.arange(n)] = np.nan  # no lookback available
        feats[name] = d

    # --- VOL (std of log-returns via cumsum) ---
    logmid = np.log(mid)
    lr = np.diff(logmid, prepend=logmid[0]) * 1e4  # lr[k] = return ending at k (bps); lr[0]=0
    C_lr = np.concatenate([[0], np.cumsum(lr)])
    C_lr2 = np.concatenate([[0], np.cumsum(lr*lr)])
    for win, name in [(WIN_5M,"vol_5min"),(WIN_30M,"vol_30min")]:
        j = np.searchsorted(ts, ts - win, side="left")
        cnt = np.arange(n) - j
        sum_lr = C_lr[np.arange(n)+1] - C_lr[j+1]
        sum_lr2 = C_lr2[np.arange(n)+1] - C_lr2[j+1]
        with np.errstate(invalid='ignore', divide='ignore'):
            mean = sum_lr / cnt
            var = sum_lr2 / cnt - mean*mean
            vol = np.sqrt(np.maximum(var, 0))
        vol[cnt < 10] = np.nan
        feats[name] = vol

    # --- TRADES (cumsum + searchsorted on trade ts) ---
    tr_ts = trades["ts_ms"].values.astype(np.int64)
    tr_not = trades["notional"].values.astype(np.float64)
    tr_buy = trades["is_buy"].values.astype(bool)
    C_not = np.concatenate([[0], np.cumsum(tr_not)])
    C_buy = np.concatenate([[0], np.cumsum(np.where(tr_buy, tr_not, 0.0))])
    C_large = np.concatenate([[0], np.cumsum((tr_not > 10000).astype(np.float64))])

    lo5 = np.searchsorted(tr_ts, ts - WIN_5M, side="left")
    hi5 = np.searchsorted(tr_ts, ts, side="right")
    ntr = hi5 - lo5
    total = C_not[hi5] - C_not[lo5]
    buy = C_buy[hi5] - C_buy[lo5]
    sell = total - buy
    with np.errstate(invalid='ignore', divide='ignore'):
        feats["tfi_5min"] = np.where(total > 0, (buy - sell)/np.maximum(total,1), np.nan)
        feats["intensity_5min"] = ntr / 300.0
        feats["volume_total_5min"] = total
        feats["avg_size_5min"] = np.where(ntr > 0, total/np.maximum(ntr,1), np.nan)
        feats["n_large_5min"] = C_large[hi5] - C_large[lo5]
    # buy_ratio_last30s
    lo30 = np.searchsorted(tr_ts, ts - WIN_30S, side="left")
    tot30 = C_not[hi5] - C_not[lo30]
    buy30 = C_buy[hi5] - C_buy[lo30]
    with np.errstate(invalid='ignore', divide='ignore'):
        feats["buy_ratio_30s"] = np.where(tot30 > 0, buy30/np.maximum(tot30,1), np.nan)
    # mark invalid where too few trades
    for k in ["tfi_5min","intensity_5min","volume_total_5min","avg_size_5min","n_large_5min","buy_ratio_30s"]:
        feats[k] = np.where(ntr < 10, np.nan, feats[k])

    # --- L2 (cumsum + searchsorted on l2 ts) ---
    l2_ts = l2["ts_ms"].values.astype(np.int64)
    b1s = l2["b1s"].values.astype(np.float64)
    a1s = l2["a1s"].values.astype(np.float64)
    liqb = l2["liq_b5"].values.astype(np.float64)
    liqa = l2["liq_a5"].values.astype(np.float64)
    spr = l2["spread_bps"].values.astype(np.float64)
    C_b1s = np.concatenate([[0], np.cumsum(b1s)])
    C_a1s = np.concatenate([[0], np.cumsum(a1s)])
    C_liqb = np.concatenate([[0], np.cumsum(liqb)])
    C_liqa = np.concatenate([[0], np.cumsum(liqa)])
    C_spr = np.concatenate([[0], np.cumsum(spr)])
    l2lo = np.searchsorted(l2_ts, ts - WIN_5M, side="left")
    l2hi = np.searchsorted(l2_ts, ts, side="right")
    nl2 = l2hi - l2lo
    sum_b1s = C_b1s[l2hi]-C_b1s[l2lo]; sum_a1s = C_a1s[l2hi]-C_a1s[l2lo]
    sum_liqb = C_liqb[l2hi]-C_liqb[l2lo]; sum_liqa = C_liqa[l2hi]-C_liqa[l2lo]
    sum_spr = C_spr[l2hi]-C_spr[l2lo]
    with np.errstate(invalid='ignore', divide='ignore'):
        feats["top_imb"] = (sum_b1s - sum_a1s)/np.maximum(sum_b1s + sum_a1s, 1e-9)
        feats["liq_imb5"] = (sum_liqb - sum_liqa)/np.maximum(sum_liqb + sum_liqa, 1e-9)
        feats["spread_mean"] = sum_spr/np.maximum(nl2,1)
    for k in ["top_imb","liq_imb5","spread_mean"]:
        feats[k] = np.where(nl2 < 50, np.nan, feats[k])

    return ts, mid, feats


def precompute_forward_outcomes(ts, mid):
    """Pour chaque tick, calcule outcome LONG et SHORT (TP+50/SL-25, hold 30min)."""
    n = len(ts)
    long_pnl = np.zeros(n)
    short_pnl = np.zeros(n)
    deadline_idx = np.searchsorted(ts, ts + TIME_MIN*60*1000, side="right")
    t0 = time.time(); last_log = t0
    for i in range(n):
        if time.time() - last_log > 20:
            print(f"      fwd outcome {i:,}/{n:,} ({100*i/n:.0f}%) {time.time()-t0:.0f}s")
            last_log = time.time()
        entry = mid[i]
        jend = min(deadline_idx[i], n)
        # LONG : TP +50, SL -25
        lpnl = None; spnl = None
        up_tp = entry*(1+TP_BPS/1e4); up_sl = entry*(1-SL_BPS/1e4)
        dn_tp = entry*(1-TP_BPS/1e4); dn_sl = entry*(1+SL_BPS/1e4)
        for j in range(i+1, jend):
            m = mid[j]
            if lpnl is None:
                if m >= up_tp: lpnl = TP_BPS
                elif m <= up_sl: lpnl = -SL_BPS
            if spnl is None:
                if m <= dn_tp: spnl = TP_BPS
                elif m >= dn_sl: spnl = -SL_BPS
            if lpnl is not None and spnl is not None:
                break
        if lpnl is None:
            mlast = mid[jend-1] if jend > i+1 else entry
            lpnl = (mlast - entry)/entry*1e4
        if spnl is None:
            mlast = mid[jend-1] if jend > i+1 else entry
            spnl = -(mlast - entry)/entry*1e4
        long_pnl[i] = lpnl
        short_pnl[i] = spnl
    print(f"      fwd outcomes done {time.time()-t0:.0f}s")
    return long_pnl, short_pnl


def build_atoms(feats, ts):
    """Genere conditions atomiques (feature, ineq, quantile_value)."""
    atoms = {}  # name -> boolean mask
    for fname, arr in feats.items():
        valid = ~np.isnan(arr)
        if valid.sum() < 1000: continue
        qs = np.nanpercentile(arr, [10,25,50,75,90])
        for q, qn in zip(qs, ["P10","P25","P50","P75","P90"]):
            for ineq in ["<",">="]:
                mask = (arr < q) if ineq=="<" else (arr >= q)
                mask = mask & valid
                if mask.sum() < 200: continue
                atoms[f"{fname}{ineq}{qn}"] = mask
    return atoms


def eval_strategy(up_mask, dn_mask, long_pnl, short_pnl, ts, seg_days):
    """Eval bot : rising edge UP -> LONG, rising edge DOWN -> SHORT, cooldown via outcome time."""
    n = len(ts)
    up_rise = np.zeros(n, dtype=bool); up_rise[1:] = up_mask[1:] & ~up_mask[:-1]
    dn_rise = np.zeros(n, dtype=bool); dn_rise[1:] = dn_mask[1:] & ~dn_mask[:-1]
    deadline_idx = np.searchsorted(ts, ts + TIME_MIN*60*1000, side="right")
    pnls = []
    exit_until = -1
    for i in range(n):
        if i <= exit_until: continue
        if up_rise[i] and not dn_rise[i]:
            pnls.append(long_pnl[i]); exit_until = deadline_idx[i]
        elif dn_rise[i] and not up_rise[i]:
            pnls.append(short_pnl[i]); exit_until = deadline_idx[i]
    if not pnls: return None
    pnls = np.array(pnls)
    n_tr = len(pnls); cum = pnls.sum()
    return {"n": n_tr, "cum": cum, "win": (pnls>0).mean(),
            "maker_d": (cum - 3*n_tr)/seg_days, "taker_d": (cum - 6*n_tr)/seg_days}


def main():
    t0 = time.time()
    print("=== EXHAUSTIVE SEARCH TICK LEVEL PUR (no sampling) ===\n")

    data = {}
    for wk, ddir in WEEKS.items():
        print(f"[{wk}] load + features + forward outcomes (tick level)")
        ticks, trades, l2 = load_all(PAIR, ddir)
        days = (ticks["ts_ms"].iloc[-1]-ticks["ts_ms"].iloc[0])/1000/86400
        print(f"  ticks={len(ticks):,} trades={len(trades):,} l2={len(l2):,} {days:.2f}j")
        ts, mid, feats = vectorized_features(ticks, trades, l2)
        long_pnl, short_pnl = precompute_forward_outcomes(ts, mid)
        atoms = build_atoms(feats, ts)
        data[wk] = {"ts": ts, "mid": mid, "feats": feats, "atoms": atoms,
                    "long_pnl": long_pnl, "short_pnl": short_pnl, "days": days}
        print(f"  {len(atoms)} atomic conditions\n")

    # Use W507 atom names as canonical (must exist in all weeks)
    common_atoms = set(data["W507"]["atoms"].keys())
    for wk in WEEKS:
        common_atoms &= set(data[wk]["atoms"].keys())
    common_atoms = sorted(common_atoms)
    print(f"Common atoms across 3 weeks : {len(common_atoms)}\n")

    # Build single-atom and 2-atom UP/DOWN conditions
    # UP candidates : atoms that have positive long edge IS (W507)
    # DOWN candidates : atoms with positive short edge IS
    print("Pre-screening atoms by W507 in-sample edge...")
    up_atoms = []; dn_atoms = []
    for a in common_atoms:
        m = data["W507"]["atoms"][a]
        lp = data["W507"]["long_pnl"][m]
        sp = data["W507"]["short_pnl"][m]
        if len(lp) >= 200 and lp.mean() > 1.0: up_atoms.append(a)
        if len(sp) >= 200 and sp.mean() > 1.0: dn_atoms.append(a)
    print(f"  UP atoms (long edge>1bps W507): {len(up_atoms)}")
    print(f"  DOWN atoms (short edge>1bps W507): {len(dn_atoms)}\n")

    # Generate UP conditions (singles + pairs) and DOWN conditions (singles + pairs)
    def gen_conditions(atom_list, week_atoms):
        conds = []
        for a in atom_list:
            conds.append((a,))
        for a, b in combinations(atom_list, 2):
            conds.append((a, b))
        return conds

    up_conds = gen_conditions(up_atoms, None)
    dn_conds = gen_conditions(dn_atoms, None)
    print(f"UP conditions: {len(up_conds)}, DOWN conditions: {len(dn_conds)}")
    print(f"Total strategy pairs (capped): evaluating...\n")

    # To limit explosion, evaluate UP and DOWN separately first, keep top robust ones, then combine
    def mask_for_cond(cond, week):
        m = np.ones(len(data[week]["ts"]), dtype=bool)
        for a in cond:
            m = m & data[week]["atoms"][a]
        return m

    # Evaluate UP conditions (LONG only) across weeks
    def eval_dir(conds, direction):
        results = []
        for cond in conds:
            r = {}
            ok = True
            for wk in WEEKS:
                m = mask_for_cond(cond, wk)
                rise = np.zeros(len(m), dtype=bool); rise[1:] = m[1:] & ~m[:-1]
                ts = data[wk]["ts"]
                deadline_idx = np.searchsorted(ts, ts + TIME_MIN*60*1000, side="right")
                pnl_arr = data[wk]["long_pnl"] if direction=="LONG" else data[wk]["short_pnl"]
                pnls = []; exit_until = -1
                for i in np.where(rise)[0]:
                    if i <= exit_until: continue
                    pnls.append(pnl_arr[i]); exit_until = deadline_idx[i]
                if len(pnls) < 5: ok = False; break
                pnls = np.array(pnls)
                r[wk] = {"n": len(pnls), "cum": pnls.sum(),
                         "maker_d": (pnls.sum()-3*len(pnls))/data[wk]["days"]}
            if ok:
                results.append({"cond": cond, "direction": direction, **{wk: r[wk] for wk in WEEKS}})
        return results

    print("Evaluating UP (LONG) conditions...")
    up_res = eval_dir(up_conds, "LONG")
    print(f"  {len(up_res)} UP conditions with >=5 trades all weeks")
    print("Evaluating DOWN (SHORT) conditions...")
    dn_res = eval_dir(dn_conds, "SHORT")
    print(f"  {len(dn_res)} DOWN conditions with >=5 trades all weeks\n")

    # Find ROBUST single-direction conditions (positive maker_d on W507 AND W516)
    def robust_filter(results):
        robust = []
        for r in results:
            m507 = r["W507"]["maker_d"]; m516 = r["W516"]["maker_d"]
            if m507 > 0 and m516 > 0:
                comb = (r["W507"]["cum"] + r["W516"]["cum"] - 3*(r["W507"]["n"]+r["W516"]["n"])) / (data["W507"]["days"]+data["W516"]["days"])
                robust.append((r, comb))
        robust.sort(key=lambda x: -x[1])
        return robust

    print("="*80)
    print("UP (LONG) conditions ROBUST (positive W507 AND W516):")
    up_robust = robust_filter(up_res)
    print(f"  {len(up_robust)} robust UP conditions")
    for r, comb in up_robust[:15]:
        print(f"  {' AND '.join(r['cond']):<55} | W507 {r['W507']['maker_d']:>+6.1f} | W515 {r['W515']['maker_d']:>+6.1f} | W516 {r['W516']['maker_d']:>+6.1f} | comb {comb:>+6.1f}")

    print("\nDOWN (SHORT) conditions ROBUST (positive W507 AND W516):")
    dn_robust = robust_filter(dn_res)
    print(f"  {len(dn_robust)} robust DOWN conditions")
    for r, comb in dn_robust[:15]:
        print(f"  {' AND '.join(r['cond']):<55} | W507 {r['W507']['maker_d']:>+6.1f} | W515 {r['W515']['maker_d']:>+6.1f} | W516 {r['W516']['maker_d']:>+6.1f} | comb {comb:>+6.1f}")

    # FINAL VERDICT
    print("\n" + "="*80)
    print("VERDICT")
    print("="*80)
    n_up_beat = sum(1 for r,c in up_robust if c > 30)
    n_dn_beat = sum(1 for r,c in dn_robust if c > 30)
    print(f"  UP conditions robust (pos both weeks): {len(up_robust)}, dont {n_up_beat} battent HLP (comb>30)")
    print(f"  DOWN conditions robust (pos both weeks): {len(dn_robust)}, dont {n_dn_beat} battent HLP (comb>30)")
    if n_up_beat > 0 or n_dn_beat > 0:
        print(f"\n  -> Des conditions robustes ET battant HLP existent au tick level !")
    elif len(up_robust) > 0 or len(dn_robust) > 0:
        print(f"\n  -> Des conditions robustes existent mais aucune ne bat HLP (>30).")
    else:
        print(f"\n  -> AUCUNE condition robuste (positive sur W507 ET W516) au tick level.")

    print(f"\n[done] total {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
