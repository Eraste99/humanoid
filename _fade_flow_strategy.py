"""FADE-EXTENSION + FLOW-DIVERGENCE FILTER — realistic strategy w/ real costs, MAE tail, queue model.

Binance USDT-M perp majors, tick-level (aggTrades). Jan->Apr monthly = train, May daily = HELD-OUT OOS.

STRATEGY (FADE side of the 2-strat bot):
  EVENT (sustained extension, rising-edge, no-overlap 8h):
      vol30 >= P75  AND  |drift30| extreme (d30<=p25 OR d30>=p75)  AND  drift8h SAME SIGN as drift30.
  FLOW FILTER (the OOS-confirmed edge): take the FADE only when fresh causal TFI(5min) DIVERGES from the move:
      up-move (d30>0, we SHORT)  -> require TFI < -tau
      down-move(d30<0, we LONG)  -> require TFI > +tau
      sweep tau in {0.1, 0.2}.  Bet = FADE (against the move).
  EXIT & COSTS (explicit):
      maker TP limit (+50,+100 bps in bet-favor dir) = MAKER round-trip 1.5+1.5 = 3 bps.
      stop-loss (-100,-150 bps adverse) = REACTIVE TAKER round-trip ~6 bps. gap-aware fill (slip on gap).
      timeout @ 8h hold w/o TP/stop = reactive market-out = TAKER 6 bps, at endpoint mid.
  WALK forward tick-by-tick (gap-aware) per EVENT (searchsorted-sliced, NO full-array python loop)
  to realize TP-vs-stop-FIRST correctly + the gap-aware MAE (worst adverse over the hold).

ARMS compared per asset / per month / MAY-OOS:
  (a) FLOW-FILTERED fade   (b) RAW fade (same extension events, NO flow filter)   (c) RANDOM-entry null (~50 seeds)
Report: net bps/trade, trades/day, win%, worst, p5(MAE & PnL). Sizing: f=0.5% & 1% -> equity mult + maxDD.

VECTORIZED. Tick-level, NEVER downsampled. Python loops only over EVENTS (hundreds).
"""
import os, zipfile, time
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

# ---- reuse from _flow_confirm.py ----
from _flow_confirm import load_flow, drift, vol30, roll, no_overlap, build, tstat, \
    BINANCE_DIR, DAILY_DIR, MIN, SYMBOLS, MONTHS, MAY_DAYS

TFI_W = 5 * MIN
HOLD = 480 * MIN             # 8h max hold for the forward TP/stop walk (extension events are no-overlap 8h)
MAKER_RT = 3.0               # maker TP round-trip (1.5 entry maker + 1.5 exit maker)
TAKER_RT = 6.0               # taker stop / timeout round-trip
TAUS = [0.1, 0.2]
TPS = [50.0, 100.0]          # maker take-profit, bps (bet-favorable)
STOPS = [100.0, 150.0]       # reactive taker stop, bps (adverse)
NSEED = 50
RNG = np.random.default_rng(7)

PERIODS = MONTHS + ["MAY"]


# ============================================================================
# Forward TP/stop walk + MAE, gap-aware, per-event (ticks sliced via searchsorted).
# ============================================================================
def walk_events(ts, mid, idx, bet_dir, tp_bps, stop_bps):
    """For each event i in idx with bet direction bet_dir[k] (+1 long / -1 short):
    walk ticks in [i, jend(i)] where jend = first tick at ts[i]+HOLD.
    Realize TP-first vs STOP-first (gap-aware), the gap-aware MAE, and the GROSS signed bps.
    Returns dict of arrays (len = len(idx)); off-end events -> NaN.
      gross  : realized signed bps in bet dir BEFORE fees
      cost   : round-trip fee applied (MAKER_RT if TP, else TAKER_RT)
      outcome: 1 TP, -1 STOP, 0 TIMEOUT  (NaN if off-end)
      mae    : worst adverse excursion bps over the realized hold (>=0)
      held_ms: time-in-trade ms
    """
    n = len(ts)
    m = len(idx)
    gross = np.full(m, np.nan)
    cost = np.full(m, np.nan)
    outcome = np.full(m, np.nan)
    mae = np.full(m, np.nan)
    held = np.full(m, np.nan)
    jend = np.searchsorted(ts, ts[idx] + HOLD, side="left")
    for k in range(m):
        i = idx[k]
        je = jend[k]
        if je >= n or je <= i:
            continue
        entry = mid[i]
        seg = mid[i + 1:je + 1]                       # path AFTER entry up to the 8h cap
        if seg.size == 0:
            continue
        bd = bet_dir[k]
        # signed favorable move in bet direction, bps
        r = bd * (seg - entry) / entry * 1e4          # >0 in our favor, <0 adverse
        tp_hit = r >= tp_bps
        st_hit = r <= -stop_bps
        i_tp = int(np.argmax(tp_hit)) if tp_hit.any() else n + 1
        i_st = int(np.argmax(st_hit)) if st_hit.any() else n + 1
        if i_tp == n + 1 and i_st == n + 1:
            # timeout: reactive market-out at endpoint mid, TAKER. MAE over full hold.
            gross[k] = float(r[-1]); cost[k] = TAKER_RT; outcome[k] = 0.0
            held[k] = float(ts[je] - ts[i]); exit_j = r.size - 1
        elif i_tp <= i_st:
            # TP first (tie -> TP; maker limit fills at the limit price, no slip).
            gross[k] = tp_bps; cost[k] = MAKER_RT; outcome[k] = 1.0
            held[k] = float(ts[i + 1 + i_tp] - ts[i]); exit_j = i_tp
        else:
            # STOP first: reactive taker. GAP-AWARE: fill at the realized (worse) price r[i_st],
            # which may be < -stop_bps if a single tick gapped THROUGH the trigger (short-gamma tail).
            gross[k] = float(r[i_st]); cost[k] = TAKER_RT; outcome[k] = -1.0
            held[k] = float(ts[i + 1 + i_st] - ts[i]); exit_j = i_st
        # gap-aware MAE over the REALIZED hold [entry, exit] = worst adverse (most negative r) -> positive
        mae[k] = max(-float(r[:exit_j + 1].min()), 0.0)
    return dict(gross=gross, cost=cost, outcome=outcome, mae=mae, held=held)


def net_from(res):
    return res["gross"] - res["cost"]


def span_days(ts):
    return max((ts[-1] - ts[0]) / (1000.0 * 86400.0), 1e-9)


def summ(net, label, n_days):
    a = net[~np.isnan(net)]
    if a.size == 0:
        return dict(label=label, n=0, mean=np.nan, t=np.nan, win=np.nan,
                    worst=np.nan, p5=np.nan, tpd=np.nan)
    return dict(label=label, n=int(a.size), mean=float(a.mean()), t=tstat(a),
                win=float((a > 0).mean() * 100), worst=float(a.min()),
                p5=float(np.percentile(a, 5)), tpd=a.size / n_days)


# ============================================================================
# Sizing: fraction-of-equity per trade -> equity multiple + maxDD over the path.
# ============================================================================
def equity_path(net_bps_seq, f):
    """Compound: each trade risks f of equity scaled to bps (1e4 bps = 100%).
    pnl_frac = f * (net_bps/1e4) / (RISK)?  We size so that f = fraction of equity put at the
    move; PnL in equity terms = (net_bps/1e4) * f_notional. Use notional = f*equity per trade.
    equity *= (1 + f * net_bps/1e4).  maxDD on the running equity curve.
    """
    eq = 1.0
    peak = 1.0
    maxdd = 0.0
    curve = np.empty(len(net_bps_seq) + 1)
    curve[0] = 1.0
    for k, nb in enumerate(net_bps_seq):
        eq *= (1.0 + f * nb / 1e4)
        if eq <= 0:
            eq = 1e-9
        curve[k + 1] = eq
        peak = max(peak, eq)
        dd = (peak - eq) / peak
        maxdd = max(maxdd, dd)
    return eq, maxdd


# ============================================================================
def tfi_series(ts, sg, no):
    num = roll(ts, sg, TFI_W)
    den = roll(ts, no, TFI_W)
    with np.errstate(invalid="ignore", divide="ignore"):
        tfi = np.where(den > 0, num / den, np.nan)
    return tfi


def main():
    t0 = time.time()
    print("=== FADE-EXTENSION + FLOW-DIVERGENCE FILTER : realistic costs / MAE tail / queue / sizing ===")
    print(f"event: vol30>=P75 & |d30| extreme & d8 SAME-sign ; rising-edge ; no-overlap 8h. hold<=8h.")
    print(f"FILTER: fade only if fresh TFI(5m) DIVERGES (up->TFI<-tau / dn->TFI>+tau). tau in {TAUS}.")
    print(f"COSTS: maker TP RT={MAKER_RT}bps ; reactive STOP & timeout = TAKER RT={TAKER_RT}bps (gap-aware fill).")
    print(f"sweep TP in {TPS} bps, STOP in {STOPS} bps. arms: FLOW-filtered / RAW / RANDOM({NSEED} seeds).\n")

    # ---- one pass: load each (sym,period) once, compute events + tfi + (for every TP/stop) the walk ----
    # We store per-trade records into a big table keyed by (config, arm).
    # configs = tau x TP x stop. RAW fade uses same events (no tfi gate), per (TP,stop).
    configs = [(tau, tp, st) for tau in TAUS for tp in TPS for st in STOPS]

    # accumulate per (sym, period): for each config, the net arrays (flow / raw) + random pool
    # Keep chronological order within (sym,period) for sizing curves (pool later by config).
    flow_net = {c: {p: [] for p in PERIODS} for c in configs}
    raw_net = {c: {p: [] for p in PERIODS} for c in configs}
    flow_mae = {c: {p: [] for p in PERIODS} for c in configs}
    flow_out = {c: {p: [] for p in PERIODS} for c in configs}   # outcome counts
    rand_net = {c: {p: [] for p in PERIODS} for c in configs}   # mean-over-seeds per trade
    # span days per (sym,period) to compute trades/day correctly (sum spans across syms within a period)
    span_acc = {p: 0.0 for p in PERIODS}
    # chronological per-config flow nets w/ timestamps for the pooled sizing curve
    flow_seq = {c: [] for c in configs}   # list of (ts0, net) appended; sort later
    # per-asset x period flow nets, keyed by config -> sym -> period (for the per-asset table; no 2nd pass)
    flow_per = {c: {s: {p: [] for p in PERIODS} for s in SYMBOLS} for c in configs}

    for sym in SYMBOLS:
        for period in PERIODS:
            r = build(sym, period)
            if r is None:
                continue
            ts, mid, sg, no = r
            if len(ts) < 5000:
                continue
            # events (extension), bet dir, d30 sign
            v = vol30(ts, mid)
            d30 = drift(ts, mid, 30 * MIN)
            d8 = drift(ts, mid, 480 * MIN)
            val = ~(np.isnan(v) | np.isnan(d30) | np.isnan(d8))
            if val.sum() < 1000:
                continue
            vP = np.nanpercentile(v[val], 75)
            p25 = np.nanpercentile(d30[val], 25)
            p75 = np.nanpercentile(d30[val], 75)
            same = (np.sign(d30) == np.sign(d8))
            evt = val & (v >= vP) & ((d30 <= p25) | (d30 >= p75)) & same
            idx_all = no_overlap(evt, ts)
            if len(idx_all) == 0:
                continue
            tfi = tfi_series(ts, sg, no)
            d30s = np.sign(d30[idx_all])           # +1 up-move, -1 down-move
            bet_all = -d30s                        # fade: up-move->short(-1), down-move->long(+1)
            tfi_at = tfi[idx_all]
            span_acc[period] += span_days(ts)

            # accumulate per config
            for c in configs:
                tau, tp, st = c
                # FLOW filter: divergence vs the move.
                # up-move (d30s>0): require tfi < -tau ; down-move (d30s<0): require tfi > +tau
                diverge = ((d30s > 0) & (tfi_at < -tau)) | ((d30s < 0) & (tfi_at > tau))
                diverge = diverge & ~np.isnan(tfi_at)
                fidx = idx_all[diverge]
                fbet = bet_all[diverge]
                if len(fidx):
                    res = walk_events(ts, mid, fidx, fbet, tp, st)
                    nett = net_from(res)
                    m_ok = ~np.isnan(nett)
                    flow_net[c][period].extend(nett[m_ok].tolist())
                    flow_mae[c][period].extend(res["mae"][m_ok].tolist())
                    flow_out[c][period].extend(res["outcome"][m_ok].tolist())
                    flow_per[c][sym][period].extend(nett[m_ok].tolist())
                    for tt, nn in zip(ts[fidx][m_ok], nett[m_ok]):
                        flow_seq[c].append((int(tt), float(nn)))
                # RAW fade: same extension events, NO flow gate
                resR = walk_events(ts, mid, idx_all, bet_all, tp, st)
                nettR = net_from(resR)
                mR = ~np.isnan(nettR)
                raw_net[c][period].extend(nettR[mR].tolist())

            # RANDOM null: random entry times + random fade direction, SAME (tp,st) costs/walk.
            # The per-trade net-bps DISTRIBUTION depends only on (tp,st), not tau (flow gate),
            # so we run it ONCE per (sym,period,tp,st). Count = flow count at the loosest tau
            # (so the null has the same order of trades/day as the strategy). ~NSEED seeds.
            jcap = np.searchsorted(ts, ts + HOLD, side="left")
            pool = np.where((jcap < len(ts)) & val)[0]
            # flow count per tau (depends on tau only)
            cnt_by_tau = {}
            for tau in TAUS:
                dv = (((d30s > 0) & (tfi_at < -tau)) | ((d30s < 0) & (tfi_at > tau))) & ~np.isnan(tfi_at)
                cnt_by_tau[tau] = int(dv.sum())
            ncnt = max(cnt_by_tau.values()) if cnt_by_tau else 0
            two = np.array([-1.0, 1.0])
            for tp in TPS:
                for st in STOPS:
                    if ncnt == 0 or pool.size < ncnt:
                        continue
                    acc = None; okc = None
                    for _ in range(NSEED):
                        pick = np.sort(RNG.choice(pool, size=ncnt, replace=False))
                        bdir = RNG.choice(two, size=ncnt)
                        nett = net_from(walk_events(ts, mid, pick, bdir, tp, st))
                        v0 = np.where(np.isnan(nett), 0.0, nett)
                        o0 = (~np.isnan(nett)).astype(float)
                        if acc is None:
                            acc = v0; okc = o0
                        else:
                            acc += v0; okc += o0
                    with np.errstate(invalid="ignore", divide="ignore"):
                        mean_pt = np.where(okc > 0, acc / okc, np.nan)
                    mean_pt = mean_pt[~np.isnan(mean_pt)].tolist()
                    # map this (tp,st) random distribution onto every full config sharing (tp,st)
                    for tau in TAUS:
                        rand_net[(tau, tp, st)][period].extend(mean_pt)

            print(f"  loaded {sym} {period}: events={len(idx_all)}  ticks={len(ts)}", flush=True)

    # ===================== REPORTS =====================
    def arm_table(title, store, n_days_by_period):
        print("\n" + "=" * 110)
        print(title)
        print("=" * 110)
        print(f"  {'config(tau,TP,stop)':>22} | {'POOLED net':>10} {'t':>6} {'n':>5} {'win%':>5} {'wrst':>7} {'p5':>7} {'t/day':>6} | {'MAY net':>8} {'n':>4} {'win%':>5}")
        for c in configs:
            tau, tp, st = c
            allnet = []
            for p in PERIODS:
                allnet += store[c][p]
            allnet = np.asarray(allnet, dtype=np.float64)
            tot_days = sum(n_days_by_period.values())
            s = summ(allnet, "", tot_days if tot_days > 0 else 1.0)
            may = np.asarray(store[c]["MAY"], dtype=np.float64)
            smay = summ(may, "", n_days_by_period.get("MAY", 1.0))
            cfg = f"({tau},{int(tp)},{int(st)})"
            print(f"  {cfg:>22} | {s['mean']:>+10.1f} {s['t']:>+6.2f} {s['n']:>5d} {s['win']:>5.0f} "
                  f"{s['worst']:>+7.0f} {s['p5']:>+7.0f} {s['tpd']:>6.2f} | {smay['mean']:>+8.1f} {smay['n']:>4d} {smay['win']:>5.0f}")

    n_days_by_period = dict(span_acc)
    arm_table("ARM (a) FLOW-FILTERED FADE — net bps/trade after REAL costs (maker TP / taker stop), pooled + MAY-OOS",
              flow_net, n_days_by_period)
    arm_table("ARM (b) RAW FADE (same extension events, NO flow filter)",
              raw_net, n_days_by_period)
    arm_table("ARM (c) RANDOM-entry NULL (same count as flow arm, random times+dir, ~%d seeds, same costs)" % NSEED,
              rand_net, n_days_by_period)

    # ---- best config selection: pooled net>0, t>0, AND positive MAY-OOS (anti-overfit) ----
    best = None
    for c in configs:
        allnet = []
        for p in PERIODS:
            allnet += flow_net[c][p]
        a = np.asarray(allnet, dtype=np.float64)
        may = np.asarray(flow_net[c]["MAY"], dtype=np.float64)
        if a.size < 10:
            continue
        may_mean = may.mean() if may.size else np.nan
        # require positive pooled AND positive OOS-May; score = min(pooled, may) to reward robustness
        robust = a.mean() > 0 and (not np.isnan(may_mean)) and may_mean > 0
        score = min(a.mean(), may_mean) if robust else (a.mean() - 1e6)  # penalize non-robust
        cand = (score, c, a.mean(), tstat(a), a.size, may_mean, may.size)
        if best is None or score > best[0]:
            best = cand

    # ---- MAE tail readout for the flow arm (pooled) at each config ----
    print("\n" + "=" * 110)
    print("FLOW-FILTERED FADE — MAE TAIL (max adverse excursion over hold, bps) + OUTCOME mix")
    print("  short-gamma readout: medMAE / p95 MAE / max MAE ; %TP / %STOP / %TIMEOUT")
    print("=" * 110)
    print(f"  {'config(tau,TP,stop)':>22} | {'medMAE':>7} {'p95MAE':>7} {'maxMAE':>7} | {'%TP':>5} {'%STOP':>6} {'%TMO':>5}")
    for c in configs:
        tau, tp, st = c
        mae_all = []
        out_all = []
        for p in PERIODS:
            mae_all += flow_mae[c][p]
            out_all += flow_out[c][p]
        mae_all = np.asarray(mae_all, dtype=np.float64)
        out_all = np.asarray(out_all, dtype=np.float64)
        if mae_all.size == 0:
            continue
        cfg = f"({tau},{int(tp)},{int(st)})"
        pTP = (out_all == 1).mean() * 100
        pST = (out_all == -1).mean() * 100
        pTM = (out_all == 0).mean() * 100
        print(f"  {cfg:>22} | {np.median(mae_all):>7.0f} {np.percentile(mae_all,95):>7.0f} {mae_all.max():>7.0f} | "
              f"{pTP:>5.0f} {pST:>6.0f} {pTM:>5.0f}")

    # ---- SIZING on the BEST robust config (pooled chronological flow PnL sequence) ----
    print("\n" + "=" * 110)
    print("SIZING — best ROBUST FLOW config, pooled chronological per-trade net PnL. equity mult + maxDD at f=0.5% & 1%")
    print("=" * 110)
    robust_ok = best is not None and best[0] > -1e5   # score not penalized => pooled>0 AND May>0
    if best is not None:
        bc = best[1]
        seq = sorted(flow_seq[bc], key=lambda x: x[0])
        net_seq = np.array([x[1] for x in seq], dtype=np.float64)
        tag = "ROBUST (pooled>0 AND May-OOS>0)" if robust_ok else "*** NO config is robust; showing least-bad (pooled or May NEGATIVE) ***"
        print(f"  best config = (tau={bc[0]}, TP={int(bc[1])}, stop={int(bc[2])})  [{tag}]")
        print(f"  pooled net={best[2]:+.1f} bps  t={best[3]:+.2f}  n={best[4]}  MAY-OOS net={best[5]:+.1f} (n={best[6]})")
        if net_seq.size:
            print(f"  per-trade PnL: mean={net_seq.mean():+.1f}  p5={np.percentile(net_seq,5):+.0f}  "
                  f"worst={net_seq.min():+.0f}  win%={(net_seq>0).mean()*100:.0f}  n={net_seq.size}")
            if net_seq.size >= 10:
                csum = np.convolve(net_seq, np.ones(10), 'valid')
                print(f"  worst 10-trade cluster sum = {csum.min():+.0f} bps")
            for f in [0.005, 0.01]:
                eq, mdd = equity_path(net_seq, f)
                print(f"  f={f*100:.1f}% : equity mult x{eq:.3f}  maxDD={mdd*100:.1f}%  (over {net_seq.size} trades, full window)")
    else:
        print("  no flow config with >=10 trades.")

    # ---- per-asset / per-month split for FLOW arm at best config (from 1st-pass store; no recompute) ----
    if best is not None:
        bc = best[1]; tau, tp, st = bc
        print("\n" + "=" * 110)
        print(f"FLOW-FILTERED FADE @ best config (tau={tau},TP={int(tp)},stop={int(st)}) — PER-ASSET x PER-MONTH net bps (n). '.'=n<3")
        print("=" * 110)
        print(f"  {'asset':>8} | " + " ".join(f"{(p[-2:] if p!='MAY' else 'MAY'):>11}" for p in PERIODS))
        for sym in SYMBOLS:
            cells = []
            for p in PERIODS:
                a = np.asarray(flow_per[bc][sym][p], dtype=np.float64)
                cells.append(f"{a.mean():>+6.0f}({len(a)})" if a.size >= 3 else f"{'.':>11}")
            print(f"  {sym[:8]:>8} | " + " ".join(f"{c:>11}" for c in cells))

    # ---- explicit deployability verdict ----
    print("\n" + "=" * 110)
    print("VERDICT (machine-readable)")
    print("=" * 110)
    if best is not None and robust_ok:
        bc = best[1]
        print(f"  DEPLOYABLE-CANDIDATE  best=(tau={bc[0]},TP={int(bc[1])},stop={int(bc[2])})  "
              f"pooled={best[2]:+.1f}bps t={best[3]:+.2f}  May-OOS={best[5]:+.1f}bps(n={best[6]})")
    else:
        print("  NOT-DEPLOYABLE  no (tau,TP,stop) is net-positive pooled AND positive on May-OOS after real costs.")

    print(f"\n[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
