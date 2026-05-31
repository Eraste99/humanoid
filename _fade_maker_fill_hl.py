"""SIM EXECUTION MAKER + ADVERSE-SELECTION — fade-extension sur le VRAI carnet L2 HL (in-sample).

La question a 1 million (user) : si on FADE en postant un ordre MAKER, le fill reel (via le carnet) capture-t-il
assez du rebond, ou l'adverse-selection le mange ? On ne suppose PAS un fill instantane au mid.

A chaque fin de jambe 50bps en REGIME TENDANCIEL (bear/bull, terciles drift_4h) :
  - FADE => long si down-leg (poste un BID), short si up-leg (poste un ASK), a un niveau de post :
      "mid"  = au mid L2 ; "best" = au best (b1p/a1p, demi-spread mieux) ; "deep3" = 3bps plus loin (dip plus profond).
  - FILL via le CARNET : bid a P rempli au 1er snapshot ou a1p<=P (l'ask descend a nous) ; ask a P rempli ou b1p>=P.
    timeout 10min : si pas rempli -> PAS de trade (le rebond est parti sans nous = adverse-selection en miss).
  - une fois rempli a P : sortie TP/SL/timeout sur le mid tick depuis le fill ; frais reels (TP maker RT3 / SL,timeout taker RT6).
Sortie par actif/regime/post : fill_rate%, net/trade rempli, win%, NET/SIGNAL (= fill_rate*net, miss compte 0).
Compare au baseline fill-instant-au-mid (sim de sortie) -> le DELTA = cout d'adverse-selection.
HL vrai mid L2 + vrai carnet, in-sample, tick-level, ZERO sampling.
"""
import os, sys, time
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features

MIN = 60 * 1000
GAP = 10 * MIN
MACRO = 4 * 60 * MIN
CAP = 120 * MIN
FILL_TO = 10 * MIN
FEE_MK = 3.0
FEE_TK = 6.0
POSTS = ["mid", "best", "deep3"]
# (name, tp, sl, close_fee) — hold se ferme par une LIMITE travaillee (maker RT3) car on n'est PAS presse (hold minutes) ;
# on garde une variante taker RT6 (sortie market immediate) pour encadrer. tp/sl : TP=maker, SL=taker.
EXITS = [("tp7sl15", 7.0, 15.0, FEE_TK), ("hold2h_mk", None, None, FEE_MK), ("hold2h_tk", None, None, FEE_TK)]
PAIRS = ["HYPEUSDC", "BTCUSDC", "ETHUSDC", "SOLUSDC", "XRPUSDC"]
WEEKS = {"W507": r"C:\Users\erast\hl_data\week_20260507",
         "W515": r"C:\Users\erast\hl_data\week_20260515",
         "W516": r"C:\Users\erast\hl_data\week_20260516"}


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


def post_price(pos, b1, a1, midl, level):
    """retourne (P, is_long). long=poste bid, short=poste ask."""
    if pos > 0:                      # long fade -> bid
        if level == "mid":
            P = midl
        elif level == "best":
            P = b1
        else:
            P = midl * (1.0 - 3e-4)
        return P, True
    else:                            # short fade -> ask
        if level == "mid":
            P = midl
        elif level == "best":
            P = a1
        else:
            P = midl * (1.0 + 3e-4)
        return P, False


def detect_fill(l2_ts, b1p, a1p, t_start, P, is_long):
    j0 = np.searchsorted(l2_ts, t_start, side="left")
    jmax = np.searchsorted(l2_ts, t_start + FILL_TO, side="right")
    if jmax <= j0:
        return None
    sub_ts = l2_ts[j0:jmax]; sub_b = b1p[j0:jmax]; sub_a = a1p[j0:jmax]
    if len(sub_ts) > 1:
        g = np.where(np.diff(sub_ts) > GAP)[0]
        if len(g):
            sub_ts = sub_ts[:g[0] + 1]; sub_b = sub_b[:g[0] + 1]; sub_a = sub_a[:g[0] + 1]
    cond = (sub_a <= P) if is_long else (sub_b >= P)
    w = np.where(cond)[0]
    return int(sub_ts[w[0]]) if len(w) else None


def exit_from(ts, mid, t_fill, P, pos, tp, sl, close_fee):
    i0 = np.searchsorted(ts, t_fill, side="left")
    if i0 >= len(ts):
        return None
    t0 = ts[i0]
    jend = np.searchsorted(ts, t0 + CAP, side="right")
    seg_ts = ts[i0:jend]; seg = mid[i0:jend]
    if len(seg) < 1:
        return None
    g = np.where(np.diff(np.concatenate([[t0], seg_ts])) > GAP)[0]
    if len(g):
        seg = seg[:g[0]] if g[0] > 0 else seg[:1]
    if len(seg) < 1:
        return None
    fav = pos * (seg - P) / P * 1e4
    if tp is None:
        return float(fav[-1]) - close_fee     # close timed = maker travaille (RT3) ou taker (RT6)
    tph = np.where(fav >= tp)[0]; slh = np.where(fav <= -sl)[0]
    ti = tph[0] if len(tph) else 10**9; si = slh[0] if len(slh) else 10**9
    if ti == 10**9 and si == 10**9:
        return float(fav[-1]) - close_fee
    if ti <= si:
        return float(tp) - FEE_MK              # TP = limite -> maker
    return float(-sl) - FEE_TK                 # SL = market urgent -> taker


def main():
    t0 = time.time()
    print("=== SIM EXECUTION MAKER (fill via carnet L2) — fade-extension HL in-sample, adverse-selection reelle ===")
    print("post bid(long)/ask(short) ; fill si l'ask descend/le bid monte a nous ; timeout 10min=miss ; frais reels.\n")
    acc = {}   # (pair,regime,post,exit) -> list of net (filled only); plus counters in cnt
    cnt = {}   # (pair,regime,post) -> [n_signals, n_fill]
    for pair in PAIRS:
        wk_data = []; dmacs = []
        for wk, ddir in WEEKS.items():
            try:
                ticks, trades, l2 = load_all(pair, ddir)
                ts, mid, _ = vectorized_features(ticks, trades, l2)
            except Exception as e:
                print(f"  [skip] {pair}/{wk}: {e}"); continue
            ts = np.asarray(ts, np.int64); mid = np.asarray(mid, np.float64)
            l2_ts = l2["ts_ms"].values.astype(np.int64)
            b1p = l2["b1p"].values.astype(np.float64); a1p = l2["a1p"].values.astype(np.float64)
            okl = (b1p > 0) & (a1p > 0) & (a1p >= b1p)
            l2_ts = l2_ts[okl]; b1p = b1p[okl]; a1p = a1p[okl]
            if len(l2_ts) < 100:
                continue
            Bs, ds = leg_chain(ts, mid)
            if len(Bs) < 10:
                continue
            dmac = drift_over(ts, mid, MACRO)[Bs]
            wk_data.append((ts, mid, Bs, ds, dmac, l2_ts, b1p, a1p)); dmacs.append(dmac)
        if not wk_data:
            continue
        alld = np.concatenate(dmacs); v = ~np.isnan(alld)
        if v.sum() < 30:
            continue
        q1, q2 = np.quantile(alld[v], [1 / 3, 2 / 3])
        for ts, mid, Bs, ds, dmac, l2_ts, b1p, a1p in wk_data:
            for k in range(len(Bs)):
                dm = dmac[k]
                if np.isnan(dm):
                    continue
                reg = "bear" if dm <= q1 else ("bull" if dm >= q2 else None)
                if reg is None:
                    continue
                d = float(ds[k]); ib = int(Bs[k]); tB = ts[ib]
                jl = np.searchsorted(l2_ts, tB, side="left")
                if jl >= len(l2_ts):
                    continue
                b1 = b1p[jl]; a1 = a1p[jl]; midl = 0.5 * (b1 + a1)
                pos = -d
                for level in POSTS:
                    cnt.setdefault((pair, reg, level), [0, 0])[0] += 1
                    P, is_long = post_price(pos, b1, a1, midl, level)
                    tf = detect_fill(l2_ts, b1p, a1p, tB, P, is_long)
                    if tf is None:
                        continue
                    cnt[(pair, reg, level)][1] += 1
                    for en, tp, sl, cf in EXITS:
                        net = exit_from(ts, mid, tf, P, pos, tp, sl, cf)
                        if net is not None:
                            acc.setdefault((pair, reg, level, en), []).append(net)
        print(f"  [ok] {pair}")

    print("\n" + "=" * 116)
    print("FADE MAKER (fill carnet L2) : fill_rate, net/trade-rempli, win%, NET/SIGNAL (miss=0) — par actif/regime/post/exit")
    print("=" * 116)
    print(f"  {'actif':>9} {'reg':>4} {'post':>5} {'exit':>9} | {'sig':>4} {'fill%':>5} | {'net/fill':>8} {'win%':>5} | {'NET/SIG':>8}")
    print("  " + "-" * 84)
    for pair in PAIRS:
        for reg in ("bear", "bull"):
            for level in POSTS:
                c = cnt.get((pair, reg, level))
                if not c or c[0] < 20:
                    continue
                fr = 100.0 * c[1] / c[0]
                for en, _, _, _ in EXITS:
                    lst = acc.get((pair, reg, level, en), [])
                    if not lst:
                        continue
                    nf = float(np.mean(lst)); win = 100.0 * np.mean(np.array(lst) > 0)
                    persig = nf * c[1] / c[0]
                    print(f"  {pair:>9} {reg:>4} {level:>5} {en:>9} | {c[0]:>4} {fr:>5.0f} | {nf:>+8.1f} {win:>5.0f} | {persig:>+8.1f}")
                print("  " + "." * 84)
    print("\n  LECTURE : NET/SIGNAL = ce que tu gagnes REELLEMENT par opportunite (fill_rate x net/fill ; un miss = 0, pas une perte).")
    print("    si NET/SIG > 0 nettement (surtout 'best'/'mid') -> le maker capture l'edge malgre l'adverse-selection = DEPLOYABLE.")
    print("    si fill_rate eleve mais net/fill s'effondre vs baseline mid -> on se fait remplir surtout sur les continuations (AS mange tout).")
    print(f"\n[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
