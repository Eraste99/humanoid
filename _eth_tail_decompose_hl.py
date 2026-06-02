"""DÉCOMPO de la QUEUE RÉSIDUELLE d'ETH (post fade+FLOW+Binance-lead) : STRUCTUREL ou ROTATIF (period-specific) ?
On classe chaque grosse perte ETH (net < SEUIL) en :
  (a) MARKET-WIDE  : d'autres paires perdent AUSSI dans la même fenêtre ±15min (cascade partagée -> ETH a juste pris le + gros coup -> ROTATIF).
  (b) BINANCE-LEAD sous-seuil : un lead Binance ETH existait (30s/60s/120s) sous le seuil du filtre (30bps) -> le filtre l'a raté de peu.
  (c) ISOLÉ        : ETH seul, pas de lead -> idiosyncratique (-> + structurel) ; ou juste 1-2 events = bruit de petit échantillon.
Compare aussi le COMPTE de tail-losses par paire (ETH en a-t-il PLUS, ou juste des PLUS GROS dans les mêmes cascades ?).
Verdict : si majorité MARKET-WIDE + petit n -> "ETH tail" = rotatif/period-specific (exclusion DYNAMIQUE) ; si ETH-isolé répétés -> structurel.
HL true-mid, causal, in-sample mai. Binance = SIGNAL only.
"""
import os, sys, datetime
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import leg_chain, drift_over, WEEKS, MACRO, MIN, GAP
from _fade_binlead_fullbook_hl import load_bin, BIN

HOUR = 60 * MIN; FEE_MK = 3.0; HOLD = 30 * MIN; THR = 30.0; SEC = 1000
MAJ = ("BTCUSDC", "ETHUSDC", "SOLUSDC", "XRPUSDC")
TAILCUT = -40.0          # une "grosse perte" = net < -40 bps
COWIN = 15 * MIN         # fenêtre de co-perte market-wide


def fwd_at(ts, mid, Bs, H):
    n = len(ts); jf = np.searchsorted(ts, ts[Bs] + H, side="left"); ok = jf < n
    f = np.full(len(Bs), np.nan); f[ok] = (mid[jf[ok]] - mid[Bs[ok]]) / mid[Bs[ok]] * 1e4
    tf = np.zeros(len(Bs), bool); tf[ok] = (ts[jf[ok]] - ts[Bs[ok]]) > (H + GAP); f[tf] = np.nan
    return f


def bdrift_w(B, T, W):
    if B is None:
        return np.nan
    bts, bmid = B
    i = np.searchsorted(bts, T, side="left"); j = np.searchsorted(bts, T - W, side="left")
    if i >= len(bts) or j >= len(bts) or i <= j:
        return np.nan
    return (bmid[i] - bmid[j]) / bmid[j] * 1e4


def collect_rich(pair, binD):
    """fade unifié + FLOW + Binance-lead(SAME). retourne [(t, d, net)] des trades GARDÉS (déployables)."""
    wkA = {}; poolD = []; poolD1 = []
    for wk, ddir in WEEKS.items():
        try:
            ticks, trades, l2 = load_all(pair, ddir)
            ts, mid, feats = vectorized_features(ticks, trades, l2)
        except Exception:
            continue
        ts = np.asarray(ts, np.int64); mid = np.asarray(mid, np.float64)
        Bs, ds = leg_chain(ts, mid, 50.0)
        if len(Bs) < 5:
            continue
        dmac = drift_over(ts, mid, MACRO)[Bs]; d1 = drift_over(ts, mid, HOUR)[Bs]
        tfi = np.asarray(feats.get("tfi_5min", np.full(len(ts), np.nan)), np.float64)[Bs]
        fwd = fwd_at(ts, mid, Bs, HOLD)
        wkA[wk] = (ts[Bs], ds, dmac, d1, tfi, fwd); poolD.append(dmac); poolD1.append(d1)
    if not wkA:
        return []
    dd = np.concatenate(poolD); v = ~np.isnan(dd)
    if v.sum() < 30:
        return []
    q1, q2 = np.quantile(dd[v], [1 / 3, 2 / 3])
    d1p = np.concatenate(poolD1); bp = v & (dd >= q2) & ~np.isnan(d1p); m1 = np.median(d1p[bp]) if bp.sum() else 0.0
    ent = []
    for wk, (tB, ds, dmac, d1, tfi, fwd) in wkA.items():
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
            ent.append((int(tB[k]), d, tfi[k], -d * fwd[k] - FEE_MK))
    if len(ent) < 12:
        return []
    qtfi = np.nanquantile(np.abs(np.array([e[2] for e in ent])), 0.70)
    rows = []
    for tB, d, tfival, net in ent:
        if (not np.isnan(tfival)) and (np.sign(tfival) == d) and (abs(tfival) >= qtfi):
            continue                                       # FLOW skip
        bs = bdrift_w(binD.get(BIN[pair]), tB, 120 * SEC)
        if (not np.isnan(bs)) and (np.sign(bs) == d) and (abs(bs) >= THR):
            continue                                       # Binance-lead SAME skip
        rows.append((tB, d, net))
    return rows


def main():
    print("=== DÉCOMPO QUEUE RÉSIDUELLE ETH (post fade+FLOW+Binance-lead) : market-wide (rotatif) vs ETH-isolé (structurel) ? ===\n")
    binD = {s: load_bin(s) for s in set(BIN.values())}
    book = {p: sorted(collect_rich(p, binD)) for p in MAJ}

    # 1. compte de tail-losses par paire : ETH en a-t-il PLUS, ou juste de PLUS GROS ?
    print(f"--- 1. TAIL-LOSSES par paire (net < {TAILCUT:.0f}) : ETH a-t-il PLUS de pertes, ou des plus GROSSES ? ---")
    print(f"  {'paire':>9} | {'n trades':>8} {'n tail':>6} {'%tail':>5} | {'worst':>6} {'2e':>6} {'3e':>6} | {'mean tail':>9}")
    alltr = []
    for p in MAJ:
        for t, d, net in book[p]:
            alltr.append((t, p, d, net))
    for p in MAJ:
        nets = np.array([x[2] for x in book[p]]) if book[p] else np.array([])
        if len(nets) == 0:
            continue
        tail = np.sort(nets[nets < TAILCUT])
        w = [tail[i] if i < len(tail) else float('nan') for i in range(3)]
        mt = np.mean(tail) if len(tail) else float('nan')
        print(f"  {p:>9} | {len(nets):>8} {len(tail):>6} {100*len(tail)/len(nets):>4.0f}% | {w[0]:>+6.0f} {w[1]:>+6.0f} {w[2]:>+6.0f} | {mt:>+9.1f}")

    # 2. décompo de CHAQUE grosse perte ETH
    print(f"\n--- 2. CHAQUE grosse perte ETH : co-perte ±15min (autres paires) + contexte Binance ETH (30/60/120s) ---")
    print(f"  {'date':>11} {'d':>2} {'net':>5} | {'#autres':>7} {'pertes':>7} {'somme_autres':>12} | {'bsETH 30/60/120':>17} {'bbBTC120':>8} | tag")
    binE = binD.get(BIN["ETHUSDC"]); binB = binD.get("BTCUSDT")
    eth_tail = [(t, d, net) for (t, d, net) in book["ETHUSDC"] if net < TAILCUT]
    tags = {"MARKET-WIDE": 0, "BINLEAD-sous-seuil": 0, "ISOLÉ": 0}
    weeks_hit = {}
    for t, d, net in sorted(eth_tail):
        co = [(pp, nn) for (tt, pp, dd2, nn) in alltr if pp != "ETHUSDC" and abs(tt - t) <= COWIN]
        co_lose = [(pp, nn) for (pp, nn) in co if nn < 0]
        npairs = len(set(pp for pp, _ in co_lose)); somme = sum(nn for _, nn in co_lose)
        bs30 = bdrift_w(binE, t, 30 * SEC); bs60 = bdrift_w(binE, t, 60 * SEC); bs120 = bdrift_w(binE, t, 120 * SEC)
        bb120 = bdrift_w(binB, t, 120 * SEC)
        leadsub = any((not np.isnan(b)) and (np.sign(b) == d) and (abs(b) >= 15.0) for b in (bs30, bs60, bs120))
        if npairs >= 2 or somme <= -60:
            tag = "MARKET-WIDE"
        elif leadsub:
            tag = "BINLEAD-sous-seuil"
        else:
            tag = "ISOLÉ"
        tags[tag] += 1
        wk = datetime.datetime.utcfromtimestamp(t / 1000).strftime('%Y-%m-%d')[:7]
        weeks_hit[wk] = weeks_hit.get(wk, 0) + 1
        dt = datetime.datetime.utcfromtimestamp(t / 1000).strftime('%m-%d %H:%M')
        bstr = f"{bs30:>+5.0f}/{bs60:>+5.0f}/{bs120:>+5.0f}"
        print(f"  {dt:>11} {d:>+2.0f} {net:>+5.0f} | {npairs:>7} {len(co_lose):>7} {somme:>+12.0f} | {bstr:>17} {bb120:>+8.0f} | {tag}")

    # 3. verdict
    n = len(eth_tail)
    print(f"\n--- 3. VERDICT sur {n} grosses pertes ETH ---")
    if n:
        for k, v in tags.items():
            print(f"  {k:>20} : {v:>2}  ({100*v/n:>3.0f}%)")
        print(f"  répartition temporelle : " + " ".join(f"{w}:{c}" for w, c in sorted(weeks_hit.items())))
    # 4. cross-check : dans les fenêtres market-wide d'ETH, les autres majors tail-aussi ? (= cascade partagée)
    mw_windows = [t for (t, d, net) in eth_tail
                  if len(set(pp for (tt, pp, dd2, nn) in alltr if pp != "ETHUSDC" and abs(tt - t) <= COWIN and nn < 0)) >= 2]
    print(f"\n  cross-check : {len(mw_windows)}/{n} pertes ETH sont dans une fenêtre où >=2 AUTRES majors perdent aussi = cascade partagée.")

    print("\n  LECTURE :")
    print("   - si (1) ETH a un nb de tail-losses SIMILAIRE aux autres mais des magnitudes plus GROSSES, et (2)/(4) la majorité sont")
    print("     MARKET-WIDE -> ETH n'est pas idiosyncratiquement toxique, il prend juste le + gros coup dans des cascades PARTAGÉES = ROTATIF.")
    print("   - si beaucoup d'ISOLÉ répétés sur les 2 semaines -> structurel (idiosyncratique ETH). si 1-2 events -> bruit petit échantillon.")
    print("   => tranche entre exclusion STRUCTURELLE (ETH dehors) vs DYNAMIQUE (moniteur de tail par paire, ETH ré-includable).")


if __name__ == "__main__":
    main()
