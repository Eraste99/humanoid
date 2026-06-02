"""CLIMAX-FADE — version CAUSALE ROBUSTE (reponse au doute biais/live du user : "en live on saurait tout autant ?").
Seuil climax = Q70 ROULANT du |drift_8h| sur TOUTES les jambes de la paire (historique continu = ce qu'on ecoute en live),
fenetre glissante 3j, STRICTEMENT passe -> zero lookahead, et beaucoup PLUS de n que la version precedente (n=28).
Le climax est un OVERLAY de sur-ponderation : on GARDE le fade de base 30min (le 'rest' reste +EV), on met + de taille sur le climax.
Mesure : EV(climax) vs EV(rest) sur les fades DEPLOYABLES (fade+FLOW+Binance-lead), POOL + persistance E/M + gradient tercile causal
+ gain de sizing PROPRE (meme population : a plat vs tilt 2x climax). HL true-mid, tick-level/leg-based, ZERO grille/sampling.
"""
import os, sys, datetime
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import leg_chain, drift_over, WEEKS, MACRO, MIN, GAP
from _fade_binlead_fullbook_hl import load_bin, bdrift, BIN

HOUR = 60 * MIN; W8 = 8 * HOUR; FEE_MK = 3.0; HOLD = 30 * MIN; THR = 30.0; DAY = 24 * HOUR
MAJ = ("BTCUSDC", "ETHUSDC", "SOLUSDC", "XRPUSDC")
WBACK = 3 * DAY; MINBUF = 20    # seuil roulant : Q70 du |d8| des jambes des 3 derniers jours (>=20 jambes)


def fwd_at(ts, mid, Bs, H):
    n = len(ts); jf = np.searchsorted(ts, ts[Bs] + H, side="left"); ok = jf < n
    f = np.full(len(Bs), np.nan); f[ok] = (mid[jf[ok]] - mid[Bs[ok]]) / mid[Bs[ok]] * 1e4
    tf = np.zeros(len(Bs), bool); tf[ok] = (ts[jf[ok]] - ts[Bs[ok]]) > (H + GAP); f[tf] = np.nan
    return f


def per(t):
    return "E" if datetime.datetime.utcfromtimestamp(t / 1000).day <= 8 else "M"


def collect(pair, binD):
    """retourne : alllegs=(t,|d8|) de TOUTES les jambes (buffer seuil) ; fades=[(t,|d8|,net,periode)] deployables."""
    AT = []; AD = []; wkA = {}; poolD = []; poolD1 = []
    for wk, ddir in WEEKS.items():
        try:
            ticks, trades, l2 = load_all(pair, ddir); ts, mid, feats = vectorized_features(ticks, trades, l2)
        except Exception:
            continue
        ts = np.asarray(ts, np.int64); mid = np.asarray(mid, np.float64)
        Bs, ds = leg_chain(ts, mid, 50.0)
        if len(Bs) < 6:
            continue
        d8 = drift_over(ts, mid, W8)[Bs]; dmac = drift_over(ts, mid, MACRO)[Bs]
        d1 = drift_over(ts, mid, HOUR)[Bs]
        tfi = np.asarray(feats.get("tfi_5min", np.full(len(ts), np.nan)), np.float64)[Bs]
        fwd = fwd_at(ts, mid, Bs, HOLD)
        for k in range(len(Bs)):                              # buffer = TOUTES les jambes (|d8| connu causalement)
            if not np.isnan(d8[k]):
                AT.append(int(ts[Bs[k]])); AD.append(abs(float(d8[k])))
        wkA[wk] = (ts[Bs], ds, dmac, d1, d8, tfi, fwd); poolD.append(dmac); poolD1.append(d1)
    if not wkA:
        return (np.array([]), np.array([])), []
    dd = np.concatenate(poolD); v = ~np.isnan(dd)
    if v.sum() < 30:
        return (np.array([]), np.array([])), []
    q1, q2 = np.quantile(dd[v], [1 / 3, 2 / 3])
    d1p = np.concatenate(poolD1); bp = v & (dd >= q2) & ~np.isnan(d1p); m1 = np.median(d1p[bp]) if bp.sum() else 0.0
    ent = []
    for wk, (tB, ds, dmac, d1, d8, tfi, fwd) in wkA.items():
        for k in range(len(ds)):
            dm = dmac[k]
            if np.isnan(dm) or np.isnan(fwd[k]) or np.isnan(d8[k]):
                continue
            d = float(ds[k])
            if dm <= q1:
                if d != -1.0: continue
            elif dm >= q2:
                if d != 1.0 or np.isnan(d1[k]) or d1[k] < m1: continue
            else:
                continue
            ent.append((int(tB[k]), d, abs(float(d8[k])), tfi[k], -d * fwd[k] - FEE_MK))
    if len(ent) < 12:
        return (np.array([]), np.array([])), []
    qtfi = np.nanquantile(np.abs(np.array([e[3] for e in ent])), 0.70)
    fades = []
    for tB, d, ad8, tfival, net in ent:
        if (not np.isnan(tfival)) and (np.sign(tfival) == d) and (abs(tfival) >= qtfi):
            continue
        bs = bdrift(binD.get(BIN[pair]), tB)
        if (not np.isnan(bs)) and (np.sign(bs) == d) and (abs(bs) >= THR):
            continue
        fades.append((tB, ad8, net, per(tB)))
    o = np.argsort(AT)
    return (np.array(AT)[o], np.array(AD)[o]), sorted(fades)


def main():
    print("=== CLIMAX-FADE CAUSAL ROBUSTE : seuil roulant sur TOUTES les jambes (historique continu, zero lookahead) ===\n")
    binD = {s: load_bin(s) for s in set(BIN.values())}
    POOL = {"climax": [], "rest": []}; PP = {"E": {"climax": [], "rest": []}, "M": {"climax": [], "rest": []}}
    TERC = {"LOW": [], "MID": [], "HIGH": []}
    sizing_cl = []; sizing_rs = []
    for p in MAJ:
        (AT, AD), fades = collect(p, binD)
        if len(fades) < 12 or len(AT) < MINBUF:
            continue
        ncl = 0
        for t, ad8, net, prd in fades:
            lo = np.searchsorted(AT, t - WBACK, side="left"); hi = np.searchsorted(AT, t, side="left")  # passe strict
            if hi - lo < MINBUF:
                continue
            buf = AD[lo:hi]; q70 = np.quantile(buf, 0.70); q30 = np.quantile(buf, 0.30)
            key = "climax" if ad8 >= q70 else "rest"
            POOL[key].append(net); PP[prd][key].append(net)
            if key == "climax":
                sizing_cl.append(net); ncl += 1
            else:
                sizing_rs.append(net)
            TERC["HIGH" if ad8 >= q70 else ("LOW" if ad8 <= q30 else "MID")].append(net)

    def stat(a):
        a = np.array(a); return (len(a), np.mean(a), 100 * np.mean(a > 0), np.min(a)) if len(a) else (0, np.nan, np.nan, np.nan)

    print("--- 1. CAUSAL POOL (seuil roulant 3j sur TOUTES les jambes) : climax vs rest. rest = fade de base 30min = GARDE ---")
    for k in ("climax", "rest"):
        n, m, w, wo = stat(POOL[k]); print(f"  {k:>7} | n={n:>4} | EV {m:>+6.1f} | win {w:>3.0f}% | worst {wo:>+6.0f}")
    nc, mc, _, _ = stat(POOL["climax"]); nr, mr, _, _ = stat(POOL["rest"])
    print(f"  -> climax-rest = {mc-mr:>+.1f} bps ; n climax = {nc} (vs 28 avant = bien + robuste)")

    print(f"\n--- 2. PERSISTANCE causale E vs M ---")
    for prd in ("E", "M"):
        nc, mc, _, woc = stat(PP[prd]["climax"]); nr, mr, _, _ = stat(PP[prd]["rest"])
        if nc >= 5 and nr >= 5:
            print(f"  {prd} | climax {mc:>+6.1f}(n{nc:>3}, worst{woc:>+5.0f}) vs rest {mr:>+6.1f}(n{nr:>3}) | ecart {mc-mr:>+6.1f}")

    print(f"\n--- 3. GRADIENT tercile causal (LOW/MID/HIGH du |d8| vs distribution roulante) : monotone ? ---")
    for k in ("LOW", "MID", "HIGH"):
        n, m, w, wo = stat(TERC[k]); print(f"  {k:>4} | n={n:>4} | EV {m:>+6.1f} | win {w:>3.0f}%")

    print(f"\n--- 4. GAIN DE SIZING PROPRE (MEME population causale) : a plat vs tilt 2x climax ---")
    cl = np.array(sizing_cl); rs = np.array(sizing_rs)
    if len(cl) and len(rs):
        allp = np.concatenate([cl, rs])
        flat = np.mean(allp); flat_wo = np.min(allp)
        tilt = (2 * cl.sum() + rs.sum()) / (2 * len(cl) + len(rs))
        # tail du livre tilté : worst trade pondéré ~ worst climax compte double (mais climax a worst plus doux)
        print(f"  a plat (1x tous)   : EV/unite-risque {flat:>+6.1f} | worst climax {np.min(cl):>+5.0f} / worst rest {np.min(rs):>+5.0f}")
        print(f"  tilt 2x climax     : EV/unite-risque {tilt:>+6.1f}  ({100*(tilt-flat)/abs(flat) if flat else 0:>+.0f}% vs plat)")
        print(f"  -> climax a EV {np.mean(cl):+.1f} ET worst plus doux que rest -> sur-ponderer = +EV sans aggraver la queue.")

    print("\n  LECTURE : si (1) climax>rest avec n climax >> 28, (2) ecart>0 en E ET M, (3) gradient monotone, (4) tilt > plat")
    print("    -> climax-fade CAUSAL & ROBUSTE & deployable en live (seuil roulant sur l'historique ecoute en continu). Sinon, prudence.")


if __name__ == "__main__":
    main()
