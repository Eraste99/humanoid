"""DETECTEUR drift_8h en VERSION LIVE = percentile ROULANT PAR ACTIF (causal), + OBSERVABILITE de l'evolution des paires.
Remplace le Q70 POOLE full-sample (cross-asset + lookahead, cf _fade_longtrend_gate_hl) par : chaque paire stocke au fur
et a mesure son |drift_8h| sur une fenetre glissante (ring-buffer 5min, WIN jours) et se compare a SON PROPRE historique.
  -> scale-free (chaque paire vs elle-meme), glissant (s'adapte au regime qui change), causal (deployable live, 0 lookahead).
Gate : skip le fade si la jambe ETEND un trend_8h fort POUR CETTE PAIRE (sign(d8)==jambe & |d8| >= percentile roulant P).
QUESTIONS : (1) la version roulante/par-actif sauve-t-elle HYPE (->+) en gardant les majors ? (2) vs l'ancien Q70 poole ?
OBSERVABILITE (ce que le user veut VOIR) : par paire, l'evolution jour par jour du |drift_8h|, sa direction, le seuil
roulant, et le %temps en etat-trend -> verifier de visu que l'algo discrimine sainement (HYPE persistant vs majors flippy).
En LIVE : ring-buffer |drift_8h| par paire persiste sur disque -> ce script EST le simulateur causal de ce buffer. HL true-mid.
"""
import os, sys, datetime
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import leg_chain, drift_over, PAIRS, WEEKS, MACRO, MIN, GAP

HOUR = 60 * MIN; W8 = 8 * 60 * MIN; FEE = 3.0; HOLD = 30 * MIN
DAY = 24 * 60 * MIN
SAMPLE_DT = 5 * MIN          # cadence du ring-buffer |drift_8h|
WIN = 3 * DAY                # fenetre glissante de reference (l'historique recent de la paire)
MIN_BUF = int(1 * DAY / SAMPLE_DT)   # >= 1 jour d'echantillons avant que le gate s'active (cold-start live)
PS = [60.0, 70.0, 80.0]


def fwd_at(ts, mid, Bs, H):
    n = len(ts); jf = np.searchsorted(ts, ts[Bs] + H, side="left"); ok = jf < n
    f = np.full(len(Bs), np.nan); f[ok] = (mid[jf[ok]] - mid[Bs[ok]]) / mid[Bs[ok]] * 1e4
    tf = np.zeros(len(Bs), bool); tf[ok] = (ts[jf[ok]] - ts[Bs[ok]]) > (H + GAP); f[tf] = np.nan
    return f


def rolling_thr(qt, st, sv, P):
    """seuil = percentile P du |drift_8h| de la paire sur [qt-WIN, qt) (STRICTEMENT passe = causal). inf si pas chaud."""
    lo = np.searchsorted(st, qt - WIN, side="left"); hi = np.searchsorted(st, qt, side="left")
    out = np.full(len(qt), np.inf)
    for i in range(len(qt)):
        if hi[i] - lo[i] >= MIN_BUF:
            out[i] = np.quantile(sv[lo[i]:hi[i]], P / 100.0)
    return out


def collect(p):
    legs = []          # (d8, d, net, thr60, thr70, thr80)  par jambe-fade
    obs = []           # (day_key, |d8|, d8, thr70, warmed)  par point de grille (observabilite)
    for wk, dd in WEEKS.items():
        try:
            ti, tr, l2 = load_all(p, dd); ts, mid, _ = vectorized_features(ti, tr, l2)
        except Exception:
            continue
        ts = np.asarray(ts, np.int64); mid = np.asarray(mid, np.float64)
        if len(ts) < 100:
            continue
        Bs, ds = leg_chain(ts, mid, 50.0)
        if len(Bs) < 5:
            continue
        dmac = drift_over(ts, mid, MACRO)[Bs]; d1 = drift_over(ts, mid, HOUR)[Bs]
        d8full = drift_over(ts, mid, W8); d8 = d8full[Bs]; fwd = fwd_at(ts, mid, Bs, HOLD)
        ok = ~np.isnan(dmac)
        if ok.sum() < 15:
            continue
        q1, q2 = np.quantile(dmac[ok], [1 / 3, 2 / 3]); bm = ok & (dmac >= q2) & ~np.isnan(d1)
        m1 = np.median(d1[bm]) if bm.sum() else 0.0
        # --- ring-buffer : |drift_8h| echantillonne tous les 5min (la memoire glissante de la paire) ---
        G = np.arange(ts[0], ts[-1], SAMPLE_DT)
        gi = np.searchsorted(ts, G, side="left"); gi = np.clip(gi, 0, len(ts) - 1)
        gd8 = d8full[gi]; gok = ~np.isnan(gd8)
        G = G[gok]; gd8 = gd8[gok]; gabs = np.abs(gd8)
        if len(G) < MIN_BUF + 5:
            continue
        gthr70 = rolling_thr(G, G, gabs, 70.0)
        for t, a, s, th in zip(G, gabs, gd8, gthr70):
            obs.append((int(t // DAY), a, s, th, np.isfinite(th)))
        # --- gate par jambe : seuil roulant PAR ACTIF aux 3 percentiles ---
        tB = ts[Bs]
        thrs = {P: rolling_thr(tB, G, gabs, P) for P in PS}
        for k in range(len(Bs)):
            dm = dmac[k]
            if np.isnan(dm) or np.isnan(fwd[k]) or np.isnan(d8[k]):
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
            legs.append((d8[k], d, -d * fwd[k] - FEE, thrs[60.0][k], thrs[70.0][k], thrs[80.0][k]))
    return legs, obs


def main():
    print("=== DETECTEUR drift_8h LIVE : percentile ROULANT PAR ACTIF (causal) + OBSERVABILITE evolution paires ===")
    print(f"    ring-buffer |drift_8h| @ {SAMPLE_DT//MIN}min, fenetre glissante {WIN//DAY}j, warm-up {MIN_BUF} ech. (~1j). HL true-mid.\n")
    data = {p: collect(p) for p in PAIRS}
    # pooled full-sample Q70 (l'ANCIENNE methode, pour comparer)
    alld8 = np.concatenate([[x[0] for x in data[p][0]] for p in PAIRS if data[p][0]]) if any(data[p][0] for p in PAIRS) else np.array([])
    q70_pool = np.nanquantile(np.abs(alld8), 0.70) if len(alld8) else np.inf

    # ---- 1. VALIDATION : baseline vs gate ROULANT-PAR-ACTIF (P70) vs ancien POOLE-fullsample, par actif (+HYPE) ----
    print("--- 1. baseline vs GATE ROULANT/ACTIF (P70 causal) vs ANCIEN POOLE-Q70 (lookahead) — net & %gardé, par actif ---")
    print(f"  {'actif':>9} | {'base net':>8} {'n':>4} | {'roll net':>8} {'%gard':>5} {'%chaud':>6} | {'pool net':>8} {'%gard':>5}")
    for p in PAIRS:
        legs, _ = data[p]
        if len(legs) < 8:
            continue
        d8 = np.array([x[0] for x in legs]); dd = np.array([x[1] for x in legs]); nt = np.array([x[2] for x in legs])
        th70 = np.array([x[4] for x in legs])
        warm = np.isfinite(th70)
        fire_roll = warm & (np.sign(d8) == dd) & (np.abs(d8) >= th70); keep_roll = ~fire_roll
        fire_pool = (np.sign(d8) == dd) & (np.abs(d8) >= q70_pool); keep_pool = ~fire_pool
        gr = nt[keep_roll]; gp = nt[keep_pool]
        rs = f"{np.mean(gr):>+8.1f} {100*keep_roll.mean():>4.0f}% {100*warm.mean():>5.0f}%" if len(gr) >= 5 else f"{'n<5':>8}"
        ps = f"{np.mean(gp):>+8.1f} {100*keep_pool.mean():>4.0f}%" if len(gp) >= 5 else f"{'n<5':>8}"
        print(f"  {p:>9} | {np.mean(nt):>+8.1f} {len(nt):>4} | {rs} | {ps}")

    # ---- 2. SWEEP percentile P (roulant/actif) : ou les majors restent intacts ET HYPE est sauve ? ----
    print(f"\n--- 2. SWEEP percentile roulant/actif — net gardé (n gardé, %gardé) par P, par actif ---")
    print(f"  {'actif':>9} | " + " ".join(f"{'P'+str(int(P)):>16}" for P in PS))
    for p in PAIRS:
        legs, _ = data[p]
        if len(legs) < 8:
            continue
        d8 = np.array([x[0] for x in legs]); dd = np.array([x[1] for x in legs]); nt = np.array([x[2] for x in legs])
        thmap = {60.0: 3, 70.0: 4, 80.0: 5}
        cells = []
        for P in PS:
            th = np.array([x[thmap[P]] for x in legs]); warm = np.isfinite(th)
            keep = ~(warm & (np.sign(d8) == dd) & (np.abs(d8) >= th)); g = nt[keep]
            cells.append(f"{np.mean(g):>+5.1f}({len(g):>3},{100*keep.mean():>3.0f}%)" if len(g) >= 5 else f"{'n<5':>16}")
        print(f"  {p:>9} | " + " ".join(cells))

    # ---- 3. OBSERVABILITE : evolution JOUR PAR JOUR de chaque paire (ce que le user veut VOIR) ----
    print(f"\n--- 3. EVOLUTION PAR PAIRE (ce que tu surveilles en live) : par jour — med|d8| / dir / seuil-roulant / %trend ---")
    print(f"    dir = drift_8h signé moyen (>0 hausse persistante, ~0 choppy/flip) ; %trend = temps |d8|>=seuil-roulant.")
    for p in PAIRS:
        _, obs = data[p]
        if not obs:
            continue
        days = sorted(set(o[0] for o in obs))
        ab = np.array([o[1] for o in obs]); sg = np.array([o[2] for o in obs])
        th = np.array([o[3] for o in obs]); wm = np.array([o[4] for o in obs]); dk = np.array([o[0] for o in obs])
        print(f"\n  {p} — moyenne globale : med|d8|={np.median(ab):>5.0f}bps  dir(frac d8>0)={np.mean(sg>0):>4.2f}  %trend(chaud)={100*np.mean(ab[wm]>=th[wm]) if wm.any() else 0:>3.0f}%")
        print(f"    {'jour':>6} | {'med|d8|':>7} {'dir':>6} {'seuil':>6} {'%trend':>6} {'chaud':>5}")
        for d in days:
            m = dk == d; abm = ab[m]; sgm = sg[m]; thm = th[m]; wmm = m & wm
            warmf = wm[m].mean()
            ptr = 100 * np.mean(ab[wmm] >= th[wmm]) if wmm.sum() else np.nan
            dt = datetime.datetime.utcfromtimestamp(d * DAY / 1000).strftime('%m-%d')
            ptr_s = f"{ptr:>5.0f}%" if not np.isnan(ptr) else f"{'--':>6}"
            thf = thm[np.isfinite(thm)]; sl = np.median(thf) if len(thf) else float('nan')
            print(f"    {dt:>6} | {np.median(abm):>7.0f} {np.mean(sgm):>+6.0f} {sl:>6.0f} {ptr_s} {100*warmf:>4.0f}%")

    print("\n  LECTURE :")
    print("   (1) si roll-P70 garde les majors ~intacts (%gard haut, net pas pire) ET sauve HYPE (net base<0 -> roll>0) -> upgrade OK.")
    print("   (2) le P ou les majors gardent ~80-100% pendant que HYPE descend a ~50% = le bon seuil live.")
    print("   (3) evolution : HYPE doit montrer dir!=0 persistant + %trend eleve (souvent en trend) ; majors dir~0 + %trend bas (choppy).")
    print("       Le seuil-roulant de HYPE (centaines de bps) >> celui des majors (dizaines) = scale-free PAR ACTIF en action.")


if __name__ == "__main__":
    main()
