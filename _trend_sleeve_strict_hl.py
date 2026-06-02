"""SLEEVE TREND — BARRE STRICTE : drift_24h > 500bps (vs 80/300) pour isoler le VRAI trender (HYPE) et EXCLURE les majors qui revertent.
Ride = PUR buy&hold / short&hold (tenir A TRAVERS les flats ; sortie = trailing-stop OU flip soutenu vers l'oppose). Pas de state-machine.
Question : a 500bps, (1) qui est flagge (HYPE seul ? majors exclus ?) (2) le tenir paie-t-il SANS les faux-positifs majors qui plombaient a 80 ?
INCLUT HYPE. drift_24h causal aux fins de jambe ; peak suivi sur les jambes. tick-level, ZERO grille/sampling.
"""
import os, sys
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import leg_chain, drift_over, PAIRS, WEEKS, MIN, GAP

HOUR = 60 * MIN; W24 = 24 * HOUR
THRS = [80.0, 300.0, 500.0]; TRAIL = 12.0


def pure_hold(px, d24, thr):
    """buy&hold long si d24>thr / short&hold si d24<-thr ; tenir a travers flats ; exit = trailing TRAIL% OU flip oppose."""
    pos = 0; entry = 0.0; peak = 0.0; tot = 0.0; nep = 0; held = 0
    for i in range(len(px)):
        s = 1 if d24[i] > thr else (-1 if d24[i] < -thr else 0)
        if pos != 0:
            held += 1
            peak = max(peak, px[i]) if pos > 0 else min(peak, px[i])
            stop = (px[i] <= peak * (1 - TRAIL / 100)) if pos > 0 else (px[i] >= peak * (1 + TRAIL / 100))
            flip = (s == -pos)
            if stop or flip:
                tot += pos * (px[i] - entry) / entry * 1e4 - 6.0; pos = 0; nep += 1
        if pos == 0 and s != 0:
            pos = s; entry = px[i]; peak = px[i]
    if pos != 0:
        tot += pos * (px[-1] - entry) / entry * 1e4 - 6.0; nep += 1
    return tot, nep, held


def main():
    print("=== SLEEVE TREND BARRE STRICTE (drift_24h 80/300/500bps) — pur buy&hold/short&hold + trailing 12% ===\n")
    DAT = {}
    for p in PAIRS:
        legs = []
        for wk, ddir in WEEKS.items():
            try:
                ti, tr, l2 = load_all(p, ddir); ts, mid, _ = vectorized_features(ti, tr, l2)
            except Exception:
                continue
            ts = np.asarray(ts, np.int64); mid = np.asarray(mid, np.float64)
            Bs, ds = leg_chain(ts, mid, 50.0)
            if len(Bs) < 6:
                continue
            d24 = drift_over(ts, mid, W24)[Bs]; bm = mid[Bs]
            ok = ~np.isnan(d24)
            legs.append((bm[ok], d24[ok], (bm[-1] - bm[0]) / bm[0] * 1e4))
        DAT[p] = legs

    for thr in THRS:
        print(f"--- SEUIL drift_24h > {thr:.0f} bps ---")
        print(f"  {'actif':>9} | {'%temps flagge':>13} | {'ride bps':>9} {'nep':>4} | {'buy&hold':>9} | qui ?")
        for p in PAIRS:
            legs = DAT[p]
            if not legs:
                continue
            ntot = sum(len(px) for px, _, _ in legs)
            nflag = sum(int(np.sum(np.abs(d) > thr)) for _, d, _ in legs)
            ride = 0.0; nep = 0; bh = 0.0
            for px, d24, bhw in legs:
                rt, ne, _ = pure_hold(px, d24, thr); ride += rt; nep += ne; bh += bhw
            pct = 100 * nflag / ntot if ntot else 0
            tag = "VRAI trender" if pct > 15 and ride > 300 else ("flag mais reverte" if nflag > 0 and ride < 0 else ("rare/exclu" if pct < 5 else ""))
            print(f"  {p:>9} | {pct:>12.0f}% | {ride:>+9.0f} {nep:>4} | {bh:>+9.0f} | {tag}")
        print()

    print("  LECTURE : a 500bps, si SEUL HYPE reste flagge (%>0) avec ride>0 ET les majors tombent a ~0% flagge (exclus) ->")
    print("    la barre stricte ISOLE le vrai trender et evite les faux-positifs majors qui plombaient a 80bps. Sinon, ajuster.")
    print("    NB : plus strict = flague moins souvent = capte moins du drift total de HYPE (on n'est dedans que sur les surges extremes).")


if __name__ == "__main__":
    main()
