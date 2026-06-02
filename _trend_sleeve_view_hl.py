"""SLEEVE TREND (complement du fade) — la VUE que le user veut : quelles paires MONTENT-et-montent / CHUTENT-et-chutent (comme HYPE) ?
Moniteur CAUSAL par actif (drift_24h & drift_48h aux fins de jambe = events tick-level, ZERO grille) : etat UP / DOWN / FLAT.
Puis on MESURE honnetement ce que le sleeve AJOUTE : rider (tenir) tant que l'etat est UP/DOWN, exit au flip OU trailing stop W% du pic.
On sait deja (router/ride) que le ride time < buy&hold ; ici on quantifie ce que ca CAPTE par paire vs buy&hold = combien ca arrondit le net.
INCLUT HYPE. drift via drift_over sur serie complete indexe aux jambes ; trailing peak suivi sur la serie de jambes. ZERO sampling.
"""
import os, sys, datetime
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import leg_chain, drift_over, PAIRS, WEEKS, MIN, GAP

HOUR = 60 * MIN; W24 = 24 * HOUR; W48 = 48 * HOUR; DAY = 86400000
THR = 80.0          # |drift_24h| > 80 bps = mouvement directionnel "soutenu"
TRAIL = 12.0        # trailing stop 12% (cf etude HYPE : large, vol des pullbacks ~10%)


def main():
    print("=== SLEEVE TREND — VUE des paires qui montent-et-montent / chutent-et-chutent (drift_24h, causal) + P&L du ride vs buy&hold ===")
    print(f"    etat UP/DOWN si |drift_24h| > {THR:.0f}bps ; ride = tenir tant que l'etat tient, exit au flip ou trailing {TRAIL:.0f}%. tick-level, zero grille.\n")
    VIEW = {}    # pair -> list (day, state)
    RIDE = {}    # pair -> (ride_bps_total, bh_bps_total, n_episodes)
    for p in PAIRS:
        days_state = []; ride_tot = 0.0; bh_tot = 0.0; neps = 0
        for wk, ddir in WEEKS.items():
            try:
                ti, tr, l2 = load_all(p, ddir); ts, mid, _ = vectorized_features(ti, tr, l2)
            except Exception:
                continue
            ts = np.asarray(ts, np.int64); mid = np.asarray(mid, np.float64)
            Bs, ds = leg_chain(ts, mid, 50.0)
            if len(Bs) < 6:
                continue
            d24 = drift_over(ts, mid, W24)[Bs]; bm = mid[Bs]; bt = ts[Bs]
            state = np.where(np.isnan(d24), 0, np.where(d24 > THR, 1, np.where(d24 < -THR, -1, 0))).astype(int)
            for t, s in zip(bt, state):
                days_state.append((int(t // DAY), int(s)))
            bh_tot += (bm[-1] - bm[0]) / bm[0] * 1e4
            # ride event-based : tenir long si UP, short si DOWN, flat sinon ; trailing stop W% du pic favorable
            pos = 0; entry = 0.0; peak = 0.0
            for i in range(len(Bs)):
                s = state[i]; pxn = bm[i]
                if pos != 0:
                    peak = max(peak, pxn) if pos == 1 else min(peak, pxn)
                    stop = (pxn <= peak * (1 - TRAIL / 100)) if pos == 1 else (pxn >= peak * (1 + TRAIL / 100))
                    if s != pos or stop:
                        ride_tot += pos * (pxn - entry) / entry * 1e4 - 6.0; pos = 0; neps += 1
                if pos == 0 and s != 0:
                    pos = s; entry = pxn; peak = pxn
            if pos != 0:
                ride_tot += pos * (bm[-1] - entry) / entry * 1e4 - 6.0; neps += 1
        VIEW[p] = days_state; RIDE[p] = (ride_tot, bh_tot, neps)

    # 1. VUE : %temps UP / DOWN / FLAT par actif (qui est "en trend" ?)
    print("--- 1. VUE par actif : %temps en etat UP / DOWN / FLAT (|drift_24h|>80bps). Qui monte-et-monte / chute-et-chute ? ---")
    print(f"  {'actif':>9} | {'n':>4} | {'%UP':>4} {'%DOWN':>5} {'%FLAT':>5} | dominant")
    for p in PAIRS:
        st = np.array([s for _, s in VIEW[p]])
        if len(st) < 5:
            continue
        up = 100 * np.mean(st == 1); dn = 100 * np.mean(st == -1); fl = 100 * np.mean(st == 0)
        dom = "UP (trend haussier)" if up > 30 else ("DOWN" if dn > 30 else "choppy/flat")
        print(f"  {p:>9} | {len(st):>4} | {up:>3.0f}% {dn:>4.0f}% {fl:>4.0f}% | {dom}")

    # 2. EVOLUTION JOUR PAR JOUR (le dashboard : qui trend chaque jour)
    print(f"\n--- 2. EVOLUTION jour par jour (U=up-trend, D=down-trend, .=flat) — le moniteur a surveiller en live ---")
    alldays = sorted(set(d for p in PAIRS for d, _ in VIEW[p]))
    print(f"    {'jour':>6} | " + " ".join(f"{p[:4]:>5}" for p in PAIRS))
    for d in alldays:
        cells = []
        for p in PAIRS:
            ss = [s for dd, s in VIEW[p] if dd == d]
            if not ss:
                cells.append(f"{'.':>5}")
            else:
                m = np.mean(ss); cells.append(f"{'U' if m>0.3 else ('D' if m<-0.3 else '.'):>5}")
        dt = datetime.datetime.utcfromtimestamp(d * DAY / 1000).strftime('%m-%d')
        print(f"    {dt:>6} | " + " ".join(cells))

    # 3. P&L du sleeve : ce que rider CAPTE vs buy&hold (combien ca arrondit le net ?)
    print(f"\n--- 3. P&L du SLEEVE TREND (ride+trailing) vs BUY&HOLD, par actif (bps cumules sur la periode) ---")
    print(f"  {'actif':>9} | {'ride bps':>9} {'nep':>4} | {'buy&hold':>9} | capture% | commentaire")
    for p in PAIRS:
        rt, bh, ne = RIDE[p]
        cap = (rt / bh * 100) if abs(bh) > 50 else float('nan')
        com = "trend reel, ride capte une partie" if abs(bh) > 200 else "pas de trend net (choppy)"
        print(f"  {p:>9} | {rt:>+9.0f} {ne:>4} | {bh:>+9.0f} | {cap:>7.0f}% | {com}")

    print("\n  LECTURE :")
    print("   (1)/(2) = LA VUE : HYPE doit ressortir UP (monte-et-monte) ; majors surtout flat (choppy). C'est le moniteur 'qui tenir'.")
    print("   (3) le sleeve ride CAPTE une partie du trend (positif si la paire trend) MAIS < buy&hold (le ride time mal) -> c'est du BETA.")
    print("       -> 'arrondir le net' = TENIR la paire flaggee en trend (beta), trailing stop comme garde-fou ; PAS un timing-alpha.")
    print("   CAVEAT : in-sample mai (HYPE up-only) ; le trailing protege a la baisse mais son payoff (vrai bear) reste NON teste.")


if __name__ == "__main__":
    main()
