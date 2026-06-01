"""BLOC ⑤ OFFENSIF — FOLLOW Binance-extreme sur HL (LE seul morceau du bot pas encore mesure end-to-end).
Hypothese user : quand Binance fait un MOVE VIOLENT, le move touche AUSSI HL -> on OUBLIE le fade, on ENTRE sur HL
DANS LE SENS du move (TAKER, vite). Si deja en position contraire -> on SORT ; meme sens -> on TIENT.
Questions : (A) SUIVRE un Binance-extreme sur HL est-il +EV net de frais TAKER ? (B) a ces memes points, FADER perd-il
(= confirme que fader un Binance-lead EST la catastrophe -> justifie l'exit-contraire) ? (C) marche par actif (+ HYPE).
Sweep W (lookback Binance) x THR (extreme) x H (hold HL). Binance = SIGNAL only (exec interdite MiCA).
Causal : drift Binance sur [T-W, T] ; forward HL sur [T+LAG, T+LAG+H]. LAG=3s (pas la ms). HL true-mid, zero sampling.
"""
import os, sys
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import WEEKS, MIN, GAP
from _fade_binlead_fullbook_hl import load_bin

SEC = 1000
FEE_TK = 6.0      # RT taker-taker (conservateur)
FEE_TKMK = 4.5    # taker-in + maker-out
HLPAIRS = ["BTCUSDC", "ETHUSDC", "SOLUSDC", "XRPUSDC", "HYPEUSDC"]
BINSYM = {"BTCUSDC": "BTCUSDT", "ETHUSDC": "ETHUSDT", "SOLUSDC": "SOLUSDT", "XRPUSDC": "XRPUSDT", "HYPEUSDC": "HYPEUSDT"}
STEP = 15 * SEC   # grille d'echantillonnage des candidats
LAG = 3 * SEC     # latence d'entree taker realiste (PAS la milliseconde)
WS = [30 * SEC, 60 * SEC, 120 * SEC]
THRS = [30.0, 50.0, 80.0]
HS = [60 * SEC, 300 * SEC, 900 * SEC, 1800 * SEC]
RNG = np.random.default_rng(7)


def bdrift_vec(B, Tg, W):
    """drift Binance causal sur [T-W, T] pour chaque T (vectorise). nan si fenetre absente / trouee."""
    bts, bmid = B
    n = len(bts)
    i = np.searchsorted(bts, Tg, side="left")
    j = np.searchsorted(bts, Tg - W, side="left")
    ic = np.clip(i, 0, n - 1); jc = np.clip(j, 0, n - 1)
    span = bts[ic] - bts[jc]
    ok = (i < n) & (j < n) & (i > j) & (span >= 0.5 * W) & (span <= 2.0 * W)
    d = np.full(len(Tg), np.nan)
    bj = bmid[jc]
    d[ok] = (bmid[ic][ok] - bj[ok]) / bj[ok] * 1e4
    return d


def hl_fwd_at_times(ts, mid, Tent, H):
    """entree au 1er tick HL >= Tent ; sortie au 1er tick >= entree+H ; nan si gap HL (entree ou sortie)."""
    n = len(ts)
    ei = np.searchsorted(ts, Tent, side="left"); ok = ei < n
    ei = np.clip(ei, 0, n - 1)
    xj = np.searchsorted(ts, ts[ei] + H, side="left"); ok &= xj < n
    xj = np.clip(xj, 0, n - 1)
    f = np.full(len(Tent), np.nan)
    f[ok] = (mid[xj[ok]] - mid[ei[ok]]) / mid[ei[ok]] * 1e4
    f[(ts[xj] - ts[ei]) > (H + GAP)] = np.nan
    f[(ts[ei] - Tent) > GAP] = np.nan     # signal tombe dans un trou HL
    return f


def collect_events(Tg, bd, cand, ts, mid, H):
    """greedy cooldown (entrees espacees de >= H, non chevauchantes) ; retourne gross = dir*fwd (suivre Binance)."""
    idx = np.where(cand)[0]
    if len(idx) == 0:
        return np.array([])
    acc = []; last = -10 ** 18
    for k in idx:
        if Tg[k] < last + H:
            continue
        acc.append(k); last = Tg[k]
    acc = np.array(acc, int)
    Tent = Tg[acc] + LAG; dirs = np.sign(bd[acc])
    fwd = hl_fwd_at_times(ts, mid, Tent, H)
    ok = ~np.isnan(fwd) & (dirs != 0)
    return dirs[ok] * fwd[ok]


def stat(g, fee):
    if len(g) == 0:
        return None
    net = g - fee
    return len(g), float(np.mean(g)), float(np.mean(net)), float(100 * np.mean(g > 0)), float(np.min(net))


def main():
    print("=== BLOC 5 OFFENSIF : FOLLOW Binance-extreme sur HL (taker) — +EV ? fade-y-perd-il ? — incl HYPE ===\n")
    binD = {s: load_bin(s) for s in set(BINSYM.values())}
    # pooled[(W,THR,H)] = list gross ; perp[(pair,W,THR,H)] = list gross ; perpQ[(pair,H)] = list gross (seuil Q90/actif)
    pooled = {}; perp = {}; perpQ = {}
    for pair in HLPAIRS:
        B = binD.get(BINSYM[pair])
        if B is None:
            continue
        for wk, ddir in WEEKS.items():
            try:
                ticks, trades, l2 = load_all(pair, ddir)
                ts, mid, _ = vectorized_features(ticks, trades, l2)
            except Exception:
                continue
            ts = np.asarray(ts, np.int64); mid = np.asarray(mid, np.float64)
            if len(ts) < 100:
                continue
            Tg = np.arange(ts[0], ts[-1], STEP)
            for W in WS:
                bd = bdrift_vec(B, Tg, W)
                for THR in THRS:
                    cand = np.abs(bd) >= THR
                    for H in HS:
                        g = collect_events(Tg, bd, cand, ts, mid, H)
                        if len(g):
                            pooled.setdefault((W, THR, H), []).append(g)
                            perp.setdefault((pair, W, THR, H), []).append(g)
                if W == 120 * SEC:                       # variante scale-free : seuil = Q90 |drift| PAR ACTIF
                    av = np.abs(bd[~np.isnan(bd)])
                    if len(av) > 50:
                        qthr = np.quantile(av, 0.90)
                        candq = np.abs(bd) >= qthr
                        for H in HS:
                            g = collect_events(Tg, bd, candq, ts, mid, H)
                            if len(g):
                                perpQ.setdefault((pair, H), []).append(g)

    def cat(d, key):
        return np.concatenate(d[key]) if key in d and d[key] else np.array([])

    # ---- PANEL A : pooled W x THR x H — gross (continuation), FOLLOW net, hit, worst, + FADE net (contraste) ----
    print("--- A. POOLED : suivre Binance-extreme sur HL. gross=move HL dans le sens Binance (bps).")
    print("       FOLLOW net = gross - frais. FADE net = -gross - frais (fader le lead). NULL net = -frais. ---")
    print(f"  {'W':>4} {'THR':>4} {'H':>5} | {'n':>4} | {'gross':>6} | {'foll6':>6} {'foll4.5':>7} {'hit%':>4} {'worst':>6} | {'fade6':>6}")
    for W in WS:
        for THR in THRS:
            for H in HS:
                g = cat(pooled, (W, THR, H))
                s6 = stat(g, FEE_TK)
                if s6 is None:
                    continue
                n, gr, net6, hit, worst = s6
                net45 = float(np.mean(g - FEE_TKMK))
                faden = float(np.mean(-g - FEE_TK))
                print(f"  {W//SEC:>4} {THR:>4.0f} {H//SEC:>5} | {n:>4} | {gr:>+6.1f} | {net6:>+6.1f} {net45:>+7.1f} {hit:>3.0f}% {worst:>+6.0f} | {faden:>+6.1f}")
        print()

    # ---- PANEL B : par actif, seuil ABSOLU W=120 THR=50, H sweep ----
    print("--- B. PAR ACTIF (W=120s, THR=50bps absolu) : FOLLOW net (RT6) — qui suit bien le lead ? (+ HYPE) ---")
    print(f"  {'actif':>9} | " + " ".join(f"H={h//SEC:>4}" for h in HS))
    for pair in HLPAIRS:
        cells = []
        for H in HS:
            g = cat(perp, (pair, 120 * SEC, 50.0, H))
            cells.append(f"{np.mean(g-FEE_TK):>+5.0f}({len(g):>3})" if len(g) >= 5 else f"{'n<5':>9}")
        print(f"  {pair:>9} | " + " ".join(cells))

    # ---- PANEL C : par actif, seuil SCALE-FREE Q90 |drift_120| PAR ACTIF (instinct user : roulant/par-paire) ----
    print(f"\n--- C. PAR ACTIF (W=120s, seuil = Q90 du |drift Binance| PROPRE a l'actif) : FOLLOW net (RT6) ---")
    print(f"  {'actif':>9} | " + " ".join(f"H={h//SEC:>4}" for h in HS))
    for pair in HLPAIRS:
        cells = []
        for H in HS:
            g = cat(perpQ, (pair, H))
            cells.append(f"{np.mean(g-FEE_TK):>+5.0f}({len(g):>3})" if len(g) >= 5 else f"{'n<5':>9}")
        print(f"  {pair:>9} | " + " ".join(cells))

    print("\n  LECTURE :")
    print("   - gross > frais (6) sur PLUSIEURS cellules -> suivre un Binance-extreme est un VRAI edge offensif (pas que defensif).")
    print("   - fade6 tres negatif aux memes points -> fader un Binance-lead = la catastrophe -> l'exit-contraire est justifie.")
    print("   - si gross monte avec THR et baisse avec H -> signal court & fort = reactivite (WS Binance live).")
    print("   - par actif : confirme si HYPE/SOL (volatils) suivent mieux ; BTC peu d'events (extreme = rare) = normal.")


if __name__ == "__main__":
    main()
