"""BLOC 5 OFFENSIF — PROBE FENETRE RAPIDE. Le test minutes (_fade_binlead_offensive_hl) montre gross HL ~= 0 a 60s-30min.
MAIS la recherche Binance-lead (2026-05-19) trouvait l'edge a 2-5s (net taker flow). Ici : la CONTINUATION HL existe-t-elle
a 2/5/10/30s apres un Binance-extreme ? Courbe de decay du gross (move HL dans le sens Binance) vs H, LAG=1s, grille 5s.
Decisif : si gross@5s < 2.5 bps (1 jambe taker) -> aucun edge offensif TRADEABLE a aucune vitesse (user ne court pas la ms).
Si gross@5s eleve mais decay avant 60s -> edge existe mais exige ms-racing (que le user refuse). HL true-mid, causal, 0 sampling.
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
HLPAIRS = ["BTCUSDC", "ETHUSDC", "SOLUSDC", "XRPUSDC", "HYPEUSDC"]
BINSYM = {"BTCUSDC": "BTCUSDT", "ETHUSDC": "ETHUSDT", "SOLUSDC": "SOLUSDT", "XRPUSDC": "XRPUSDT", "HYPEUSDC": "HYPEUSDT"}
STEP = 5 * SEC
LAG = 1 * SEC
WS = [30 * SEC, 60 * SEC]
THRS = [30.0, 50.0]
HS = [1 * SEC, 2 * SEC, 3 * SEC, 5 * SEC, 10 * SEC, 20 * SEC, 30 * SEC, 60 * SEC, 120 * SEC, 300 * SEC]
FEE_1TK = 2.5; FEE_RTTK = 6.0


def bdrift_vec(B, Tg, W):
    bts, bmid = B; n = len(bts)
    i = np.searchsorted(bts, Tg, side="left"); j = np.searchsorted(bts, Tg - W, side="left")
    ic = np.clip(i, 0, n - 1); jc = np.clip(j, 0, n - 1); span = bts[ic] - bts[jc]
    ok = (i < n) & (j < n) & (i > j) & (span >= 0.5 * W) & (span <= 2.0 * W)
    d = np.full(len(Tg), np.nan); bj = bmid[jc]
    d[ok] = (bmid[ic][ok] - bj[ok]) / bj[ok] * 1e4
    return d


def hl_fwd(ts, mid, Tent, H):
    n = len(ts); ei = np.searchsorted(ts, Tent, side="left"); ok = ei < n
    ei = np.clip(ei, 0, n - 1); xj = np.searchsorted(ts, ts[ei] + H, side="left"); ok &= xj < n
    xj = np.clip(xj, 0, n - 1); f = np.full(len(Tent), np.nan)
    f[ok] = (mid[xj[ok]] - mid[ei[ok]]) / mid[ei[ok]] * 1e4
    tol = 2 * SEC + H                                   # serre : pas de contamination par ticks epars aux H courts
    f[(ts[xj] - ts[ei]) > tol] = np.nan
    f[(ts[ei] - Tent) > 5 * SEC] = np.nan
    return f


def main():
    print("=== BLOC 5 OFFENSIF — PROBE RAPIDE : continuation HL apres Binance-extreme, courbe vs H (2s..300s), LAG=1s ===\n")
    binD = {s: load_bin(s) for s in set(BINSYM.values())}
    pooled = {}; perp = {}
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
                    cand = np.where(np.abs(bd) >= THR)[0]
                    if len(cand) == 0:
                        continue
                    dirs = np.sign(bd[cand]); Tent = Tg[cand] + LAG
                    for H in HS:
                        fwd = hl_fwd(ts, mid, Tent, H); ok = ~np.isnan(fwd) & (dirs != 0)
                        g = dirs[ok] * fwd[ok]
                        if len(g):
                            pooled.setdefault((W, THR, H), []).append(g)
                            if W == 60 * SEC and THR == 50.0:
                                perp.setdefault((pair, H), []).append(g)

    def cat(d, k):
        return np.concatenate(d[k]) if k in d and d[k] else np.array([])

    print("--- POOLED : gross = continuation HL (bps) dans le sens du Binance-extreme. Repere : 1 jambe taker=2.5, RT taker=6 ---")
    for W in WS:
        for THR in THRS:
            print(f"\n  W={W//SEC}s THR={THR:.0f}bps :")
            print(f"    {'H':>5} | {'n':>4} | {'gross':>6} | {'hit%':>4} | {'>2.5?':>5} {'>6?':>4}")
            for H in HS:
                g = cat(pooled, (W, THR, H))
                if len(g) < 5:
                    continue
                gr = float(np.mean(g)); hit = 100 * np.mean(g > 0)
                print(f"    {H//SEC if H>=SEC else 0:>4}s | {len(g):>4} | {gr:>+6.2f} | {hit:>3.0f}% | {'OUI' if gr>FEE_1TK else 'non':>5} {'OUI' if gr>FEE_RTTK else 'non':>4}")

    print(f"\n--- PAR ACTIF (W=60s, THR=50) : gross continuation vs H — HYPE/SOL suivent-ils mieux ? ---")
    print(f"  {'actif':>9} | " + " ".join(f"{h//SEC if h>=SEC else 0:>3}s" for h in HS))
    for pair in HLPAIRS:
        cells = []
        for H in HS:
            g = cat(perp, (pair, H))
            cells.append(f"{np.mean(g):>+4.0f}" if len(g) >= 5 else f"{'.':>4}")
        print(f"  {pair:>9} | " + " ".join(cells))

    print("\n  LECTURE : si la colonne gross reste < 2.5 a TOUS les H -> aucun edge offensif tradeable (meme en taker rapide).")
    print("    si gross culmine a 2-5s puis decay -> edge ms-only (le user ne court pas la ms) -> bloc offensif = ABANDON.")
    print("    Binance-extreme reste utile DEFENSIVEMENT (skip/exit le fade) — pas comme entree offensive.")


if __name__ == "__main__":
    main()
