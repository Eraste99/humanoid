# Fade-bot research — Hyperliquid mean-reversion (mai 2026)

Backup **recherche** (scripts d'analyse + conclusions). Pas de secrets, pas de data/modèles (cf `.gitignore`).
Données brutes (HL L2/ticks, Binance) hors-repo. Tout est **in-sample (mai, 2 semaines) — PENDING juge OOS 11 juin.**

## Le bot déployable (état final)
À chaque fin de jambe zig-zag **50 bps** en **régime tendanciel** (drift_4h tercile bas=bear / haut=bull, chop exclu) :
1. **FADE l'extension** — bear : down-leg → long le dip ; bull : up-leg → short le rip (+ filtre accel-1h).
2. **Filtre FLUX** — skip si `sign(tfi_5min)==jambe & |tfi_5min|≥Q70` (flux taker soutient la jambe = toxique).
3. **Filtre BINANCE-LEAD** — skip si `|drift Binance 120s| ≥ 30bps & sign==jambe` (le marché leader court déjà = continuation informée). *Binance = SIGNAL only (exec interdite MiCA), usage **DÉFENSIF** ; le mode OFFENSIF (FOLLOW un Binance-extrême en taker) a été **testé & réfuté** — continuation HL sub-1bps à tous horizons, rien à chasser au timescale non-ms.*
4. **Filtre TREND-PERSISTANT (`drift_8h`)** — skip si la jambe étend un trend 8h fort (`sign(drift_8h)==jambe & |drift_8h|≥Q70`). = le **détecteur choppy-vs-trend** (net-drift multi-heures, PAS l'Efficiency-Ratio qui a été réfuté) → gère **HYPE dynamiquement** (fadé quand il choppe, skippé quand il trend), sans hardcode.
5. **Hold 30 min, sortie maker, PAS de stop dur, sizing petit.** Univers = perps liquides (cœur **BTC/SOL/XRP**) ; **HYPE non hardcode-exclu** (géré par le gate `drift_8h`).

**Perf in-sample (fills réels + 2 filtres) :** net ~+11 bps/trade, worst −71/−101 (vs −238 sans Binance-lead), maxDD ÷2, ~13+ trades/j, cross-week + 2 régimes.

## Résultats clés (détail complet : `research_memory/unified_fade_bot_spec_2026-05-30.md`)
- **Le FADE est le seul vrai alpha** (mean-reversion, null-contrôlé, décompo, cross-week clean, exécution réelle).
- **Momentum / HYPE-trend = BÉTA, pas alpha** (pire que buy&hold) → abandonné.
- **Stop dur = contre-productif** ; on size pour la queue.
- **Queue short-gamma RÉDUCTIBLE** via filtre Binance-lead (cross-venue) — qui améliore l'EV en plus.
- Détecteurs d'état single-venue (OFI/thinning/breadth) = échouent ; seuls FLUX (Ahern) + Binance-lead marchent.

## Scripts (sélection)
`_oscillation_capture.py` (matrice RIDE/FADE/NULL) · `_oscillation_decompose.py` · `_fade_maker_fill_hl.py` (fill carnet L2) · `_fade_unified_validate_hl.py` (bot unifié) · `_fade_regime_filter_hl.py` (filtre FLUX) · `_crossvenue_lead_catastrophe.py` + `_fade_binlead_fullbook_hl.py` (le breakthrough Binance-lead, défensif) · `_fade_longtrend_gate_hl.py` (détecteur choppy/trend `drift_8h`) · `_fade_er_windows_hl.py` (Efficiency-Ratio réfuté) · `_fade_binlead_offensive_hl.py` + `_fade_binlead_offensive_fast_hl.py` (mode offensif FOLLOW = réfuté) · `_fade_sizing_*.py` (sizing). Loader commun : `_tick_exhaustive_search.py`.

⚠️ In-sample/mai. Le juge = OOS 11 juin (carnet HL frais). Ne pas sizer avant.
