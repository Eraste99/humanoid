# -*- coding: utf-8 -*-
from __future__ import annotations

"""
EOD PnL Runner — Reconciliation CEX ↔ DB (M5-D-3 + M5-E-1)

Rôle:
- Charger BotConfig.from_env()
- Instancier un LoggerHistoriqueManager en lecture seule (DB SQLite)
- Appeler LoggerHistoriqueManager.run_pnl_reconciliation_for_day(...)
- Pousser les métriques Prometheus pnl_reco_* (via LHM)
- Produire un rapport JSON (stdout ou fichier)

Le runner N'EFFECTUE AUCUN TRADE.
Il ne fait que lire la DB + CEX et calculer des écarts PnL.

Usage typique (CLI):

    python -m modules.LOGGERHISTORIQUE.eod_pnl_runner \\
        --out-dir /data/loggerh \\
        --local-day 2025-12-02 \\
        --region EU \\
        --output /data/reports/pnl_reco-20251202.json

Pour la partie CEX, tu peux:
- soit fournir un adaptateur "réel" (modules.reconciler_cex_adapter.fetch_pnl_cex_for_day),
- soit utiliser --skip-cex pour ne travailler que côté DB.

L'adaptateur attendu (signature):

    def fetch_pnl_cex_for_day(exchange: str,
                              account_alias: str,
                              local_day: str,
                              region: str) -> Mapping:
        return {
            "net_profit_quote": float,
            "fees_quote": float,
            "turnover_quote": float,
            "trades": int,
        }

"""

import asyncio
import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Callable, Mapping, Optional

# Ces imports supposent la structure modules/...
try:
    from modules.bot_config import BotConfig
    from modules.logger_historique.logger_historique_manager import LoggerHistoriqueManager
except Exception as exc:  # pragma: no cover - erreur d'import précoce
    print(f"[eod_pnl_runner] Import error: {exc}", file=sys.stderr)
    raise


logger = logging.getLogger("EODPnLRunner")


# ---------------------------------------------------------------------------
# Helpers CEX adapter
# ---------------------------------------------------------------------------

FetchPnlCexFn = Callable[[str, str, str, str], Mapping[str, Any]]


def _make_fetch_pnl_cex_fn_or_stub(cfg: BotConfig) -> FetchPnlCexFn | None:
    """
    Construit un adaptateur PnL CEX ↔ DB basé sur BalanceFetcher.

    On s'attend à trouver dans modules.balance_fetcher une classe
    PnlRecoCexAdapter(cfg, balance_fetcher=None) exposant:

        async def fetch_pnl_for_day(
            self,
            exchange: str,
            account_alias: str,
            local_day: str,
            region: str,
        ) -> Mapping[str, Any]:

    Cette méthode pourra utiliser les mêmes clients que BalanceFetcher
    pour reconstruire un PnL jour par (exchange,account_alias).

    Si l'import échoue, on retourne None et le runner pourra soit
    utiliser --skip-cex, soit accepter un "skipped" côté LHM.
    """
    try:
        from modules.balance_fetcher import PnlRecoCexAdapter  # type: ignore
    except Exception:
        logger.warning(
            "PnlRecoCexAdapter introuvable dans modules.balance_fetcher. "
            "Utilise --skip-cex ou implémente PnlRecoCexAdapter.fetch_pnl_for_day()."
        )
        return None

    try:
        adapter = PnlRecoCexAdapter(cfg)
    except Exception:
        logger.exception("Impossible d'instancier PnlRecoCexAdapter(cfg)")
        return None

    # On renvoie directement la méthode de l'adaptateur; le runner gère
    # le fait qu'elle soit sync ou async (via asyncio.iscoroutine).
    return adapter.fetch_pnl_for_day


# ---------------------------------------------------------------------------
# Core runner
# ---------------------------------------------------------------------------

async def run_eod_pnl_reconciliation(
    *,
    out_dir: str,
    local_day: Optional[str] = None,
    region: Optional[str] = None,
    output_path: Optional[str] = None,
    skip_cex: bool = False,
    dry_run: bool = False,
) -> dict:
    """
    EOD runner PnL (jour local) pour une région donnée.

    Paramètres
    ----------
    out_dir:
        Répertoire racine utilisé par LoggerHistoriqueManager (DB SQLite).
        Doit pointer vers le même répertoire que celui utilisé en prod par le bot.
    local_day:
        Jour local 'YYYY-MM-DD'. Si None, le runner laisse le LHM dériver
        'aujourd'hui' dans la timezone régionale.
    region:
        Région logique ("EU", "US", ...). Si None, utilise cfg.g.pod_region.
    output_path:
        Si fourni, chemin d'un fichier JSON où écrire le rapport.
        Sinon, le rapport est écrit sur stdout (JSON pretty).
    skip_cex:
        Si True, n'appelle pas les CEX (fetch_pnl_cex_fn=None).
        Le LHM renverra un "skipped: no_fetch_pnl_cex_fn".
    dry_run:
        Si True, ne pousse pas les métriques Prometheus côté LHM.
        Utile en test local / développement.

    Retour
    ------
    dict : structure obtenue depuis LoggerHistoriqueManager.run_pnl_reconciliation_for_day()
    """
    cfg = BotConfig.from_env()

    # Résolution de la région effective
    if region is None or not str(region).strip():
        region_effective = str(getattr(cfg.g, "pod_region", "EU")).upper()
    else:
        region_effective = str(region).strip().upper()

    logger.info(
        "EOD PnL Reconciliation — out_dir=%s, local_day=%s, region=%s, skip_cex=%s, dry_run=%s",
        out_dir,
        local_day,
        region_effective,
        skip_cex,
        dry_run,
    )

    lhm = LoggerHistoriqueManager(cfg, out_dir)

    if skip_cex:
        fetch_fn = None
    else:
        fetch_fn = _make_fetch_pnl_cex_fn_or_stub(cfg)
        if fetch_fn is None:
            # On logge fort, mais on laisse la méthode retourner un "skipped"
            logger.warning(
                "Aucun fetch_pnl_cex_fn disponible via BalanceFetcher/PnlRecoCexAdapter ; "
                "la reco PnL CEX↔DB sera probablement SKIPPED. "
                "Utilise --skip-cex pour un run DB-only propre."
            )
            fetch_fn = None

    result = await lhm.run_pnl_reconciliation_for_day(
        local_day=local_day,
        pod_region=region_effective,
        fetch_pnl_cex_fn=fetch_fn,
        dry_run=dry_run,
    )

    # Écriture du rapport
    if output_path:
        try:
            path = Path(output_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False, indent=2, sort_keys=True, default=str)
            logger.info("EOD PnL reco report written to %s", path)
        except Exception:
            logger.exception("Failed to write EOD PnL reco report to %s", output_path)
    else:
        # stdout pour les usages en CLI simple / dev
        json.dump(result, sys.stdout, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        sys.stdout.write("\n")
        sys.stdout.flush()

    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="EOD PnL Reconciliation Runner (CEX ↔ DB)",
    )
    parser.add_argument(
        "--out-dir",
        required=True,
        help="Répertoire racine utilisé par LoggerHistoriqueManager (DB SQLite).",
    )
    parser.add_argument(
        "--local-day",
        default=None,
        help="Jour local au format YYYY-MM-DD (défaut: aujourd'hui dans la timezone de la région).",
    )
    parser.add_argument(
        "--region",
        default=None,
        help="Région logique (EU/US/...). Défaut: cfg.g.pod_region.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Chemin d'un fichier JSON où écrire le rapport (sinon: stdout).",
    )
    parser.add_argument(
        "--skip-cex",
        action="store_true",
        help="Ne pas appeler les CEX (reco PnL basée uniquement sur la DB).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="N'émet PAS les métriques pnl_reco_* (lecture seule).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Niveau de logs (défaut: INFO).",
    )
    return parser.parse_args(argv)


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    _setup_logging(args.log_level)

    try:
        asyncio.run(
            run_eod_pnl_reconciliation(
                out_dir=args.out_dir,
                local_day=args.local_day,
                region=args.region,
                output_path=args.output,
                skip_cex=args.skip_cex,
                dry_run=args.dry_run,
            )
        )
    except KeyboardInterrupt:
        logger.warning("Interrupted by user (Ctrl+C).")
        return 130
    except Exception:
        logger.exception("EOD PnL Reconciliation failed")
        return 1

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
