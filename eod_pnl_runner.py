# -*- coding: utf-8 -*-
from __future__ import annotations

"""
EOD Runner — orchestration canonique via LoggerHistoriqueManager

Rôle:
- Charger BotConfig.from_env()
- Instancier un LoggerHistoriqueManager
- Appeler la nouvelle API canonique LoggerHistoriqueManager.run_eod(...)
- Produire un meta dict stable (stdout ou fichier) identique à celui retourné par l'API

Le runner n'effectue aucun trade. Il déclenche seulement le flush/finalize EOD
du logger historique et émet un marqueur JSONL canon côté LHM.
"""

import asyncio
import argparse
import json
import logging
import sys
from typing import Callable, Any
from pathlib import Path
from typing import Optional

# Ces imports supposent la structure modules/...
try:
    from modules.bot_config import BotConfig
    from modules.logger_historique.logger_historique_manager import LoggerHistoriqueManager
except Exception as exc:  # pragma: no cover - erreur d'import précoce
    print(f"[eod_pnl_runner] Import error: {exc}", file=sys.stderr)
    raise


logger = logging.getLogger("EODPnLRunner")


# ---------------------------------------------------------------------------
# Core runner
# ---------------------------------------------------------------------------

async def run_eod(
    *,
    out_dir: str,
    local_day: Optional[str] = None,
    region: Optional[str] = None,
    output_path: Optional[str] = None,
    sign_priv_pem_path: Optional[str] = None,
    skip_cex: bool = False,
    dry_run: bool = False,
    skip_finalize: bool = False,
) -> dict:
    """
    EOD runner canonique (flush + finalize + marker JSONL).

    Paramètres
    ----------
    out_dir:
        Répertoire racine utilisé par LoggerHistoriqueManager (DB SQLite).
    local_day:
        Jour local 'YYYY-MM-DD'. Si None, le LHM dérive le jour courant.
    region:
        Région logique ("EU", "US", ...). Si None, utilise cfg.g.pod_region.
    output_path:
        Si fourni, chemin d'un fichier JSON où écrire le rapport (sinon stdout).
    sign_priv_pem_path:
        Clé privée optionnelle pour signature du backup DB via LogWriter.

    Retour
    ------
    dict : meta dict retourné par LoggerHistoriqueManager.run_eod()
    """
    cfg = BotConfig.from_env()

    # Résolution de la région effective
    if region is None or not str(region).strip():
        region_effective = str(getattr(cfg.g, "pod_region", "EU")).upper()
    else:
        region_effective = str(region).strip().upper()

    logger.info(
        "EOD run — out_dir=%s, local_day=%s, region=%s",
        out_dir,
        local_day,
        region_effective,
    )

    lhm = LoggerHistoriqueManager(cfg, out_dir)

    fetch_pnl_cex_fn: Callable[[str, str, str, str], Any] | None = None
    if not skip_cex:
        try:
            from modules.balance_fetcher import PnlRecoCexAdapter  # type: ignore

            adapter = PnlRecoCexAdapter(cfg)
            fetch_pnl_cex_fn = adapter.fetch_pnl_for_day
        except Exception:
            logger.warning("PnlRecoCexAdapter unavailable; skipping CEX reconciliation")
            skip_cex = True
    await lhm.start()
    try:
        result = await lhm.run_eod(
            local_day=local_day,
            pod_region=region_effective,
            sign_priv_pem_path=sign_priv_pem_path,
            fetch_pnl_cex_fn=fetch_pnl_cex_fn,
            skip_cex=skip_cex,
            dry_run=dry_run,
            skip_finalize=skip_finalize,
        )
    finally:
        await lhm.stop()

    # Écriture du rapport
    if output_path:
        try:
            path = Path(output_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False, indent=2, sort_keys=True, default=str)
            logger.info("EOD report written to %s", path)
        except Exception:
            logger.exception("Failed to write EOD report to %s", output_path)
    else:
        # stdout pour les usages en CLI simple / dev
        json.dump(result, sys.stdout, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        sys.stdout.write("\n")
        sys.stdout.flush()

    return result


# Backward compatibility alias
async def run_eod_pnl_reconciliation(**kwargs):
    return await run_eod(**kwargs)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="EOD Runner (LoggerHistoriqueManager)",
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
        "--sign-priv-pem",
        default=None,
        help="Chemin d'une clé privée PEM pour signer le backup DB (optionnel).",
    )
    parser.add_argument(
        "--skip-cex",
        action="store_true",
        help="Ne pas tenter la réconciliation PnL CEX ↔ DB (reco marquée SKIPPED).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Mode dry-run pour la réconciliation PnL (pas de métriques).",
    )
    parser.add_argument(
        "--skip-eod-finalize",
        action="store_true",
        help="Ne pas appeler finalize_day côté LogWriter (utile pour reco seule).",
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
            run_eod(
                out_dir=args.out_dir,
                local_day=args.local_day,
                region=args.region,
                output_path=args.output,
                sign_priv_pem_path=args.sign_priv_pem,
                skip_cex=args.skip_cex,
                dry_run=args.dry_run,
                skip_finalize=args.skip_eod_finalize,
            )
        )
    except KeyboardInterrupt:
        logger.warning("Interrupted by user (Ctrl+C).")
        return 130
    except Exception:
        logger.exception("EOD run failed")
        return 1

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())