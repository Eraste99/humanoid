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
import os
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

def _resolve_window(local_day: Optional[str], region: str) -> tuple[int | None, int | None]:
    try:
        from datetime import datetime, timezone, timedelta
        try:
            from zoneinfo import ZoneInfo  # type: ignore
        except Exception:
            ZoneInfo = None  # type: ignore
        tz_name = {
            "EU": "Europe/Rome",
            "US": "America/New_York",
            "JP": "Asia/Tokyo",
        }.get(region, "Europe/Rome")
        if local_day and str(local_day).strip():
            day = str(local_day).strip()
        else:
            if ZoneInfo is not None:
                dt_now = datetime.now(ZoneInfo(tz_name))
            else:
                dt_now = datetime.now()
            day = dt_now.strftime("%Y-%m-%d")
        dt_start = datetime.strptime(day, "%Y-%m-%d")
        if ZoneInfo is not None:
            dt_start = dt_start.replace(tzinfo=ZoneInfo(tz_name))
        else:
            dt_start = dt_start.replace(tzinfo=timezone.utc)
        dt_end = dt_start + timedelta(days=1)
        dt_start_utc = dt_start.astimezone(timezone.utc)
        dt_end_utc = dt_end.astimezone(timezone.utc)
        since_ms = int(dt_start_utc.timestamp() * 1000)
        until_ms = int(dt_end_utc.timestamp() * 1000) - 1
        return since_ms, until_ms
    except Exception:
        return None, None


def _atomic_write_json(path: Path, payload: dict) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.parent.mkdir(parents=True, exist_ok=True)
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=True, default=str)
    tmp_path.replace(path)

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
    g_cfg = getattr(cfg, "g", None)
    if g_cfg is None:
        raise ValueError("cfg.g is required for EOD runner")

    # Résolution de la région effective
    if region is None or not str(region).strip():
        region_effective = str(getattr(cfg.g, "pod_region", "EU")).upper()
        logger.info("REGION_DEFAULTED=1 region=%s", region_effective)
    else:
        region_effective = str(region).strip().upper()

    logger.info(
        "EOD run — out_dir=%s, local_day=%s, region=%s",
        out_dir,
        local_day,
        region_effective,
    )

    enable_jp = bool(getattr(g_cfg, "enable_jp", False))
    if region_effective == "JP" and not enable_jp:
        reason_code = "REGION_UNSUPPORTED_JP"
        since_ms, until_ms = _resolve_window(local_day, region_effective)
        result = {
            "status": "ERROR",
            "reason_code": reason_code,
            "region": region_effective,
            "day_local": local_day,
            "window_since_ms": since_ms,
            "window_until_ms": until_ms,
            "pnl_reconciliation": {
                "state": "SKIPPED",
                "reason_code": reason_code,
            },
            "cutoff_definition": f"local_day[{region_effective}] -> UTC window",
            "thresholds": {
                "abs_diff_quote": None,
                "ratio_diff": None,
            },
            "conventions": {"base_ccy": "USDC", "quote_ccy": "USDC"},
        }
        if output_path:
            try:
                _atomic_write_json(Path(output_path), result)
                logger.info("EOD report written to %s", output_path)
            except Exception:
                logger.exception("Failed to write EOD report to %s", output_path)
        else:
            json.dump(result, sys.stdout, ensure_ascii=False, indent=2, sort_keys=True, default=str)
            sys.stdout.write("\n")
            sys.stdout.flush()
        return result
    if skip_cex:
        exchanges = getattr(g_cfg, "enabled_exchanges", None)
        logger.info("SKIPPED_CEX=1 exchanges=%s", exchanges)
    if skip_finalize:
        logger.info("SKIPPED_FINALIZE=1")

    if sign_priv_pem_path:
        priv_path = Path(sign_priv_pem_path)
        if not priv_path.exists():
            raise FileNotFoundError(f"sign-priv-pem not found: {priv_path}")
        if not os.access(priv_path, os.R_OK):
            raise PermissionError(f"sign-priv-pem not readable: {priv_path}")
        try:
            mode = priv_path.stat().st_mode
            if mode & 0o004:
                logger.warning("sign-priv-pem is world-readable: %s", priv_path)
        except Exception:
            logger.warning("sign-priv-pem stat failed for path: %s", priv_path)
    lhm = LoggerHistoriqueManager(cfg, out_dir)

    fetch_pnl_cex_fn: Callable[[str, str, str, str], Any] | None = None
    if not skip_cex:
        try:
            from modules.balance_fetcher import PnlRecoCexAdapter  # type: ignore

            adapter = PnlRecoCexAdapter(cfg)
            fetch_pnl_cex_fn = adapter.fetch_pnl_for_day
        except Exception as exc:
            logger.error("PnlRecoCexAdapter unavailable; cannot run CEX reconciliation", exc_info=True)
            raise RuntimeError("PNL_RECO_ADAPTER_UNAVAILABLE") from exc
    await lhm.start()
    try:
        if fetch_pnl_cex_fn is None and not skip_cex:
            lhm._emit_pnl_reco_metrics(
                region=region_effective,
                exchange="NA",
                account_alias="NA",
                state="ERROR",
                reason_code="PNL_RECO_SKIPPED_ADAPTER_MISSING",
                abs_diff_quote=0.0,
                error_stage="adapter_missing",
            )
            result = {
                "status": "ERROR",
                "reason_code": "PNL_RECO_SKIPPED_ADAPTER_MISSING",
                "region": region_effective,
                "day_local": local_day,
                "pnl_reconciliation": {
                    "state": "ERROR",
                    "reason_code": "PNL_RECO_SKIPPED_ADAPTER_MISSING",
                },
            }
        else:
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
    since_ms, until_ms = _resolve_window(local_day, region_effective)
    pnl_reco = result.get("pnl_reconciliation") if isinstance(result, dict) else {}
    report = dict(result or {})
    report.setdefault("region", region_effective)
    report.setdefault("day_local", local_day)
    report.setdefault("window_since_ms", pnl_reco.get("window_since_ms") or since_ms)
    report.setdefault("window_until_ms", pnl_reco.get("window_until_ms") or until_ms)
    if isinstance(pnl_reco, dict):
        report.setdefault("pnl_reconciliation", pnl_reco)
    report.setdefault(
        "cutoff_definition",
        f"local_day[{region_effective}] -> UTC window",
    )
    report.setdefault(
        "thresholds",
        {
            "abs_diff_quote": getattr(lhm, "_pnl_reco_tol_abs_quote", None),
            "ratio_diff": getattr(lhm, "_pnl_reco_tol_pct", None),
        },
    )


    report.setdefault("conventions", {"base_ccy": "USDC", "quote_ccy": "USDC"})

    if output_path:
        try:
            _atomic_write_json(Path(output_path), report)
            logger.info("EOD report written to %s", output_path)
        except Exception:
            logger.exception("Failed to write EOD report to %s", output_path)
    else:
        # stdout pour les usages en CLI simple / dev
        json.dump(report, sys.stdout, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        sys.stdout.write("\n")
        sys.stdout.flush()

    return report

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
        help=(
            "Ne pas tenter la réconciliation PnL CEX ↔ DB (rapport partiel, reco marquée SKIPPED,"
            " non acceptable pour un GO institutionnel)."
        ),
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
        "--mismatch-exit-code",
        type=int,
        default=2,
        help="Exit code en cas de mismatch PnL détecté.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Niveau de logs (défaut: INFO).",
    )
    return parser.parse_args(argv)


def _setup_logging(level: str) -> None:
    try:
        sys.path.append(os.getcwd())
        from boot import configure_logging
        # On fabrique une config factice pour passer le niveau
        class FakeCfg:
            class FakeObs:
                def __init__(self, lvl): self.log_level = lvl
            def __init__(self, lvl): self.obs = self.FakeObs(lvl)
        
        configure_logging(FakeCfg(level))
    except Exception:
        # Fallback si boot.py n'est pas accessible
        logging.basicConfig(
            level=getattr(logging, level.upper(), logging.INFO),
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        )


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    _setup_logging(args.log_level)

    try:
        report = asyncio.run(
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

    try:
        if isinstance(report, dict):
            pnl_reco = report.get("pnl_reconciliation") or {}
            state = pnl_reco.get("state") or report.get("state") or report.get("status")

            if state == "MISMATCH":
                if int(args.mismatch_exit_code) != 0:
                    return int(args.mismatch_exit_code)
                logger.info("MISMATCH_DETECTED=1")
    except Exception:
        logger.exception("Mismatch detection failed")

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())