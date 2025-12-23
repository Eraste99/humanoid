# -*- coding: utf-8 -*-
"""
Boot — orchestrateur (from scratch, cohorte-aware, industry‑grade)
=================================================================
- Câble et démarre UNIQUEMENT le chemin critique (pas de watchdogs ni d'obs ici).
- Respecte la rotation/cohortes Scanner (PRIMARY/AUDITION) avec univers large en entrée.
- 100% piloté par BotConfig : discovery optionnelle avec fallback config.
- RM instancié selon sa signature (kwargs), BalanceFetcher avec start/loop/stop natifs.
- Gates READY + état degraded/reasons + API get_status()/wait_ready().

Séquence :
 1) LHM.start → 2) Discovery (optionnelle) → 3) WS Publics.start → 4) Router.start
 5) RM.init + start → 6) Scanner.init + set_universe + start (+bind Router)
 7) Private plane: Hub.start → Reconciler.start → Balances.start (+loop si PROD)
 8) Engine.init + hub.register_callback + Engine.start
 9) RPCServer.start (si activé) → Evaluate READY (warmup/timeout) → ctx
"""
from __future__ import annotations

import asyncio
import logging
import time
import contextlib
import inspect
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
from modules.risk_manager.risk_manager import RiskManager
from modules.logger_historique.logger_historique_manager import LoggerHistoriqueManager
from modules.pairs_discovery import discover_pairs_3cex
from modules.websockets_clients import WebSocketExchangeClient
from modules.slippage_handler import SlippageHandler
from modules.market_data_router import MarketDataRouter
from modules.volatility_monitor import VolatilityMonitor
from modules.opportunity_scanner import OpportunityScanner
from modules.private_ws_hub import PrivateWSHub
from modules.private_ws_reconciler import PrivateWSReconciler
from modules.execution_engine import ExecutionEngine
from modules.rpc_gateway import RPCServer
from modules.balance_fetcher import MultiBalanceFetcher
from contracts.payloads import normalize_reason_code
from modules.utils.rate_limiter import RateLimiter
from modules import obs_metrics
from modules.retry_policy import BackoffPolicy
from contracts import  payloads as contracts

# Config type (indicatif)
try:
    from modules.bot_config import BotConfig  # pragma: no cover
except Exception:  # pragma: no cover
    BotConfig = Any  # fallback typing only

LOGGER = logging.getLogger("Boot")


# ----------------------------- Contexte de Boot -----------------------------
@dataclass
class BootContext:
    cfg: Any
    # I/O & pipeline
    in_queue: Optional[asyncio.Queue] = None
    ws_public: Any | None = None
    router: Any | None = None
    volatility: Any | None = None
    slippage: Any | None = None
    # cœur
    rm: Any | None = None
    scanner: Any | None = None
    engine: Any | None = None
    rate_limiter: Any | None = None
    retry_policy: Any | None = None
    # privé
    pws_hub: Any | None = None
    reconciler: Any | None = None
    balances: Any | None = None
    # rpc
    rpc_server: Any | None = None
    rpc_client: Any | None = None
    # utilitaires
    lhm: Any | None = None
    # univers
    pairs_map: Dict[str, Any] = field(default_factory=dict)
    discovered_pairs: List[str] = field(default_factory=list)
    active_pairs: List[str] = field(default_factory=list)


# --------------------------- Proxy Scanner paresseux ------------------------
class _LazyScannerProxy:
    """Brise la dépendance circulaire Router⇄Scanner. Droppe tant que pas bindé."""
    __slots__ = ("_target", "dropped")

    def __init__(self) -> None:
        self._target: Any | None = None
        self.dropped: int = 0

    def bind(self, scanner: Any) -> None:
        self._target = scanner

    def update_orderbook(self, ev: Dict[str, Any]) -> None:
        t = self._target
        if t is None:
            self.dropped += 1
            return None
        return t.update_orderbook(ev)


# --------------------------------- Boot ------------------------------------
class Boot:
    def __init__(self, cfg: BotConfig, *, logger: Optional[logging.Logger] = None) -> None:
        self.cfg = cfg
        self.log = logger or LOGGER
        self.ctx = BootContext(cfg=cfg)

        # Readiness flags (events)
        self.ready_ws = asyncio.Event()
        self.ready_router = asyncio.Event()
        self.ready_scanner = asyncio.Event()
        self.ready_rm = asyncio.Event()
        self.ready_rm_loops = asyncio.Event()
        self.ready_engine = asyncio.Event()
        self.ready_private = asyncio.Event()
        self.ready_balances = asyncio.Event()
        self.ready_rpc = asyncio.Event()
        self.ready_all = asyncio.Event()

        # État structuré
        self.state: Dict[str, Any] = {
            "stage": "init",
            "degraded": False,
            "reasons": [],  # warmup_pairs_missing, balances_stale, rpc_unavailable, warmup_timeout
            "timestamps": {},
        }

        # Tâches périodiques
        self._balances_task: Optional[asyncio.Task] = None

        self._running = False
        self._scanner_proxy = _LazyScannerProxy()
        self._log = logging.getLogger(getattr(self, "__class__", type("Boot", (object,), {})).__name__)
        self._status_sink = getattr(self.cfg, "status_sink", None)  # callable(payload)|None
        self.boot_complete = asyncio.Event()
        self._started = False
        self._private_health_task: Optional[asyncio.Task] = None
        self._rm_ready_sync_task: Optional[asyncio.Task] = None
        # Moniteur santé pipeline PnL (LHM/JSONL/DB)
        self._pnl_pipeline_task: Optional[asyncio.Task] = None
        self._pnl_unhealthy_since: Optional[float] = None
        self._router_watchdog_task: Optional[asyncio.Task] = None
        self._router_health_state: Dict[str, Any] = {}
        self._router_health_pairs_seen: Dict[str, Dict[str, int]] = {}
        self._router_health_stale_counts: Dict[str, int] = {}
        self._router_health_warned: bool = False
        self._obs_inc_cb: Optional[Any] = None
        self._obs_counter_cache: Dict[Tuple[str, tuple[str, ...]], Any] = {}
        self._critical_deps = ["config", "obs", "rate_limiter", "pacer"]
        self._module_state: Dict[str, Any] = {}

        # --- boot.py ---
    def _ensure_engine_dependencies(self) -> None:
        """Instancie RateLimiter + RetryPolicy si absents.

        Le RateLimiter est no-op si la cfg ne fournit pas de caps.
        La RetryPolicy utilise BackoffPolicy.from_cfg pour compat v2/v3.
        """
        try:
            if getattr(self.ctx, "rate_limiter", None) is None:
                self.ctx.rate_limiter = RateLimiter(getattr(self.cfg, "rl", None))
        except Exception:
            # fallback défensif : no-op RL pour éviter de casser le boot
            try:
                self.ctx.rate_limiter = RateLimiter(None)
            except Exception:
                self.ctx.rate_limiter = None
                self._mark_degraded("BOOT_DEP_START_FAIL", where="rate_limiter")

        try:
            if getattr(self.ctx, "retry_policy", None) is None:
                self.ctx.retry_policy = BackoffPolicy.from_cfg(getattr(self.cfg, "retry", None))
        except Exception:
            self.ctx.retry_policy = BackoffPolicy()
            self._mark_degraded("BOOT_DEP_START_FAIL", where="retry_policy")

        if getattr(self.ctx, "rate_limiter", None) is None:
            self._mark_degraded("BOOT_DEP_MISSING", where="rate_limiter")
        if getattr(self.ctx, "retry_policy", None) is None:
            self._mark_degraded("BOOT_DEP_MISSING", where="retry_policy")
        pacer = getattr(self.cfg, "pacer", None) or getattr(self.cfg, "engine_pacer", None)
        if pacer is None:
            self._mark_degraded("BOOT_DEP_MISSING", where="pacer")

    def _get_obs_inc_callback(self) -> Optional[Any]:
        """Retourne un callback best-effort pour obs_inc (RM/Engine).

        Crée dynamiquement les counters Prometheus en fonction des labels
        fournis. Jamais bloquant ni exceptionnel.
        """

        if self._obs_inc_cb is not None:
            return self._obs_inc_cb

        def _obs_inc(metric: str, **labels: Any) -> None:
            cache = self._obs_counter_cache
            label_names = tuple(sorted(labels.keys()))
            try:
                counter = cache.get((metric, label_names))
                if counter is None:
                    counter = obs_metrics.get_counter(metric, metric, labelnames=label_names)
                    cache[(metric, label_names)] = counter
                obs_metrics.safe_inc(counter, metric, "boot.obs_inc", **labels)
            except Exception:
                try:
                    obs_metrics.OBS_NOOP_TOTAL.labels(metric=metric, where="boot.obs_inc").inc()
                except Exception:
                    pass

        self._obs_inc_cb = _obs_inc
        return self._obs_inc_cb
    # ------------------------------- Public API ----------------------------
    async def start(self) -> BootContext:
        # boot.py — dans async def start(self): juste au début
        mode = (
                getattr(self.cfg, "DEPLOYMENT_MODE", None)
                or getattr(self.cfg, "deployment_mode", None)
                or getattr(getattr(self.cfg, "bot_cfg", None), "deployment_mode", None)
                or "EU_ONLY"
        )
        try:
            snapshot_hash = getattr(self.cfg, "snapshot_hash", None)
            snapshot_dict = getattr(self.cfg, "snapshot_dict", None)
            if callable(snapshot_hash):
                cfg_hash = snapshot_hash()
                self.log.info("[Boot] config snapshot hash=%s", cfg_hash)
            if callable(snapshot_dict):
                self.log.debug("[Boot] config snapshot=%s", snapshot_dict())
        except Exception:
            self.log.warning("[Boot] failed to log config snapshot", exc_info=True)

        region = str(getattr(getattr(self.cfg, "g", object()), "pod_region", "EU")).upper()
        enable_jp = False
        try:
            g_cfg = getattr(self.cfg, "g", None)
            enable_jp = bool(getattr(g_cfg, "enable_jp", False)) if g_cfg is not None else bool(
                getattr(self.cfg, "enable_jp", False)
            )
        except Exception:
            enable_jp = False

        if self._running:
            return self.ctx
        # reset des events pour un restart propre
        for ev in (
                self.ready_ws,
                self.ready_router,
                self.ready_scanner,
                self.ready_rm,
                self.ready_engine,
                self.ready_private,
                self.ready_balances,
                self.ready_rpc,
                self.ready_all,
        ):
            ev.clear()
        self._running = True
        self._mark_stage("starting")
        try:
            obs_metrics.safe_inc(obs_metrics.BOT_STARTUPS_TOTAL, "bot_startups_total", "boot.start")
            obs_metrics.safe_set(obs_metrics.BOT_STATE, "bot_state", "boot.start", 1.0)
        except Exception:
            pass
        self._emit_lifecycle("boot.starting")
        self.log.info("[Boot] 🚀 start… mode=%s", str(mode).upper())

        if region == "JP" and not enable_jp:
            self._mark_degraded("REGION_JP_DISABLED_BY_DEFAULT", where="region_guard")
            return self.ctx

        # 1) LHM
        try:
            # 1) LHM
            await self._start_lhm()
            # 2) Discovery (optionnelle) -> active pairs
            await self._discover_pairs_and_compute_active()
            # 3) Public WS + Router
            await self._start_public_pipeline()

            # 4) Balances plane (MBF) avant RM
            await self._start_balances_plane()
            # 5) RM (kwargs signature)
            await self._start_rm()
            # 6) Scanner (bind Router push)
            await self._start_scanner()
            # 7) Private plane (Hub, Reco, Balances)
            await self._start_private_plane()
            # 7bis) Dépendances Engine (RL + retry)
            self._ensure_engine_dependencies()

            if any(
                    r in self.state.get("reasons", [])
                    for r in ("BOOT_DEP_MISSING", "BOOT_DEP_START_FAIL")
            ):
                self.log.error("[Boot] critical dependencies missing; aborting start")
                return self.ctx

            # 8) Engine + RPC
            await self._start_engine_and_rpc()
            # 9) READY
            await self._evaluate_ready()
        except Exception:
            reason = f"boot_start_exception:{self.state.get('stage', 'starting')}"
            self._mark_degraded("BOOT_DEP_START_FAIL", where=reason)
            self.log.exception("[Boot] start failed, rolling back")
            try:
                await self.stop()
            except Exception:
                pass
            self._running = False
            return self.ctx

        self.log.info(
            "[Boot] ✅ ready=%s degraded=%s reasons=%s",
            self.ready_all.is_set(), self.state["degraded"], ",".join(self.state["reasons"]) or "none",
        )
        if self.ready_all.is_set():
            self._emit_lifecycle("boot.ready")
        else:
            self._emit_lifecycle("boot.degraded", reason_code="BOOT_READY_BLOCKED")
        return self.ctx

    # dans Boot (section API publique, juste après async def start(...))
    async def run(self) -> "BootContext":
        """
        Démarre la séquence et publie la barrière READY globale (boot_complete).
        Compatible avec le main qui attend boot.boot_complete.wait().
        """
        ctx = await self.start()
        # Si tu veux forcer l’attente de readys spécifiques ici, tu peux:
        # await asyncio.gather(self.ready_router.wait(), self.ready_engine.wait())
        self.boot_complete.set()
        return ctx

    # --- boot.py (dans class Boot) ---

    async def stop(self, timeout_s: float = 5.0) -> None:
        """
        Arrêt global en ordre inverse strict. Idempotent.
        Chaque étape est protégée (best-effort) et loggée.
        """
        self._emit_lifecycle("boot.stopping")
        try:
            obs_metrics.safe_set(obs_metrics.BOT_STATE, "bot_state", "boot.stopping", 3.0)
        except Exception:
            pass
        self._running = False
        # Annule la tâche de sync cohortes si active
        t = getattr(self, "_cohort_sync_task", None)
        if t:
            t.cancel()
            try:
                await asyncio.wait_for(t, timeout=timeout_s)
            except asyncio.TimeoutError:
                self._mark_degraded("BOOT_STOP_TIMEOUT", where="cohort_sync_task")
            except Exception:
                self._mark_degraded("BOOT_STOP_TIMEOUT", where="cohort_sync_task")

        # Moniteur santé private WS
        t = getattr(self, "_private_health_task", None)
        if t:
            t.cancel()
            try:
                await asyncio.wait_for(t, timeout=timeout_s)
            except asyncio.TimeoutError:
                self._mark_degraded("BOOT_STOP_TIMEOUT", where="private_health_task")
            except Exception:
                self._mark_degraded("BOOT_STOP_TIMEOUT", where="private_health_task")
        self._private_health_task = None
        t = getattr(self, "_router_watchdog_task", None)
        if t:
            t.cancel()
            try:
                await asyncio.wait_for(t, timeout=timeout_s)
            except asyncio.TimeoutError:
                self._mark_degraded("BOOT_STOP_TIMEOUT", where="router_watchdog_task")
            except Exception:
                self._mark_degraded("BOOT_STOP_TIMEOUT", where="router_watchdog_task")
        self._router_watchdog_task = None

        # Moniteur santé pipeline PnL (LHM/JSONL/DB)
        t = getattr(self, "_pnl_pipeline_task", None)
        if t:
            t.cancel()
            try:
                await asyncio.wait_for(t, timeout=timeout_s)
            except asyncio.TimeoutError:
                self._mark_degraded("BOOT_STOP_TIMEOUT", where="pnl_pipeline_task")
            except Exception:
                self._mark_degraded("BOOT_STOP_TIMEOUT", where="pnl_pipeline_task")
        self._pnl_pipeline_task = None
        t = getattr(self, "_rm_ready_sync_task", None)
        if t:
            t.cancel()
            try:
                await asyncio.wait_for(t, timeout=timeout_s)
            except asyncio.TimeoutError:
                self._mark_degraded("BOOT_STOP_TIMEOUT", where="rm_ready_sync_task")
            except Exception:
                self._mark_degraded("BOOT_STOP_TIMEOUT", where="rm_ready_sync_task")
            self._rm_ready_sync_task = None

        await self._stop_component("rpc_server", getattr(self.ctx, "rpc_server", None), "stop", timeout_s)
        await self._stop_component("rpc_client", getattr(self.ctx, "rpc_client", None), "close", timeout_s)
        await self._stop_component("rpc", getattr(self.ctx, "rpc", None), "stop", timeout_s)

        # 2) Scanner
        await self._stop_component("scanner", getattr(self.ctx, "scanner", None), "stop", timeout_s)

        # 3) RM
        rm_timeout = timeout_s
        with contextlib.suppress(Exception):
            cfg_rm = getattr(getattr(self, "cfg", None), "rm", getattr(self, "cfg", None))
            rm_timeout = float(getattr(cfg_rm, "rm_stop_join_timeout_s", rm_timeout))
            if rm_timeout != timeout_s:
                self.log.debug("[Boot] using RM-specific stop timeout %.2fs", rm_timeout)
        await self._stop_component("rm", getattr(self.ctx, "rm", None), "stop", rm_timeout)

        # 4) Engine
        await self._stop_component("engine", getattr(self.ctx, "engine", None), "stop", timeout_s)

        # 5) Private plane (Reconciler → Hub → Balances)
        await self._stop_component("reconciler", getattr(self.ctx, "reconciler", None), "stop", timeout_s)
        await self._stop_component("private", getattr(self.ctx, "pws_hub", None), "stop", timeout_s)
        await self._stop_component("balances", getattr(self.ctx, "balances", None), "stop", timeout_s)


        # 6) Router/WS publics
        await self._stop_component("router", getattr(self.ctx, "router", None), "stop", timeout_s)
        await self._stop_component("ws", getattr(self.ctx, "ws_public", None), "stop", timeout_s)

        # 7) LoggerHistoriqueManager (LHM)
        await self._stop_component("lhm", getattr(self.ctx, "lhm", None), "stop", timeout_s)

        # Nettoyage des flags/events pour autoriser un restart sain
        for ev in (
                self.ready_ws,
                self.ready_router,
                self.ready_scanner,
                self.ready_rm,
                self.ready_engine,
                self.ready_private,
                self.ready_balances,
                self.ready_rpc,
                self.ready_all,
                self.boot_complete,
        ):
            try:
                ev.clear()
            except Exception:
                pass
            self._mark_stage("stopped")
            try:
                obs_metrics.safe_set(obs_metrics.BOT_STATE, "bot_state", "boot.stop", 0.0)
            except Exception:
                pass
            self._emit_lifecycle("boot.stopped")


    async def wait_ready(self, timeout_s: Optional[float] = None) -> bool:
        try:
            if timeout_s is None:
                await self.ready_all.wait()
            else:
                await asyncio.wait_for(self.ready_all.wait(), timeout=timeout_s)
            return True
        except asyncio.TimeoutError:
            return False

    def get_status(self) -> Dict[str, Any]:
        s = dict(self.state)
        s.update({
            # Service-ready (historique)
            "ready": self.ready_all.is_set(),
            "ready_all": self.ready_all.is_set(),

            "ws_ready": self.ready_ws.is_set(),
            "router_ready": self.ready_router.is_set(),
            "scanner_ready": self.ready_scanner.is_set(),
            "rm_loops_ready": self.ready_rm_loops.is_set(),
            "rm_ready": self.ready_rm.is_set(),
            "engine_ready": self.ready_engine.is_set(),
            "private_ready": self.ready_private.is_set(),
            "balances_ready": self.ready_balances.is_set(),
            "rpc_ready": self.ready_rpc.is_set(),

            "scanner_proxy_dropped": getattr(self._scanner_proxy, "dropped", 0),
            "active_pairs": list(self.ctx.active_pairs),
            "discovered": len(self.ctx.discovered_pairs),
            "module_state": dict(self._module_state),
        })

        rc = self._get_router_ready_pairs_count()
        if rc is not None:
            s["router_l2_ready_pairs"] = rc

        # Vue consolidée du marché privé (Ticket 6)
        rm = getattr(self.ctx, "rm", None)
        engine = getattr(self.ctx, "engine", None)
        rm_private: Dict[str, Any] = {}
        engine_private: Dict[str, Any] = {}
        rm_status: Dict[str, Any] = {}
        eng_status: Dict[str, Any] = {}

        try:
            if rm and hasattr(rm, "get_status"):
                rm_status = rm.get_status() or {}
                rm_private = rm_status.get("private_ws") or {}
        except Exception:
            rm_status = {}
            rm_private = {}

        try:
            if engine and hasattr(engine, "get_status"):
                eng_status = engine.get_status() or {}
                engine_private = eng_status.get("private_ws") or {}
        except Exception:
            eng_status = {}
            engine_private = {}

        s["private_ws"] = {
            "ready_flag": self.ready_private.is_set(),
            "rm": rm_private,
            "engine": engine_private,
        }

        # --- Trading readiness canonique (utilisé par /ready strict) ---
        # On expose ce que le RM sait (si dispo), et on calcule un "trading_ready" strict minimal.
        try:
            s["rm_trading_ready"] = bool(rm_status.get("rm_trading_ready")) if isinstance(rm_status, dict) else None
        except Exception:
            s["rm_trading_ready"] = None

        try:
            s["trading_state"] = str(rm_status.get("trading_state", "READY")) if isinstance(rm_status,
                                                                                            dict) else "READY"
            s["trading_state_reason"] = rm_status.get("trading_state_reason") if isinstance(rm_status, dict) else None
        except Exception:
            s["trading_state"] = "READY"
            s["trading_state_reason"] = None

        # Trading-ready strict minimal: Engine ready + RM trading ready + trading_state == READY
        # (Fail-safe: si on ne sait pas, on considère NOT READY)
        try:
            eng_ok = bool(s.get("engine_ready"))
            rm_ok = bool(s.get("rm_trading_ready"))
            tstate = str(s.get("trading_state", "READY")).upper()
            s["trading_ready"] = bool(eng_ok and rm_ok and tstate == "READY")
        except Exception:
            s["trading_ready"] = False

        return s

    def set_status_callback(self, callback) -> None:
        """
        Configure ou remplace le callback de statut externe.

        Le callback reçoit un dict du type:
            {
                "component": str,
                "status": str,
                "payload": dict,
                "ts": float,  # epoch seconds
            }
        Si aucun callback n'est configuré, les appels à _send_status sont des no-op.
        """
        self._status_sink = callback

    def _normalize_reason(self, reason: str) -> str:
        try:
            return contracts.normalize_reason_code(reason) or str(reason)
        except Exception:
            return str(reason)

    def _mark_degraded(self, reason: str, *, where: str | None = None) -> None:
        reason_code = self._normalize_reason(reason)
        self.state["degraded"] = True
        self.state.setdefault("reasons", [])
        if reason_code not in self.state["reasons"]:
            self.state["reasons"].append(reason_code)
        try:
            obs_metrics.safe_inc(
                obs_metrics.NONFATAL_ERRORS_TOTAL,
                "nonfatal_errors_total",
                "boot.degraded",
                module="boot",
                kind=reason_code,
            )
        except Exception:
            pass
        try:
            obs_metrics.safe_set(obs_metrics.BOT_STATE, "bot_state", "boot.degraded", 2.0)
        except Exception:
            pass
        if where:
            try:
                obs_metrics.safe_inc(
                    obs_metrics.BLOCKED_TOTAL,
                    "blocked_total",
                    "boot.degraded",
                    module="boot",
                    reason=reason_code,
                    pair=str(where),
                )
            except Exception:
                pass
        self._emit_lifecycle("boot.degraded", reason_code=reason_code, payload={"where": where})

    def _emit_lifecycle(self, event: str, *, reason_code: str | None = None,
                        payload: Dict[str, Any] | None = None) -> None:
        meta = dict(payload or {})
        if reason_code:
            meta["reason_code"] = self._normalize_reason(reason_code)
        self._send_status("boot", event, meta)

    async def _stop_component(self, name: str, obj: Any, method: str, timeout_s: float) -> None:
        if not obj or not hasattr(obj, method):
            return
        try:
            await asyncio.wait_for(getattr(obj, method)(), timeout=timeout_s)
            self._send_status(name, "stopped")
        except asyncio.TimeoutError:
            self._mark_degraded("BOOT_STOP_TIMEOUT", where=name)
        except Exception:
            self._mark_degraded("BOOT_STOP_TIMEOUT", where=name)

    def _send_status(self, component: str, status: str, payload: Optional[Dict[str, Any]] = None) -> None:
        """
        Helper interne: publie un statut vers le status_sink s'il est configuré.

        - Ne lève jamais d'exception côté Boot (robustesse).
        - Si aucun status_sink n'est configuré, no-op silencieux.
        """
        sink = getattr(self, "_status_sink", None)
        if not callable(sink):
            try:
                obs_metrics.OBS_NOOP_TOTAL.labels(metric="boot_status_sink", where="boot._send_status").inc()
            except Exception:
                pass
            try:
                self._module_state[str(component)] = {
                    "status": status,
                    "payload": payload or {},
                    "ts": time.time(),
                }
            except Exception:
                pass
            return

        try:
            msg = {
                "component": component,
                "status": status,
                "payload": payload or {},
                "ts": time.time(),
            }
            self._module_state[str(component)] = {"status": status, "payload": payload or {}, "ts": msg["ts"]}
            sink(msg)
        except Exception:
            # On loggue mais on ne casse jamais le Boot à cause du sink.
            self.log.exception(
                "[Boot] status_sink a échoué pour component=%s status=%s",
                component,
                status,
            )

    # ------------------------------- Étapes ---------------------------------
    # boot.py


    async def _start_lhm(self) -> None:
        """
        Instancie le LoggerHistoriqueManager avec un out_dir explicite,
        crée le répertoire si besoin, démarre le LHM, marque l'étape,
        et branche les sinks Engine/Scanner si disponibles.
        """

        lhm_cfg = getattr(self.cfg, "lhm", None)
        # 1) Résolution robuste de l'out_dir (priorité décroissante)
        out_dir = (
                getattr(lhm_cfg, "out_dir", None)
                or getattr(self.cfg, "LHM_OUT_DIR", None)
                or getattr(self.cfg, "LOG_DIR", None)
                or getattr(self.cfg, "HISTORY_DIR", None)
                or "/srv/app/logs"
        )
        try:
            obs_metrics.set_strict_obs(
                getattr(lhm_cfg, "strict_obs", getattr(getattr(self.cfg, "g", object()), "strict_obs", None)))
            obs_metrics.init_lhm_slo_targets(
                write_ms=getattr(lhm_cfg, "LHM_SLO_WRITE_MS_P95_TARGET", None),
                queue_max=getattr(lhm_cfg, "LHM_SLO_QUEUE_DEPTH_MAX_TARGET", None),
                lag_max=getattr(lhm_cfg, "LHM_SLO_PIPELINE_LAG_MAX_SECONDS_TARGET", None),
                dropped_budget=getattr(lhm_cfg, "LHM_SLO_DROPPED_TRADES_BUDGET", None),
            )
        except Exception:
            self.log.exception("[Boot][LHM] Unable to init LHM obs metrics from cfg")

        # 2) Création idempotente du dossier
        try:
            Path(out_dir).mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self.log.warning("[Boot][LHM] Impossible de créer %s (%s). On continue.", out_dir, e)

        # 3) Instanciation + start (évite le TypeError)
        self.ctx.lhm = LoggerHistoriqueManager(self.cfg, out_dir)
        await self.ctx.lhm.start()

        # 4) Marque de progression + log
        self._mark_stage("lhm_started")
        self.log.info("[Boot] LHM prêt (out_dir=%s)", out_dir)

        # 5) Démarrer la surveillance du pipeline PnL (LHM/JSONL/DB)
        try:
            self._ensure_pnl_pipeline_health_task()
        except Exception:
            self.log.exception("[Boot][LHM] impossible de démarrer le moniteur santé PnL")


        # 6) (Optionnel) Brancher les sinks si présents
        try:
            self._bind_lhm_sinks()
        except Exception as e:
            self.log.warning("[Boot][LHM] Branchements sinks partiels : %s", e)

    def _bind_lhm_sinks(self) -> None:
        lhm = getattr(self.ctx, "lhm", None)
        if not lhm:
            return
        try:
            engine = getattr(self.ctx, "engine", None)
            if engine and hasattr(engine, "set_history_logger"):
                engine.set_history_logger(lhm)
        except Exception:
            self.log.warning("[Boot][LHM] engine.set_history_logger failed")
        try:
            scanner = getattr(self.ctx, "scanner", None)
            if scanner and hasattr(scanner, "set_history_logger"):
                sink = getattr(lhm, "opportunity", None)
                if callable(sink):
                    scanner.set_history_logger(sink)
                scanner.logger_historique_manager = lhm
        except Exception:
            self.log.warning("[Boot][LHM] scanner history sinks failed")
    async def _discover_pairs_and_compute_active(self) -> None:
        """Discovery optionnelle → pool (discovered_pairs) → sous‑ensemble actif (active_pairs).
        Fallback 100% config si discovery disabled/échec.
        """
        # Params discovery
        dcfg = getattr(self.cfg, "discovery", object())
        use_discovery = bool(getattr(dcfg, "enabled", True))
        top_n = int(getattr(dcfg, "top_n", 120))

        discovered: List[str] = []
        pairs_map: Dict[str, Any] = {}
        disc_result = None
        if use_discovery:
            try:

                pairs_map, discovered, disc_result = await discover_pairs_3cex(self.cfg, include_result=True)
                if top_n > 0 and len(discovered) > top_n:
                    discovered = discovered[:top_n]
                self.log.info("[Boot] discovery: %d pairs (top_n=%d)", len(discovered), top_n)
                if disc_result:
                    self.log.info(
                        "[Boot] discovery audit stages=%s filtered=%s api_errors=%s run_ms=%.1f",
                        disc_result.stage_counts,
                        disc_result.filtered_counts,
                        disc_result.api_errors,
                        float(disc_result.run_ms or 0.0),
                    )
            except Exception as e:
                self.log.warning("[Boot] discovery failed: %s → fallback config", e)
                discovered = []
                pairs_map = {}
        if not discovered:
            # Fallback config 100% piloté par BotConfig
            discovered = list(getattr(getattr(self.cfg, "g", object()), "pairs", []) or [])
        self.ctx.discovered_pairs = discovered
        self.ctx.pairs_map = pairs_map

        # Calcul du sous‑ensemble ACTIF (PRIMARY+AUDITION)
        scfg = getattr(self.cfg, "scanner", object())
        # Priorité: champ explicite, sinon somme des tailles de cohortes, sinon défaut 120
        active_max = int(getattr(scfg, "active_max_pairs", 0) or 0)
        if not active_max:
            primary_sz = int(getattr(scfg, "primary_size", 0) or 0)
            audition_sz = int(getattr(scfg, "audition_size", 0) or 0)
            active_max = (primary_sz + audition_sz) if (primary_sz or audition_sz) else 120
        self.ctx.active_pairs = discovered[:active_max]
        self._mark_stage("pairs_ready")
        self.log.info("[Boot] univers: discovered=%d, active=%d", len(discovered), len(self.ctx.active_pairs))



    async def _start_public_pipeline(self) -> None:
        """
        Démarre la pipeline publique sans bloquer le bootstrap :
          1) crée la in_queue
          2) instancie et lance WS Public en tâche + attend readiness (timeout)
          3) instancie et lance le Router en tâche + attend readiness (timeout)
          4) garde tes logiques (volatility/slippage, combos, stages, logs)
        Pose/alimente self._ws_task, self._router_task, ready_ws, ready_router.
        """
        # --- Queue WS → Router
        inq_len = int(getattr(getattr(self.cfg, "router", object()), "in_queue_maxlen", 5000))
        self.ctx.in_queue = asyncio.Queue(maxsize=inq_len)

        # --- 3.1) WS Publics : instanciation identique à ta version

        pairs = list(self.ctx.active_pairs)
        self.ctx.ws_public = WebSocketExchangeClient(
            cfg=self.cfg,
            url="multi",
            pairs=pairs,
            out_queue=self.ctx.in_queue,
            pair_mapping=self.ctx.pairs_map,
            enabled_exchanges=list(
                getattr(getattr(self.cfg, "g", object()), "enabled_exchanges", ["BINANCE", "COINBASE", "BYBIT"])
            ),
            config=self.cfg,
        )

        # Events de readiness (créés si absents)
        if not hasattr(self, "ready_ws"):
            self.ready_ws = asyncio.Event()
        if not hasattr(self, "ready_router"):
            self.ready_router = asyncio.Event()

        # Lance WS en tâche non-bloquante
        # APRÈS (tolerant à None / "", "none", "null")
        _raw = getattr(self.cfg, "WS_READY_TIMEOUT_S", None)
        ws_ready_to = float(
            5.0 if (_raw is None or (isinstance(_raw, str) and _raw.strip().lower() in {"", "none", "null"})) else _raw)

        # start() peut être une coroutine longue; on la met en tâche:
        self._ws_task = asyncio.create_task(self.ctx.ws_public.start(), name="boot-ws-start")

        # Attente best-effort d’un signal de readiness (si exposé)
        try:
            if hasattr(self.ctx.ws_public, "ready_event"):
                await asyncio.wait_for(self.ctx.ws_public.ready_event.wait(), timeout=ws_ready_to)
            else:
                # fallback léger pour laisser le temps de bootstrapper
                await asyncio.sleep(0.2)
        except asyncio.TimeoutError:
            self.log.warning("[Boot] WS public pas prêt après %.1fs, on continue.", ws_ready_to)

        # Marque ready + stage + log (comme chez toi)
        self.ready_ws.set()
        self._mark_stage("ws_public_started")
        self.log.info("[Boot] WS publics: %d pairs", len(pairs))

        # --- 3.2) Capteurs auxiliaires (inchangés)
        try:

            self.ctx.volatility = VolatilityMonitor(self.cfg)
        except Exception:
            self.ctx.volatility = None
        try:

            self.ctx.slippage = SlippageHandler(self.cfg)
        except Exception:
            self.ctx.slippage = None

        # --- 3.3) Router (combos via cfg) — instanciation identique à ta version

        combos = getattr(getattr(self.cfg, "router", object()), "combos", None)
        if not combos:
            combos = [("BINANCE", "COINBASE"), ("BINANCE", "BYBIT"), ("BYBIT", "COINBASE")]

        self.ctx.router = MarketDataRouter(
            in_queue=self.ctx.in_queue,
            scanner=self._scanner_proxy,
            volatility_monitor=self.ctx.volatility,
            slippage_handler=self.ctx.slippage,
            publish_combo_to_bus=True,
            push_to_scanner=True,
            require_l2_first=bool(
                getattr(getattr(self.cfg, "router", object()), "require_l2_first", True)
            ),
            combos=combos,
            bot_cfg=self.cfg,
            router_cfg=getattr(self.cfg, "router", None),
        )

        try:
            self._wire_volatility_monitor()
        except Exception:
            self.log.exception("[Boot] Wiring volatility monitor failed")

        try:
            self._wire_slippage_handler()
        except Exception:
            self.log.exception("[Boot] Wiring slippage handler failed")

        # Lance Router en tâche non-bloquante
        self._router_task = asyncio.create_task(self.ctx.router.start(), name="boot-router-start")
        
        # Attente best-effort d’un signal de readiness (si exposé)
        _raw = getattr(self.cfg, "ROUTER_READY_TIMEOUT_S", None)
        router_ready_to = float(5.0 if (_raw is None or (isinstance(_raw, str) and _raw.strip().lower() in {"", "none", "null"})) else _raw)
        try:
            if hasattr(self.ctx.router, "ready_event"):
                await asyncio.wait_for(self.ctx.router.ready_event.wait(), timeout=router_ready_to)
            else:
                await asyncio.sleep(0.2)
        except asyncio.TimeoutError:
            self.log.warning("[Boot] Router pas prêt après %.1fs, on continue.", router_ready_to)

        self.ready_router.set()
        self._mark_stage("router_started")
        self.log.info("[Boot] Router démarré. combos=%s", combos)
        self._ensure_router_health_watchdog()

    def _wire_volatility_monitor(self) -> None:
        """Attache les files bus vol et forward scanner/config pour la Volatility."""
        vol = getattr(self.ctx, "volatility", None)
        router = getattr(self.ctx, "router", None)
        if not vol or not router:
            return

        vol_cfg = getattr(self.cfg, "vol", None)
        if vol_cfg and hasattr(vol, "set_bps_mapping"):
            vol.set_bps_mapping(
                midprice_to_bps=getattr(vol_cfg, "midprice_to_bps", 1e4),
                floor_bps=getattr(vol_cfg, "to_bps_floor", 0.0),
                cap_bps=getattr(vol_cfg, "to_bps_cap", 250.0),
            )

        if hasattr(vol, "set_scanner"):
            scanner = getattr(self.ctx, "scanner", None) or self._scanner_proxy
            try:
                vol.set_scanner(scanner)
            except Exception:
                self.log.debug("[Boot] set_scanner on volatility monitor failed", exc_info=True)

        queues: Dict[str, asyncio.Queue] = {}
        for ex in getattr(router, "exchanges", []):
            qmap = router.out_queues.get(MarketDataRouter._cex_key(ex), {})
            q = qmap.get("vol") if isinstance(qmap, dict) else None
            if q:
                queues[str(ex).upper()] = q
        if queues and hasattr(vol, "attach_bus_vol_queues"):
            vol.attach_bus_vol_queues(queues)

    def _wire_slippage_handler(self) -> None:
        """Attache les files bus slip pour le SlippageHandler (mode bus only)."""
        slip = getattr(self.ctx, "slippage", None)
        router = getattr(self.ctx, "router", None)
        if not slip or not router:
            return

        queues: Dict[str, asyncio.Queue] = {}
        for ex in getattr(router, "exchanges", []):
            qmap = router.out_queues.get(MarketDataRouter._cex_key(ex), {})
            q = qmap.get("slip") if isinstance(qmap, dict) else None
            if q:
                queues[str(ex).upper()] = q

        if queues and hasattr(slip, "attach_bus_slip_queues"):
            slip.attach_bus_slip_queues(queues)

    async def _start_balances_plane(self) -> None:
        """
        Instancie et démarre le MultiBalanceFetcher si activé.

        Démarre également sa boucle de synchronisation via une tâche dédiée.
        Idempotent et feature-flag aware.
        """
        g = getattr(self.cfg, "g", None)
        fs = getattr(g, "feature_switches", {}) if g else {}
        enable_balances = bool(fs.get("balance_fetcher", False))

        if not enable_balances:
            self._send_status("balances", "skipped", {"reason": "feature_flag_off"})
            return

        if not getattr(self.ctx, "balances", None):
            cfg = getattr(self, "cfg", None)
            dry_run = bool(getattr(getattr(cfg, "g", object()), "dry_run", False))
            verbose = bool(getattr(getattr(cfg, "g", object()), "verbose", True))
            mbf_kwargs = {
                "binance_accounts": getattr(cfg, "binance_accounts", None),
                "coinbase_at_accounts": getattr(cfg, "coinbase_at_accounts", None),
                "bybit_accounts": getattr(cfg, "bybit_accounts", None),
                "config": cfg,
                "dry_run": dry_run,
                "verbose": verbose,
            }
            self.ctx.balances = MultiBalanceFetcher(**mbf_kwargs)

        mbf = getattr(self.ctx, "balances", None)
        if hasattr(mbf, "start"):
            await mbf.start()

        if hasattr(mbf, "start_sync_loop") and not self._balances_task:
            try:
                self._balances_task = asyncio.create_task(
                    mbf.start_sync_loop(), name="mbf-sync-loop"
                )
            except Exception:
                self.log.exception("[Boot] unable to start MBF sync loop")

        self.ready_balances.set()
        self._send_status("balances", "ready")

        try:
            self._wire_mbf_ws_status_providers()
        except Exception:
            self.log.exception("[Boot] wiring MBF ws_status_providers depuis _start_balances_plane a échoué")

    # --- boot.py (dans class Boot) ---
    async def _start_rm(self) -> None:
        """
        Démarre le RiskManager avec un getter d'orderbooks sûr, puis
        recâble engine/reconciler si déjà créés. Idempotent.
        """
        if getattr(self.ctx, "rm", None):
            # déjà démarré
            self._send_status("rm", "ready")
            self.ready_rm_loops.set()
            if getattr(getattr(self.ctx, "rm", None), "trading_ready_event", None):
                if self.ctx.rm.trading_ready_event.is_set():
                    self.ready_rm.set()
            else:
                self.ready_rm.set()
            self._mark_stage("rm_ready")
            return

        # Getter orderbooks robuste (latest|alias)
        router = getattr(self.ctx, "router", None)
        get_books = None
        if router:
            get_books = getattr(router, "get_latest_orderbooks", None) or \
                        getattr(router, "get_orderbooks", None)

        # dépendances optionnelles
        volatility = getattr(self.ctx, "volatility", None)
        slippage = getattr(self.ctx, "slippage", None)
        balances = getattr(self.ctx, "balances", None)
        simulator = getattr(self.ctx, "simulator", None)

        # Construction RM (tolérante aux noms d'args)
        rm_kwargs = {
            "bot_cfg": getattr(self, "cfg", None),
            "config": getattr(self, "cfg", None),
            "get_orderbooks_callback": get_books,
            "volatility_monitor": volatility,
            "slippage_handler": slippage,
            "balance_fetcher": balances,
            "simulator": simulator,
            "execution_engine": getattr(self.ctx, "engine", None),
            "history_logger": getattr(self.ctx, "lhm", None),
        }
        rm_kwargs["exchanges"] = list(
            getattr(getattr(self.cfg, "g", object()), "enabled_exchanges", ["BINANCE", "COINBASE", "BYBIT"]))
        rm_kwargs["symbols"] = list(self.ctx.active_pairs)

        # filtrage: ne passer que les clés supportées par __init__

        sig = inspect.signature(RiskManager.__init__)
        rm_kwargs = {k: v for k, v in rm_kwargs.items() if k in sig.parameters}

        self.ctx.rm = RiskManager(**rm_kwargs)
        try:
            cb = self._get_obs_inc_callback()
            if cb and hasattr(self.ctx.rm, "set_obs_inc_callback"):
                self.ctx.rm.set_obs_inc_callback(cb)
        except Exception:
            pass
        try:
            lhm = getattr(self.ctx, "lhm", None)
            if lhm is not None and hasattr(lhm, "set_fail_closed_callback"):
                lhm.set_fail_closed_callback(self.ctx.rm.set_logging_fail_closed)
        except Exception:
            self.log.exception("[Boot] set_fail_closed_callback failed")
        if hasattr(self.ctx.rm, "start"):
            await self.ctx.rm.start()

        if getattr(self.ctx.rm, "ready_event", None):
            with contextlib.suppress(Exception):
                self.ready_rm_loops.set()

        # recâblages idempotents
        try:
            if hasattr(self.ctx.rm, "set_engine") and getattr(self.ctx, "engine", None):
                self.ctx.rm.set_engine(self.ctx.engine)

            reco = getattr(self.ctx, "reconciler", None)
            if reco and hasattr(self.ctx.rm, "bind_reconciler"):
                self.ctx.rm.bind_reconciler(reco)
        except Exception:
            pass
        self.ready_rm.set()
        self._mark_stage("rm_ready")
        self._send_status("rm", "ready")

        self._wire_private_hub_callbacks()

        # Brancher MBF sur Hub/Reconciler si déjà disponibles (Ticket 7)
        try:
            self._wire_mbf_ws_status_providers()
        except Exception:
            self.log.exception("[Boot] wiring MBF ws_status_providers depuis _start_rm a échoué")

        # Brancher l'event_sink MBF vers le RM (Ticket 8)
        try:
            self._wire_mbf_event_sink()
        except Exception:
            self.log.exception("[Boot] wiring MBF event_sink depuis _start_rm a échoué")

        self._send_status("rm", "ready")
        if getattr(self.ctx.rm, "trading_ready_event", None):
            if self.ctx.rm.trading_ready_event.is_set():
                self.ready_rm.set()
            try:
                self._rm_ready_sync_task = asyncio.create_task(self._sync_rm_trading_ready(), name="boot-rm-ready-sync")
            except Exception:
                self.log.exception("[Boot] unable to start rm trading ready sync")
        else:
            self.ready_rm.set()
        self._mark_stage("rm_ready")

    async def _start_scanner(self) -> None:

        self.ctx.scanner = OpportunityScanner(self.cfg, self.ctx.rm, self.ctx.router, history_logger=self.ctx.lhm)
        dry_run = bool(getattr(getattr(self.cfg, "g", object()), "dry_run", False))
        live = not dry_run
        if live:
            rm = getattr(self.ctx, "rm", None)
            cb = getattr(rm, "on_scanner_opportunity", None) if rm else None
            if callable(cb):
                self.ctx.scanner.on_opportunity = cb
        self._bind_lhm_sinks()
        with contextlib.suppress(Exception):
            lhm = getattr(self.ctx, "lhm", None)
            coh = None

            # (1) LHM expose get_cohorts(): API publique (Pivot Boot)
            get_coh = getattr(lhm, "get_cohorts", None)
            if callable(get_coh):
                coh = get_coh()

            # (3) Applique au Scanner (fallback binaire si 4-tiers non fournis)
            dep_mode = str(getattr(getattr(self.cfg, "g", object()), "deployment_mode", "EU_ONLY"))
            if coh:
                self.ctx.scanner.set_universe(
                    mode=dep_mode,
                    core=coh.get("CORE", []),
                    primary=coh.get("PRIMARY", []),
                    audition=coh.get("AUDITION", []),
                    sandbox=coh.get("SANDBOX", []),
                )
            else:
                self.ctx.scanner.set_universe(
                    mode=dep_mode,
                    primary=list(self.ctx.active_pairs),
                    audition=[],
                )
        self.ctx.scanner.apply_runtime_config(self.cfg)
        self._scanner_proxy.bind(self.ctx.scanner)
        await self.ctx.scanner.start()

        self.ready_scanner.set()
        self._mark_stage("scanner_ready")
        # P0: bind du proxy pour que le Router n’ait plus 100% de drops
        with contextlib.suppress(Exception):
            self._scanner_proxy.bind(self.ctx.scanner)

        # P0: readiness explicite (sinon _evaluate_ready() peut bloquer)
        self.ready_scanner.set()
        self._mark_stage("scanner_ready")
        self._send_status("scanner", "ready")

        # (4) Démarre la mini-loop de sync périodique (voir méthode plus bas)
        try:
            self._cohort_sync_task = asyncio.create_task(
                self._sync_scanner_cohorts_loop(), name="boot-cohort-sync"
            )
        except Exception:
            self.log.exception("[Boot] cohort sync task failed to start")

    # --- boot.py (dans class Boot) ---
    async def _start_private_plane(self) -> None:
        """
        Démarre le Hub privé et attache un unique PrivateWSReconciler.
        Idempotent, DRY_RUN-gated via feature_switches.private_ws.
        """
        # DRY_RUN / feature flags: on peut skipper si nécessaire
        g = getattr(self.cfg, "g", None)
        fs = getattr(g, "feature_switches", {}) if g else {}
        enable_private = bool(fs.get("private_ws", False))

        if not enable_private:
            self._send_status("private", "skipped", {"reason": "feature_flag_off"})
            return

        # 1) Hub privé
        if not getattr(self.ctx, "pws_hub", None):
            cfg = getattr(self, "cfg", None)
            binance_accounts = getattr(cfg, "binance_accounts", None) or None
            bybit_accounts = getattr(cfg, "bybit_accounts", None) or None
            coinbase_accounts = getattr(cfg, "coinbase_at_accounts", None) or None
            transfer_clients = None
            rm = getattr(self.ctx, "rm", None)
            if rm:
                tc = getattr(rm, "transfer_clients", None)
                if tc:
                    transfer_clients = dict(tc)

            hub_kwargs = {
                "config": cfg,
                "binance_accounts": binance_accounts,
                "bybit_accounts": bybit_accounts,
                "coinbase_at_accounts": coinbase_accounts,
                "transfer_clients": transfer_clients,
            }
            hub_kwargs = {k: v for k, v in hub_kwargs.items() if v}

            self.ctx.pws_hub = PrivateWSHub(**hub_kwargs)
            setattr(self.ctx, "priv", self.ctx.pws_hub)

        if hasattr(self.ctx.pws_hub, "start"):
            await self.ctx.pws_hub.start()


        # 2) Reconciler (unique)
        if not getattr(self.ctx, "reconciler", None):
            reco_cfg = getattr(self.cfg, "reconciler", None)
            reco_kwargs = {
                "venue_name": "TRI-CEX",
                "cooldown_s": getattr(reco_cfg, "cooldown_s", 60.0),
                "stale_ms": getattr(reco_cfg, "stale_ms", 1500),
                "poll_every_s": getattr(reco_cfg, "poll_every_s", 2.0),
                "dedup_max": getattr(reco_cfg, "dedup_max", 20000),
                "cold_every_h": getattr(
                    reco_cfg, "cold_every_h", getattr(reco_cfg, "cold_resync_interval_h", 6.0)
                ),
            }
            self.ctx.reconciler = PrivateWSReconciler(**reco_kwargs)

        try:
            self.ctx.reconciler.cfg = getattr(self.cfg, "reconciler", None)
        except Exception:
            pass

        try:
            self.ctx.reconciler.start()
        except Exception:
            self.log.exception("[Boot] unable to start PrivateWSReconciler")

        # Attacher au Hub s'il expose un hook
        try:
            if hasattr(self.ctx.pws_hub, "attach_reconciler"):
                self.ctx.pws_hub.attach_reconciler(self.ctx.reconciler)
        except Exception:
            pass

        # 3) Wiring via RiskManager (source de vérité du privé)
        rm = getattr(self.ctx, "rm", None)
        if rm:
            # Si RM déjà lancé, lier maintenant le Reconciler
            if hasattr(rm, "bind_reconciler"):
                try:
                    rm.bind_reconciler(self.ctx.reconciler)
                    wiring_flag = getattr(rm, "reconciler_wiring_ok", None)
                    if wiring_flag is not None:
                        self.log.info("[Boot] rm.bind_reconciler wiring_ok=%s", wiring_flag)
                except Exception:
                    self.log.exception("[Boot] rm.bind_reconciler failed")

            # Confier au RM le binding du Hub (callbacks + health + flags wiring)

            if hasattr(rm, "bind_private_ws_hub"):
                try:
                    rm.bind_private_ws_hub(self.ctx.pws_hub)
                    wiring_flag = getattr(rm, "private_ws_wiring_ok", None)
                    if wiring_flag is not None:
                        self.log.info("[Boot] rm.bind_private_ws_hub wiring_ok=%s", wiring_flag)
                except Exception:
                    self.log.exception("[Boot] rm.bind_private_ws_hub failed")

        engine = getattr(self.ctx, "engine", None)
        if engine and hasattr(engine, "set_private_ws_reconciler"):
            try:
                engine.set_private_ws_reconciler(self.ctx.reconciler)
            except Exception:
                self.log.exception("[Boot] engine.set_private_ws_reconciler failed")

        # 4) Tâche de santé privée (Hub→RM)
        self._propagate_private_ws_health()
        self._ensure_private_health_task()
        # 3bis) Brancher MBF sur les statuts Hub/Reconciler si présent (Ticket 7)
        try:
            self._wire_mbf_ws_status_providers()
        except Exception:
            self.log.exception("[Boot] wiring MBF ws_status_providers depuis _start_private_plane a échoué")
            # 3ter) Brancher l'event_sink MBF vers le RM (Ticket 8)
        try:
            self._wire_mbf_event_sink()
        except Exception:
            self.log.exception("[Boot] wiring MBF event_sink depuis _start_private_plane a échoué")

        reco_task = getattr(self.ctx.reconciler, "_task", None)
        hub_started = bool(getattr(self.ctx.pws_hub, "__class__", None))
        reco_status: Dict[str, Any] = {}
        try:
            if hasattr(self.ctx.reconciler, "get_status"):
                reco_status = self.ctx.reconciler.get_status() or {}
        except Exception:
            reco_status = {}

        if reco_task or reco_status.get("running"):
            self.ready_private.set()
            self._mark_stage("private_ready")
        self.state["private"] = {
            "hub_present": hub_started,
            "reconciler_started": bool(reco_task),
            "reconciler_status": reco_status,
        }
        self._send_status("private", "ready", dict(self.state.get("private", {})))

    async def _sync_scanner_cohorts_loop(self) -> None:
        """
        Tâche périodique: relit les cohortes du LHM/Tracker et pousse au Scanner.
        Respecte le double 'rate making':
          - LHM décide qui va dans CORE/PRIMARY/AUDITION/SANDBOX (quotas/rotation),
          - Scanner cadence chaque tier via ses token-buckets (Hz par tier).
        """
        interval = float(getattr(self.cfg, "SCANNER_COHORT_SYNC_EVERY_S", 600.0))  # 10 min défaut
        # NB: harmonise avec ta cadence LHM.rotate_* (ex: 10–15 min)
        while getattr(self, "_running", False):
            try:
                lhm = getattr(self.ctx, "lhm", None)
                scn = getattr(self.ctx, "scanner", None)
                if not (lhm and scn and hasattr(scn, "set_universe")):
                    await asyncio.sleep(interval)
                    continue

                # Appel best-effort: demander au LHM de faire sa rotation interne avant la sync (Pivot Boot)
                try:
                    rot_pairs = getattr(lhm, "rotate_pairs", None)
                    if callable(rot_pairs):
                        rot_pairs(now=None)
                    rot_coh = getattr(lhm, "rotate_cohorts", None)
                    if callable(rot_coh):
                        rot_coh(now=None)
                except Exception:
                    self.log.exception("[Boot] lhm.rotate_* during cohort-sync failed")

                # Récupère les cohortes via API publique uniquement (Pivot Boot)
                coh = None
                get_coh = getattr(lhm, "get_cohorts", None)
                if callable(get_coh):
                    coh = get_coh()

                if coh:
                    dep_mode = str(getattr(getattr(self.cfg, "g", object()), "deployment_mode", "EU_ONLY"))
                    scn.set_universe(
                        mode=dep_mode,
                        core=coh.get("CORE", []),
                        primary=coh.get("PRIMARY", []),
                        audition=coh.get("AUDITION", []),
                        sandbox=coh.get("SANDBOX", []),
                    )
            except asyncio.CancelledError:
                break
            except Exception:
                self.log.exception("[Boot] cohort-sync tick failed")

            await asyncio.sleep(interval)

    # --- boot.py (dans class Boot) ---
    async def _start_engine_and_rpc(self) -> None:
        """
        Démarre Engine puis RPC. Idempotent. Injecte /status dans le RPC si possible.
        Recâble RM↔Engine/Reconciler si RM existe déjà.
        """
        # 1) Engine
        if not getattr(self.ctx, "engine", None):
            # Garantit la présence du hub privé avant l'Engine
            if getattr(self.ctx, "pws_hub", None) is None:
                self.ctx.pws_hub = PrivateWSHub(getattr(self, "cfg", None))

            self.ctx.engine = ExecutionEngine(
                private_ws=self.ctx.pws_hub,
                rate_limiter=getattr(self.ctx, "rate_limiter", None),
                retry_policy=getattr(self.ctx, "retry_policy", None),
                cfg=getattr(self, "cfg", None),
                risk_manager=getattr(self.ctx, "rm", None),
                ready_event=self.ready_engine,
            )

            try:
                cb = self._get_obs_inc_callback()
                if cb is not None:
                    setattr(self.ctx.engine, "obs_inc", cb)
            except Exception:
                pass

        engine = self.ctx.engine

        if hasattr(engine, "start"):
            await engine.start()


        # Wiring initial Engine ↔ Hub (callback "engine" + set_private_ws_hub)
        self._wire_private_hub_callbacks()
        self._send_status("engine", "ready")

        # 2) RPC
        rpc_cfg = getattr(self.cfg, "rpc", None)
        rpc_enabled = bool(getattr(rpc_cfg, "enabled", False))

        # Si RPC désactivé, on ne bloque pas le boot.
        if not rpc_enabled:
            self._send_status("rpc", "disabled")
            self.ready_rpc.set()
            self._mark_stage("rpc_disabled")
            return

        self._send_status("rpc", "starting")

        # Handlers (pour la signature RPCServer(cfg, handlers, ...))
        # => On évite d'importer un type spécifique (RPCHandlers) pour rester compatible.
        from types import SimpleNamespace

        async def _rpc_submit_bundle(payload: dict) -> dict:
            # Prefer RM si tu exposes une entrée “guarded”.
            rm = getattr(self.ctx, "rm", None)
            if rm is not None and hasattr(rm, "submit_bundle_from_rpc"):
                return await rm.submit_bundle_from_rpc(payload)

            eng = self.ctx.engine
            if hasattr(eng, "_execute_bundle"):
                return await eng._execute_bundle(payload)
            if hasattr(eng, "execute_bundle"):
                return await eng.execute_bundle(payload)
            raise RuntimeError("No submit_bundle entrypoint available")

        async def _rpc_get_pnl(*, days: int = 7) -> dict:
            lhm = getattr(self.ctx, "lhm", None)
            if lhm is None or not hasattr(lhm, "get_pnl_multiview"):
                return {"ok": False, "reason": "LHM_NOT_AVAILABLE"}
            return await lhm.get_pnl_multiview(window_s=int(days) * 86400)

        def _rpc_get_open_orders() -> dict:
            eng = self.ctx.engine
            orders = getattr(eng, "_orders", None)
            try:
                n = len(orders) if orders is not None else 0
            except Exception:
                n = None
            return {"count": n}

        async def _rpc_pause_trading(reason: str = "rpc_pause") -> dict:
            rm = getattr(self.ctx, "rm", None)
            if rm is None or not hasattr(rm, "pause_all_symbols"):
                return {"ok": False, "reason": "RM_NOT_AVAILABLE"}
            await rm.pause_all_symbols(reason=reason)
            return {"ok": True}

        async def _rpc_resume_trading(reason: str = "rpc_resume") -> dict:
            rm = getattr(self.ctx, "rm", None)
            if rm is None or not hasattr(rm, "resume_all_symbols"):
                return {"ok": False, "reason": "RM_NOT_AVAILABLE"}
            await rm.resume_all_symbols(reason=reason)
            return {"ok": True}

        handlers = SimpleNamespace(
            submit_bundle=_rpc_submit_bundle,
            get_status=self.get_status,
            get_pnl=_rpc_get_pnl,
            get_open_orders=_rpc_get_open_orders,
            pause_trading=_rpc_pause_trading,
            resume_trading=_rpc_resume_trading,
        )

        # Instanciation compatible avec les 2 versions de RPCServer
        if not getattr(self.ctx, "rpc", None):
            try:
                # Nouvelle signature: RPCServer(cfg, handlers, ...)
                self.ctx.rpc = RPCServer(self.cfg, handlers)
            except TypeError:
                # Ancienne signature: RPCServer(cfg)
                self.ctx.rpc = RPCServer(self.cfg)

        # Ancienne API: on enregistre les endpoints si les méthodes existent
        try:
            if hasattr(self.ctx.rpc, "register_status_endpoint"):
                self.ctx.rpc.register_status_endpoint(self)  # passe le Boot
            if hasattr(self.ctx.rpc, "register_health_endpoint"):
                self.ctx.rpc.register_health_endpoint(self)
            if hasattr(self.ctx.rpc, "register_engine_bundle_endpoint"):
                self.ctx.rpc.register_engine_bundle_endpoint(self.ctx.engine)
        except Exception:
            logger.exception("RPC endpoint registration failed (continuing)")

        await self.ctx.rpc.start()
        self.ready_rpc.set()
        self._send_status("rpc", "ready")
        self._mark_stage("rpc_ready")

        # 3) Recâblage tardif RM ↔ Engine/Reconciler (idempotent)
        rm = getattr(self.ctx, "rm", None)
        if rm:
            try:
                if hasattr(rm, "set_engine"):
                    rm.set_engine(engine)
                reconciler = getattr(self.ctx, "reconciler", None)
                if reconciler and hasattr(rm, "bind_reconciler"):
                    rm.bind_reconciler(reconciler)
                    hub = getattr(self.ctx, "pws_hub", None)
                    if hub and hasattr(rm, "bind_private_ws_hub"):
                        rm.bind_private_ws_hub(hub)
            except Exception:
                pass

        # Dernier wiring Engine ↔ Hub (idempotent)
        self._wire_private_hub_callbacks()

    # ------------------------------- Arrêts ---------------------------------

    async def _stop_router_and_ws(self) -> None:
        """
        Stoppe proprement Router et WS Public (si présents),
        puis annule leurs tasks de bootstrap au besoin.
        """
        to_router = float(getattr(self.cfg, "ROUTER_STOP_TIMEOUT_S", 5.0))
        to_ws = float(getattr(self.cfg, "WS_STOP_TIMEOUT_S", 5.0))

        # 1) Stop Router (avec timeout + suppression d'erreurs)
        if getattr(self.ctx, "router", None) and hasattr(self.ctx.router, "stop"):
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self.ctx.router.stop(), timeout=to_router)

        # 2) Stop WS public (idem)
        if getattr(self.ctx, "ws_public", None) and hasattr(self.ctx.ws_public, "stop"):
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self.ctx.ws_public.stop(), timeout=to_ws)

        # 3) Annulation défensive des tasks de bootstrap s'il en reste
        tasks = []
        for attr in ("_router_task", "_ws_task"):
            t = getattr(self, attr, None)
            if t:
                t.cancel()
                tasks.append(t)
                setattr(self, attr, None)
        if tasks:
            with contextlib.suppress(Exception):
                await asyncio.gather(*tasks, return_exceptions=True)

    async def _stop_private_plane(self) -> None:
        with contextlib.suppress(Exception):
            if self.ctx.reconciler:
                await self.ctx.reconciler.stop()
        with contextlib.suppress(Exception):
            if self.ctx.pws_hub:
                await self.ctx.pws_hub.stop()
        with contextlib.suppress(Exception):
            if self.ctx.balances:
                await self.ctx.balances.stop()
        if self._balances_task:
            self._balances_task.cancel()
            with contextlib.suppress(Exception):
                await self._balances_task
            self._balances_task = None
        self._publish_private_hub_status(reason="stopped")


    async def _stop_scanner(self) -> None:
        with contextlib.suppress(Exception):
            if self.ctx.scanner:
                await self.ctx.scanner.stop()

    async def _stop_engine_and_rpc(self) -> None:
        with contextlib.suppress(Exception):
            if getattr(self.ctx, "engine", None) and hasattr(self.ctx.engine, "stop"):
                await self.ctx.engine.stop()
        with contextlib.suppress(Exception):
            rpc = getattr(self.ctx, "rpc", None) or getattr(self.ctx, "rpc_server", None)
            if rpc and hasattr(rpc, "stop"):
                await rpc.stop()

    async def _stop_lhm(self) -> None:
        with contextlib.suppress(Exception):
            if self.ctx.lhm:
                await self.ctx.lhm.stop()

    def _propagate_private_ws_health(self) -> None:
        hub = getattr(self.ctx, "pws_hub", None)
        rm = getattr(self.ctx, "rm", None)
        if not (hub and rm and hasattr(rm, "set_private_ws_health")):
            return
        status = None
        try:
            if hasattr(hub, "get_status"):
                status = hub.get_status()
        except Exception:
            self.log.exception("[Boot] private WS hub status read failed")
        try:
            rm.set_private_ws_health(status)
        except Exception:
            self.log.exception("[Boot] propagate private WS health failed")

    def _ensure_router_health_watchdog(self) -> None:
        if self._router_watchdog_task or not self._running:
            return
        router = getattr(self.ctx, "router", None)
        if not router:
            return
        try:
            self._router_watchdog_task = asyncio.create_task(
                self._router_health_watchdog_loop(),
                name="boot-router-health",
            )
        except Exception:
            self.log.exception("[Boot] unable to start router health watchdog")

    async def _router_health_watchdog_loop(self) -> None:
        router = getattr(self.ctx, "router", None)
        if not router:
            return
        router_cfg = getattr(self.cfg, "router", object())
        stale_ms = int(getattr(router_cfg, "health_stale_ms", 0) or 0)
        if not stale_ms:
            stale_ms = int(getattr(router_cfg, "stale_ms", 3000) or 3000)
        coverage_ratio = getattr(router_cfg, "health_min_coverage_ratio", None)
        if coverage_ratio is None:
            coverage_ratio = 0.5
            if not self._router_health_warned:
                self._router_health_warned = True
                self.log.warning(
                    "[Boot] router health coverage ratio fallback=%.2f (set cfg.router.health_min_coverage_ratio)",
                    float(coverage_ratio),
                )
        coverage_ratio = float(coverage_ratio)
        poll_s = float(getattr(router_cfg, "health_watchdog_interval_s", 0.5))
        expected_pairs = len(getattr(self.ctx, "active_pairs", []) or [])
        if expected_pairs <= 0:
            expected_pairs = int(getattr(getattr(self.cfg, "scanner", object()), "active_max_pairs", 0) or 0)
        expected_pairs = int(expected_pairs or 0)
        exchanges = list(getattr(router, "exchanges", []) or [])

        while getattr(self, "_running", False):
            try:
                now_ms = int(time.time() * 1000)
                for ex in exchanges:
                    key = MarketDataRouter._cex_key(ex)
                    qmap = router.out_queues.get(key, {}) if hasattr(router, "out_queues") else {}
                    q = qmap.get("health") if isinstance(qmap, dict) else None
                    if not q:
                        continue
                    while True:
                        try:
                            payload = q.get_nowait()
                        except asyncio.QueueEmpty:
                            break
                        try:
                            exu = str(payload.get("exchange") or ex).upper()
                            pair = payload.get("pair_key")
                            ts_ms = int(payload.get("recv_ts_ms") or payload.get("exchange_ts_ms") or now_ms)
                            per_ex = self._router_health_pairs_seen.setdefault(exu, {})
                            if pair:
                                per_ex[str(pair)] = ts_ms
                            state = self._router_health_state.setdefault(exu, {})
                            state["last_ts_ms"] = ts_ms
                            state["last_payload_excerpt"] = {
                                "pair_key": pair,
                                "seq": payload.get("seq"),
                            }
                        except Exception:
                            self.log.debug("[Boot] router health payload parse failed", exc_info=True)
                        finally:
                            with contextlib.suppress(Exception):
                                q.task_done()

                now_ms = int(time.time() * 1000)
                for ex in exchanges:
                    exu = str(ex).upper()
                    state = self._router_health_state.setdefault(exu, {})
                    last_ts = int(state.get("last_ts_ms") or 0)
                    age_ms = max(0, now_ms - last_ts) if last_ts else None
                    state["age_ms"] = age_ms
                    per_ex = self._router_health_pairs_seen.get(exu, {})
                    pairs_recent = 0
                    if per_ex:
                        for ts in per_ex.values():
                            if (now_ms - int(ts)) <= stale_ms:
                                pairs_recent += 1
                    state["pairs_seen_count"] = pairs_recent
                    state["expected_pairs"] = expected_pairs
                    cov_ratio = (pairs_recent / expected_pairs) if expected_pairs > 0 else 0.0
                    state["coverage_ratio"] = cov_ratio
                    coverage_ok = True if expected_pairs <= 0 else cov_ratio >= coverage_ratio
                    state["coverage_ok"] = coverage_ok
                    stale = (age_ms is None) or (age_ms > stale_ms)
                    state["stale"] = stale
                    stale_count = self._router_health_stale_counts.get(exu, 0)
                    if stale or not coverage_ok:
                        stale_count += 1
                    else:
                        stale_count = 0
                    self._router_health_stale_counts[exu] = stale_count
                    state["stale_cycles"] = stale_count
                    state["stale_ms_threshold"] = stale_ms
                self.state["router_health"] = dict(self._router_health_state)
            except asyncio.CancelledError:
                break
            except Exception:
                self.log.exception("[Boot] router health watchdog crashed")
            await asyncio.sleep(poll_s)
    def _ensure_private_health_task(self) -> None:
        if self._private_health_task or not self._running:
            return
        interval = float(getattr(getattr(self.cfg, "boot", object()), "private_ws_health_interval_s", 5.0))
        if interval <= 0:
            return
        try:
            self._private_health_task = asyncio.create_task(
                self._monitor_private_ws_health(interval), name="boot-private-health"
            )
        except Exception:
            self.log.exception("[Boot] unable to start private WS health monitor")

    async def _monitor_private_ws_health(self, interval: float) -> None:
        try:
            while self._running:
                self._propagate_private_ws_health()
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            return

    def _ensure_pnl_pipeline_health_task(self) -> None:
        """
        Démarre un petit moniteur périodique qui observe l'état du pipeline PnL
        (flags exposés par le LHM) et publie un status vers le status_sink
        (typiquement CentralWatchdog).
        """
        if self._pnl_pipeline_task or not self._running:
            return

        boot_cfg = getattr(self.cfg, "boot", object())
        interval = float(getattr(boot_cfg, "pnl_pipeline_health_interval_s", 10.0))
        if interval <= 0:
            return

        try:
            self._pnl_pipeline_task = asyncio.create_task(
                self._monitor_pnl_pipeline_health(interval),
                name="boot-pnl-pipeline-health",
            )
        except Exception:
            self.log.exception("[Boot] unable to start PnL pipeline health monitor")

    async def _monitor_pnl_pipeline_health(self, interval: float) -> None:
        """
        Boucle simple :
        - lit les flags du LHM (get_pnl_pipeline_flags si disponible),
        - maintient depuis quand c'est "unhealthy",
        - envoie un status 'pnl_pipeline' vers le status_sink (CentralWatchdog).
        """
        try:
            while self._running:
                lhm = getattr(self.ctx, "lhm", None)
                flags: Dict[str, Any] = {}

                if lhm is not None:
                    get_flags = getattr(lhm, "get_pnl_pipeline_flags", None)
                    if callable(get_flags):
                        try:
                            flags = get_flags() or {}
                        except Exception:
                            self.log.exception("[Boot] get_pnl_pipeline_flags a échoué")
                            flags = {}
                # Flags connus (définis côté LHM dans M5-B4)
                critical = bool(flags.get("critical_drop_seen"))
                storage = bool(flags.get("storage_error_seen"))
                healthy = not (critical or storage)

                now = time.time()
                if healthy:
                    self._pnl_unhealthy_since = None
                else:
                    if self._pnl_unhealthy_since is None:
                        self._pnl_unhealthy_since = now

                unhealthy_for = 0.0
                if self._pnl_unhealthy_since is not None:
                    unhealthy_for = max(0.0, now - self._pnl_unhealthy_since)

                # On projette aussi dans self.state pour le /status ou debug
                try:
                    self.state.setdefault("pnl_pipeline", {})
                    self.state["pnl_pipeline"].update(
                        {
                            "healthy": healthy,
                            "critical_drop_seen": critical,
                            "storage_error_seen": storage,
                            "unhealthy_since": self._pnl_unhealthy_since,
                            "unhealthy_for_s": unhealthy_for,
                        }
                    )
                except Exception:
                    # ne doit jamais casser la boucle
                    pass

                # Envoie vers CentralWatchdog (ou autre status_sink)
                status = "healthy" if healthy else "unhealthy"
                payload = dict(flags)
                payload["healthy"] = healthy
                payload["unhealthy_for_s"] = unhealthy_for
                self._send_status("pnl_pipeline", status, payload)

                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            return
        except Exception:
            self.log.exception("[Boot] PnL pipeline health monitor crashed")


    # ------------------------------- Gates ----------------------------------
    def _get_router_ready_pairs_count(self) -> Optional[int]:
        r = self.ctx.router
        if not r:
            return None
        for attr in ("ready_pairs_count", "l2_ready_pairs", "ready_pairs"):
            v = getattr(r, attr, None)
            if v is None:
                continue
            if isinstance(v, int):
                return v
            if isinstance(v, (list, tuple, set)):
                return len(v)
        return None

    async def _sync_rm_trading_ready(self) -> None:
        try:
            while self._running:
                rm = getattr(self.ctx, "rm", None)
                if not rm:
                    break
                ev = getattr(rm, "trading_ready_event", None) or getattr(rm, "ready_event", None)
                if ev and ev.is_set():
                    self.ready_rm.set()
                else:
                    self.ready_rm.clear()
                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            return
        except Exception:
            self.log.exception("[Boot] rm trading ready sync crashed")

    async def _evaluate_ready(self) -> None:
        self._mark_stage("evaluating_ready")
        reasons: List[str] = []

        # Warmup config
        boot_cfg = getattr(self.cfg, "boot", object())
        warmup_n = int(getattr(boot_cfg, "warmup_pairs", 0) or 0)
        warmup_timeout = float(getattr(boot_cfg, "warmup_timeout_s", 0.0) or 0.0)

        required = getattr(getattr(self.cfg, "boot", object()), "required_ready_components", None)
        if not required:
            required = ["router", "scanner", "rm", "engine"]
        required = [str(x).lower() for x in required]
        req_map = {
            "ws": self.ready_ws,
            "router": self.ready_router,
            "scanner": self.ready_scanner,
            "rm": self.ready_rm,
            "engine": self.ready_engine,
            "private": self.ready_private,
            "balances": self.ready_balances,
            "rpc": self.ready_rpc,
        }
        req = [req_map[k] for k in required if k in req_map]

        g = getattr(self.cfg, "g", None)
        fs = getattr(g, "feature_switches", {}) if g else {}
        live = (str(getattr(self.cfg, "mode", "DRY_RUN")).upper() == "PROD") or bool(fs.get("engine_real", False))
        private_ws_enabled = bool(fs.get("private_ws", False))

        rm_status: Dict[str, Any] = {}
        engine_status: Dict[str, Any] = {}

        if live and self.ready_engine not in req:
            req.append(self.ready_engine)
            if live and private_ws_enabled and self.ready_private not in req:
                req.append(self.ready_private)

        rpc_cfg = getattr(self.cfg, "rpc", object())
        rpc_enabled = bool(getattr(rpc_cfg, "enabled", False))
        if rpc_enabled and self.ready_rpc not in req:
            req.append(self.ready_rpc)

        # Attente des flags READY avec timeout optionnel
        try:
            if warmup_timeout > 0:
                await asyncio.wait_for(asyncio.gather(*(ev.wait() for ev in req)), timeout=warmup_timeout)
            else:
                await asyncio.gather(*(ev.wait() for ev in req))
        except asyncio.TimeoutError:
            reasons.append("warmup_timeout")

        # Vérif warmup L2
        if warmup_n > 0:
            rc = self._get_router_ready_pairs_count()
            if rc is not None and rc < warmup_n:
                reasons.append("warmup_pairs_missing")

        # TTL balances
        if bool(fs.get("balance_fetcher", False)):
            balances_ready = self.ready_balances.is_set()
            mbf = getattr(self.ctx, "balances", None)
            if mbf and hasattr(mbf, "get_balances_freshness_status"):
                try:
                    freshness = mbf.get_balances_freshness_status() or {}
                    if not freshness.get("ready", True):
                        balances_ready = False
                        reason_code = normalize_reason_code(freshness.get("reason_code") or "RM_BALANCE_TTL_BLOCK")
                        if reason_code and reason_code not in reasons:
                            reasons.append(reason_code)
                except Exception:
                    balances_ready = False
            if not balances_ready and "balances_stale" not in reasons:
                reasons.append("balances_stale")

        # RM readiness (loops vs trading)
        rm = getattr(self.ctx, "rm", None)
        if rm and hasattr(rm, "get_status"):
            try:
                rm_status = rm.get_status() or {}
            except Exception:
                rm_status = {}

        if rm_status:
            if not rm_status.get("rm_loops_ready", True):
                reasons.append("rm_loops_not_ready")
            if not rm_status.get("rm_trading_ready", True):
                reasons.append("rm_trading_not_ready")
        else:
            if not self.ready_rm_loops.is_set():
                reasons.append("rm_loops_not_ready")
            if not self.ready_rm.is_set():
                reasons.append("rm_trading_not_ready")

        # RPC obligatoire si activé
        if rpc_enabled and not self.ready_rpc.is_set():
            reasons.append("rpc_unavailable")
        dry_run = bool(getattr(getattr(self.cfg, "g", object()), "dry_run", False))
        live = not dry_run
        scanner = getattr(self.ctx, "scanner", None)
        lhm = getattr(self.ctx, "lhm", None)
        if live:
            if not scanner or not callable(getattr(scanner, "on_opportunity", None)):
                reason_code = self._normalize_reason("SCANNER_RM_CALLBACK_MISSING")
                if reason_code not in reasons:
                    reasons.append(reason_code)
            if lhm and scanner and not callable(getattr(scanner, "_hist_logger", None)):
                reason_code = self._normalize_reason("SCANNER_HISTORY_SINK_MISSING")
                if reason_code not in reasons:
                    reasons.append(reason_code)
            router_watchdog = getattr(self, "_router_watchdog_task", None)
            if not router_watchdog or router_watchdog.done():
                reason_code = self._normalize_reason("ROUTER_HEALTH_UNCONSUMED")
                if reason_code not in reasons:
                    reasons.append(reason_code)
            else:
                stale_cycles_target = int(
                    getattr(getattr(self.cfg, "router", object()), "health_stale_cycles", 3) or 3
                )
                for _, st in (self._router_health_state or {}).items():
                    if int(st.get("stale_cycles", 0) or 0) >= stale_cycles_target:
                        reason_code = self._normalize_reason("PUBLIC_FEED_STALE")
                        if reason_code not in reasons:
                            reasons.append(reason_code)
                        break
        # Ticket 6 – wiring privé obligatoire en live + private_ws ON
        if live and private_ws_enabled:
            rm = getattr(self.ctx, "rm", None)
            engine = getattr(self.ctx, "engine", None)

            if not rm_status and rm and hasattr(rm, "get_status"):
                try:
                    rm_status = rm.get_status() or {}
                except Exception:
                    reasons.append("rm_status_unavailable")

            if engine and hasattr(engine, "get_status"):
                try:
                    engine_status = engine.get_status() or {}
                except Exception:
                    reasons.append("engine_status_unavailable")

            rm_private = rm_status.get("private_ws") or {}
            eng_private = engine_status.get("private_ws") or {}

            if rm_private:
                if not rm_private.get("hub_wiring_ok", False):
                    reasons.append("private_hub_wiring_incomplete")
                if not rm_private.get("reconciler_wiring_ok", False):
                    reasons.append("reconciler_wiring_incomplete")
                if not rm_private.get("engine_attached", False):
                    reasons.append("rm_engine_not_attached_private")

            if eng_private:
                if not eng_private.get("hub_present", False):
                    reasons.append("engine_private_hub_missing")
                elif not eng_private.get("hub_wiring_ok", False):
                    reasons.append("engine_private_hub_wiring_incomplete")

        dep_mode = str(getattr(getattr(self.cfg, "g", object()), "deployment_mode", "EU_ONLY")).upper()
        if dep_mode not in {"EU_ONLY", "SPLIT", "JP"}:
            reasons.append("BOOT_MODE_FALLBACK")

        # Décision READY / DEGRADED

        if not reasons:
            self.ready_all.set()
            try:
                obs_metrics.safe_set(obs_metrics.BOT_STATE, "bot_state", "boot.ready", 2.0)
            except Exception:
                pass
        else:
            if self.ready_all.is_set():
                self.ready_all.clear()
            blocked_reason = self._normalize_reason("BOOT_READY_BLOCKED")
            if blocked_reason not in reasons:
                reasons.append(blocked_reason)
            try:
                obs_metrics.safe_inc(
                    obs_metrics.BLOCKED_TOTAL,
                    "blocked_total",
                    "boot.ready_blocked",
                    module="boot",
                    reason=blocked_reason,
                    pair="ready",
                )
            except Exception:
                pass
        self.state["degraded"] = bool(reasons)
        self.state["reasons"] = reasons
        self._mark_stage("ready")


    # ----------------------------- Utilitaires ------------------------------
    def _wire_private_hub_callbacks(self) -> None:
        """
        Brancher hub ↔ Engine dès qu'ils sont disponibles.

        Le callback RM est désormais câblé par RiskManager.bind_private_ws_hub().
        Ici on ne traite que:
          - le callback Engine vers le Hub,
          - l'injection du Hub dans l'Engine (set_private_ws_hub).
        """
        hub = getattr(self.ctx, "pws_hub", None)
        if not hub or not hasattr(hub, "register_callback"):
            return

        engine = getattr(self.ctx, "engine", None)
        if not engine:
            return

        # Callback Engine principal
        eng_cb = getattr(engine, "on_private_order_update", None)
        # Compat: fallback sur handle_order_update si ancien nom
        if not callable(eng_cb):
            eng_cb = getattr(engine, "handle_order_update", None)

        if callable(eng_cb):
            try:
                hub.register_callback(eng_cb, role="engine")
            except TypeError:
                hub.register_callback(eng_cb)
            except Exception:
                self.log.exception("[Boot] unable to register Engine callback on hub")

        # Informer l'Engine du Hub pour qu'il puisse vérifier son wiring
        if hasattr(engine, "set_private_ws_hub"):
            try:
                engine.set_private_ws_hub(hub)
            except Exception:
                self.log.exception("[Boot] engine.set_private_ws_hub failed")

    def _wire_mbf_ws_status_providers(self) -> None:
        """
        Brancher le statut comptes WS (Hub + Reconciler) dans le MultiBalanceFetcher, si présent.

        Hypothèses d'API côté MBF :
          - self.ctx.balances est une instance de MultiBalanceFetcher (ou équivalent),
          - expose une méthode set_ws_status_providers(hub=..., reconciler=...).

        Effet :
          - permet à MBF de construire meta["ws_accounts"][alias] avec les statuts Hub/Reconciler
            et un flag capital_at_risk exploité par le RM.
        """
        mbf = getattr(self.ctx, "balances", None)
        if not mbf:
            return
        if not hasattr(mbf, "set_ws_status_providers"):
            # MBF pas encore migré Ticket 7 : on sort silencieusement.
            return

        hub = getattr(self.ctx, "pws_hub", None)
        reco = getattr(self.ctx, "reconciler", None)
        if not (hub or reco):
            # Rien à brancher tant qu'on n'a pas au moins Hub ou Reconciler.
            return

        try:
            # API recommandée: set_ws_status_providers(hub=..., reconciler=...)
            mbf.set_ws_status_providers(hub=hub, reconciler=reco)
            self.log.info(
                "[Boot] MBF ws_status_providers branchés (hub=%s, reco=%s)",
                type(hub).__name__ if hub else "None",
                type(reco).__name__ if reco else "None",
            )
        except TypeError:
            # Compat si MBF a gardé une signature positionnelle (ancien patch).
            try:
                mbf.set_ws_status_providers(hub, reco)
                self.log.info(
                    "[Boot] MBF ws_status_providers branchés (positional, hub=%s, reco=%s)",
                    type(hub).__name__ if hub else "None",
                    type(reco).__name__ if reco else "None",
                )
            except Exception:
                self.log.exception("[Boot] set_ws_status_providers positional a échoué")
        except Exception:
            self.log.exception("[Boot] set_ws_status_providers a échoué")

    def _wire_mbf_event_sink(self) -> None:
        """
        Brancher l'event_sink du MultiBalanceFetcher vers le hub d'évènements du RM (Ticket 8).

        Hypothèses d'API :
          - self.ctx.balances expose set_event_sink(callable),
          - le RM expose en priorité on_mbf_event(event: dict),
            sinon un hub générique _submodule_event(event: dict).
        """
        mbf = getattr(self.ctx, "balances", None)
        if not mbf or not hasattr(mbf, "set_event_sink"):
            # Pas de MBF ou pas encore migré avec event_sink : no-op.
            return

        rm = getattr(self.ctx, "rm", None)
        if not rm:
            # RM pas encore démarré : on sort silencieusement.
            return

        # Priorité à un handler dédié si présent
        handler = getattr(rm, "on_mbf_event", None)
        if not callable(handler):
            # Fallback sur le hub générique des sous-modules
            handler = getattr(rm, "_submodule_event", None)

        if not callable(handler):
            # RM pas encore équipé pour consommer les évènements MBF : no-op.
            return

        try:
            mbf.set_event_sink(handler)
            self.log.info(
                "[Boot] MBF event_sink branché vers %s.%s",
                type(rm).__name__,
                getattr(handler, "__name__", "handler"),
            )
        except Exception:
            self.log.exception("[Boot] MBF.set_event_sink a échoué")


    def _publish_private_hub_status(self, *, reason: str = "update") -> Optional[Dict[str, Any]]:
        """Expose l'état du hub vers Boot.status_sink et RiskManager."""
        hub = getattr(self.ctx, "pws_hub", None)
        if not hub or not hasattr(hub, "get_status"):
            return None
        try:
            status = hub.get_status()
        except Exception:
            self.log.exception("[Boot] unable to fetch PrivateWSHub status")
            return None

        payload = {"reason": reason, "status": status}
        try:
            self._send_status("private_hub", "state", payload)
        except Exception:
            pass

        rm = getattr(self.ctx, "rm", None)
        emitter = getattr(rm, "_emit_private_plane_event", None)
        if callable(emitter):
            try:
                emitter("hub_status", reason=reason, status=status)
            except Exception:
                self.log.exception("[Boot] unable to publish hub status to RM")
        return status

    def _mark_stage(self, stage: str) -> None:
        self.state["stage"] = stage
        self.state.setdefault("timestamps", {})[stage] = time.time()


__all__ = ["Boot", "BootContext"]
