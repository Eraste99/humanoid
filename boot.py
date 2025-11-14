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
        self.ctx = BootContext(cfg)
        self._started = False

    # ------------------------------- Public API ----------------------------
    async def start(self) -> BootContext:
        # boot.py — dans async def start(self): juste au début
        mode = (
                getattr(self.cfg, "DEPLOYMENT_MODE", None)
                or getattr(self.cfg, "deployment_mode", None)
                or getattr(getattr(self.cfg, "bot_cfg", None), "deployment_mode", None)
                or "EU_ONLY"
        )


        if self._running:
            return self.ctx
        self._running = True
        self._mark_stage("starting")
        self.log.info("[Boot] 🚀 start… mode=%s", str(mode).upper())

        # 1) LHM
        await self._start_lhm()
        # 2) Discovery (optionnelle) -> active pairs
        await self._discover_pairs_and_compute_active()
        # 3) Public WS + Router
        await self._start_public_pipeline()
        # 4) RM (kwargs signature)
        await self._start_rm()
        # 5) Scanner (bind Router push)
        await self._start_scanner()
        # 6) Private plane (Hub, Reco, Balances)
        await self._start_private_plane()
        # 7) Engine + RPC
        await self._start_engine_and_rpc()
        # 8) READY
        await self._evaluate_ready()

        self.log.info(
            "[Boot] ✅ ready=%s degraded=%s reasons=%s",
            self.ready_all.is_set(), self.state["degraded"], ",".join(self.state["reasons"]) or "none",
        )
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
        # Annule la tâche de sync cohortes si active
        with contextlib.suppress(Exception):
            t = getattr(self, "_cohort_sync_task", None)
            if t:
                t.cancel()
                try:
                    await asyncio.wait_for(t, timeout=timeout_s)
                except asyncio.TimeoutError:
                    pass

        # 1) RPC
        with contextlib.suppress(Exception):
            srv = getattr(self.ctx, "rpc_server", None)
            if srv and hasattr(srv, "stop"):
                await asyncio.wait_for(srv.stop(), timeout=timeout_s)
                self._send_status("rpc_server", "stopped")
        with contextlib.suppress(Exception):
            cli = getattr(self.ctx, "rpc_client", None)
            if cli and hasattr(cli, "close"):
                await asyncio.wait_for(cli.close(), timeout=timeout_s)

        # 1) RPC
        with contextlib.suppress(Exception):
            rpc = getattr(self.ctx, "rpc", None)
            if rpc and hasattr(rpc, "stop"):
                await asyncio.wait_for(rpc.stop(), timeout=timeout_s)
                self._send_status("rpc", "stopped")

        # 2) Scanner
        with contextlib.suppress(Exception):
            sc = getattr(self.ctx, "scanner", None)
            if sc and hasattr(sc, "stop"):
                await asyncio.wait_for(sc.stop(), timeout=timeout_s)
                self._send_status("scanner", "stopped")

        # 3) RM
        with contextlib.suppress(Exception):
            rm = getattr(self.ctx, "rm", None)
            if rm and hasattr(rm, "stop"):
                await asyncio.wait_for(rm.stop(), timeout=timeout_s)
                self._send_status("rm", "stopped")

        # 4) Engine
        with contextlib.suppress(Exception):
            eng = getattr(self.ctx, "engine", None)
            if eng and hasattr(eng, "stop"):
                await asyncio.wait_for(eng.stop(), timeout=timeout_s)
                self._send_status("engine", "stopped")

        # 5) Private plane (Reconciler → Hub → Balances)
        with contextlib.suppress(Exception):
            # Reconciler: no-op si pas de stop()
            rec = getattr(self.ctx, "reconciler", None)
            if rec and hasattr(rec, "stop"):
                await asyncio.wait_for(rec.stop(), timeout=timeout_s)
        with contextlib.suppress(Exception):
            hub = getattr(self.ctx, "pws_hub", None)
            if hub and hasattr(hub, "stop"):
                await asyncio.wait_for(hub.stop(), timeout=timeout_s)
                self._send_status("private", "stopped")
        with contextlib.suppress(Exception):
            bal = getattr(self.ctx, "balances", None)
            if bal and hasattr(bal, "stop"):
                await asyncio.wait_for(bal.stop(), timeout=timeout_s)
                self._send_status("balances", "stopped")

        # 6) Router/WS publics
        with contextlib.suppress(Exception):
            rt = getattr(self.ctx, "router", None)
            if rt and hasattr(rt, "stop"):
                await asyncio.wait_for(rt.stop(), timeout=timeout_s)
                self._send_status("router", "stopped")
        with contextlib.suppress(Exception):
            ws = getattr(self.ctx, "ws_public", None)
            if ws and hasattr(ws, "stop"):
                await asyncio.wait_for(ws.stop(), timeout=timeout_s)
                self._send_status("ws", "stopped")

        # 7) LoggerHistoriqueManager (LHM)
        with contextlib.suppress(Exception):
            lhm = getattr(self.ctx, "lhm", None)
            if lhm and hasattr(lhm, "stop"):
                await asyncio.wait_for(lhm.stop(), timeout=timeout_s)
                self._send_status("lhm", "stopped")

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
            "ready": self.ready_all.is_set(),
            "ws_ready": self.ready_ws.is_set(),
            "router_ready": self.ready_router.is_set(),
            "scanner_ready": self.ready_scanner.is_set(),
            "rm_ready": self.ready_rm.is_set(),
            "engine_ready": self.ready_engine.is_set(),
            "private_ready": self.ready_private.is_set(),
            "balances_ready": self.ready_balances.is_set(),
            "rpc_ready": self.ready_rpc.is_set(),
            "scanner_proxy_dropped": getattr(self._scanner_proxy, "dropped", 0),
            "active_pairs": list(self.ctx.active_pairs),
            "discovered": len(self.ctx.discovered_pairs),
        })
        rc = self._get_router_ready_pairs_count()
        if rc is not None:
            s["router_l2_ready_pairs"] = rc
        return s

    # ------------------------------- Étapes ---------------------------------
    # boot.py


    async def _start_lhm(self) -> None:
        """
        Instancie le LoggerHistoriqueManager avec un out_dir explicite,
        crée le répertoire si besoin, démarre le LHM, marque l'étape,
        et branche les sinks Engine/Scanner si disponibles.
        """


        # 1) Résolution robuste de l'out_dir (priorité décroissante)
        out_dir = (
                getattr(self.cfg, "LHM_OUT_DIR", None)
                or getattr(self.cfg, "LOG_DIR", None)
                or getattr(self.cfg, "HISTORY_DIR", None)
                or "/srv/app/logs"
        )

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

        # 5) (Optionnel) Brancher les sinks si présents
        try:
            if getattr(self.ctx, "engine", None) and hasattr(self.ctx.engine, "set_history_logger"):
                self.ctx.engine.set_history_logger(self.ctx.lhm.sink)
            if getattr(self.ctx, "scanner", None) and hasattr(self.ctx.scanner, "set_history_logger"):
                self.ctx.scanner.set_history_logger(self.ctx.lhm.opportunity)
        except Exception as e:
            self.log.warning("[Boot][LHM] Branchements sinks partiels : %s", e)

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
        if use_discovery:
            try:

                pairs_map, discovered = await discover_pairs_3cex(self.cfg)
                if top_n > 0 and len(discovered) > top_n:
                    discovered = discovered[:top_n]
                self.log.info("[Boot] discovery: %d pairs (top_n=%d)", len(discovered), top_n)
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
            require_l2_first=bool(getattr(getattr(self.cfg, "router", object()), "require_l2_first", True)),
            combos=combos,
        )

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

    # --- boot.py (dans class Boot) ---
    async def _start_rm(self) -> None:
        """
        Démarre le RiskManager avec un getter d'orderbooks sûr, puis
        recâble engine/reconciler si déjà créés. Idempotent.
        """
        if getattr(self.ctx, "rm", None):
            # déjà démarré
            self._send_status("rm", "ready")
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
        }
        rm_kwargs["exchanges"] = list(
            getattr(getattr(self.cfg, "g", object()), "enabled_exchanges", ["BINANCE", "COINBASE", "BYBIT"]))
        rm_kwargs["symbols"] = list(self.ctx.active_pairs)

        # filtrage: ne passer que les clés supportées par __init__

        sig = inspect.signature(RiskManager.__init__)
        rm_kwargs = {k: v for k, v in rm_kwargs.items() if k in sig.parameters}

        self.ctx.rm = RiskManager(**rm_kwargs)
        if hasattr(self.ctx.rm, "start"):
            await self.ctx.rm.start()

        # recâblages idempotents
        try:
            if hasattr(self.ctx.rm, "set_engine") and getattr(self.ctx, "engine", None):
                self.ctx.rm.set_engine(self.ctx.engine)
            if hasattr(self.ctx, "reconciler", None) and getattr(self.ctx, "reconciler", None):
                if hasattr(self.ctx.rm, "bind_reconciler"):
                    self.ctx.rm.bind_reconciler(self.ctx.reconciler)
        except Exception:
            pass

        self._send_status("rm", "ready")

    async def _start_scanner(self) -> None:

        self.ctx.scanner = OpportunityScanner(self.cfg, self.ctx.rm, self.ctx.router, history_logger=self.ctx.lhm)
        with contextlib.suppress(Exception):
            lhm = getattr(self.ctx, "lhm", None)
            coh = None

            # (1) Si un jour LHM expose get_cohorts(), on le préfère
            get_coh = getattr(lhm, "get_cohorts", None)
            if callable(get_coh):
                coh = get_coh()

            # (2) Sinon on interroge directement le tracker interne (actuel)
            if coh is None:
                tr = getattr(lhm, "_tracker", None)
                if tr and hasattr(tr, "get_cohorts"):
                    coh = tr.get_cohorts()

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

        await self.ctx.scanner.start()

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
        Idempotent, DRY_RUN-gated si tu utilises des feature flags.
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



            self.ctx.pws_hub = PrivateWSHub(getattr(self, "cfg", None))
        if hasattr(self.ctx.pws_hub, "start"):
            await self.ctx.pws_hub.start()
        self._send_status("private", "ready")

        # 2) Reconciler (unique)
        if not getattr(self.ctx, "reconciler", None):


            self.ctx.reconciler = PrivateWSReconciler(venue_name="TRI-CEX")

        # Attacher au Hub s'il expose un hook
        try:
            if hasattr(self.ctx.pws_hub, "attach_reconciler"):
                self.ctx.pws_hub.attach_reconciler(self.ctx.reconciler)
        except Exception:
            pass

        # Si RM déjà lancé, lier maintenant
        rm = getattr(self.ctx, "rm", None)
        if rm and hasattr(rm, "bind_reconciler"):
            try:
                rm.bind_reconciler(self.ctx.reconciler)
            except Exception:
                pass

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

                # Appel best-effort: d’abord rotate_pairs, puis rotate_cohorts si dispo
                try:
                    tr = getattr(lhm, "_tracker", None)
                    if tr:
                        # laisse le LHM/Tracker décider des mouvements avant la sync
                        if hasattr(tr, "rotate_pairs"):
                            tr.rotate_pairs(now=None)
                        if hasattr(tr, "rotate_cohorts"):
                            tr.rotate_cohorts(now=None)
                except Exception:
                    # on ne bloque pas la sync pour ça
                    self.log.exception("[Boot] tracker rotate_* during cohort-sync failed")

                # Récupère les cohortes
                coh = None
                get_coh = getattr(lhm, "get_cohorts", None)
                if callable(get_coh):
                    coh = get_coh()
                if coh is None and tr and hasattr(tr, "get_cohorts"):
                    coh = tr.get_cohorts()

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



            self.ctx.engine = ExecutionEngine(getattr(self, "cfg", None))

        if hasattr(self.ctx.engine, "start"):
            await self.ctx.engine.start()
        self._send_status("engine", "ready")

        # 2) RPC
        if not getattr(self.ctx, "rpc", None):



            self.ctx.rpc = RPCServer(getattr(self, "cfg", None))

        # (P2) tenter de monter /status via RPC existant AVANT start
        try:
            if hasattr(self.ctx.rpc, "register_status_endpoint"):
                self.ctx.rpc.register_status_endpoint(self)  # passe le Boot
            elif hasattr(self.ctx.rpc, "enable_status_endpoint"):
                # compat alternative si tu avais un autre nom
                self.ctx.rpc.enable_status_endpoint(self)
        except Exception:
            pass

        # lancer le RPC
        if hasattr(self.ctx.rpc, "start"):
            await self.ctx.rpc.start()
        self._send_status("rpc", "ready")

        # 3) Recâblage tardif RM ↔ Engine/Reconciler (idempotent)
        rm = getattr(self.ctx, "rm", None)
        if rm:
            try:
                if hasattr(rm, "set_engine"):
                    rm.set_engine(self.ctx.engine)
                reconciler = getattr(self.ctx, "reconciler", None)
                if reconciler and hasattr(rm, "bind_reconciler"):
                    rm.bind_reconciler(reconciler)
            except Exception:
                pass

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

    async def _evaluate_ready(self) -> None:
        self._mark_stage("evaluating_ready")
        reasons: List[str] = []
        # Warmup config
        warmup_n = int(getattr(getattr(self.cfg, "boot", object()), "warmup_pairs", 0) or 0)
        warmup_timeout = float(getattr(getattr(self.cfg, "boot", object()), "warmup_timeout_s", 0.0) or 0.0)

        req = [self.ready_ws, self.ready_router, self.ready_scanner, self.ready_rm]
        live = (str(getattr(self.cfg, "mode", "DRY_RUN")).upper() == "PROD") or bool(
            getattr(getattr(self.cfg, "g", object()), "feature_switches", {}).get("engine_real", False)
        )
        if live:
            req.append(self.ready_engine)
            if bool(getattr(getattr(self.cfg, "g", object()), "feature_switches", {}).get("private_ws", False)):
                req.append(self.ready_private)
        if bool(getattr(getattr(self.cfg, "rpc", object()), "enabled", False)):
            req.append(self.ready_rpc)

        try:
            if warmup_timeout > 0:
                await asyncio.wait_for(asyncio.gather(*(ev.wait() for ev in req)), timeout=warmup_timeout)
            else:
                await asyncio.gather(*(ev.wait() for ev in req))
        except asyncio.TimeoutError:
            reasons.append("warmup_timeout")

        if warmup_n > 0:
            rc = self._get_router_ready_pairs_count()
            if rc is not None and rc < warmup_n:
                reasons.append("warmup_pairs_missing")

        if bool(getattr(getattr(self.cfg, "g", object()), "feature_switches", {}).get("balance_fetcher", False)):
            if not self.ready_balances.is_set():
                reasons.append("balances_stale")

        if bool(getattr(getattr(self.cfg, "rpc", object()), "enabled", False)) and not self.ready_rpc.is_set():
            reasons.append("rpc_unavailable")

        if reasons:
            self.state["degraded"] = True
            self.state["reasons"] = reasons
            base_ready = all(
                ev.is_set() for ev in [self.ready_ws, self.ready_router, self.ready_scanner, self.ready_rm])
            if live:
                base_ready = base_ready and self.ready_engine.is_set()
            if base_ready:
                self.ready_all.set()
        else:
            self.ready_all.set()
        self._mark_stage("ready")

    # ----------------------------- Utilitaires ------------------------------
    def _mark_stage(self, stage: str) -> None:
        self.state["stage"] = stage
        self.state.setdefault("timestamps", {})[stage] = time.time()


__all__ = ["Boot", "BootContext"]
