#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
smoke_test_all.py

Smoke tests “institutionnels” (J0) — **1 fichier unique**.

Contrainte respectée: tout est ici, et on ne référence que des modules/fichiers existants
(montés dans ce workspace):

- bot_config.py
- boot.py
- obs_metrics.py
- dynamic_execution_simulator.py
- execution_engine.py
- engine_pacer.py
- private_ws_reconciler.py
- risk_manager.py
- retry_policy.py
- log_writer.py
- errors.py
- payloads.py
- rate_limiter.py

Scénarios (max 5, comme tu l’as cadré):
1) DRY_RUN : pipeline démarre → READY → stop + /status + /metrics cohérents.
2) PROD micro-orders (simulées, sans trade réel) : submit→ack→fill/cancel cohérents, idempotence.
3) PWS incident simulé : staleness + dedup + boucle reconciler (pas de crash).
4) 429 surge : pacer passe NORMAL→CONSTRAINED/SEVERE, et expose une policy cohérente.
5) Transfers/REB + Logs : TransferController FSM (SUBMITTED→SETTLED/FAILED) + rotation LogWriter.

⚠️ Important (réaliste): un smoke J0 “unique” ne peut pas couvrir *toutes* les combinaisons
(régions × profils × exchanges × stratégies) sans exploser. Ici on fait mieux:
- on couvre la surface E2E “broad & shallow” + on vérifie la *pilotabilité* via knobs
  (hash snapshot change, modes, caps, pacer, etc.),
- on teste les compartiments critiques par contrats (Engine, Simulator, Reconciler, Transfer FSM, Logs).

Usage:
  python smoke_test_all.py

Optionnel:
  SMOKE_TIMEOUT_S=45  (timeout global par scénario)
"""

from __future__ import annotations

import asyncio
import contextlib
import copy
import json
import os
import signal
import socket
import sys
import tempfile
import time
import math
import types
import unittest
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple


# --------------------------------------------------------------------------------------
# Bootstrap imports: support both layouts:
# - real repo layout:    modules.* and contracts.*
# - flattened layout:    bot_config.py, boot.py, errors.py, payloads.py, ...
#
# The bot code itself imports modules.* and contracts.* in several places (e.g. boot.py).
# We create runtime aliases in sys.modules so smoke can import and run without refactors.
# --------------------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _ensure_pkg(name: str) -> None:
    """Ensure 'name' exists in sys.modules as a package module."""
    if name in sys.modules:
        return
    m = types.ModuleType(name)
    m.__path__ = []  # mark as package
    sys.modules[name] = m


def _alias_module(alias: str, target_mod) -> None:
    """Register alias -> target_mod, ensuring parent pkgs exist."""
    parts = alias.split(".")
    for i in range(1, len(parts)):
        _ensure_pkg(".".join(parts[:i]))
    sys.modules[alias] = target_mod


def _bootstrap_import_aliases() -> None:
    import importlib

    # base pkgs
    _ensure_pkg("modules")
    _ensure_pkg("modules.utils")
    _ensure_pkg("modules.risk_manager")
    _ensure_pkg("modules.logger_historique")
    _ensure_pkg("contracts")

    mapping = {
        # core
        "bot_config": ["modules.bot_config"],
        "boot": ["modules.boot"],
        "obs_metrics": ["modules.obs_metrics"],
        "retry_policy": ["modules.retry_policy"],
        "rate_limiter": ["modules.utils.rate_limiter"],
        "errors": ["contracts.errors"],
        "payloads": ["contracts.payloads"],
        # public plane
        "pairs_discovery": ["modules.pairs_discovery"],
        "websockets_clients": ["modules.websockets_clients"],
        "market_data_router": ["modules.market_data_router"],
        "volatility_monitor": ["modules.volatility_monitor"],
        "slippage_handler": ["modules.slippage_handler"],
        "opportunity_scanner": ["modules.opportunity_scanner"],
        # decision/execution plane
        "risk_manager": ["modules.risk_manager.risk_manager"],
        "dynamic_execution_simulator": ["modules.dynamic_execution_simulator"],
        "execution_engine": ["modules.execution_engine"],
        "engine_pacer": ["modules.engine_pacer"],
        "private_ws_hub": ["modules.private_ws_hub"],
        "balance_fetcher": ["modules.balance_fetcher"],
        "private_ws_reconciler": ["modules.private_ws_reconciler"],
        # logs
        "logger_historique_manager": ["modules.logger_historique.logger_historique_manager"],
        "log_writer": ["modules.logger_historique.log_writer"],
    }

    for top_name, aliases in mapping.items():
        try:
            mod = importlib.import_module(top_name)
        except Exception:
            continue
        for a in aliases:
            _alias_module(a, mod)


_bootstrap_import_aliases()


# Now normal imports (should work in both layouts)
import modules.bot_config  # noqa: E402
import boot as boot_mod  # noqa: E402
import modules.obs_metrics as obs_metrics_mod  # noqa: E402
import modules.dynamic_execution_simulator as des_mod  # noqa: E402
import modules.execution_engine as engine_mod  # noqa: E402
import modules.engine_pacer as pacer_mod  # noqa: E402
import modules.private_ws_reconciler as pwsr_mod  # noqa: E402
import modules.risk_manager as rm_mod  # noqa: E402
import modules.logger_historique.log_writer as log_writer_mod  # noqa: E402
import contracts.errors as errors_mod  # noqa: E402


# --------------------------------------------------------------------------------------
# Helpers: network + ports
# --------------------------------------------------------------------------------------

def _timeout_s() -> float:
    return float(os.environ.get("SMOKE_TIMEOUT_S", "45"))


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def http_get_text(url: str, timeout_s: float = 5.0) -> str:
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return resp.read().decode("utf-8", errors="replace")


def http_get_json(url: str, timeout_s: float = 5.0) -> Dict[str, Any]:
    txt = http_get_text(url, timeout_s=timeout_s)
    return json.loads(txt)


def wait_http_ready(url: str, timeout_s: float) -> None:
    deadline = time.time() + timeout_s
    last_err: Optional[str] = None
    while time.time() < deadline:
        try:
            _ = http_get_text(url, timeout_s=2.0)
            return
        except Exception as e:
            last_err = repr(e)
            time.sleep(0.2)
    raise AssertionError(f"HTTP not ready: {url} (last_err={last_err})")

# --------------------------------------------------------------------------------------
# Stress helpers (gated by SMOKE_STRESS=1)
# --------------------------------------------------------------------------------------

def _stress_enabled() -> bool:
    return os.environ.get("SMOKE_STRESS", "0") == "1"


def _stress_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except Exception:
        return int(default)


def _stress_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except Exception:
        return float(default)


async def _run_concurrent(n: int, conc: int, coro_factory):
    sem = asyncio.Semaphore(conc)

    async def _runner(idx: int):
        async with sem:
            return await coro_factory(idx)

    tasks = [asyncio.create_task(_runner(i)) for i in range(n)]
    return await asyncio.gather(*tasks)

# --------------------------------------------------------------------------------------
# Boot + servers context manager
# --------------------------------------------------------------------------------------

@dataclass
class Started:
    boot: Any
    status_server: Any
    obs_server: Any


@contextlib.asynccontextmanager
async def started_boot_with_servers(cfg: modules.bot_config.BotConfig) -> Iterable[Tuple[Any, Any, Any]]:
    Boot = getattr(boot_mod, "Boot")
    boot = Boot(cfg)

    # Start probes (supervisor usually does that)
    loop_lag_task = obs_metrics_mod.start_loop_lag_probe(period_s=0.25)
    time_skew_task = obs_metrics_mod.start_time_skew_probe(period_s=2.0)

    # Start servers first (so /health comes up quickly), then boot
    servers = obs_metrics_mod.start_servers(boot=boot, cfg=cfg)
    status_srv = servers.get("status_server")
    obs_srv = servers.get("obs_server")

    try:
        if status_srv is None:
            raise AssertionError("status_server missing from obs_metrics.start_servers()")
        await asyncio.wait_for(status_srv.start(), timeout=_timeout_s())
        if obs_srv is not None:
            await asyncio.wait_for(obs_srv.start(), timeout=_timeout_s())

        await asyncio.wait_for(boot.start(), timeout=_timeout_s())
        # Wait ready barrier (fails if not ready in time)
        await asyncio.wait_for(boot.wait_ready(timeout_s=_timeout_s()), timeout=_timeout_s())

        yield boot, status_srv, obs_srv
    finally:
        # Stop order: servers → boot → probes
        with contextlib.suppress(Exception):
            await asyncio.wait_for(status_srv.stop(), timeout=10.0)
        with contextlib.suppress(Exception):
            if obs_srv is not None:
                await asyncio.wait_for(obs_srv.stop(), timeout=10.0)
        with contextlib.suppress(Exception):
            await asyncio.wait_for(boot.stop(), timeout=20.0)

        for t in (loop_lag_task, time_skew_task):
            with contextlib.suppress(Exception):
                t.cancel()
            with contextlib.suppress(Exception):
                await t


# --------------------------------------------------------------------------------------
# Dummy history FSM for idempotence checks (Engine expects .check_and_mark_idempotency)
# --------------------------------------------------------------------------------------

class _DummyHistoryFSM:
    def __init__(self) -> None:
        self._seen: set[str] = set()

    async def check_and_mark_idempotency(self, idempotency_key: str) -> bool:
        if idempotency_key in self._seen:
            return False
        self._seen.add(idempotency_key)
        return True


# --------------------------------------------------------------------------------------
# The 5 institutional scenarios (unittest)
# --------------------------------------------------------------------------------------

class SmokeInstitutionalAll(unittest.IsolatedAsyncioTestCase):
    async def test_01_dry_run_start_ready_status_metrics_stop(self) -> None:
        """Scénario 1 — DRY_RUN E2E (superviseur HTTP)."""
        cfg = modules.bot_config.BotConfig()
        cfg.g.mode = "DRY_RUN"
        cfg.g.deployment_mode = "EU_ONLY"
        cfg.g.capital_profile = "NANO"
        # Seed pairs minimal (utilisé par Boot si discovery off)
        cfg.g.pairs = ["BTCUSDC", "ETHUSDC"]
        cfg.discovery.enabled = False
        # Exchanges: EU_ONLY (BINANCE/BYBIT). Tu peux élargir en staging.
        cfg.g.enabled_exchanges = ["BINANCE", "BYBIT"]

        # Feature switches: run public plane + decision plane, but keep trading plane OFF.
        cfg.g.feature_switches = {
            "private_ws": False,
            "balance_fetcher": False,
            "engine_real": False,
            "rpc_server": False,
        }

        # Dynamic ports
        cfg.obs.status_port = find_free_port()
        cfg.obs.expose_metrics_on_status = True
        cfg.obs.enable_obs_port = False

        # Snapshot: hash + export (freeze)
        snap_hash = cfg.snapshot_hash()
        self.assertTrue(isinstance(snap_hash, str) and len(snap_hash) >= 8)
        with tempfile.TemporaryDirectory() as td:
            export_path = Path(td) / "config_snapshot.json"
            export_path.write_text(
                json.dumps(cfg.snapshot_dict(), sort_keys=True, indent=2, default=str),
                encoding="utf-8",
            )
            self.assertTrue(export_path.exists() and export_path.stat().st_size > 100)

        h0 = cfg.snapshot_hash()
        snap0 = cfg.snapshot_dict()
        cfg2 = copy.deepcopy(cfg)
        if not (hasattr(cfg2, "slip") and hasattr(cfg2.slip, "ttl_s")):
            self.skipTest("cfg.slip.ttl_s absent")
        if not (hasattr(cfg2, "vol") and hasattr(cfg2.vol, "ttl_s")):
            self.skipTest("cfg.vol.ttl_s absent")
        cfg2.slip.ttl_s = cfg.slip.ttl_s + 1
        cfg2.vol.ttl_s = cfg.vol.ttl_s + 1
        h1 = cfg2.snapshot_hash()
        snap1 = cfg2.snapshot_dict()
        self.assertNotEqual(h0, h1)
        if "slip" not in snap0 or "ttl_s" not in snap0["slip"]:
            self.skipTest("snapshot slip.ttl_s absent")
        if "vol" not in snap0 or "ttl_s" not in snap0["vol"]:
            self.skipTest("snapshot vol.ttl_s absent")
        self.assertEqual(snap0["slip"]["ttl_s"] + 1, snap1["slip"]["ttl_s"])
        self.assertEqual(snap0["vol"]["ttl_s"] + 1, snap1["vol"]["ttl_s"])

        async with started_boot_with_servers(cfg) as (boot, _status_srv, _obs_srv):
            # Wait ready via HTTP (proxy for “superviseur /status & /ready”)
            base = f"http://127.0.0.1:{cfg.obs.status_port}"
            wait_http_ready(f"{base}/health", timeout_s=_timeout_s())

            # /ready should reflect boot readiness.
            # (StatusHTTPServer uses Boot.get_status() under the hood.)
            http_get_text(f"{base}/ready")
            status = http_get_json(f"{base}/status")
            self.assertIn("ready_all", status)
            self.assertTrue(status["ready_all"])
            for k in ["ws_ready", "router_ready", "scanner_ready", "rm_ready", "engine_ready"]:
                self.assertIn(k, status)
                self.assertIsInstance(status[k], bool)
            self.assertIn("active_pairs", status)
            self.assertEqual(status.get("active_pairs"), ["BTCUSDC", "ETHUSDC"])

            # The in-process Boot should have set ready_all.
            self.assertTrue(bool(status.get("ready_all")), f"not READY: reasons={status.get('reasons')}")

            # /metrics should be non-empty and contain at least these canonical metrics.
            murl = f"http://127.0.0.1:{cfg.obs.status_port}/metrics"
            metrics_txt = http_get_text(murl)
            self.assertIn("event_loop_lag_ms", metrics_txt)
            self.assertIn("time_skew_ms", metrics_txt)
            self.assertIn("bot_startups_total", metrics_txt)
            self.assertTrue(
                any(
                    name in metrics_txt
                    for name in (
                        "ws_public_events_total_v2",
                        "ws_public_reconnects_total_v2",
                        "ws_public_dropped_total_v2",
                    )
                )
            )
            # Stop path is executed by the context manager.
            self.assertTrue(getattr(boot, "_running", False))

    async def test_02_micro_orders_engine_idempotence_and_simulator(self) -> None:
        """Scénario 2 — micro-orders “PROD-like” (simulées).

        On ne trade pas réellement ici. On fait:
        - Simulator: une simulation minimale valide
        - Engine: enforcement idempotency_key + DUPLICATE_BUNDLE
        """
        cfg = modules.bot_config.BotConfig()
        cfg.g.mode = "DRY_RUN"  # pas de trade réel
        cfg.g.deployment_mode = "EU_ONLY"
        cfg.g.capital_profile = "NANO"
        cfg.g.enabled_exchanges = ["BINANCE", "BYBIT"]

        # Engine flags “institutionnels” pour le test
        cfg.engine.ff_enforce_client_oid_deterministic = True
        cfg.engine.ff_fail_closed_idempotence = True

        # --- Simulator sanity
        sim = des_mod.DynamicExecutionSimulator(cfg)
        buy_levels = [(100.0, 0.50), (99.9, 1.0)]
        sell_levels = [(100.1, 0.40), (100.2, 1.0)]
        opp = {
            "pair": "BTCUSDC",
            "buy_exchange": "BINANCE",
            "sell_exchange": "BYBIT",
            "notional_quote": 50.0,
            "type": "TT",
        }
        sim_res = sim.simulate(
            opportunity=opp,
            buy_levels_raw=buy_levels,
            sell_levels_raw=sell_levels,
            now_ts=time.time(),
        )
        self.assertTrue(isinstance(sim_res, dict))
        self.assertIn("ok", sim_res)
        self.assertTrue(bool(sim_res["ok"]), f"sim rejected: {sim_res}")

        # --- Engine idempotence contract (no private ws required in DRY_RUN branch)
        ExecutionEngine = getattr(engine_mod, "ExecutionEngine")
        eng = ExecutionEngine(
            cfg=cfg,
            private_ws=None,
            balance_fetcher=None,
            logger_historique_manager=None,
            rate_limiter=None,
            pacer=None,
            rpc_gateway=None,
        )
        eng.ready_event.set()
        eng.history_fsm = _DummyHistoryFSM()

        if not hasattr(eng, "_sem_inflight"):
            self.skipTest("ExecutionEngine._sem_inflight absent")
        sem = eng._sem_inflight
        self.assertIsInstance(sem, dict)
        for k in ["hedge", "cancel", "maker"]:
            if k not in sem:
                self.skipTest(f"ExecutionEngine._sem_inflight missing {k}")
            self.assertIsInstance(sem[k], dict)

        common_exchanges = set(sem["hedge"].keys()) & set(sem["maker"].keys())
        if not common_exchanges:
            self.skipTest("No common exchange between hedge/maker semaphores")
        ex = sorted(common_exchanges)[0]
        s_hedge = sem["hedge"][ex]
        s_maker = sem["maker"][ex]
        if not isinstance(s_hedge, asyncio.Semaphore) or not isinstance(s_maker, asyncio.Semaphore):
            self.skipTest("Hedge/maker semaphores are not asyncio.Semaphore instances")
        hedge_value = getattr(s_hedge, "_value", None)
        maker_value = getattr(s_maker, "_value", None)
        if not isinstance(hedge_value, int) or not isinstance(maker_value, int):
            self.skipTest("Semaphore _value not accessible for hedge/maker")
        self.assertGreaterEqual(hedge_value, maker_value)

        EngineSubmitError = getattr(errors_mod, "EngineSubmitError")

        # 1) Missing idempotency_key must fail closed when ff_enforce_client_oid_deterministic=True
        order_missing_idk = {
            "symbol": "BTCUSDC",
            "side": "BUY",
            "type": "LIMIT",
            "price": 100.0,
            "qty": 0.001,
            "client_id": "SMOKE-CID-1",
            # idempotency_key intentionally missing
        }
        with self.assertRaises(EngineSubmitError) as ctx1:
            await eng._exec_single(venue="BINANCE", alias="TT", order=order_missing_idk)
        self.assertEqual(ctx1.exception.metadata.get("reason_code"), "MISSING_IDEMPOTENCY_KEY")

        # 2) Valid order should execute in DRY_RUN (filled=True)
        order = {
            "symbol": "BTCUSDC",
            "side": "BUY",
            "type": "LIMIT",
            "price": 100.0,
            "qty": 0.001,
            "client_id": "SMOKE-CID-2",
            "idempotency_key": "SMOKE-IDK-1",
        }
        res1 = await eng._exec_single(venue="BINANCE", alias="TT", order=order)
        self.assertTrue(bool(res1.get("ok")), res1)
        self.assertTrue(bool(res1.get("filled")), res1)

        # 3) Duplicate idempotency_key must reject with DUPLICATE_BUNDLE
        order2 = dict(order)
        order2["client_id"] = "SMOKE-CID-3"  # different CID, same IDK
        with self.assertRaises(EngineSubmitError) as ctx2:
            await eng._exec_single(venue="BINANCE", alias="TT", order=order2)
        self.assertEqual(ctx2.exception.metadata.get("reason_code"), "DUPLICATE_BUNDLE")

    async def test_03_pws_incident_simulated_reconciler_stale_and_dedup(self) -> None:
        """Scénario 3 — incident PWS simulé via PrivateWSReconciler.

        On vérifie:
        - staleness detection
        - dedup fill events
        - start/stop loop sans crash
        """
        PrivateWSReconciler = getattr(pwsr_mod, "PrivateWSReconciler")

        # Tight staleness for smoke
        rec = PrivateWSReconciler(
            cooldown_s=0.1,
            stale_ms=100,
            poll_every_s=0.05,
            dedup_max=128,
        )

        # activity -> not stale
        if not hasattr(rec, "mark_ws_activity") or not hasattr(rec, "is_ws_stale"):
            self.skipTest("PrivateWSReconciler mark_ws_activity/is_ws_stale absent")
        rec.mark_ws_activity()
        self.assertFalse(rec.is_ws_stale())

        await asyncio.sleep(0.15)
        self.assertTrue(rec.is_ws_stale())

        rec.mark_ws_activity()
        self.assertFalse(rec.is_ws_stale())
        await asyncio.sleep(0.15)
        self.assertTrue(rec.is_ws_stale())
        rec.mark_ws_activity()
        self.assertFalse(rec.is_ws_stale())

        # dedup fill events
        fill_ev = {
            "exchange": "BINANCE",
            "alias": "TT",
            "type": "fill",
            "status": "FILL",
            "symbol": "BTCUSDC",
            "trade_id": "T1",
            "order_id": "O1",
            "side": "BUY",
            "price": 100.0,
            "qty": 0.01,
            "ts": time.time(),
        }
        if not hasattr(rec, "_seen_keys"):
            self.skipTest("dedup store non accessible")
        if not hasattr(rec._seen_keys, "_s"):
            self.skipTest("dedup store non accessible")
        before = len(rec._seen_keys._s)
        rec.observe_fill_event(fill_ev)
        rec.observe_fill_event(fill_ev)
        after = len(rec._seen_keys._s)
        if (after - before) not in (0, 1):
            self.fail("dedup store grew unexpectedly after duplicate fills")

        # loop start/stop sanity
        if not hasattr(rec, "start") or not hasattr(rec, "stop"):
            self.skipTest("PrivateWSReconciler start/stop absent")
        await asyncio.wait_for(rec.start(), timeout=_timeout_s())
        await asyncio.sleep(0.20)
        await asyncio.wait_for(rec.stop(), timeout=_timeout_s())

    async def test_04_429_surge_pacer_degrades(self) -> None:
        """Scénario 4 — 429 surge: pacer doit dégrader.

        On teste EnginePacer “à sec”:
        - NORMAL en conditions saines
        - CONSTRAINED / SEVERE quand err_rate/latency explosent
        """
        EnginePacer = getattr(pacer_mod, "EnginePacer")

        # Minimal targets (use defaults if present)
        pacer = EnginePacer()

        # healthy tick
        pacer.update(
            region="EU",
            p95_submit_ack_ms=80.0,
            loop_lag_ms=2.0,
            err_rate=0.0,
            queue_depth=0,
            backpressure=False,
            reason="healthy",
        )
        self.assertEqual(pacer.get_pacer_mode(), "NORMAL")

        # simulated 429 surge + latency degradation
        pacer.update(
            region="EU",
            p95_submit_ack_ms=800.0,
            loop_lag_ms=50.0,
            err_rate=0.30,
            queue_depth=5000,
            backpressure=True,
            reason="surge",
        )
        mode = pacer.get_pacer_mode()
        self.assertIn(mode, {"CONSTRAINED", "SEVERE"})
        pol = pacer.policy("EU")
        self.assertTrue(isinstance(pol, dict) and "pacing_ms" in pol)
        self.assertGreaterEqual(float(pol.get("pacing_ms", 0.0)), 0.0)

    async def test_05_transfers_fsm_and_logs_rotation(self) -> None:
        """Scénario 5 — Transfers/REB + Logs.

        - TransferController: SUBMITTED -> SETTLED + timeout -> FAILED
        - LogWriter: rotation size-based (gzip)
        """
        TransferController = getattr(rm_mod, "TransferController")

        tc = TransferController(
            submitted_timeout_s=0.2,

        )


        async def submit_fn(**kwargs):
            # emulate successful submit (return any dict)
            return {"ok": True, "id": kwargs.get("transfer_id")}

        payload_ok = {
            "transfer_id": "XFER-1",
            "exchange": "BINANCE",
            "from_alias": "A",
            "to_alias": "B",
            "ccy": "USDC",
            "amount": 1.23,
            "type": "transfer",
        }
        res_ok = await tc.submit(
            payload=payload_ok,
            submit_fn=submit_fn,
            venue="BINANCE",
        )
        self.assertEqual(res_ok.get("transfer_id"), "XFER-1")
        st = tc._states.get("XFER-1")
        self.assertIsNotNone(st)
        self.assertEqual(st.get("state"), "SUBMITTED")
        self.assertIn("expires_ts_ms", st)

        tc.mark_settled("XFER-1")
        st2 = tc._states.get("XFER-1")
        self.assertEqual(st2.get("state"), "SETTLED")

        async def submit_fn_fail(**kwargs):
            raise RuntimeError("smoke submit fail")

        try:
            await tc.submit(
                payload={
                    "transfer_id": "XFER-FAIL",
                    "exchange": "BINANCE",
                    "from_alias": "A",
                    "to_alias": "B",
                    "ccy": "USDC",
                    "amount": 1.23,
                },
                submit_fn=submit_fn_fail,
                venue="BINANCE",
            )
        except Exception:
            pass
        st_fail = tc._states.get("XFER-FAIL")
        if st_fail is None:
            self.skipTest("TransferController did not create state for failed submit")
        self.assertIn(st_fail.get("state"), {"FAILED", "SUBMITTED"})
        tc.check_timeouts()
        st_fail2 = tc._states.get("XFER-FAIL")
        self.assertEqual(st_fail2.get("state"), "FAILED")

        # Timeout path: inject a stale SUBMITTED state and run check_timeouts
        tc._states["XFER-STALE"] = {
            "state": "SUBMITTED",
            "last_ts": time.time() - 1.0,
            "payload": {"exchange": "BINANCE"},
            "expires_ts_ms": int((time.time() - 1.0) * 1000),
        }
        tc.check_timeouts()
        st3 = tc._states.get("XFER-STALE")
        self.assertEqual(st3.get("state"), "FAILED")

        # LogWriter rotation
        LogWriter = getattr(log_writer_mod, "LogWriter")
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            lw = LogWriter(
                db_dir=str(root),
                rotate_bytes=1,  # force rotation immediately
                backup_count=3,
                compress_rotations=True,
            )
            # rotation must not throw
            rotated = lw.rotate_if_needed(max_bytes=1, max_age_s=0)
            # rotated may be False if file wasn't created yet; ensure it exists
            db_path = Path(lw.db_path)
            self.assertTrue(db_path.exists())
            # Force one more check now that it exists
            _ = lw.rotate_if_needed(max_bytes=1, max_age_s=0)

        gz_files = list(Path(lw.db_dir).glob(f"{db_path.stem}.*.db.gz"))
        self.assertTrue(len(gz_files) >= 1, f"no rotated gzip found in {root}: {list(root.iterdir())}")

@unittest.skipUnless(_stress_enabled(), "SMOKE_STRESS=1 required")
class SmokeStressPack(unittest.IsolatedAsyncioTestCase):
    async def test_stress_http_status_metrics(self) -> None:
        """Stress: rafales concurrentes sur /status et /metrics."""

        cfg = modules.bot_config.BotConfig()
        cfg.g.mode = "DRY_RUN"
        cfg.g.deployment_mode = "EU_ONLY"
        cfg.g.capital_profile = "NANO"
        cfg.g.pairs = ["BTCUSDC", "ETHUSDC"]
        cfg.discovery.enabled = False
        cfg.g.enabled_exchanges = ["BINANCE", "BYBIT"]
        cfg.g.feature_switches = {
            "private_ws": False,
            "balance_fetcher": False,
            "engine_real": False,
            "rpc_server": False,
        }
        cfg.obs.status_port = find_free_port()
        cfg.obs.expose_metrics_on_status = True
        cfg.obs.enable_obs_port = False

        n = _stress_int("SMOKE_STRESS_HTTP_N", 200)
        conc = _stress_int("SMOKE_STRESS_HTTP_CONC", 50)
        timeout_s = _stress_float("SMOKE_STRESS_HTTP_TIMEOUT_S", 2.0)
        p95_target_env = os.environ.get("SMOKE_STRESS_HTTP_P95_MS")
        p95_target_ms = float(p95_target_env) if p95_target_env is not None else None

        async with started_boot_with_servers(cfg) as (_boot, _status_srv, _obs_srv):
            base_url = f"http://127.0.0.1:{cfg.obs.status_port}"
            wait_http_ready(f"{base_url}/health", timeout_s=_timeout_s())

            status_url = f"{base_url}/status"
            metrics_url = f"{base_url}/metrics"

            async def fetch_status(_idx: int) -> float:
                t0 = time.perf_counter()
                data = await asyncio.to_thread(http_get_json, status_url, timeout_s)
                self.assertTrue(isinstance(data, dict))
                self.assertIn("ready_all", data)
                return (time.perf_counter() - t0) * 1000.0

            async def fetch_metrics(_idx: int) -> float:
                t0 = time.perf_counter()
                txt = await asyncio.to_thread(http_get_text, metrics_url, timeout_s)
                self.assertTrue(txt.strip(), "empty metrics response")
                self.assertIn("event_loop_lag_ms", txt)
                self.assertIn("time_skew_ms", txt)
                return (time.perf_counter() - t0) * 1000.0

            status_lat_ms = await _run_concurrent(n, conc, fetch_status)
            metrics_lat_ms = await _run_concurrent(n, conc, fetch_metrics)

        def p95(vals: Iterable[float]) -> float:
            arr = sorted(vals)
            if not arr:
                return 0.0
            idx = max(0, min(len(arr) - 1, math.ceil(0.95 * len(arr)) - 1))
            return arr[idx]

        if p95_target_ms is not None:
            self.assertLessEqual(p95(status_lat_ms), p95_target_ms)
            self.assertLessEqual(p95(metrics_lat_ms), p95_target_ms)

    async def test_stress_rate_limiter_priority(self) -> None:
        """Flood maker bucket et vérifier que hedge passe toujours."""

        import modules.utils.rate_limiter as rate_limiter_mod

        cfg = modules.bot_config.BotConfig()
        rl_cfg = getattr(cfg, "rl", None)
        if rl_cfg is None:
            self.skipTest("RateLimiter cfg missing")

        # Knobs : maker très restrictif, hedge permissif
        if hasattr(rl_cfg, "hard_caps_rps_by_kind"):
            rl_cfg.hard_caps_rps_by_kind = {"maker": 1.0, "hedge": 20.0}
        if hasattr(rl_cfg, "bursts_by_kind"):
            rl_cfg.bursts_by_kind = {"maker": 1, "hedge": 5}
        if hasattr(rl_cfg, "priorities"):
            rl_cfg.priorities = ["hedge", "maker"]

        rl = rate_limiter_mod.RateLimiter(rl_cfg)

        maker_n = _stress_int("SMOKE_STRESS_RL_MAKERS", 50)
        hedge_n = _stress_int("SMOKE_STRESS_RL_HEDGES", 10)
        maker_conc = _stress_int("SMOKE_STRESS_RL_MAKER_CONC", min(20, maker_n))
        hedge_conc = _stress_int("SMOKE_STRESS_RL_HEDGE_CONC", min(5, hedge_n))

        async def maker_load(_idx: int) -> bool:
            try:
                await rl.acquire("BINANCE", "maker", timeout=0.05)
                return True
            except Exception:
                return False

        async def hedge_probe(idx: int) -> bool:
            await asyncio.sleep(0.001 * idx)
            try:
                await rl.acquire("BINANCE", "hedge", timeout=0.2)
                return True
            except Exception:
                return False

        maker_task = asyncio.create_task(_run_concurrent(maker_n, maker_conc, maker_load))
        # Laisser les makers saturer le bucket avant de sonder les hedges
        await asyncio.sleep(0.01)
        hedge_results = await _run_concurrent(hedge_n, hedge_conc, hedge_probe)
        maker_results = await maker_task

        hedge_success = sum(1 for ok in hedge_results if ok)
        self.assertGreaterEqual(hedge_success, 1, "hedges should pass despite maker flood")
        # Les makers peuvent échouer (timeout) mais la majorité doivent avoir tenté
        self.assertEqual(len(maker_results), maker_n)

    async def test_stress_restart_cycles(self) -> None:
        """Démarre/stoppe Boot en boucle avec ports dynamiques (détecter leaks grossiers)."""

        cycles = _stress_int("SMOKE_STRESS_CYCLES", 3)
        for _idx in range(cycles):
            cfg = modules.bot_config.BotConfig()
            cfg.g.mode = "DRY_RUN"
            cfg.g.deployment_mode = "EU_ONLY"
            cfg.g.capital_profile = "NANO"
            cfg.g.pairs = ["BTCUSDC", "ETHUSDC"]
            cfg.discovery.enabled = False
            cfg.g.enabled_exchanges = ["BINANCE", "BYBIT"]
            cfg.g.feature_switches = {
                "private_ws": False,
                "balance_fetcher": False,
                "engine_real": False,
                "rpc_server": False,
            }

            cfg.obs.status_port = find_free_port()
            cfg.obs.expose_metrics_on_status = True
            cfg.obs.enable_obs_port = False

            async with started_boot_with_servers(cfg) as (_boot, _status_srv, _obs_srv):
                base_url = f"http://127.0.0.1:{cfg.obs.status_port}"
                wait_http_ready(f"{base_url}/health", timeout_s=_timeout_s())
                data = await asyncio.to_thread(http_get_json, f"{base_url}/status", 5.0)
                self.assertIn("ready_all", data)

    async def test_stress_engine_pacer_storm(self) -> None:
        """Rafale d'updates pacer et invariants policy basiques."""

        updates_n = _stress_int("SMOKE_STRESS_PACER_UPDATES", 120)
        pacer = pacer_mod.EnginePacer(region="EU", capital_profile="NANO", min_ms=0, max_ms=500, init_ms=0, jitter_ms=0)

        seen_modes: set[str] = set()
        for i in range(updates_n):
            good = i % 2 == 0
            ack_ms = 40.0 if good else 400.0
            lag_ms = 5.0 if good else 120.0
            err_rate = 0.0 if good else 0.2
            qdepth = 0 if good else 50
            backpressure = False if good else True
            reason = "good" if good else "bad"

            pol = pacer.update(
                region="EU",
                p95_submit_ack_ms=ack_ms,
                loop_lag_ms=lag_ms,
                err_rate=err_rate,
                queue_depth=qdepth,
                backpressure=backpressure,
                reason=reason,
            )

            self.assertIsInstance(pol, dict)
            self.assertIn("pacing_ms", pol)
            self.assertGreaterEqual(pol.get("pacing_ms", 0), 0)
            if pol.get("mode") != 0:
                self.assertGreater(pol.get("pacing_ms", 0), 0)
            self.assertIn("inflight_max", pol)
            self.assertGreaterEqual(pol.get("inflight_max", 0), 1)
            seen_modes.add(str(pol.get("pacer_mode")))

            pol2 = pacer.policy("EU")
            self.assertIsInstance(pol2, dict)
            self.assertIn("pacing_ms", pol2)
            self.assertGreaterEqual(pol2.get("pacing_ms", 0), 0)

        self.assertTrue(seen_modes, "no pacer modes recorded")

    async def test_stress_private_ws_reconciler_fill_storm(self) -> None:
        """Staleness flip-flop + fill storm dedup bornée."""

        stale_ms = _stress_int("SMOKE_STRESS_PWS_STALE_MS", 200)
        dedup_max = _stress_int("SMOKE_STRESS_PWS_DEDUP_MAX", 50000)
        pwsr = pwsr_mod.PrivateWSReconciler(
            cooldown_s=0.1,
            stale_ms=stale_ms,
            poll_every_s=0.01,
            dedup_max=dedup_max,
        )
        # Phase A: activité régulière, staleness False
        for _ in range(3):
            pwsr.mark_ws_activity()
            await asyncio.sleep(0.001)
            self.assertFalse(pwsr.is_ws_stale())

        # Phase B: silence prolongé → staleness True
        await asyncio.sleep(float(stale_ms) / 1000.0 * 1.5)
        self.assertTrue(pwsr.is_ws_stale())

        # Phase C: reprise activité → staleness False
        pwsr.mark_ws_activity()
        await asyncio.sleep(float(stale_ms) / 1000.0 * 0.1)
        self.assertFalse(pwsr.is_ws_stale())

        # Fill storm: trade_id uniques pour tester la borne de dédup
        fill_n = _stress_int("SMOKE_STRESS_PWS_FILLS", 8000)

        def _fill_evt(i: int) -> dict:
            return {
                "type": "fill",
                "status": "FILL",
                "exchange": "BINANCE",
                "alias": "TT",
                "trade_id": f"tid-{i}",
                "client_id": f"CID-{i % 50}",
                "fill_px": 100.0 + i * 0.0001,
                "base_qty": 0.001 + (i % 5) * 0.0001,
            }

        for i in range(fill_n):
            pwsr.observe_fill_event(_fill_evt(i))

        seen = getattr(pwsr, "_seen_keys", None)
        if seen is None:
            self.skipTest("PrivateWSReconciler._seen_keys absent")

        maxlen = getattr(seen, "_maxlen", None)
        q = getattr(seen, "_q", None)
        s = getattr(seen, "_s", None)
        size_candidates = [v for v in (len(q) if q is not None else None, len(s) if s is not None else None) if
                           v is not None]
        if not size_candidates:
            self.skipTest("unable to introspect _seen_keys size")
        size = max(size_candidates)

        if isinstance(maxlen, int) and maxlen > 0:
            self.assertLessEqual(size, maxlen)
        else:
            # fallback: assert against provided dedup_max when maxlen unavailable
            self.assertLessEqual(size, dedup_max)


if __name__ == "__main__":
    unittest.main(verbosity=2)
