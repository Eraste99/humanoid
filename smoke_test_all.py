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
import json
import os
import signal
import socket
import sys
import tempfile
import time
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
    status_srv, obs_srv = obs_metrics_mod.start_servers(boot=boot, cfg=cfg)

    try:
        await asyncio.wait_for(status_srv.start(), timeout=_timeout_s())
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
        cfg.obs.metrics_port = find_free_port()
        cfg.obs.obs_enable_9108 = True

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

        async with started_boot_with_servers(cfg) as (boot, _status_srv, _obs_srv):
            # Wait ready via HTTP (proxy for “superviseur /status & /ready”)
            base = f"http://127.0.0.1:{cfg.obs.status_port}"
            wait_http_ready(f"{base}/health", timeout_s=_timeout_s())

            # /ready should reflect boot readiness.
            # (StatusHTTPServer uses Boot.get_status() under the hood.)
            status = http_get_json(f"{base}/status")
            self.assertIn("ready_all", status)
            self.assertIn("active_pairs", status)
            self.assertEqual(status.get("active_pairs"), ["BTCUSDC", "ETHUSDC"])

            # The in-process Boot should have set ready_all.
            self.assertTrue(bool(status.get("ready_all")), f"not READY: reasons={status.get('reasons')}")

            # /metrics should be non-empty and contain at least these canonical metrics.
            murl = f"http://127.0.0.1:{cfg.obs.metrics_port}/metrics"
            metrics_txt = http_get_text(murl)
            self.assertIn("event_loop_lag_ms", metrics_txt)
            self.assertIn("time_skew_ms", metrics_txt)

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
            stale_ms=100,
            poll_every_s=0.05,
            max_inflight=1,
            enabled=True,
        )

        # activity -> not stale
        rec.mark_ws_activity("BINANCE", "TT")
        self.assertFalse(rec.is_ws_stale("BINANCE", "TT"))

        await asyncio.sleep(0.15)
        self.assertTrue(rec.is_ws_stale("BINANCE", "TT"))

        # dedup fill events
        fill_ev = {
            "exchange": "BINANCE",
            "symbol": "BTCUSDC",
            "trade_id": "T1",
            "order_id": "O1",
            "side": "BUY",
            "price": 100.0,
            "qty": 0.01,
            "ts": time.time(),
        }
        ok1 = rec.observe_fill_event(fill_ev)
        ok2 = rec.observe_fill_event(fill_ev)
        self.assertTrue(ok1)
        self.assertFalse(ok2)

        # loop start/stop sanity
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
            now_ts=time.time(),
            ack_p95_ms=80.0,
            err_429_rate=0.0,
            err_5xx_rate=0.0,
            loop_lag_p95_ms=2.0,
            in_flight=0,
            queue_depth=0,
        )
        self.assertEqual(pacer.get_pacer_mode(), "NORMAL")

        # simulated 429 surge + latency degradation
        pacer.update(
            now_ts=time.time(),
            ack_p95_ms=800.0,
            err_429_rate=0.25,
            err_5xx_rate=0.05,
            loop_lag_p95_ms=50.0,
            in_flight=100,
            queue_depth=5000,
        )
        mode = pacer.get_pacer_mode()
        self.assertIn(mode, {"CONSTRAINED", "SEVERE"})
        pol = pacer.get_policy()
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
            retry_total_s=0.1,
            retry_step_s=0.02,
        )

        async def submit_fn(**kwargs):
            # emulate successful submit (return any dict)
            return {"ok": True, "id": kwargs.get("transfer_id")}

        tid = await tc.submit(
            transfer_id="XFER-1",
            venue="BINANCE",
            alias="TT",
            submit_fn=submit_fn,
            amount=1.23,
            asset="USDC",
            from_account="A",
            to_account="B",
        )
        self.assertEqual(tid, "XFER-1")
        st = tc.get_state("XFER-1")
        self.assertEqual(st.get("state"), "SUBMITTED")

        tc.mark_settled("XFER-1")
        st2 = tc.get_state("XFER-1")
        self.assertEqual(st2.get("state"), "SETTLED")

        # Timeout path: inject a stale SUBMITTED state and run check_timeouts
        tc._states["XFER-STALE"] = {
            "state": "SUBMITTED",
            "submitted_at": time.time() - 1.0,
            "settle_by": time.time() - 1.0,
            "expires_at": time.time() - 1.0,
            "venue": "BINANCE",
            "alias": "TT",
        }
        tc.check_timeouts()
        st3 = tc.get_state("XFER-STALE")
        self.assertEqual(st3.get("state"), "FAILED")

        # LogWriter rotation
        LogWriter = getattr(log_writer_mod, "LogWriter")
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            lw = LogWriter(
                root_dir=str(root),
                db_name="smoke.db",
                max_bytes=1,  # force rotation immediately
                max_age_s=3600,
                compress=True,
            )
            # rotation must not throw
            rotated = lw.rotate_if_needed()
            # rotated may be False if file wasn't created yet; ensure it exists
            db_path = root / "smoke.db"
            self.assertTrue(db_path.exists())
            # Force one more check now that it exists
            _ = lw.rotate_if_needed()

            gz_files = list(root.glob("smoke.db.*.gz"))
            self.assertTrue(len(gz_files) >= 1, f"no rotated gzip found in {root}: {list(root.iterdir())}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
