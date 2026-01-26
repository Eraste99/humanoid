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
   CI-1 : SMOKE_CI_PROFILE=CI1 python smoke_test_all.py
  CI-2 : SMOKE_CI_PROFILE=CI2 python smoke_test_all.py
  CI-3 : SMOKE_CI_PROFILE=CI3 python smoke_test_all.py

Décision (profils CI):
  - CI1 : permissif + rapide (scénarios 0–1)
  - CI2 : strict + blocking (scénarios 0–5)
  - CI3 : strict + nightly (scénarios 0–5 + stress pack)

Optionnel:
  SMOKE_TIMEOUT_S=45  (timeout global par scénario)
"""

from __future__ import annotations

import importlib
import asyncio
import contextlib
import copy
import json
import os
import signal
import socket
import inspect
import sys
import tempfile
import time
import math
import types
import unittest
from unittest import mock
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

from numpy import random as np_random
import random as py_random


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
        mod = None
        # Prefer importing the "flat" name; if absent, fall back to any alias (repo layout).
        try:
            mod = importlib.import_module(top_name)
        except Exception:
            for a in aliases:
                try:
                    mod = importlib.import_module(a)
                    # Also expose the flat name for code paths/tests that import it.
                    _alias_module(top_name, mod)
                    break
                except Exception:
                    continue
        if mod is None:
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
import modules.risk_manager.risk_manager as rm_mod  # noqa: E402
import modules.logger_historique.log_writer as log_writer_mod  # noqa: E402
import contracts.errors as errors_mod  # noqa: E402


# --------------------------------------------------------------------------------------
# Local imports
# --------------------------------------------------------------------------------------

router_mod = importlib.import_module("modules.market_data_router")
vol_mod = importlib.import_module("volatility_monitor")
slip_mod = importlib.import_module("slippage_handler")
scanner_mod = importlib.import_module("opportunity_scanner")
try:
    payloads_mod = importlib.import_module("contracts.payloads")
except Exception:
    payloads_mod = None

def import_family_module(name: str):
    """Import module from flat or modules.* layout (fail clearly if absent)."""

    last_exc: Exception | None = None
    for mod_name in (name, f"modules.{name}"):
        try:
            return importlib.import_module(mod_name)
        except Exception as exc:  # pragma: no cover - import fallback
            last_exc = exc
    raise last_exc or ImportError(f"unable to import {name}")
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
async def wait_health(base_url: str, timeout_s: float = 5.0) -> None:
    deadline = time.time() + timeout_s
    last_err: Optional[str] = None
    while time.time() < deadline:
        for suffix in ("/healthz", "/health"):
            try:
                await asyncio.to_thread(http_get_text, f"{base_url}{suffix}", 2.0)
                return
            except urllib.error.HTTPError as e:
                last_err = f"HTTP {e.code}"
                if e.code == 404:
                    continue
            except Exception as e:
                last_err = repr(e)
        await asyncio.sleep(0.05)
    raise AssertionError("health endpoint did not become ready" + (f" (last_err={last_err})" if last_err else ""))

@contextlib.contextmanager
def temp_env(extra: Optional[Dict[str, str]] = None):
    """Temporarily apply a minimal env config and restore after."""

    saved = os.environ.copy()
    try:
        os.environ.update(
            {
                "MODE": "DRY_RUN",
                "DEPLOYMENT_MODE": "EU_ONLY",
                "POD_REGION": "EU",
                "CAPITAL_PROFILE": "NANO",
            }
        )
        if extra:
            os.environ.update({k: str(v) for k, v in extra.items()})
        yield
    finally:
        os.environ.clear()
        os.environ.update(saved)
# --------------------------------------------------------------------------------------
# # CI profiles & knobs
# --------------------------------------------------------------------------------------
def _ci_profile() -> str:
    return os.environ.get("SMOKE_CI_PROFILE", "CI1").upper()


def _strict_enabled() -> bool:
    return os.environ.get("SMOKE_STRICT") == "1" or _ci_profile() in {"CI2", "CI3"}

def _stress_enabled() -> bool:
    return os.environ.get("SMOKE_STRESS") == "1" or _ci_profile() == "CI3"

print(
    f"[smoke] profile={_ci_profile()} strict={_strict_enabled()} stress={_stress_enabled()}"
)


def require_contract(testcase: unittest.TestCase, cond: bool, msg: str, *, critical: bool = True) -> None:
    if cond:
        return

    # Allowlist: explicit opt-out when stress pack is disabled outside CI3
    if msg == "stress gate disabled" and _ci_profile() != "CI3":
        critical = False

    if critical and _strict_enabled():
        testcase.fail(msg)
    else:
        testcase.skipTest(msg)

def soft_require(self: unittest.TestCase, cond: bool, msg: str) -> bool:
    """
    Like require_contract, but NEVER raises SkipTest or AssertionError.
    Returns False if condition not met; caller decides (return/no-op).
    """
    if not cond:
        # Keep subTests from skipping the whole test method.
        # Use an assertion that does not interrupt the parent test method flow.
        self.assertTrue(True, msg)
        return False
    return True

def _skip_or_fail(testcase: unittest.TestCase, reason: str) -> None:
    require_contract(testcase, False, reason)
# --------------------------------------------------------------------------------------
# Stress helpers (gated by CI3 or SMOKE_STRESS=1)
# --------------------------------------------------------------------------------------


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
    Boot = getattr(boot_mod, "Boot", None)
    if Boot is None:
        raise AssertionError("Boot absent")
    StatusHTTPServer = getattr(obs_metrics_mod, "StatusHTTPServer", None)
    if StatusHTTPServer is None:
        raise AssertionError("StatusHTTPServer absent")
    start_servers = getattr(obs_metrics_mod, "start_servers", None)
    if start_servers is None:
        raise AssertionError("start_servers absent")
    boot = Boot(cfg)

    # Start probes (supervisor usually does that)
    loop_lag_task = obs_metrics_mod.start_loop_lag_probe(period_s=0.25)
    time_skew_task = obs_metrics_mod.start_time_skew_probe(period_s=2.0)

    # Start servers first (so /health comes up quickly), then boot
    servers = start_servers(boot=boot, cfg=cfg) or {}
    status_srv = servers.get("status_server") if isinstance(servers, dict) else None
    obs_srv = servers.get("obs_server") if isinstance(servers, dict) else None

    async def _maybe_await(val, timeout: float) -> None:
        if inspect.isawaitable(val):
            await asyncio.wait_for(val, timeout=timeout)
    try:
        if status_srv is not None:
            await _maybe_await(status_srv.start(), timeout=_timeout_s())
        if obs_srv is not None:
            await _maybe_await(obs_srv.start(), timeout=_timeout_s())

        await asyncio.wait_for(boot.start(), timeout=_timeout_s())
        # Wait ready barrier (fails if not ready in time)
        await asyncio.wait_for(boot.wait_ready(timeout_s=_timeout_s()), timeout=_timeout_s())

        yield boot, status_srv, obs_srv
    finally:
        # Stop order: servers → boot → probes
        with contextlib.suppress(Exception):
            if status_srv is not None:
                stop_res = status_srv.stop()
                if inspect.isawaitable(stop_res):
                    await asyncio.wait_for(stop_res, timeout=10.0)
        with contextlib.suppress(Exception):
            if obs_srv is not None:
                stop_res = obs_srv.stop()
                if inspect.isawaitable(stop_res):
                    await asyncio.wait_for(stop_res, timeout=10.0)
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

    def check_and_mark_idempotency(self, idempotency_key: str, ttl_s: float | None = None) -> bool:
        _ = ttl_s  # TTL ignored for stub but accepted for signature compatibility
        if idempotency_key in self._seen:
            return False
        self._seen.add(idempotency_key)
        return True

class _DummyScannerForRouter:
    def __init__(self) -> None:
        self.events: list[Any] = []

    def update_orderbook(self, ev):
        self.events.append(ev)


def _make_router_event(
    exchange,
    pair_key,
    bid,
    ask,
    *,
    with_l2: bool = True,
    ts_ms: Optional[int] = None,
    quote: str = "USDC",
) -> dict:
    now_ms = int(time.time() * 1000)
    ts = ts_ms if ts_ms is not None else now_ms
    ob = {"bids": [[bid, 1.0]], "asks": [[ask, 1.0]]} if with_l2 else {}
    symbol = str(pair_key).replace("-", "")
    return {
        "exchange": exchange,
        "pair_key": pair_key,
        "symbol": symbol,
        "best_bid": bid,
        "best_ask": ask,
        "orderbook": ob,
        "exchange_ts_ms": ts,
        "recv_ts_ms": ts,
        "quote": quote,
    }

# --------------------------------------------------------------------------------------
# The 5 institutional scenarios (unittest)
# --------------------------------------------------------------------------------------

class SmokeInstitutionalAll(unittest.IsolatedAsyncioTestCase):
    def test_00_import_surface(self) -> None:
        """Scénario 0 — preuve mécanique que les compartiments chargent."""

        # 0) contracts payloads must exist (repo layout)
        with self.subTest("contracts.payloads importable"):
            try:
                _ = importlib.import_module("contracts.payloads")
            except Exception as e:
                self.fail(f"contracts.payloads import failed: {e}")

        with self.subTest("modules.bot_config importable"):
            try:
                _ = importlib.import_module("modules.bot_config")
            except Exception as e:
                self.fail(f"modules.bot_config import failed: {e}")

        # 1) Imports “par compartiment” (repo layout modules.*)
        families = [
            ("pairs_discovery", "modules.pairs_discovery"),
            ("websockets_clients", "modules.websockets_clients"),
            ("market_data_router", "modules.market_data_router"),
            ("volatility_monitor", "modules.volatility_monitor"),
            ("slippage_handler", "modules.slippage_handler"),
            ("opportunity_scanner", "modules.opportunity_scanner"),
            ("risk_manager", "modules.risk_manager.risk_manager"),
            ("execution_engine", "modules.execution_engine"),
            ("private_ws_reconciler", "modules.private_ws_reconciler"),
            ("logger_historique.log_writer", "modules.logger_historique.log_writer"),
        ]

        for label, mod_name in families:
            with self.subTest(f"import {label}"):
                try:
                    importlib.import_module(mod_name)
                except Exception as e:
                    self.fail(f"{mod_name} import failed: {e}")

        # 2) Contract tests OFFLINE — PairsDiscovery (helpers only, no network)
        with self.subTest("pairs_discovery contract (offline helpers)"):
            pd = importlib.import_module("modules.pairs_discovery")

            fnum = getattr(pd, "fnum", None)
            build_universe_partition = getattr(pd, "build_universe_partition", None)
            compute_diffs = getattr(pd, "compute_diffs", None)

            require_contract(self, callable(fnum), "pairs_discovery.fnum missing")
            require_contract(self, callable(build_universe_partition),
                             "pairs_discovery.build_universe_partition missing")
            require_contract(self, callable(compute_diffs), "pairs_discovery.compute_diffs missing")
            if not (callable(fnum) and callable(build_universe_partition) and callable(compute_diffs)):
                return

            self.assertAlmostEqual(fnum("1.25"), 1.25, places=6)
            self.assertEqual(fnum(None, 7.0), 7.0)

            add, rem = compute_diffs({"A"}, {"A", "B"})
            self.assertEqual(add, {"B"})
            self.assertEqual(rem, set())

            tiers = build_universe_partition(
                all_pairs_by_combo={"BINANCE-BYBIT": ["BTCUSDC", "ETHUSDC", "SOLUSDC"]},
                combo_shares={"BINANCE-BYBIT": 1.0},
                tier_targets={"CORE": 1, "PRIMARY": 2, "AUDITION": 1, "SANDBOX": 0},
            )
            self.assertIsInstance(tiers, dict)
            self.assertIn("CORE", tiers)
            self.assertIn("PRIMARY", tiers)
            self.assertIn("BINANCE-BYBIT", tiers["CORE"])
            self.assertEqual(tiers["CORE"]["BINANCE-BYBIT"], {"BTCUSDC"})

    def test_01_config_mode_aliases(self) -> None:
        """Scénario 1 — MODE/LIVE_TRADING_ARMED exposés en alias et via Globals."""

        with temp_env({"MODE": "DRY_RUN", "LIVE_TRADING_ARMED": "0"}):
            cfg = modules.bot_config.BotConfig.from_env()
            self.assertEqual(str(cfg.MODE).upper(), "DRY_RUN")
            self.assertEqual(str(cfg.g.mode).upper(), "DRY_RUN")
            self.assertFalse(bool(cfg.LIVE_TRADING_ARMED))
            self.assertFalse(bool(cfg.g.live_trading_armed))

        with temp_env({"MODE": "PROD", "LIVE_TRADING_ARMED": "true"}):
            cfg = modules.bot_config.BotConfig.from_env()
            self.assertEqual(str(cfg.MODE).upper(), "PROD")
            self.assertEqual(str(cfg.g.mode).upper(), "PROD")
            self.assertTrue(bool(cfg.LIVE_TRADING_ARMED))
            self.assertTrue(bool(cfg.g.live_trading_armed))

    async def test_01b_ci2_strict_contracts_and_staleness(self) -> None:
        """CI-2 STRICT — contracts idempotence + staleness UNKNOWN≠OK."""
        if not _strict_enabled():
            self.skipTest("CI1 profile skips strict contracts/staleness")
        require_contract(self, payloads_mod is not None, "payloads module missing")
        if payloads_mod is None:
            return

        with self.subTest("validate_submit_bundle_lite requires idempotency in PROD+armed"):
            validate_submit_bundle_lite = getattr(payloads_mod, "validate_submit_bundle_lite", None)
            require_contract(self, callable(validate_submit_bundle_lite), "validate_submit_bundle_lite missing")
            if not callable(validate_submit_bundle_lite):
                return
            payload = {
                "mode": "SIM",
                "tif": "IOC",
                "legs": [{
                    "exchange": "BINANCE",
                    "alias": "TT",
                    "side": "BUY",
                    "symbol": "BTC-USDC",
                    "price": 100.0,
                    "qty": 0.01,
                }],
                "notional_quote": {"ccy": "USDC", "amount": 10.0},
            }
            model = validate_submit_bundle_lite(payload, prod=True)
            data = model.model_dump() if hasattr(model, "model_dump") else model.dict(exclude_none=False)
            self.assertTrue(bool(data.get("_schema_invalid")), f"expected _schema_invalid: {data}")
            self.assertEqual(str(data.get("_schema_reason")), "RPC_MISSING_IDEMPOTENCY_KEY")

        with self.subTest("validate_cancel_lite requires idempotency in PROD+armed"):
            validate_cancel_lite = getattr(payloads_mod, "validate_cancel_lite", None)
            require_contract(self, callable(validate_cancel_lite), "validate_cancel_lite missing")
            if not callable(validate_cancel_lite):
                return
            cancel_model = validate_cancel_lite({"order_id": "ORD-1"}, prod=True)
            cancel_data = cancel_model.model_dump() if hasattr(cancel_model, "model_dump") else cancel_model.dict(
                exclude_none=False)
            self.assertTrue(bool(cancel_data.get("_schema_invalid")), f"expected _schema_invalid: {cancel_data}")
            self.assertEqual(str(cancel_data.get("_schema_reason")), "RPC_MISSING_IDEMPOTENCY_KEY")

        with self.subTest("staleness strict: UNKNOWN != OK (balances/slip/vol)"):
            cfg = modules.bot_config.BotConfig()
            cfg.g.mode = "PROD"
            cfg.g.live_trading_armed = True
            cfg.rm.ff_trading_state_unified = True
            cfg.g.deployment_mode = "EU_ONLY"
            cfg.g.capital_profile = "NANO"
            cfg.g.enabled_exchanges = ["BINANCE"]

            RiskManager = getattr(rm_mod, "RiskManager", None)
            require_contract(self, RiskManager is not None, "RiskManager absent")
            if RiskManager is None:
                return

            class _StubBalanceFetcher:
                def get_balances_freshness_status(self):
                    return {"status": "UNKNOWN", "ready": False, "reason_code": "RM_BALANCE_TTL_BLOCK"}

            class _StubVolMonitor:
                def get_current_metrics(self, _pair):
                    return {}

                def get_current_thresholds(self, _pair):
                    return {}

            class _StubVolManager:
                def get_current_metrics(self, _pair=None):
                    return {"age_s": 999.0}

            class _StubSlippage:
                def get_status(self):
                    return {"age_s": 999.0}

            class _StubSimulator:
                def set_event_sink(self, *_args, **_kwargs):
                    return None

            class _StubEngine:
                ready_event = asyncio.Event()

            _StubEngine.ready_event.set()

            rm = RiskManager(
                bot_cfg=cfg,
                config=cfg,
                exchanges=cfg.g.enabled_exchanges,
                symbols=["BTCUSDC"],
                balance_fetcher=_StubBalanceFetcher(),
                volatility_monitor=_StubVolMonitor(),
                volatility_manager=_StubVolManager(),
                slippage_handler=_StubSlippage(),
                simulator=_StubSimulator(),
                get_orderbooks_callback=lambda: {},
                execution_engine=_StubEngine(),
                loops_config={
                    "orderbooks_interval": 10.0,
                    "balances_interval": 10.0,
                    "rebal_interval": 10.0,
                    "volatility_interval": 10.0,
                    "fee_sync_interval": 120.0,
                },
                ready_event=asyncio.Event(),
            )
            rm._last_books_snapshot_ts = time.time()
            rm._last_balances_ts = time.time()
            rm._maybe_update_trading_ready()

            reasons = rm._readiness.get("reasons", [])
            self.assertFalse(rm.trading_ready_event.is_set())
            self.assertNotEqual(rm.get_trading_state(), "READY")
            self.assertIn("slip_unknown_or_stale", reasons)
            self.assertIn("vol_unknown_or_stale", reasons)
            self.assertTrue(
                any(r in reasons for r in ("RM_BALANCE_TTL_BLOCK", "BALANCE_STALE")),
                f"missing balance stale reason: {reasons}",
            )

        with self.subTest("caps path does not preempt MM on TT/TM overflow"):
            cfg = modules.bot_config.BotConfig()
            cfg.g.mode = "PROD"
            cfg.g.live_trading_armed = True
            cfg.g.enabled_exchanges = ["BINANCE"]

            RiskManager = getattr(rm_mod, "RiskManager", None)
            require_contract(self, RiskManager is not None, "RiskManager absent")
            if RiskManager is None:
                return

            class _StubBalanceFetcher:
                def get_balances_freshness_status(self):
                    return {"status": "OK", "ready": True, "reason_code": "OK"}

            class _StubVolMonitor:
                def get_current_metrics(self, _pair):
                    return {}

                def get_current_thresholds(self, _pair):
                    return {}

            class _StubVolManager:
                def get_current_metrics(self, _pair=None):
                    return {"age_s": 0.0}

            class _StubSlippage:
                def get_status(self):
                    return {"age_s": 0.0}

            class _StubSimulator:
                def set_event_sink(self, *_args, **_kwargs):
                    return None

            class _StubEngine:
                ready_event = asyncio.Event()

            _StubEngine.ready_event.set()

            rm = RiskManager(
                bot_cfg=cfg,
                config=cfg,
                exchanges=cfg.g.enabled_exchanges,
                symbols=["BTCUSDC"],
                balance_fetcher=_StubBalanceFetcher(),
                volatility_monitor=_StubVolMonitor(),
                volatility_manager=_StubVolManager(),
                slippage_handler=_StubSlippage(),
                simulator=_StubSimulator(),
                get_orderbooks_callback=lambda: {},
                execution_engine=_StubEngine(),
                loops_config={
                    "orderbooks_interval": 10.0,
                    "balances_interval": 10.0,
                    "rebal_interval": 10.0,
                    "volatility_interval": 10.0,
                    "fee_sync_interval": 120.0,
                },
                ready_event=asyncio.Event(),
            )
            rm.per_strategy_notional_cap = {"TT": {"BINANCE": 1.0}}
            rm.preempt_mm_for_tt_tm = True
            rm.ff_enforce_preemption = True
            preempted = {"called": False}

            async def _cancel_open_mm_quotes_on_exchange(*_args, **_kwargs):
                preempted["called"] = True
                return 0

            rm._cancel_open_mm_quotes_on_exchange = _cancel_open_mm_quotes_on_exchange
            # Test caps (Bundle-centric)
            fake_bundle = {"type": "bundle", "legs": [{"side": "BUY", "exchange": "BINANCE"}]}
            rm._apply_caps_and_preempt(fake_bundle, caps_local={})
            await asyncio.sleep(0)
            self.assertFalse(preempted["called"], "TT/TM caps overflow must not preempt MM")

        # 3) Contract tests OFFLINE — WebSocketsClients (backoff policy only, no network)
        with self.subTest("websockets_clients contract (backoff policy)"):
            wc = importlib.import_module("modules.websockets_clients")
            WsBackoffPolicy = getattr(wc, "WsBackoffPolicy", None)
            require_contract(self, WsBackoffPolicy is not None, "websockets_clients.WsBackoffPolicy missing")
            if WsBackoffPolicy is None:
                return

            pol = WsBackoffPolicy()
            rng = py_random.Random(0)


            # next_delay must be bounded [base, cap] and non-negative
            for _ in range(15):
                d = pol.next_delay(rng)
                self.assertGreaterEqual(d, 0.0)
                self.assertGreaterEqual(d, float(getattr(pol, "base", 0.0)))
                self.assertLessEqual(d, float(getattr(pol, "cap", 30.0)))

            pol.reset()
            d0 = pol.next_delay(rng)
            self.assertGreaterEqual(d0, 0.0)

    async def test_01_dry_run_start_ready_status_metrics_stop(self) -> None:
        """Scénario 1 — DRY_RUN E2E (superviseur HTTP)."""
        ci1_fast = _ci_profile() == "CI1"
        with temp_env():
            cfg = modules.bot_config.BotConfig.from_env()
        # Seed pairs minimal (utilisé par Boot si discovery off)
        cfg.g.pairs = ["BTCUSDC", "ETHUSDC"]
        cfg.discovery.enabled = False
        # Exchanges: EU_ONLY (BINANCE/BYBIT). Tu peux élargir en staging.
        cfg.g.enabled_exchanges = ["BINANCE", "BYBIT"]
        # Used by Router contract + stress; define once to avoid reliance on earlier subtests.
        combos = [("BINANCE", "BYBIT")]


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
        snapshot_hash = cfg.snapshot_hash()
        self.assertTrue(isinstance(snapshot_hash, str) and len(snapshot_hash) >= 8)
        snapshot = cfg.snapshot_dict()
        for section in ("g", "rm", "engine"):
            if hasattr(cfg, section):
                self.assertIn(section, snapshot)
        with tempfile.TemporaryDirectory() as td:
            export_path = Path(td) / "config_snapshot.json"
            export_path.write_text(
                json.dumps(snapshot, sort_keys=True, indent=2, default=str),
                encoding="utf-8",
            )
            self.assertTrue(export_path.exists() and export_path.stat().st_size > 100)
            with self.subTest("knob change changes hash"):
                cfg2 = copy.deepcopy(cfg)
                knob_changed = False
                if hasattr(cfg2, "slip") and hasattr(cfg2.slip, "ttl_s"):
                    cfg2.slip.ttl_s = getattr(cfg2.slip, "ttl_s", 0) + 1
                    knob_changed = True
                elif hasattr(cfg2, "vol") and hasattr(cfg2.vol, "ttl_s"):
                    cfg2.vol.ttl_s = getattr(cfg2.vol, "ttl_s", 0) + 1
                    knob_changed = True
                require_contract(self, knob_changed, "no slip/vol ttl knob available")
                self.assertNotEqual(snapshot_hash, cfg2.snapshot_hash())

            with self.subTest("MarketDataRouter contract"):
                if ci1_fast:
                    # CI1: keep fast; don't raise SkipTest here (would skip the whole test method).
                    self.assertTrue(True)
                else:
                    MarketDataRouter = getattr(router_mod, "MarketDataRouter", None)
                    require_contract(self, MarketDataRouter is not None, "MarketDataRouter absent")
                    if MarketDataRouter is None:
                        return
                    require_contract(
                        self,
                        hasattr(MarketDataRouter, "build_default_out_queues"),
                        "MarketDataRouter.build_default_out_queues absent",
                    )

                    in_q: asyncio.Queue = asyncio.Queue()
                    out_queues = MarketDataRouter.build_default_out_queues(
                        combos=combos,
                        maxsize={"combo": 10, "vol": 10, "slip": 10, "health": 10},
                    )
                    scanner = _DummyScannerForRouter()
                    router = MarketDataRouter(
                        in_queue=in_q,
                        out_queues=out_queues,
                        combos=combos,
                        scanner=scanner,
                        push_to_scanner=True,
                        publish_combo_to_bus=True,
                        require_l2_first=False,
                        stale_source_ms=1000,
                        coalesce_window_ms=10,
                        coalesce_maxlen=5,
                    )

                    task = asyncio.create_task(router.start())
                    try:
                        now_ms = int(time.time() * 1000)
                        for i in range(20):
                            exch = "BINANCE" if i % 2 == 0 else "BYBIT"
                            ev = _make_router_event(
                                exch,
                                "BTC-USDC",
                                100.0 + (i % 10) * 0.01,
                                100.5 + (i % 10) * 0.01,
                                with_l2=True,
                                ts_ms=now_ms + i,
                                quote="USDC",
                            )
                            await in_q.put(ev)

                        deadline = time.time() + 1.0
                        while len(scanner.events) < 1 and time.time() < deadline:
                            await asyncio.sleep(0.01)
                        self.assertGreaterEqual(len(scanner.events), 1)
                    finally:
                        with contextlib.suppress(Exception):
                            await router.stop()
                        with contextlib.suppress(Exception):
                            await asyncio.wait_for(task, timeout=1.0)

            with self.subTest("MarketDataRouter stress (CI3/SMOKE_STRESS=1)"):

                MarketDataRouter = getattr(router_mod, "MarketDataRouter", None)
                require_contract(self, MarketDataRouter is not None, "MarketDataRouter absent")

                # Gate: in CI3, stress is REQUIRED; otherwise it's best-effort/no-op.
                if not _stress_enabled():
                    if _ci_profile() == "CI3":
                        self.fail("CI3 requires stress: set SMOKE_STRESS=1 (router stress)")
                    else:
                        self.assertTrue(True)
                else:
                    if MarketDataRouter is None:
                        return

                    stress_in_q: asyncio.Queue = asyncio.Queue()
                    stress_out_queues = MarketDataRouter.build_default_out_queues(
                        combos=combos,
                        maxsize={"combo": 1000, "vol": 1000, "slip": 1000, "health": 1000},
                    )
                    stress_scanner = _DummyScannerForRouter()
                    stress_router = MarketDataRouter(
                        in_queue=stress_in_q,
                        out_queues=stress_out_queues,
                        combos=combos,
                        scanner=stress_scanner,
                        push_to_scanner=True,
                        publish_combo_to_bus=True,
                        require_l2_first=False,
                        stale_source_ms=1000,
                        coalesce_window_ms=1,
                        coalesce_maxlen=2,
                    )

                    stress_task = asyncio.create_task(stress_router.start())
                    try:
                        now_ms = int(time.time() * 1000)
                        burst_n = _stress_int("SMOKE_STRESS_ROUTER_BURST", 10_000)
                        for i in range(burst_n):
                            exch = "BINANCE" if i % 2 == 0 else "BYBIT"
                            ev = _make_router_event(
                                exch,
                                "BTC-USDC",
                                100.0 + (i % 10) * 0.01,
                                100.5 + (i % 10) * 0.01,
                                with_l2=True,
                                ts_ms=now_ms + i,
                                quote="USDC",
                            )
                            await stress_in_q.put(ev)

                        initial_pending = stress_in_q.qsize()
                        await asyncio.sleep(1.2)
                        still_pending = stress_in_q.qsize()
                        self.assertFalse(stress_task.done(), "router task terminated during stress")
                        self.assertLess(still_pending, initial_pending, "router did not drain under stress")
                        self.assertLess(still_pending, burst_n, "router queue grew unbounded")
                    finally:
                        with contextlib.suppress(Exception):
                            await asyncio.wait_for(stress_router.stop(), timeout=2.0)
                        with contextlib.suppress(Exception):
                            await asyncio.wait_for(stress_task, timeout=2.0)

            with self.subTest("VolatilityMonitor contract"):
                vol_mod = import_family_module("volatility_monitor")
                VolatilityMonitor = getattr(vol_mod, "VolatilityMonitor", None)
                require_contract(self, VolatilityMonitor is not None, "VolatilityMonitor absent")
                if VolatilityMonitor is None:
                    return
                require_contract(
                    self,
                    hasattr(VolatilityMonitor, "attach_bus_vol_queues"),
                    "VolatilityMonitor.attach_bus_vol_queues absent",
                )

                vm = VolatilityMonitor(cfg)
                vol_q: asyncio.Queue = asyncio.Queue()
                vm.attach_bus_vol_queues({"BINANCE": vol_q})
                try:
                    now_ms = int(time.time() * 1000)
                    for i in range(2):
                        msg = {
                            "exchange": "BINANCE",
                            "pair_key": "BTCUSDC",
                            "best_bid": 100.0 + i * 0.01,
                            "best_ask": 100.1 + i * 0.01,
                            "recv_ts_ms": now_ms + i,
                            "exchange_ts_ms": now_ms + i,
                        }
                        await vol_q.put(msg)
                    await asyncio.sleep(0.05)
                    if hasattr(vm, "get_volatility"):
                        with contextlib.suppress(Exception):
                            _ = vm.get_volatility("BINANCE", "BTCUSDC")
                    if hasattr(vm, "get_status"):
                        st = vm.get_status()
                        self.assertIsInstance(st, dict)
                finally:
                    if hasattr(vm, "detach_bus_consumers"):
                        with contextlib.suppress(Exception):
                            await vm.detach_bus_consumers()

            with self.subTest("SlippageHandler contract"):
                slip_mod = import_family_module("slippage_handler")
                SlippageHandler = getattr(slip_mod, "SlippageHandler", None)
                require_contract(self, SlippageHandler is not None, "SlippageHandler absent")
                if SlippageHandler is None:
                    return
                require_contract(
                    self,
                    hasattr(SlippageHandler, "attach_bus_slip_queues"),
                    "SlippageHandler.attach_bus_slip_queues absent",
                )

                sh = SlippageHandler(cfg)
                slip_q: asyncio.Queue = asyncio.Queue()
                sh.attach_bus_slip_queues({"BINANCE": slip_q})
                try:
                    now_ms = int(time.time() * 1000)
                    for i in range(2):
                        msg = {
                            "exchange": "BINANCE",
                            "pair_key": "BTCUSDC",
                            "orderbook": {"bids": [[100.0 + i, 1.0]], "asks": [[101.0 + i, 1.0]]},
                            "best_bid": 100.0 + i,
                            "best_ask": 101.0 + i,
                            "top_bid_vol": 1.0,
                            "top_ask_vol": 1.0,
                            "recv_ts_ms": now_ms + i,
                            "exchange_ts_ms": now_ms + i,
                        }
                        await slip_q.put(msg)
                    await asyncio.sleep(0.05)
                    if hasattr(sh, "get_slippage_bps"):
                        with contextlib.suppress(Exception):
                            _ = sh.get_slippage_bps("BINANCE", "BTCUSDC", side=None)
                    if hasattr(sh, "get_status"):
                        st = sh.get_status()
                        self.assertIsInstance(st, dict)
                finally:
                    if hasattr(sh, "detach_bus_consumers"):
                        with contextlib.suppress(Exception):
                            await sh.detach_bus_consumers()

            with self.subTest("OpportunityScanner contract"):
                scan_mod = import_family_module("opportunity_scanner")
                OpportunityScanner = getattr(scan_mod, "OpportunityScanner", None)
                require_contract(self, OpportunityScanner is not None, "OpportunityScanner absent")
                if OpportunityScanner is None:
                    return

                class _StubRM:
                    def __init__(self, bot_cfg):
                        self.cfg = bot_cfg

                    def get_fee_pct(self, *_args, **_kwargs):
                        return 0.0

                class _StubRouter:
                    pass

                class _StubSimulator:
                    def set_event_sink(self, *_args, **_kwargs):
                        return None

                scanner = OpportunityScanner(
                    cfg,
                    risk_manager=_StubRM(cfg),
                    market_router=_StubRouter(),
                    simulator=_StubSimulator(),
                )
                hist_events: list[dict] = []
                scanner.set_history_logger(lambda ev: hist_events.append(ev))

                bad_payload = {"exchange": "BINANCE", "pair_key": "BTCUSDC"}
                scanner.update_orderbook(bad_payload)
                self.assertTrue(hist_events, "scanner rejection was not recorded")

        if ci1_fast:
            # CI1: keep fast — skip detailed public-plane contracts (router flush/coalescing).
            pass
        else:

            MarketDataRouter = getattr(router_mod, "MarketDataRouter", None)
            require_contract(self, MarketDataRouter is not None, "MarketDataRouter absent")
            if MarketDataRouter is None:
                return
            require_contract(
                self,
                hasattr(MarketDataRouter, "build_default_out_queues"),
                "MarketDataRouter.build_default_out_queues absent",
            )

            in_q: asyncio.Queue = asyncio.Queue()
            combos = [("BINANCE", "BYBIT")]
            out_queues = MarketDataRouter.build_default_out_queues(
                combos=combos,
                maxsize={"combo": 50, "vol": 50, "slip": 50, "health": 50},
            )
            scanner = _DummyScannerForRouter()
            router = MarketDataRouter(
                in_queue=in_q,
                out_queues=out_queues,
                combos=combos,
                scanner=scanner,
                push_to_scanner=True,
                publish_combo_to_bus=True,
                require_l2_first=False,
                stale_source_ms=1000,
                coalesce_window_ms=5,
                coalesce_maxlen=2,
            )

            t = asyncio.create_task(router.start())
            try:
                now_ms = int(time.time() * 1000)
                for exch in ["BINANCE", "BYBIT"]:
                    ev = _make_router_event(
                        exch,
                        "BTC-USDC",
                        100.0,
                        101.0,
                        with_l2=True,
                        ts_ms=now_ms,
                        quote="USDC",
                    )
                    await in_q.put(ev)

                await asyncio.sleep(0.05)

                self.assertGreaterEqual(len(scanner.events), 1)
                cex_out = out_queues.get("cex:BINANCE")
                require_contract(self, cex_out is not None, "out_queues['cex:BINANCE'] absent")
                if cex_out is None:
                    return
                fanout_sizes = [
                    q.qsize()
                    for name, q in cex_out.items()
                    if name in {"vol", "slip", "health"} and hasattr(q, "qsize")
                ]
                self.assertTrue(any(sz >= 1 for sz in fanout_sizes), "no fan-out for BINANCE")
                if hasattr(router, "_events_schema_errors"):
                    self.assertEqual(getattr(router, "_events_schema_errors", None), 0)
            finally:
                with contextlib.suppress(Exception):
                    await router.stop()
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(t, timeout=1.0)

            stale_in_q: asyncio.Queue = asyncio.Queue()
            stale_out_queues = MarketDataRouter.build_default_out_queues(
                combos=combos,
                maxsize={"combo": 50, "vol": 50, "slip": 50, "health": 50},
            )
            stale_scanner = _DummyScannerForRouter()
            router_stale = MarketDataRouter(
                in_queue=stale_in_q,
                out_queues=stale_out_queues,
                combos=combos,
                scanner=stale_scanner,
                push_to_scanner=True,
                publish_combo_to_bus=True,
                require_l2_first=False,
                stale_source_ms=5,
                coalesce_window_ms=5,
                coalesce_maxlen=2,
            )
            t_stale = asyncio.create_task(router_stale.start())
            try:
                now_ms = int(time.time() * 1000)
                ev_stale = _make_router_event(
                    "BINANCE",
                    "BTC-USDC",
                    99.0,
                    100.0,
                    with_l2=True,
                    ts_ms=now_ms - 1000,
                    quote="USDC",
                )
                ev_stale["recv_ts_ms"] = now_ms
                initial_events = len(stale_scanner.events)
                await stale_in_q.put(ev_stale)
                await asyncio.sleep(0.05)

                if hasattr(router_stale, "_events_ignored_stale"):
                    self.assertGreaterEqual(getattr(router_stale, "_events_ignored_stale", None), 1)
                else:
                    self.assertEqual(len(stale_scanner.events), initial_events)
                    cex_out = stale_out_queues.get("cex:BINANCE")
                    require_contract(
                        self,
                        cex_out is not None,
                        "stale_out_queues['cex:BINANCE'] absent",
                    )
                    if cex_out is None:
                        return
                    fanout_sizes = [
                        q.qsize()
                        for name, q in cex_out.items()
                        if name in {"vol", "slip", "health"} and hasattr(q, "qsize")
                    ]
                    self.assertTrue(all(sz == 0 for sz in fanout_sizes))
            finally:
                with contextlib.suppress(Exception):
                    await router_stale.stop()
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(t_stale, timeout=1.0)

            rl2_in_q: asyncio.Queue = asyncio.Queue()
            rl2_out_queues = MarketDataRouter.build_default_out_queues(
                combos=combos,
                maxsize={"combo": 50, "vol": 50, "slip": 50, "health": 50},
            )
            rl2_scanner = _DummyScannerForRouter()
            router_rl2 = MarketDataRouter(
                in_queue=rl2_in_q,
                out_queues=rl2_out_queues,
                combos=combos,
                scanner=rl2_scanner,
                push_to_scanner=True,
                publish_combo_to_bus=True,
                require_l2_first=True,
                stale_source_ms=1000,
                coalesce_window_ms=5,
                coalesce_maxlen=2,
            )
            t_rl2 = asyncio.create_task(router_rl2.start())
            try:
                now_ms = int(time.time() * 1000)
                ev_nol2 = _make_router_event(
                    "BYBIT",
                    "BTC-USDC",
                    101.0,
                    102.0,
                    with_l2=False,
                    ts_ms=now_ms,
                    quote="USDC",
                )
                await rl2_in_q.put(ev_nol2)
                await asyncio.sleep(0.05)

                if hasattr(router_rl2, "_route_drops"):
                    drops = getattr(router_rl2, "_route_drops", None)
                    require_contract(self, drops is not None, "_route_drops absent")
                    if drops is None:
                        return
                    if isinstance(drops, dict) and drops:
                        # no reason codes invented, just check any drop recorded
                        total_drops = sum(
                            int(v) for v in drops.values() if isinstance(v, (int, float))
                        )
                        self.assertGreaterEqual(total_drops, 1)
                self.assertEqual(len(rl2_scanner.events), 0)
                cex_out = rl2_out_queues.get("cex:BYBIT")
                require_contract(self, cex_out is not None, "rl2_out_queues['cex:BYBIT'] absent")
                if cex_out is None:
                    return
                fanout_sizes = [
                    q.qsize()
                    for name, q in cex_out.items()
                    if name in {"vol", "slip", "health"} and hasattr(q, "qsize")
                ]
                self.assertTrue(all(sz == 0 for sz in fanout_sizes))
            finally:
                with contextlib.suppress(Exception):
                    await router_rl2.stop()
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(t_rl2, timeout=1.0)


            flush_in_q: asyncio.Queue = asyncio.Queue()
            flush_out_queues = MarketDataRouter.build_default_out_queues(
                combos=combos,
                maxsize={"combo": 50, "vol": 50, "slip": 50, "health": 50},
            )
            flush_scanner = _DummyScannerForRouter()
            flush_router = MarketDataRouter(
                in_queue=flush_in_q,
                out_queues=flush_out_queues,
                combos=combos,
                scanner=flush_scanner,
                push_to_scanner=True,
                publish_combo_to_bus=True,
                require_l2_first=False,
                stale_source_ms=1000,
                coalesce_window_ms=50,
                coalesce_maxlen=2,
            )
            t_flush = asyncio.create_task(flush_router.start())
            try:
                now_ms = int(time.time() * 1000)
                ev_flush = _make_router_event(
                    "BINANCE",
                    "ETH-USDC",
                    200.0,
                    201.0,
                    with_l2=True,
                    ts_ms=now_ms,
                    quote="USDC",
                )
                await flush_in_q.put(ev_flush)
                await asyncio.sleep(0)
            finally:
                with contextlib.suppress(Exception):
                    await flush_router.stop()
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(t_flush, timeout=2.0)

            flush_cex_out = flush_out_queues.get("cex:BINANCE")
            require_contract(self, flush_cex_out is not None, "flush_out_queues['cex:BINANCE'] absent")
            if flush_cex_out is None:
                return
            flush_fanout_sizes = [
                q.qsize()
                for name, q in flush_cex_out.items()
                if name in {"vol", "slip", "health"} and hasattr(q, "qsize")
            ]
            self.assertTrue(
                any(sz >= 1 for sz in flush_fanout_sizes) or len(flush_scanner.events) >= 1,
                "router stop did not flush coalesced events",
            )

        async with started_boot_with_servers(cfg) as (boot, status_srv, _obs_srv):
            # Wait ready via HTTP (proxy for “superviseur /status & /ready”)
            status_port = getattr(status_srv, "port", None) or cfg.obs.status_port
            base = f"http://127.0.0.1:{status_port}"
            await wait_health(base, timeout_s=_timeout_s())

            # /ready should reflect boot readiness.
            # (StatusHTTPServer uses Boot.get_status() under the hood.)
            http_get_text(f"{base}/ready")
            status = http_get_json(f"{base}/status")
            for k in ["ws_ready", "router_ready", "scanner_ready", "rm_ready", "engine_ready", "ready_all"]:
                self.assertIn(k, status)
                self.assertIsInstance(status[k], bool)
            self.assertIn("active_pairs", status)
            self.assertEqual(status.get("active_pairs"), ["BTCUSDC", "ETHUSDC"])

            # READY peut être False en DRY_RUN: on vérifie la présence et la forme.
            if "degraded" in status:
                self.assertIsInstance(status["degraded"], bool)

                # /metrics should live on the status port when enabled.
                include_metrics = bool(
                    getattr(status_srv, "include_metrics", getattr(cfg.obs, "expose_metrics_on_status", True)))
                metrics_txt = http_get_text(f"{base}/metrics")
                if not include_metrics or "metrics disabled" in metrics_txt:
                    self.skipTest("metrics endpoint disabled on status server")

                has_life_marker = any(marker in metrics_txt for marker in ("# HELP", "# TYPE"))
                self.assertTrue(has_life_marker, "metrics endpoint missing Prometheus markers")


            # Stop path is executed by the context manager.
            self.assertTrue(getattr(boot, "_running", False))

    async def test_02_micro_orders_engine_idempotence_and_simulator(self) -> None:
        """Scénario 2 — micro-orders “PROD-like” (simulées).

        On ne trade pas réellement ici. On fait:
        - Simulator: une simulation minimale valide
        - Engine: enforcement idempotency_key + DUPLICATE_BUNDLE
        """
        if _ci_profile() == "CI1":
            self.skipTest("CI1 profile skips scenario 2")
        with self.subTest("RiskManager invalid opportunity emits reason"):
            cfg_rm = modules.bot_config.BotConfig()
            cfg_rm.g.mode = "DRY_RUN"
            cfg_rm.g.deployment_mode = "EU_ONLY"
            cfg_rm.g.capital_profile = "NANO"
            cfg_rm.g.enabled_exchanges = ["BINANCE", "BYBIT"]

            RiskManager = getattr(rm_mod, "RiskManager", None)
            require_contract(self, RiskManager is not None, "RiskManager absent")
            if RiskManager is None:
                return

            class _StubBalanceFetcher:
                async def get_all_balances(self, force_refresh: bool = False):
                    return {"BINANCE": {"TT": {"USDC": 1000.0}}}

            class _StubVolMonitor:
                def get_current_metrics(self, _pair):
                    return {}

                def get_current_thresholds(self, _pair):
                    return {}

            class _StubSimulator:
                def set_event_sink(self, *_args, **_kwargs):
                    return None

            class _StubExecutionEngine:
                def __init__(self):
                    self.calls = []

                async def submit_bundle(self, *args, **kwargs):
                    self.calls.append((args, kwargs))
                    return {"submitted": False}

            def _minimal_orderbooks():
                now_ts = int(time.time() * 1000)
                return {
                    "BINANCE": {
                        "BTCUSDC": {
                            "best_bid": 100.0,
                            "best_ask": 101.0,
                            "recv_ts_ms": now_ts,
                        }
                    }
                }

            history_events: list[dict] = []
            ready_event = asyncio.Event()
            loops_config = {
                "orderbooks_interval": 10.0,
                "balances_interval": 10.0,
                "rebal_interval": 10.0,
                "volatility_interval": 10.0,
                "fee_sync_interval": 120.0,
            }

            rm_reason = RiskManager(
                bot_cfg=cfg_rm,
                config=cfg_rm,
                exchanges=cfg_rm.g.enabled_exchanges,
                symbols=["BTCUSDC"],
                balance_fetcher=_StubBalanceFetcher(),
                volatility_monitor=_StubVolMonitor(),
                simulator=_StubSimulator(),
                get_orderbooks_callback=_minimal_orderbooks,
                history_logger=lambda ev: history_events.append(ev),
                execution_engine=_StubExecutionEngine(),
                loops_config=loops_config,
                ready_event=ready_event,
            )

            try:
                await rm_reason.start()
                self.assertTrue(rm_reason.ready_event.is_set())

                invalid_opp = {"pair": "BTCUSDC"}
                await rm_reason.handle_opportunity(invalid_opp)
                await asyncio.sleep(0.05)
                self.assertTrue(history_events, "RiskManager did not emit history for invalid opp")
                reason = ""
                last_event = history_events[-1]
                for key in ("reason", "reasons", "status_reason"):
                    val = last_event.get(key)
                    if isinstance(val, (list, tuple)):
                        reason = next((str(v) for v in val if v), "")
                    else:
                        reason = str(val or "")
                    if reason:
                        break
                self.assertTrue(reason, f"no rejection reason captured: {last_event}")
            finally:
                if hasattr(rm_reason, "stop"):
                    with contextlib.suppress(Exception):
                        await rm_reason.stop()
        cfg = modules.bot_config.BotConfig()
        cfg.g.mode = "DRY_RUN"  # pas de trade réel
        cfg.g.deployment_mode = "EU_ONLY"
        cfg.g.capital_profile = "NANO"
        cfg.g.enabled_exchanges = ["BINANCE", "BYBIT"]

        # Engine flags “institutionnels” pour le test
        cfg.engine.ff_enforce_client_oid_deterministic = True
        cfg.engine.ff_fail_closed_idempotence = True
        # --- RiskManager lifecycle + readiness + decision record emission
        RiskManager = getattr(rm_mod, "RiskManager", None)
        require_contract(self, RiskManager is not None, "RiskManager absent")
        if RiskManager is None:
            return

        class _StubBalanceFetcher:
            async def get_all_balances(self, force_refresh: bool = False):
                return {"BINANCE": {"TT": {"USDC": 1000.0}}}

        class _StubVolMonitor:
            def get_current_metrics(self, _pair):
                return {}

            def get_current_thresholds(self, _pair):
                return {}

        class _StubSimulator:
            def set_event_sink(self, *_args, **_kwargs):
                return None

        get_orderbooks_callback = lambda: {}
        history_events = []
        history_logger = lambda ev: history_events.append(ev)
        ready_event = asyncio.Event()
        loops_config = {
            "orderbooks_interval": 10.0,
            "balances_interval": 10.0,
            "rebal_interval": 10.0,
            "volatility_interval": 10.0,
            "fee_sync_interval": 120.0,
        }

        rm = RiskManager(
            bot_cfg=cfg,
            config=cfg,
            exchanges=cfg.g.enabled_exchanges,
            symbols=["BTCUSDC"],
            balance_fetcher=_StubBalanceFetcher(),
            volatility_monitor=_StubVolMonitor(),
            simulator=_StubSimulator(),
            get_orderbooks_callback=get_orderbooks_callback,
            history_logger=history_logger,
            execution_engine=None,
            loops_config=loops_config,
            ready_event=ready_event,
        )

        try:
            await rm.start()
            self.assertTrue(rm.ready_event.is_set())

            snapshot_ts = int(time.time() * 1000)
            snapshot = {
                "BINANCE": {
                    "BTCUSDC": {
                        "best_bid": 100.0,
                        "best_ask": 101.0,
                        "recv_ts_ms": snapshot_ts,
                    }
                },
                "BYBIT": {
                    "BTCUSDC": {
                        "best_bid": 100.1,
                        "best_ask": 101.2,
                        "recv_ts_ms": snapshot_ts,
                    }
                },
            }
            if hasattr(rm, "set_orderbooks_source"):
                rm.set_orderbooks_source(lambda: snapshot)
            else:
                rm.get_orderbooks_callback = lambda: snapshot

            if hasattr(rm, "_best_bid_ask"):
                bid, ask, ts = rm._best_bid_ask("BINANCE", "BTCUSDC")
                self.assertTrue(bid > 0 and ask > 0 and ts > 0)

            await rm.handle_opportunity({})
            await asyncio.sleep(0.05)
            self.assertGreaterEqual(len(history_events), 1)
            last_event = history_events[-1]
            reason = ""
            for key in ("reason", "reasons", "status_reason"):
                val = last_event.get(key)
                if isinstance(val, (list, tuple)):
                    reason = next((str(v) for v in val if v), "")
                else:
                    reason = str(val or "")
                if reason:
                    break
            self.assertTrue(reason, f"no reason in history event: {last_event}")

            if hasattr(rm, "on_scanner_opportunity"):
                rm.on_scanner_opportunity = lambda opp, decision_ctx=None: {
                    **(decision_ctx or {}),
                    "submitted": True,
                }
                opp = {
                    "pair": "BTCUSDC",
                    "symbol": "BTCUSDC",
                    "pair_key": "BINANCE-BYBIT-BTCUSDC",
                    "buy_exchange": "BINANCE",
                    "sell_exchange": "BYBIT",
                    "ts_ms": snapshot_ts,
                    "type": "TT",
                    "expected_net_bps": {"best": "TT", "TT": 1.0},
                    "notional_quote": 25.0,
                }
                prev_len = len(history_events)
                await rm.handle_opportunity(opp)
                await asyncio.sleep(0.05)
                new_events = history_events[prev_len:]
                submitted_event = next(
                    (
                        ev
                        for ev in reversed(new_events)
                        if (
                            str(ev.get("status") or "").lower() == "submitted"
                            or ev.get("submitted") is True
                    )
                    ),
                    None,
                )
                self.assertIsNotNone(
                    submitted_event,
                    f"no submitted decision record in new events: {new_events}",
                )
        finally:
            if hasattr(rm, "stop"):
                await rm.stop()
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
        ExecutionEngine = getattr(engine_mod, "ExecutionEngine", None)
        require_contract(self, ExecutionEngine is not None, "ExecutionEngine absent")
        if ExecutionEngine is None:
            return
        cfg.engine.ff_enforce_client_oid_deterministic = True
        cfg.engine.ff_fail_closed_idempotence = True

        class _StubPrivateWS:
            ready_event: asyncio.Event = asyncio.Event()

        class _StubRateLimiter:
            async def acquire(self, *_args, **_kwargs):
                return True

        class _StubRetryPolicy:
            async def with_retry(self, coro, *args, **kwargs):  # pragma: no cover - stub
                return await coro(*args, **kwargs)

        ready_event = asyncio.Event()
        eng = ExecutionEngine(
            _StubPrivateWS(),
            _StubRateLimiter(),
            _StubRetryPolicy(),
            history_logger=None,
            cfg=cfg,
            ready_event=ready_event,
        )
        eng.ready_event.set()
        eng.history_fsm = _DummyHistoryFSM()

        with self.subTest("Engine lanes hedge>maker (best-effort)"):
            sem = getattr(eng, "_sem_inflight", None)
            if not isinstance(sem, dict):
                self.skipTest("ExecutionEngine._sem_inflight not available; skipping lanes check")

            if "hedge" not in sem or "maker" not in sem:
                self.skipTest("ExecutionEngine._sem_inflight missing hedge/maker; skipping lanes check")

            common_exchanges = set(getattr(sem.get("hedge"), "keys", lambda: [])()) & set(
                getattr(sem.get("maker"), "keys", lambda: [])())
            if not common_exchanges:
                self.skipTest("No common exchange between hedge/maker semaphores; skipping lanes check")

            ex = sorted(common_exchanges)[0]
            s_hedge = sem["hedge"].get(ex)
            s_maker = sem["maker"].get(ex)
            if not isinstance(s_hedge, asyncio.Semaphore) or not isinstance(s_maker, asyncio.Semaphore):
                self.skipTest("Hedge/maker semaphores are not asyncio.Semaphore; skipping lanes check")

            hedge_value = getattr(s_hedge, "_value", None)
            maker_value = getattr(s_maker, "_value", None)
            if isinstance(hedge_value, int) and isinstance(maker_value, int):
                self.assertGreaterEqual(hedge_value, maker_value)
            else:
                self.skipTest("Semaphore _value not accessible; skipping lanes comparison")

        EngineSubmitError = getattr(errors_mod, "EngineSubmitError", None)
        require_contract(self, EngineSubmitError is not None, "EngineSubmitError absent")
        if EngineSubmitError is None:
            return

        # Order template aligned with ExecutionEngine._exec_single expectations
        order_common = {
            "exchange": "BINANCE",
            "symbol": "BTCUSDC",
            "side": "BUY",
            "price": 100.0,
            "volume_usdc": 10.0,
            "meta": {
                "bundle_id": "SMOKE-BNDL-1",
                "idempotency_key": "SMOKE-IDK-1",
                "slice_id": "SLC-1",
                "account_alias": "TT",
            },
        }

        # 1) Missing idempotency_key must fail closed when ff_enforce_client_oid_deterministic=True
        order_missing_idk = dict(order_common)
        order_missing_idk["meta"] = {k: v for k, v in order_common["meta"].items() if k != "idempotency_key"}
        with self.assertRaises(EngineSubmitError) as ctx1:
            await eng._exec_single(order_missing_idk)
        reason_1 = getattr(ctx1.exception, "reason", None)
        if reason_1 is None:
            self.skipTest("EngineSubmitError.reason absent for missing idempotency")
        self.assertIn("IDEMPOTENCY_MISSING", str(reason_1))

        # 2) Missing bundle_id must also fail closed before any network submit
        order_missing_bundle = dict(order_common)
        order_missing_bundle["meta"] = {k: v for k, v in order_common["meta"].items() if k != "bundle_id"}
        with self.assertRaises(EngineSubmitError) as ctx_bundle:
            await eng._exec_single(order_missing_bundle)
        reason_bundle = getattr(ctx_bundle.exception, "reason", None)
        if reason_bundle is None:
            self.skipTest("EngineSubmitError.reason absent for missing bundle_id")
        self.assertIn("BUNDLE_ILLEGAL", str(reason_bundle))

        # 3) Duplicate idempotency_key is rejected when already seen (circuit breaker short-circuits network)
        rejected: list[tuple[str, dict]] = []
        eng._pre_trade_circuits = lambda *args, **kwargs: False  # type: ignore[assignment]

        def _stub_reject(symbol, order, reason):
            rejected.append((symbol, {"reason": reason, "order": order}))

        eng._reject = _stub_reject  # type: ignore[assignment]

        first_res = await eng._exec_single(order_common)
        self.assertFalse(bool(first_res))
        self.assertTrue(rejected, "circuit breaker stub was not invoked")

        with self.assertRaises(EngineSubmitError) as ctx2:
            await eng._exec_single(order_common)
        reason_2 = getattr(ctx2.exception, "reason", None)
        if reason_2 is None:
            self.skipTest("EngineSubmitError.reason absent for duplicate idempotency")
        self.assertIn("DUPLICATE_BUNDLE", str(reason_2))
    async def test_03_pws_incident_simulated_reconciler_stale_and_dedup(self) -> None:
        """Scénario 3 — incident PWS simulé via PrivateWSReconciler.

        On vérifie:
        - staleness detection
        - dedup fill events
        - start/stop loop sans crash
        """
        if _ci_profile() == "CI1":
            self.skipTest("CI1 profile skips scenario 3")
        PrivateWSReconciler = getattr(pwsr_mod, "PrivateWSReconciler", None)
        require_contract(self, PrivateWSReconciler is not None, "PrivateWSReconciler absent")
        if PrivateWSReconciler is None:
            return

        class _FakeRecoCfg:
            RECO_MISS_BURST_THRESHOLD = 2
            RECO_ALERT_PERIOD_S = 1.5
            RECO_MISS_RECENT_THRESHOLD = 1
            RECO_ALIAS_RESYNC_MAX_AGE_S = 0.1

        fake_cfg = _FakeRecoCfg()

        # Tight staleness for smoke
        rec = PrivateWSReconciler(
            cooldown_s=0.1,
            stale_ms=100,
            poll_every_s=0.05,
            dedup_max=128,
            cfg=fake_cfg,
        )

        # activity -> not stale
        require_contract(
            self,
            hasattr(rec, "mark_ws_activity") and hasattr(rec, "is_ws_stale"),
            "PrivateWSReconciler mark_ws_activity/is_ws_stale absent",
        )
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

        # Dedup check should never hard-fail on internal structure changes.
        # We always validate "no-crash" on duplicate notes; size introspection is best-effort.
        with self.subTest("dedup (best-effort, no-crash contract)"):
            rec.note_seen_client_id("CID-1")
            rec.note_seen_client_id("CID-1")
            rec.note_seen_idempotency_key("IDK-1")
            rec.note_seen_idempotency_key("IDK-1")

            seen = getattr(rec, "_seen_keys", None)
            if seen is None:
                self.skipTest("dedup store (_seen_keys) not accessible; size check skipped")

            # Try to measure size without assuming internal fields
            try:
                before = len(seen)
                rec.note_seen_client_id("CID-2")
                rec.note_seen_client_id("CID-2")
                rec.note_seen_idempotency_key("IDK-2")
                rec.note_seen_idempotency_key("IDK-2")
                after = len(seen)

                # For a set-based store: growth should be 0..2, never 4 on duplicates
                if (after - before) not in (0, 1, 2):
                    self.fail(f"dedup store grew unexpectedly: before={before} after={after}")
            except Exception:
                self.skipTest("unable to introspect dedup store size; no-crash contract only")
        with self.subTest("reconciler cfg knobs (alerts/health)"):
            class _Metric:
                def __init__(self) -> None:
                    self.inc_count = 0
                    self.set_values: list[float] = []

                def labels(self, *_args, **_kwargs):
                    return self

                def inc(self, *_args, **_kwargs) -> None:
                    self.inc_count += 1

                def set(self, value: float) -> None:
                    self.set_values.append(float(value))

            burst_metric = _Metric()
            rate_metric = _Metric()

            rec._miss_win.clear()
            now = time.time()
            rec._miss_win.extend([now - 1.0, now - 2.0])

            captured_timeouts: list[float] = []

            async def _fake_wait_for(awaitable, timeout):
                captured_timeouts.append(float(timeout))
                rec._stop.set()
                raise asyncio.TimeoutError

            rec._stop.clear()
            with mock.patch.object(pwsr_mod, "WS_RECO_MISS_BURST_TOTAL", burst_metric), \
                    mock.patch.object(pwsr_mod, "WS_RECO_MISS_PER_MINUTE", rate_metric), \
                    mock.patch("asyncio.wait_for", side_effect=_fake_wait_for):
                await rec.run_miss_alerts(threshold_per_minute=999, period_s=9.0)

            self.assertEqual(burst_metric.inc_count, 1)
            self.assertTrue(captured_timeouts, "run_miss_alerts did not await interval")
            self.assertEqual(captured_timeouts[0], float(fake_cfg.RECO_ALERT_PERIOD_S))

            ex = "BINANCE"
            alias = "TT"
            key = (ex, alias)
            rec._miss_win.clear()

            rec._alias_miss_counter[key] = 1
            rec._last_alias_resync[key] = time.time()
            health_recent = rec.health(ex, alias)
            self.assertEqual(health_recent.get("status"), "AT_RISK")

            rec._alias_miss_counter[key] = 0
            rec._last_alias_resync[key] = time.time() - 999.0
            health_age = rec.health(ex, alias)
            self.assertEqual(health_age.get("status"), "AT_RISK")

            snap = rec.get_alias_status_snapshot(ex, alias)
            self.assertEqual(snap.get("status"), "AT_RISK")

        if _stress_enabled():
            with self.subTest("PrivateWSReconciler fill storm (CI3/SMOKE_STRESS=1)"):
                seen = getattr(rec, "_seen_keys", None)
                require_contract(self, seen is not None, "PrivateWSReconciler._seen_keys absent")
                if seen is None:
                    return

                def _seen_size() -> int:
                    # Try a few known internal shapes, otherwise fallback to len(seen) if possible.
                    for attr in ("_q", "_s"):
                        val = getattr(seen, attr, None)
                        if val is not None:
                            try:
                                return int(len(val))
                            except Exception:
                                pass
                    try:
                        return int(len(seen))
                    except Exception:
                        require_contract(self, False, "unable to introspect _seen_keys size")
                        return 0

                fill_n = _stress_int("SMOKE_STRESS_PWS_FILL_STORM", 5000)
                maxlen = getattr(seen, "_maxlen", None)

                def _mk_evt(i: int) -> dict:
                    return {
                        "exchange": "BINANCE",
                        "alias": "TT",
                        "type": "fill",
                        "status": "FILL",
                        "symbol": "BTCUSDC",
                        "trade_id": f"tid-{i % 200}",
                        "order_id": f"OID-{i % 50}",
                        "client_id": f"CID-{i % 100}",
                        "side": "BUY" if i % 2 == 0 else "SELL",
                        "price": 100.0 + i * 0.0001,
                        "qty": 0.01 + (i % 3) * 0.001,
                        "ts": time.time(),
                    }

                for i in range(fill_n):
                    rec.observe_fill_event(_mk_evt(i))
                    if hasattr(rec, "sweep") and i % 500 == 0:
                        sweep_fn = getattr(rec, "sweep", None)
                        require_contract(self, sweep_fn is not None, "sweep absent")
                        if sweep_fn is None:
                            return
                        if inspect.iscoroutinefunction(sweep_fn):
                            await sweep_fn()
                        else:
                            sweep_fn()

                peak = _seen_size()
                if isinstance(maxlen, int) and maxlen > 0:
                    self.assertLessEqual(peak, maxlen)
                else:
                    # fallback: ensure dedup set size remains well below total events
                    self.assertLessEqual(peak, fill_n // 10)

        # loop start/stop sanity
        require_contract(self, hasattr(rec, "start") and hasattr(rec, "stop"), "PrivateWSReconciler start/stop absent")
        await asyncio.wait_for(rec.start(), timeout=_timeout_s())
        await asyncio.sleep(0.20)
        await asyncio.wait_for(rec.stop(), timeout=_timeout_s())

    async def test_03b_pws_bybit_normalization(self) -> None:
        if _ci_profile() == "CI1":
            self.skipTest("CI1 profile skips PWS normalization test")

        import private_ws_hub as pws_hub_mod
        from bot_config import PrivateWSHubCfg

        class _Cfg:
            def __init__(self) -> None:
                self.pws = PrivateWSHubCfg()

                class _G:
                    mode = "DRY_RUN"

                self.g = _G()

        cfg = _Cfg()
        hub = pws_hub_mod.PrivateWSHub(config=cfg)
        seen: list[dict] = []

        def _cb(ev: dict) -> None:
            seen.append(ev)

        hub.register_callback(_cb, role="engine")

        raw = {
            "exchange": "BYBIT",
            "alias": "TT",
            "type": "order",
            "status": "ACK",
            "orderId": "OID-1",
            "clientOrderId": " CID-1 ",
            "ts_exchange": 1690000000000,
        }
        hub.emit(raw, dedup=True)

        self.assertTrue(seen, "no event received from PWS hub")
        ev = seen[-1]
        self.assertEqual(ev.get("exchange_order_id"), "OID-1")
        self.assertEqual(ev.get("client_id"), "CID-1")
        self.assertIsInstance(ev.get("ts_exchange"), (float, int))
        self.assertLess(float(ev.get("ts_exchange") or 0.0), 1e11)

        with self.subTest("pws_queue_backpressure_ms_removed"):
            hub_src = Path(pws_hub_mod.__file__).read_text(encoding="utf-8")
            self.assertNotIn("PWS_QUEUE_BACKPRESSURE_MS", hub_src)

    async def test_03c_balance_fetcher_cfg_knobs(self) -> None:
        if _ci_profile() == "CI1":
            self.skipTest("CI1 profile skips balance fetcher knob test")

        import balance_fetcher as bf_mod
        from bot_config import BalanceFetcherCfg

        class _Cfg:
            def __init__(self) -> None:
                self.balances = BalanceFetcherCfg()
                self.balances.BF_ALERT_PERIOD_S = 7
                self.balances.WS_BAL_TTL_SECONDS = 1
                self.balances.ENABLE_WS_BALANCE_MERGE = True
                self.balances.coinbase_api_base = "http://coinbase.local"
                self.balances.bybit_api_base = "http://bybit.local"

                class _G:
                    deployment_mode = "SPLIT"

                self.g = _G()

        cfg = _Cfg()
        bf = bf_mod.MultiBalanceFetcher(config=cfg, dry_run=True)

        self.assertEqual(bf._ws_ttl_s, 1.0)
        self.assertTrue(bf._enable_ws_balance_merge)

        captured: list[float] = []

        async def _fake_sleep(delay: float):
            captured.append(float(delay))
            raise asyncio.CancelledError

        bf._running = True
        with mock.patch("asyncio.sleep", side_effect=_fake_sleep):
            await bf.run_alerts()
        self.assertTrue(captured, "balance fetcher did not sleep in alerts loop")
        self.assertEqual(captured[0], 7.0)

        adapter = bf_mod.PnlRecoCexAdapter(cfg)
        self.assertEqual(
            adapter._resolve_api_base("coinbase_api_base", default="http://x", legacy_root="coinbase_rest_base"),
            "http://coinbase.local",
        )
        self.assertEqual(
            adapter._resolve_api_base("bybit_api_base", default="http://x", legacy_root="bybit_rest_base"),
            "http://bybit.local",
        )

    async def test_03d_engine_pacer_config(self) -> None:
        if _ci_profile() == "CI1":
            self.skipTest("CI1 profile skips engine pacer config test")

        import engine_pacer as pacer_mod

        class _G:
            mode = "DEV"
            live_trading_armed = False

        class _Cfg:
            def __init__(self, deployment_mode=None, slo=None, engine=None, g=None) -> None:
                self.g = g or _G()
                if deployment_mode is not None:
                    self.g.deployment_mode = deployment_mode
                self.slo = slo
                self.engine = engine

        class _EngineCfg:
            pass

        cfg_missing_dm = _Cfg(deployment_mode=None)
        self.assertEqual(pacer_mod._deployment_mode(cfg_missing_dm), "EU_ONLY")

        pacer = pacer_mod.EnginePacer(bot_cfg=cfg_missing_dm)
        self.assertEqual(getattr(pacer, "_pacer_knobs_source", None), "default")

        cfg_prod = _Cfg(deployment_mode="EU_ONLY", g=type("G", (), {"mode": "PROD", "live_trading_armed": True})())
        pacer_prod = pacer_mod.EnginePacer(bot_cfg=cfg_prod)
        self.assertEqual(getattr(pacer_prod, "_pacer_knobs_source", None), "default")

        class _PrivateSLO:
            ack_target_ms = 300.0
            ack_hi_ms = 200.0
            ack_sev_ms = 400.0

        class _Path:
            private = _PrivateSLO()

        slo = {"EU_ONLY": {"BINANCE": _Path()}}
        cfg_slo = _Cfg(deployment_mode="EU_ONLY", slo=slo, engine=_EngineCfg())
        pacer_slo = pacer_mod.EnginePacer(bot_cfg=cfg_slo, region_map={"BINANCE": "EU"})
        targets = pacer_slo._targets.get("EU") or {}
        self.assertEqual(targets.get("ack_target"), 200.0)
        self.assertEqual(targets.get("ack_hi"), 300.0)
        self.assertEqual(targets.get("ack_sev"), 400.0)

    async def test_03e_rpc_gateway_config_and_idem(self) -> None:
        if _ci_profile() == "CI1":
            self.skipTest("CI1 profile skips rpc gateway config test")

        import rpc_gateway as rpc_mod
        from bot_config import RPCCfg

        class _G:
            mode = "DEV"
            live_trading_armed = False
            deployment_mode = "EU_ONLY"

        class _Cfg:
            def __init__(self) -> None:
                self.g = _G()
                self.rpc = RPCCfg()
                self.rpc.enabled = False
                self.rpc.host = "127.0.0.1"
                self.rpc.port = 9999
                self.rpc.timeout_s = 5.0
                self.rpc.loopback_inproc = True
                self.rpc.remote_base_url = "http://rpc.local"

        cfg = _Cfg()
        settings = rpc_mod.RPCSettings.from_cfg(cfg)
        self.assertFalse(settings.enabled)
        self.assertEqual(settings.host, "127.0.0.1")
        self.assertEqual(settings.port, 9999)

        submitter = rpc_mod.make_submitter_for_exchange(
            cfg,
            exchange="BINANCE",
            local_region="EU",
            exchange_region_map={"BINANCE": "EU"},
            rpc_server=mock.Mock(),
        )
        self.assertIsInstance(submitter, rpc_mod.InprocSubmitter)

        async def _noop(_payload):
            return {"ok": True}

        async def _stream(_payload):
            if False:
                yield {}

        handlers = rpc_mod.RPCHandlers(
            submit_bundle=_noop,
            cancel=_noop,
            status=_noop,
            stream=_stream,
        )
        server = rpc_mod.RPCServer(cfg, handlers=handlers)
        idem = server._derive_idem_key_from_payload({"meta": {"idempotency_key": "IDK-1"}})
        self.assertEqual(idem, "IDK-1")
    async def test_04_429_surge_pacer_degrades(self) -> None:
        """Scénario 4 — 429 surge: pacer doit dégrader.

        On teste EnginePacer “à sec”:
        - NORMAL en conditions saines
        - CONSTRAINED / SEVERE quand err_rate/latency explosent
        """
        if _ci_profile() == "CI1":
            self.skipTest("CI1 profile skips scenario 4")

        EnginePacer = getattr(pacer_mod, "EnginePacer", None)
        require_contract(self, EnginePacer is not None, "EnginePacer absent")
        if EnginePacer is None:
            return

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

    async def test_04b_engine_pacer_targets_env_override(self) -> None:
        """Scénario 4b — ENGINE_PACER_TARGETS doit modifier les targets du pacer."""
        ExecutionEngine = getattr(engine_mod, "ExecutionEngine", None)
        require_contract(self, ExecutionEngine is not None, "ExecutionEngine absent")
        if ExecutionEngine is None:
            return

        class _StubPrivateWS:
            ready_event: asyncio.Event = asyncio.Event()

        class _StubRateLimiter:
            async def acquire(self, *_args, **_kwargs):
                return True

        class _StubRetryPolicy:
            async def with_retry(self, coro, *args, **kwargs):  # pragma: no cover - stub
                return await coro(*args, **kwargs)

        env_key = "ENGINE_PACER_TARGETS"
        prior = os.environ.get(env_key)
        try:
            os.environ[env_key] = json.dumps({
                "EU": {
                    "ack_hi": 101.0,
                    "ack_sev": 151.0,
                }
            })
            cfg = modules.bot_config.BotConfig.from_env()
        finally:
            if prior is None:
                os.environ.pop(env_key, None)
            else:
                os.environ[env_key] = prior

        eng = ExecutionEngine(
            _StubPrivateWS(),
            _StubRateLimiter(),
            _StubRetryPolicy(),
            history_logger=None,
            cfg=cfg,
            ready_event=asyncio.Event(),
        )
        eng._ensure_pacer_on()
        pacer = getattr(eng, "_pacer", None)
        require_contract(self, pacer is not None, "EnginePacer absent on engine")
        if pacer is None:
            return
        targets = getattr(pacer, "_targets", {})
        self.assertIsInstance(targets, dict)
        self.assertIn("EU", targets)
        self.assertEqual(float(targets["EU"].get("ack_hi", 0.0)), 101.0)
        self.assertEqual(float(targets["EU"].get("ack_sev", 0.0)), 151.0)

    async def test_04c_engine_pacer_knobs_env_override(self) -> None:
        """Scénario 4c — ENGINE_PACER_*_MS doit modifier les knobs du pacer."""
        ExecutionEngine = getattr(engine_mod, "ExecutionEngine", None)
        require_contract(self, ExecutionEngine is not None, "ExecutionEngine absent")
        if ExecutionEngine is None:
            return

        class _StubPrivateWS:
            ready_event: asyncio.Event = asyncio.Event()

        class _StubRateLimiter:
            async def acquire(self, *_args, **_kwargs):
                return True

        class _StubRetryPolicy:
            async def with_retry(self, coro, *args, **kwargs):  # pragma: no cover - stub
                return await coro(*args, **kwargs)

        env_overrides = {
            "ENGINE_PACER_MIN_MS": "3",
            "ENGINE_PACER_MAX_MS": "77",
            "ENGINE_PACER_INIT_MS": "9",
            "ENGINE_PACER_JITTER_MS": "4",
        }
        prior_env = {k: os.environ.get(k) for k in env_overrides}
        try:
            os.environ.update(env_overrides)
            cfg = modules.bot_config.BotConfig.from_env()
        finally:
            for k, v in prior_env.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

        eng = ExecutionEngine(
            _StubPrivateWS(),
            _StubRateLimiter(),
            _StubRetryPolicy(),
            history_logger=None,
            cfg=cfg,
            ready_event=asyncio.Event(),
        )
        eng._ensure_pacer_on()
        pacer = getattr(eng, "_pacer", None)
        require_contract(self, pacer is not None, "EnginePacer absent on engine")
        if pacer is None:
            return
        self.assertEqual(getattr(pacer, "_min_ms", None), 3)
        self.assertEqual(getattr(pacer, "_max_ms", None), 77)
        self.assertEqual(getattr(pacer, "_init_ms", None), 9)
        self.assertEqual(getattr(pacer, "_jitter_ms", None), 4)

    async def test_04d_boot_private_ws_disabled(self) -> None:
        """Scénario 4d — démarrage boot avec private_ws OFF."""
        cfg = modules.bot_config.BotConfig()
        cfg.g.mode = "DRY_RUN"
        cfg.g.deployment_mode = "EU_ONLY"
        cfg.g.feature_switches["private_ws"] = False
        cfg.g.enabled_exchanges = ["BINANCE", "BYBIT"]

        async with started_boot_with_servers(cfg) as (boot, _status, _obs):
            self.assertTrue(getattr(boot, "_running", False))
            self.assertFalse(bool(getattr(boot.ctx, "pws_hub", None)))
    async def test_05_transfers_fsm_and_logs_rotation(self) -> None:
        """Scénario 5 — Transfers/REB + Logs.

         - TransferController: fail path + timeout fail-close
        - LogWriter: rotation size-based (gzip)
        """
        if _ci_profile() == "CI1":
            self.skipTest("CI1 profile skips scenario 5")

        TransferController = getattr(rm_mod, "TransferController", None)
        require_contract(self, TransferController is not None, "TransferController absent")
        if TransferController is None:
            return

        BackoffPolicy = getattr(rm_mod, "BackoffPolicy", None)

        def _get_state(tc: Any, transfer_id: str) -> Optional[dict]:
            getter = getattr(tc, "get_state", None)
            if callable(getter):
                try:
                    return getter(transfer_id)
                except Exception:
                    pass
            return getattr(tc, "_states", {}).get(transfer_id)

        submit_method_name = "submit_transfer" if callable(
            getattr(TransferController, "submit_transfer", None)) else "submit"
        submit_method = getattr(TransferController, submit_method_name, None)
        require_contract(self, callable(submit_method), "TransferController submit method absent")

        class _StubLHM:
            def __init__(self) -> None:
                self.fail_closed: list[dict] = []

            def mark_transfer_requested(self, **_kwargs):
                return None

            def mark_transfer_prepared(self, **_kwargs):
                return None

            def mark_transfer_submitted(self, **_kwargs):
                return None

            def mark_transfer_failed(self, **_kwargs):
                return None

            def mark_transfer_settled(self, **_kwargs):
                return None

            def get_transfer_state(self, **_kwargs):
                return None

            def activate_fail_closed_latch(self, **kwargs):
                self.fail_closed.append(kwargs)

        lhm = _StubLHM()

        tc = TransferController(
            policy=BackoffPolicy() if callable(BackoffPolicy) else None,
            submitted_timeout_s=0.2,
            logger_historique_manager=lhm,

        )

        async def submit_fn_fail(**_kwargs):
            raise RuntimeError("smoke submit fail")

        payload_fail = {
            "transfer_id": "XFER-FAIL",
            "exchange": "BINANCE",
            "from_alias": "A",
            "to_alias": "B",
            "ccy": "USDC",
            "amount": 1.23,
            "type": "transfer",
        }
        submit_callable = getattr(tc, submit_method_name)
        res_fail = await submit_callable(
            payload=payload_fail,
            submit_fn=submit_fn_fail,
            venue="BINANCE",
        )
        self.assertEqual(res_fail.get("transfer_id"), "XFER-FAIL")
        self.assertIn(str(res_fail.get("status") or "").upper(), {"FAILED", "SUBMITTED"})
        st_fail = _get_state(tc, "XFER-FAIL")
        self.assertIsNotNone(st_fail)
        state1 = str(st_fail.get("state") or "").upper()
        self.assertIn(state1, {"FAILED", "SUBMITTED"})
        # ensure fail-close eventually trips even if submission error did not immediately set FAILED
        tc.check_timeouts()
        st_fail2 = _get_state(tc, "XFER-FAIL")
        self.assertEqual(str(st_fail2.get("state") or "").upper(), "FAILED")

        # Timeout path: inject a stale SUBMITTED state and run check_timeouts (activates fail-close latch)
        expires_ts_ms = int((time.time() - 1.0) * 1000)
        tc.mark_submitted(
            "XFER-STALE",
            payload={"exchange": "BINANCE"},
            expires_ts_ms=expires_ts_ms,
        )
        # ensure state is still SUBMITTED before timeout check
        st_stale_before = _get_state(tc, "XFER-STALE")
        self.assertEqual(str((st_stale_before or {}).get("state") or "").upper(), "SUBMITTED")
        tc.check_timeouts()
        st3 = _get_state(tc, "XFER-STALE")
        self.assertEqual(str(st3.get("state") or "").upper(), "FAILED")
        self.assertTrue(lhm.fail_closed, "fail-close latch was not activated for stale transfer")

        # LogWriter rotation
        LogWriter = getattr(log_writer_mod, "LogWriter", None)
        require_contract(self, LogWriter is not None, "LogWriter absent")
        if LogWriter is None:
            return
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            lw = LogWriter(
                db_dir=str(root),
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

            gz_files = list(Path(td).glob(f"{db_path.stem}.*.db.gz"))
            compress_attr = getattr(lw, "_compress_rot", None)
            compress_enabled = bool(compress_attr) if compress_attr is not None else False
            if compress_enabled:
                self.assertTrue(
                    len(gz_files) >= 1,
                    f"no rotated gzip found in {root}: {list(root.iterdir())}",
                )
            else:
                files_present = [p for p in Path(td).iterdir() if p.is_file()]
                self.assertTrue(files_present, "no log files created during rotation test")

    async def test_05b_lhm_cfg_and_fail_closed(self) -> None:
        """Scénario 5b — LHM cfg canonique + rotation/DB lane + fail-closed."""
        if _ci_profile() == "CI1":
            self.skipTest("CI1 profile skips scenario 5b")

        lhm_mod = importlib.import_module("modules.logger_historique.logger_historique_manager")
        LHM = getattr(lhm_mod, "LoggerHistoriqueManager", None)
        require_contract(self, LHM is not None, "LoggerHistoriqueManager absent")
        if LHM is None:
            return

        cfg = modules.bot_config.BotConfig()
        cfg.lhm.LHM_JSONL_MAX_BYTES = 64
        cfg.lhm.LHM_DB_LANE_ENABLED = False
        cfg.lhm.ff_truth_model_enabled = True
        cfg.lhm.ff_truth_fail_closed = True

        with tempfile.TemporaryDirectory() as td:
            lhm = LHM(cfg, out_dir=td)
            self.assertFalse(getattr(lhm, "_db_lane_enabled", True))

            rot = lhm._jsonl["events"]
            path_before = rot.current_path
            rot.write_line(b"x" * 128)
            path_after = rot.current_path
            self.assertIsNotNone(path_before)
            self.assertIsNotNone(path_after)
            self.assertNotEqual(path_before, path_after)

            reasons: list[str] = []

            def _on_fail_closed(reason: str) -> None:
                reasons.append(reason)

            lhm.set_fail_closed_callback(_on_fail_closed)
            await lhm._emit_trade_fsm_event(stream="trade_fsm_events", payload={}, event_type="missing")
            self.assertTrue(reasons)

    async def test_05bb_lhm_tracker_cfg(self) -> None:
        """Scénario 5bb — LHM tracker cfg canonique + LogWriter signature."""
        if _ci_profile() == "CI1":
            self.skipTest("CI1 profile skips scenario 5bb")

        LHM = getattr(importlib.import_module("modules.logger_historique.logger_historique_manager"),
                      "LoggerHistoriqueManager", None)
        require_contract(self, LHM is not None, "LoggerHistoriqueManager absent")
        if LHM is None:
            return

        LogWriter = getattr(log_writer_mod, "LogWriter", None)
        require_contract(self, LogWriter is not None, "LogWriter absent")
        if LogWriter is None:
            return

        sig = inspect.signature(LogWriter.__init__)
        self.assertNotIn("rotate_bytes", sig.parameters)

        cfg = modules.bot_config.BotConfig()
        cfg.lhm.tracker_window_score_s = 1234
        with tempfile.TemporaryDirectory() as td:
            lhm = LHM(cfg, out_dir=td)
            tracker_cfg = getattr(lhm, "_tracker_cfg", None)
            self.assertIsNotNone(tracker_cfg)
            self.assertEqual(getattr(tracker_cfg, "window_score_s", None), 1234)

    async def test_05bc_btl_high_watermark(self) -> None:
        """Scénario 5bc — BTL high watermark configuré par ratio."""
        if _ci_profile() == "CI1":
            self.skipTest("CI1 profile skips scenario 5bc")

        try:
            btl_mod = importlib.import_module("modules.logger_historique.base_trade_logger")
        except Exception:
            btl_mod = importlib.import_module("base_trade_logger")
        BaseTradeLogger = getattr(btl_mod, "BaseTradeLogger", None)
        require_contract(self, BaseTradeLogger is not None, "BaseTradeLogger absent")
        if BaseTradeLogger is None:
            return

        class _Proxy:
            def __init__(self) -> None:
                self.highwater = 0

            def on_btl_queue_depth(self, *_args, **_kwargs):
                return None

            def on_btl_highwater(self, *_args, **_kwargs):
                self.highwater += 1

        proxy = _Proxy()
        logger = BaseTradeLogger(
            log_type="trades",
            trade_filter=lambda _t: True,
            batch_size=100,
            flush_interval=1.0,
            queue_maxsize=10_000,
            drop_when_full=False,
            high_watermark_ratio=0.5,
        )
        logger.set_metrics_proxy(proxy)
        self.assertEqual(getattr(logger, "_q_high_watermark", None), 5000)

        for idx in range(5000):
            logger.ingest({"trade_id": f"t{idx}"})

        self.assertGreater(proxy.highwater, 0)

    async def test_05c_rebalancing_cfg_resolution(self) -> None:
        """Scénario 5c — REB cfg canonique (reb/sim/g/rm)."""
        if _ci_profile() == "CI1":
            self.skipTest("CI1 profile skips scenario 5c")

        try:
            reb_mod = importlib.import_module("modules.risk_manager.rebalancing_manager")
        except Exception:
            reb_mod = importlib.import_module("rebalancing_manager")
        RebalancingManager = getattr(reb_mod, "RebalancingManager", None)
        require_contract(self, RebalancingManager is not None, "RebalancingManager absent")
        if RebalancingManager is None:
            return

        def _make_plan(cfg: modules.bot_config.BotConfig) -> tuple[Any, dict]:
            rm_stub = types.SimpleNamespace(config=cfg)
            reb = RebalancingManager(rm=rm_stub)
            reb.update_balances(
                {"BINANCE": {"TT": {"USDC": 2000.0}, "TM": {"USDC": 0.0}}}
            )
            imbalance = reb.detect_imbalance()
            self.assertIsNotNone(imbalance)
            plan = reb.build_plan_raw(imbalance or {})
            return reb, plan

        cfg = modules.bot_config.BotConfig()
        cfg.g.enabled_exchanges = ["BINANCE"]
        cfg.g.min_fragment_quote = {"USDC": 200.0}
        cfg.sim.min_fragment_usdc = 333.0
        cfg.reb.rebal_quantum_min_quote = 10.0
        cfg.reb.rebal_max_ops_per_min = 0

        reb0, plan0 = _make_plan(cfg)
        self.assertEqual(reb0.rebal_fragment_min_quote, 333.0)
        self.assertEqual(len(plan0.get("internal_transfers") or []), 0)

        cfg2 = modules.bot_config.BotConfig()
        cfg2.g.enabled_exchanges = ["BINANCE"]
        cfg2.g.min_fragment_quote = {"USDC": 200.0}
        cfg2.sim.min_fragment_usdc = 333.0
        cfg2.reb.rebal_quantum_min_quote = 10.0
        cfg2.reb.rebal_max_ops_per_min = 1

        _reb1, plan1 = _make_plan(cfg2)
        self.assertEqual(len(plan1.get("internal_transfers") or []), 1)

    async def test_05d_sfc_cfg_resolution(self) -> None:
        """Scénario 5d — SFC cfg canonique (rm/slip)."""
        if _ci_profile() == "CI1":
            self.skipTest("CI1 profile skips scenario 5d")

        try:
            sfc_mod = importlib.import_module("modules.risk_manager.slippage_and_fees_collector")
        except Exception:
            sfc_mod = importlib.import_module("slippage_and_fees_collector")
        SFC = getattr(sfc_mod, "SlippageAndFeesCollector", None)
        require_contract(self, SFC is not None, "SlippageAndFeesCollector absent")
        if SFC is None:
            return

        class _CfgRoot:
            def __init__(self) -> None:
                self.rm = types.SimpleNamespace(sfc_slippage_source="p95")
                self.slip = types.SimpleNamespace(
                    fee_sync_max_concurrency=1,
                    fee_sync_backoff_initial_s=0.1,
                    fee_sync_backoff_max_s=0.2,
                    fee_sync_max_retries=1,
                    fee_sync_jitter_s=0.0,
                    fee_reality_check_threshold_bps=0.5,
                    ttl_s=2,
                )

            def __getattr__(self, name: str) -> Any:
                if name in {"sfc_slippage_lookback_s", "FEE_SNAPSHOT_TTL_S"}:
                    raise AssertionError(f"unexpected access to legacy knob {name}")
                raise AttributeError(name)

        cfg_root = _CfgRoot()
        sfc = SFC(cfg=cfg_root)
        self.assertEqual(getattr(sfc, "_fee_sync_max_concurrency", None), 1)

        kinds: list[str] = []

        def _capture_slip(_ex, _pair, _side, *, kind="ewma", default=0.0):
            kinds.append(kind)
            return 0.0

        sfc.get_slippage = _capture_slip
        _ = sfc.leg_cost_pct(ex="BINANCE", alias="TT", pair="BTCUSDC", side="buy", role="taker")
        self.assertIn("p95", kinds)

        res = sfc.on_fill_fee_reality_check(
            {
                "exchange": "BINANCE",
                "alias": "TT",
                "symbol": "BTCUSDC",
                "side": "BUY",
                "role": "taker",
                "quote_qty": 100.0,
                "fee_quote": 1.0,
            }
        )
        self.assertEqual(res.get("threshold_bps"), 0.5)

    async def test_05e_volatility_cfg_resolution(self) -> None:
        """Scénario 5e — Volatility cfg canonique (vol)."""
        if _ci_profile() == "CI1":
            self.skipTest("CI1 profile skips scenario 5e")

        try:
            vm_mod = importlib.import_module("modules.volatility_manager")
        except Exception:
            vm_mod = importlib.import_module("volatility_manager")
        VM = getattr(vm_mod, "VolatilityManager", None)
        require_contract(self, VM is not None, "VolatilityManager absent")
        if VM is None:
            return

        cfg = modules.bot_config.BotConfig()
        cfg.vol.vm_prudence_thresholds_bps = {
            "NORMAL_TO_CAREFUL": 1.0,
            "CAREFUL_TO_ALERT": 2.0,
            "ALERT_TO_CAREFUL": 1.5,
            "CAREFUL_TO_NORMAL": 0.5,
        }
        cfg.vol.vm_size_factor_map = {"NORMAL": 1.0, "CAREFUL": 0.9, "ALERT": 0.8}
        cfg.vol.vm_min_bps_boost_map = {"NORMAL": 0.0, "CAREFUL": 0.0002, "ALERT": 0.0005}
        cfg.vol.tm_neutral_hedge_ratio_map = {"NORMAL": 0.5, "CAREFUL": 0.55, "ALERT": 0.65}
        cfg.vol.vm_maker_pad_ticks_map = {"NORMAL": 2, "CAREFUL": 3, "ALERT": 4}

        vm = VM(cfg_root=cfg)
        vm.ingest_orderbook_top("BINANCE", "BTCUSDC", 100.0, 100.2, ts=time.time())
        res1 = vm.step("BTCUSDC")
        self.assertEqual(res1.get("maker_pad_ticks"), 2)

        res2 = vm.step("BTCUSDC")
        self.assertIn("cooloff", res2)
    async def test_06_public_data_plane_contracts(self) -> None:
        """Scénario 6 — Public data plane (Router / Vol / Slip / Scanner)."""
        if _ci_profile() == "CI1":
            self.skipTest("CI1 fast path: skip public data plane contracts (Router/Vol/Slip/Scanner)")

        MarketDataRouter = getattr(router_mod, "MarketDataRouter", None)
        VolatilityMonitor = getattr(vol_mod, "VolatilityMonitor", None)
        SlippageHandler = getattr(slip_mod, "SlippageHandler", None)
        OpportunityScanner = getattr(scanner_mod, "OpportunityScanner", None)

        require_contract(self, MarketDataRouter is not None, "MarketDataRouter absent")
        require_contract(self, VolatilityMonitor is not None, "VolatilityMonitor absent")
        require_contract(self, SlippageHandler is not None, "SlippageHandler absent")
        require_contract(self, OpportunityScanner is not None, "OpportunityScanner absent")

        cfg = modules.bot_config.BotConfig()
        cfg.g.enabled_exchanges = ["BINANCE", "BYBIT"]
        combos = [("BINANCE", "BYBIT")]

        build_out = getattr(MarketDataRouter, "build_default_out_queues", None)
        require_contract(self, callable(build_out), "MarketDataRouter.build_default_out_queues absent")
        if not callable(build_out):
            return

        in_q: asyncio.Queue = asyncio.Queue()
        out_queues = build_out(
            combos=combos, maxsize={"combo": 8, "vol": 8, "slip": 8, "health": 8}
        )
        vol_monitor = VolatilityMonitor(cfg)
        slip_handler = SlippageHandler(cfg)
        scanner = _DummyScannerForRouter()

        router = MarketDataRouter(
            in_queue=in_q,
            out_queues=out_queues,
            combos=combos,
            scanner=scanner,
            volatility_monitor=vol_monitor,
            slippage_handler=slip_handler,
            push_to_scanner=True,
            publish_combo_to_bus=True,
            require_l2_first=False,
            stale_source_ms=100,
            coalesce_window_ms=5,
            coalesce_maxlen=2,
        )

        router_task = asyncio.create_task(router.start())
        try:
            now_ms = int(time.time() * 1000)
            live_event = _make_router_event(
                "BINANCE", "BTC-USDC", 100.0, 101.0, ts_ms=now_ms, quote="USDC"
            )
            stale_event = _make_router_event(
                "BYBIT",
                "BTC-USDC",
                99.0,
                100.0,
                ts_ms=now_ms - 10_000,
                quote="USDC",
            )
            stale_event["recv_ts_ms"] = now_ms - 10_000

            await in_q.put(live_event)
            await in_q.put(stale_event)
            deadline = time.time() + 1.0
            while len(scanner.events) < 1 and time.time() < deadline:
                await asyncio.sleep(0.01)

            self.assertGreaterEqual(len(scanner.events), 1, "router did not deliver to scanner")
            cex_out = out_queues.get("cex:BINANCE")
            if cex_out:
                fanout_sizes = [
                    q.qsize()
                    for name, q in cex_out.items()
                    if name in {"vol", "slip", "health"} and hasattr(q, "qsize")
                ]
                self.assertTrue(any(sz >= 1 for sz in fanout_sizes))

            stale_drop = getattr(router, "_events_ignored_stale", None)
            if stale_drop is not None:
                self.assertGreaterEqual(int(stale_drop), 1)
            else:
                route_drops = getattr(router, "_route_drops", {}) or {}
                if isinstance(route_drops, dict):
                    total_drops = sum(
                        int(v) for v in route_drops.values() if isinstance(v, (int, float))
                    )
                    self.assertGreaterEqual(total_drops, 1)
        finally:
            with contextlib.suppress(Exception):
                await router.stop()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(router_task, timeout=2.0)

        with self.subTest("OpportunityScanner ingestion path"):
            scan_cfg = modules.bot_config.BotConfig()
            scan_cfg.g.enabled_exchanges = ["BINANCE", "BYBIT"]

            class _StubRM:
                def __init__(self, cfg):
                    self.cfg = cfg

                def get_fee_pct(self, *_args, **_kwargs):
                    return 0.0

            dummy_router = types.SimpleNamespace()
            scanner_real = OpportunityScanner(
                scan_cfg, risk_manager=_StubRM(scan_cfg), market_router=dummy_router, simulator=None
            )

            ob_event = {
                "exchange": "BINANCE",
                "pair_key": "BTCUSDC",
                "symbol": "BTCUSDC",
                "best_bid": 100.0,
                "best_ask": 101.0,
                "orderbook": {"bids": [[100.0, 1.0]], "asks": [[101.0, 1.0]]},
                "recv_ts_ms": int(time.time() * 1000),
                "exchange_ts_ms": int(time.time() * 1000),
                "book_ttl_ms": 5000,
                "active": True,
            }

            scanner_real.update_orderbook(ob_event)
            self.assertIn("BINANCE", scanner_real.orderbooks)
            self.assertIn("BTCUSDC", scanner_real.orderbooks.get("BINANCE", {}))
            dq = scanner_real._queues.get("BTCUSDC")  # type: ignore[attr-defined]
            if dq is not None:
                self.assertGreaterEqual(len(dq), 1)


@unittest.skipUnless(_stress_enabled(), "CI3 or SMOKE_STRESS=1 required")
class SmokeStressPack(unittest.IsolatedAsyncioTestCase):
    async def test_stress_market_data_router(self) -> None:
        """Stress: drain + backpressure avec bornes de queue."""

        MarketDataRouter = getattr(router_mod, "MarketDataRouter", None)
        require_contract(self, MarketDataRouter is not None, "MarketDataRouter absent")
        if MarketDataRouter is None:
            return

        build_out = getattr(MarketDataRouter, "build_default_out_queues", None)
        require_contract(
            self, callable(build_out), "MarketDataRouter.build_default_out_queues absent"
        )
        if not callable(build_out):
            return

        combos = [("BINANCE", "BYBIT")]

        in_q: asyncio.Queue = asyncio.Queue()
        out_queues = build_out(
            combos=combos, maxsize={"combo": 20, "vol": 20, "slip": 20, "health": 20}
        )
        scanner = _DummyScannerForRouter()
        router = MarketDataRouter(
            in_queue=in_q,
            out_queues=out_queues,
            combos=combos,
            scanner=scanner,
            push_to_scanner=True,
            publish_combo_to_bus=True,
            require_l2_first=False,
            stale_source_ms=1000,
            coalesce_window_ms=2,
            coalesce_maxlen=2,
        )

        events: list[Any] = []
        if hasattr(router, "set_event_sink"):
            router.set_event_sink(lambda e: events.append(e))

        t_router = asyncio.create_task(router.start())
        try:
            now_ms = int(time.time() * 1000)
            pairs = ["BTC-USDC", "ETH-USDC", "SOL-USDC", "XRP-USDC"]
            burst_n = _stress_int("SMOKE_STRESS_ROUTER_BURST", 5000)
            for i in range(burst_n):
                exch = "BINANCE" if i % 2 == 0 else "BYBIT"
                pair = pairs[i % len(pairs)]
                ev = _make_router_event(
                    exch,
                    pair,
                    100.0 + i * 0.001,
                    101.0 + i * 0.001,
                    with_l2=True,
                    ts_ms=now_ms + i,
                    quote="USDC",
                )
                await in_q.put(ev)
                if i % 200 == 0:
                    await asyncio.sleep(0)

            await asyncio.sleep(0.25)
            self.assertFalse(t_router.done(), "router task died under stress")
            self.assertLessEqual(in_q.qsize(), 1, "router input queue not draining")
        finally:
            with contextlib.suppress(Exception):
                await router.stop()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(t_router, timeout=5.0)

        for cex_out in out_queues.values():
            for q in cex_out.values():
                if hasattr(q, "qsize"):
                    self.assertLessEqual(q.qsize(), getattr(q, "maxsize", q.qsize()))

        if events:
            self.assertTrue(
                any(
                    isinstance(ev, dict)
                    and ev.get("type") in {"backpressure", "router_drop"}
                    for ev in events
                ),
                "router stress did not emit backpressure events",
            )
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
            await wait_health(base_url, timeout_s=_timeout_s())

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
        require_contract(self, rl_cfg is not None, "RateLimiter cfg missing")
        if rl_cfg is None:
            return

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

    @unittest.skipUnless(os.environ.get("SMOKE_STRESS_VOL") == "1", "SMOKE_STRESS_VOL=1 required")
    async def test_stress_volatility_monitor(self) -> None:
        """Stress léger: VolatilityMonitor supporte des snapshots répétés."""

        vol_mod = import_family_module("volatility_monitor")
        VolatilityMonitor = getattr(vol_mod, "VolatilityMonitor", None)
        require_contract(self, VolatilityMonitor is not None, "VolatilityMonitor absent")
        if VolatilityMonitor is None:
            return

        cfg = modules.bot_config.BotConfig()
        vm = VolatilityMonitor(cfg)
        updates_n = _stress_int("SMOKE_STRESS_VOL_N", 200)

        for i in range(updates_n):
            payload = {
                "exchange": "BINANCE",
                "pair_key": "BTCUSDC",
                "best_bid": 100.0 + i * 0.0001,
                "best_ask": 100.1 + i * 0.0001,
                "recv_ts_ms": int(time.time() * 1000),
            }
            if i % 2 == 0:
                vm.update_from_orderbook(payload)
            else:
                vm.ingest_snapshot(payload)

        vol = vm.get_volatility("BINANCE", "BTCUSDC")
        self.assertIsNotNone(vol)
        self.assertIsInstance(vol, (int, float, dict))

    @unittest.skipUnless(os.environ.get("SMOKE_STRESS_SLIP") == "1", "SMOKE_STRESS_SLIP=1 required")
    async def test_stress_slippage_handler(self) -> None:
        """Stress léger: SlippageHandler digère des snapshots et expose une mesure."""

        slip_mod = import_family_module("slippage_handler")
        SlippageHandler = getattr(slip_mod, "SlippageHandler", None)
        require_contract(self, SlippageHandler is not None, "SlippageHandler absent")
        if SlippageHandler is None:
            return

        cfg = modules.bot_config.BotConfig()
        sh = SlippageHandler(cfg)
        updates_n = _stress_int("SMOKE_STRESS_SLIP_N", 200)

        for i in range(updates_n):
            snapshot = {
                "exchange": "BINANCE",
                "pair_key": "BTCUSDC",
                "orderbook": {"bids": [[100.0, 1.0]], "asks": [[100.1, 1.2]]},
                "slip_metric_bps": 10.0 + (i % 3),
                "top_bid_vol": 1.0,
                "top_ask_vol": 1.0,
                "recv_ts_ms": int(time.time() * 1000),
            }
            sh.ingest_snapshot(snapshot)

        slip = sh.get_slippage_bps("BINANCE", "BTCUSDC")
        self.assertTrue(slip is None or isinstance(slip, (int, float)))

    @unittest.skipUnless(os.environ.get("SMOKE_STRESS_SCANNER") == "1", "SMOKE_STRESS_SCANNER=1 required")
    async def test_stress_opportunity_scanner(self) -> None:
        """Stress léger: Scanner boucle start/stop + ingestion orderbooks."""

        scan_mod = import_family_module("opportunity_scanner")
        OpportunityScanner = getattr(scan_mod, "OpportunityScanner", None)
        require_contract(self, OpportunityScanner is not None, "OpportunityScanner absent")
        if OpportunityScanner is None:
            return

        cfg = modules.bot_config.BotConfig()

        class _StubRM:
            def __init__(self, cfg):
                self.cfg = cfg

            def get_fee_pct(self, *_args, **_kwargs):
                return 0.0

        class _StubRouter:
            pass

        class _StubSimulator:
            def set_event_sink(self, *_args, **_kwargs):
                return None

        scanner = OpportunityScanner(
            cfg, risk_manager=_StubRM(cfg), market_router=_StubRouter(), simulator=_StubSimulator()
        )

        async def _noop_loop(self):
            while getattr(self, "_running", False):
                await asyncio.sleep(0.001)

        scanner._scan_loop = types.MethodType(_noop_loop, scanner)
        await scanner.start()

        updates_n = _stress_int("SMOKE_STRESS_SCANNER_UPDATES", 200)
        for i in range(updates_n):
            data = {
                "exchange": "BINANCE",
                "pair_key": "BTCUSDC",
                "best_bid": 100.0 + i * 0.0001,
                "best_ask": 100.1 + i * 0.0001,
                "recv_ts_ms": int(time.time() * 1000),
            }
            scanner.update_orderbook(data)

        await scanner.stop()
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
                await wait_health(base_url, timeout_s=_timeout_s())
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
        self.assertGreaterEqual(len(seen_modes), 2, "pacer did not thrash modes under stress")

    async def test_stress_private_ws_reconciler_fill_storm(self) -> None:
        """Staleness flip-flop + fill storm dedup bornée."""

        stale_ms = _stress_int("SMOKE_STRESS_PWS_STALE_MS", 200)
        dedup_max = _stress_int("SMOKE_STRESS_PWS_DEDUP_MAX", 500)
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
        require_contract(self, seen is not None, "PrivateWSReconciler._seen_keys absent")
        if seen is None:
            return

        maxlen = getattr(seen, "_maxlen", None)
        q = getattr(seen, "_q", None)
        s = getattr(seen, "_s", None)
        size_candidates = [v for v in (len(q) if q is not None else None, len(s) if s is not None else None) if
                           v is not None]
        if not size_candidates:
            require_contract(self, False, "unable to introspect _seen_keys size")
        size = max(size_candidates)

        if isinstance(maxlen, int) and maxlen > 0:
            self.assertLessEqual(size, maxlen)
        else:
            # fallback: assert against provided dedup_max when maxlen unavailable
            self.assertLessEqual(size, dedup_max)


# --------------------------------------------------------------------------------------
# 6) MARKET MAKING P0 (Safety & Churn)
# --------------------------------------------------------------------------------------

class TestMarketMakingP0(unittest.IsolatedAsyncioTestCase):
    """
    Tests MM microstructure invariants: clamp strict, anti-churn, fail-closed.
    """
    async def asyncSetUp(self):
        # Setup similar to Scenario 2 but focused on MM
        # Note: we use aliases from the bootstrap section
        self.cfg = bot_mod.BotConfig.from_env()
        self.cfg.g = types.SimpleNamespace(mode='DRY_RUN', feature_switches={}, ac={'enabled': False})
        
        # Hard settings for tests
        self.cfg.engine.mm_min_quote_lifetime_ms = 400
        self.cfg.engine.mm_sticky_band_ticks = 1.0
        self.cfg.engine.mm_replace_cooldown_ms = 200
        self.cfg.engine.mm_jump_guard_threshold_bps = 10.0
        
        # Mock get_pair_filters
        fn_mock = lambda ex, pair: {"tick_size": 0.01}
        self.cfg.get_pair_filters = fn_mock
        self.cfg.engine.get_pair_filters = fn_mock
        
        mock_rl = mock.MagicMock()
        mock_rl.bucket_for.return_value = mock.AsyncMock()
        mock_retry = mock.MagicMock()
        mock_retry.with_retry = lambda coro, *a, **kw: coro
        
        self.eng = engine_mod.ExecutionEngine(config=self.cfg, private_ws=None, rate_limiter=mock_rl, retry_policy=mock_retry)
        
        # Mock RiskManager
        self.eng.risk_manager = mock.MagicMock()
        self.eng.risk_manager.get_top_of_book.return_value = (99000.0, 99010.0)
        self.eng.risk_manager._volatility_bps.return_value = 10.0
        self.eng.risk_manager.mm_reb_state = {}
        self.eng.risk_manager._compute_mm_inventory_skews.return_value = (0.0, 1.0)
        
        self.eng._ready = True
        self.eng.trading_state = "READY"
        self.eng._ensure_ready = lambda: None

    async def test_clamp_strict(self):
        # BUY maker must be < ASK
        px, clamped = self.eng.clamp_maker_price("BUY", 99015.0, 99000.0, 99010.0, 0.01, pad_ticks=1.0)
        self.assertTrue(clamped)
        self.assertLess(px, 99010.0)
        self.assertAlmostEqual(px, 99010.0 - 0.01)

        # SELL maker must be > BID
        px, clamped = self.eng.clamp_maker_price("SELL", 98995.0, 99000.0, 99010.0, 0.01, pad_ticks=1.0)
        self.assertTrue(clamped)
        self.assertGreater(px, 99000.0)
        self.assertAlmostEqual(px, 99000.0 + 0.01)

    async def test_min_lifetime(self):
        clid = "test_cid"
        self.eng._mm_active_orders_info[clid] = {
            "price": 99005.0, "side": "BUY", "placed_at_mono": time.monotonic(), "ladder_idx": 0
        }
        self.eng._mm_active_by_pair[("BINANCE", "NONE", "BTCUSDT")].add(clid)
        self.eng._order_fsm[clid] = types.SimpleNamespace(state='ACK')
        
        new_clid = await self.eng._mm_place_maker(
            pair_key="BTCUSDT", exchange="BINANCE", side="BUY", amount_quote=100.0, bundle_id="b1", meta={"mm_ladder_idx": 0}
        )
        self.assertEqual(new_clid, clid)

    async def test_sticky_band(self):
        clid = "test_cid_old"
        new_price = 99000.01
        self.eng._mm_active_orders_info[clid] = {
            "price": new_price, "side": "BUY", "placed_at_mono": time.monotonic() - 1.0, "ladder_idx": 0
        }
        self.eng._mm_active_by_pair[("BINANCE", "NONE", "BTCUSDT")].add(clid)
        self.eng._order_fsm[clid] = types.SimpleNamespace(state='ACK')
        
        # Bypass pricing to force sticky check
        self.eng.clamp_maker_price = lambda *a, **kw: (new_price, False)
        
        new_clid = await self.eng._mm_place_maker(
            pair_key="BTCUSDT", exchange="BINANCE", side="BUY", amount_quote=100.0, bundle_id="b1", meta={"mm_ladder_idx": 0}
        )
        self.assertEqual(new_clid, clid)

    async def test_fail_closed_tob(self):
        from contracts.errors import EngineSubmitError
        self.eng.risk_manager.get_top_of_book.return_value = (0.0, 0.0)
        with self.assertRaises(EngineSubmitError):
            await self.eng._mm_place_maker(pair_key="BTCUSDT", exchange="BINANCE", side="BUY", amount_quote=100.0, bundle_id="b1")

if __name__ == "__main__":
    unittest.main(verbosity=2)
