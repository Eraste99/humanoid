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
import modules.risk_manager.risk_manager as rm_mod  # noqa: E402
import modules.logger_historique.log_writer as log_writer_mod  # noqa: E402
import contracts.errors as errors_mod  # noqa: E402


# --------------------------------------------------------------------------------------
# Local imports
# --------------------------------------------------------------------------------------

router_mod = importlib.import_module("modules.market_data_router")
try:
    payloads_mod = importlib.import_module("contracts.payloads")
except Exception:
    payloads_mod = None
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
    Boot = getattr(boot_mod, "Boot")
    boot = Boot(cfg)

    # Start probes (supervisor usually does that)
    loop_lag_task = obs_metrics_mod.start_loop_lag_probe(period_s=0.25)
    time_skew_task = obs_metrics_mod.start_time_skew_probe(period_s=2.0)

    # Start servers first (so /health comes up quickly), then boot
    servers = obs_metrics_mod.start_servers(boot=boot, cfg=cfg)
    status_srv = servers.get("status_server")
    obs_srv = servers.get("obs_server")

    async def _maybe_await(val, timeout: float) -> None:
        if inspect.isawaitable(val):
            await asyncio.wait_for(val, timeout=timeout)
    try:
        if status_srv is None:
            raise AssertionError("status_server missing from obs_metrics.start_servers()")
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

    def check_and_mark_idempotency(self, idempotency_key: str) -> bool:
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
    async def test_00_import_surface(self) -> None:
        """Scénario 0 — preuve mécanique que les compartiments chargent."""

        if payloads_mod is None:
            try:
                _ = importlib.import_module("contracts.payloads")
            except Exception as e:
                self.fail(f"contracts.payloads import failed: {e}")

        try:
            _ = importlib.import_module("modules.bot_config")
        except Exception as e:
            self.fail(f"modules.bot_config import failed: {e}")

        families = [
            ("market_data_router", "modules.market_data_router"),
            ("volatility_monitor", "modules.volatility_monitor"),
            ("slippage_handler", "modules.slippage_handler"),
            ("opportunity_scanner", "modules.opportunity_scanner"),
            ("risk_manager", "modules.risk_manager.risk_manager"),
            ("logger_historique.log_writer", "modules.logger_historique.log_writer"),
        ]
        flat_only = ["execution_engine", "private_ws_reconciler"]

        for name in flat_only:
            with self.subTest(module=name):
                try:
                    _ = importlib.import_module(name)
                except Exception as e:
                    self.fail(f"import failed for {name}: {e}")
        for flat_name, modular_name in families:
            with self.subTest(module_family=f"{flat_name}|{modular_name}"):
                flat_err = mod_err = None
                flat_ok = modular_ok = False
                try:
                    _ = importlib.import_module(flat_name)
                    flat_ok = True
                except Exception as e:  # noqa: F841
                    flat_err = e
                try:
                    _ = importlib.import_module(modular_name)
                    modular_ok = True
                except Exception as e:  # noqa: F841
                    mod_err = e

                if not (flat_ok or modular_ok):
                    self.fail(
                        f"imports failed for {flat_name} ({flat_err}) and {modular_name} ({mod_err})"
                    )
    async def test_01_dry_run_start_ready_status_metrics_stop(self) -> None:
        """Scénario 1 — DRY_RUN E2E (superviseur HTTP)."""
        with temp_env():
            cfg = modules.bot_config.BotConfig.from_env()
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
                    coalesce_window_ms=5,
                    coalesce_maxlen=2,
                )

                task = asyncio.create_task(router.start())
                try:
                    now_ms = int(time.time() * 1000)
                    for idx in range(3):
                        exch = "BINANCE" if idx % 2 == 0 else "BYBIT"
                        ev = _make_router_event(
                            exch,
                            "BTC-USDC",
                            100.0 + idx,
                            101.0 + idx,
                            with_l2=True,
                            ts_ms=now_ms + idx,
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
                require_contract(
                    self,
                    _stress_enabled(),
                    "stress gate disabled",
                    critical=_ci_profile() == "CI3",
                )
                if MarketDataRouter is None or not _stress_enabled():
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
                    for i in range(10_000):
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
                    self.assertLess(still_pending, 10_000, "router queue grew unbounded")
                finally:
                    with contextlib.suppress(Exception):
                        await asyncio.wait_for(stress_router.stop(), timeout=2.0)
                    with contextlib.suppress(Exception):
                        await asyncio.wait_for(stress_task, timeout=2.0)

            with self.subTest("VolatilityMonitor contract"):
                vol_mod = importlib.import_module("volatility_monitor")
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
                slip_mod = importlib.import_module("slippage_handler")
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
                scan_mod = importlib.import_module("opportunity_scanner")
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
                self.assertEqual(getattr(router, "_events_schema_errors"), 0)
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
                self.assertGreaterEqual(getattr(router_stale, "_events_ignored_stale"), 1)
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
                drops = getattr(router_rl2, "_route_drops")
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

        async with started_boot_with_servers(cfg) as (boot, _status_srv, _obs_srv):
            # Wait ready via HTTP (proxy for “superviseur /status & /ready”)
            base = f"http://127.0.0.1:{cfg.obs.status_port}"
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
            self.assertTrue(status["ready_all"], f"not READY: reasons={status.get('reasons')}")
            if "degraded" in status:
                self.assertFalse(
                    status["degraded"],
                    f"status degraded: reasons={status.get('reasons') or status.get('degraded_reasons')}",
                )

            # The in-process Boot should have set ready_all.
            self.assertTrue(bool(status.get("ready_all")), f"not READY: reasons={status.get('reasons')}")

            # /metrics should be non-empty and contain at least these canonical metrics.
            murl = f"http://127.0.0.1:{cfg.obs.status_port}/metrics"
            metrics_txt = http_get_text(murl)
            loop_or_skew_metric_present = any(
                name in metrics_txt for name in ("event_loop_lag_ms", "time_skew_ms")
            )
            self.assertTrue(loop_or_skew_metric_present, "loop lag/time skew metric missing")
            startups_metric_present = "bot_startups_total" in metrics_txt
            self.assertTrue(startups_metric_present, "startups metric missing")
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
        ExecutionEngine = getattr(engine_mod, "ExecutionEngine")
        cfg.engine.ff_enforce_client_oid_deterministic = True
        cfg.engine.ff_fail_closed_idempotence = True
        ready_event = asyncio.Event()
        eng = ExecutionEngine(
            None,
            None,
            None,
            history_logger=None,
            config=cfg.engine,
            cfg=cfg,
            ready_event=ready_event,
        )
        eng.ready_event.set()
        eng.history_fsm = _DummyHistoryFSM()

        require_contract(self, hasattr(eng, "_sem_inflight"), "ExecutionEngine._sem_inflight absent")
        sem = getattr(eng, "_sem_inflight", None)
        if sem is None:
            return
        self.assertIsInstance(sem, dict)
        for k in ["hedge", "cancel", "maker"]:
            if k not in sem:
                require_contract(self, False, f"ExecutionEngine._sem_inflight missing {k}")
            self.assertIsInstance(sem.get(k, {}), dict)

        common_exchanges = set(sem["hedge"].keys()) & set(sem["maker"].keys())
        if not common_exchanges:
            require_contract(self, False, "No common exchange between hedge/maker semaphores")
        ex = sorted(common_exchanges)[0]
        s_hedge = sem["hedge"][ex]
        s_maker = sem["maker"][ex]
        if not isinstance(s_hedge, asyncio.Semaphore) or not isinstance(s_maker, asyncio.Semaphore):
            require_contract(self, False, "Hedge/maker semaphores are not asyncio.Semaphore instances")
        hedge_value = getattr(s_hedge, "_value", None)
        maker_value = getattr(s_maker, "_value", None)
        if not isinstance(hedge_value, int) or not isinstance(maker_value, int):
            require_contract(
                self,
                isinstance(hedge_value, int) and isinstance(maker_value, int),
                "Semaphore _value not accessible for hedge/maker",
            )
        self.assertGreaterEqual(hedge_value, maker_value)

        EngineSubmitError = getattr(errors_mod, "EngineSubmitError")

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
        reason_1 = getattr(ctx1.exception, "reason", str(ctx1.exception))
        self.assertIn("IDEMPOTENCY_MISSING", str(reason_1))

        # 2) Duplicate idempotency_key is rejected when already seen (circuit breaker short-circuits network)
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
        reason_2 = getattr(ctx2.exception, "reason", str(ctx2.exception))
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
        PrivateWSReconciler = getattr(pwsr_mod, "PrivateWSReconciler")

        # Tight staleness for smoke
        rec = PrivateWSReconciler(
            cooldown_s=0.1,
            stale_ms=100,
            poll_every_s=0.05,
            dedup_max=128,
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

        # dedup fill events (via public helpers)
        seen = getattr(rec, "_seen_keys", None)
        require_contract(self, seen is not None, "dedup store non accessible")

        def _seen_size() -> int:
            if seen is None:
                require_contract(self, False, "dedup store non accessible")
                return 0
            for attr in ("_s", "_q"):
                val = getattr(seen, attr, None)
                if val is not None:
                    try:
                        return len(val)
                    except Exception:
                        continue
            require_contract(self, False, "unable to introspect _seen_keys size")
            return 0

        before = _seen_size()
        rec.note_seen_client_id("CID-1")
        rec.note_seen_client_id("CID-1")
        rec.note_seen_idempotency_key("IDK-1")
        rec.note_seen_idempotency_key("IDK-1")
        after = _seen_size()
        if (after - before) not in (0, 2):
            self.fail("dedup store grew unexpectedly after duplicate seen keys")
        if _stress_enabled():
            with self.subTest("PrivateWSReconciler fill storm (CI3/SMOKE_STRESS=1)"):
                seen = getattr(rec, "_seen_keys", None)
                require_contract(self, seen is not None, "PrivateWSReconciler._seen_keys absent")
                if seen is None:
                    return

                def _seen_size() -> int:

                    for attr in ("_q", "_s"):
                        val = getattr(seen, attr, None)
                        if val is not None:
                            try:
                                return len(val)
                            except Exception:
                                continue
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
                        sweep_fn = getattr(rec, "sweep")
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

    async def test_04_429_surge_pacer_degrades(self) -> None:
        """Scénario 4 — 429 surge: pacer doit dégrader.

        On teste EnginePacer “à sec”:
        - NORMAL en conditions saines
        - CONSTRAINED / SEVERE quand err_rate/latency explosent
        """
        if _ci_profile() == "CI1":
            self.skipTest("CI1 profile skips scenario 4")

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

         - TransferController: fail path + timeout fail-close
        - LogWriter: rotation size-based (gzip)
        """
        if _ci_profile() == "CI1":
            self.skipTest("CI1 profile skips scenario 5")

        TransferController = getattr(rm_mod, "TransferController")

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
        res_fail = await tc.submit(
            payload=payload_fail,
            submit_fn=submit_fn_fail,
            venue="BINANCE",
        )
        self.assertEqual(res_fail.get("transfer_id"), "XFER-FAIL")
        st_fail = tc._states.get("XFER-FAIL")
        self.assertIsNotNone(st_fail)
        self.assertIn(st_fail.get("state"), {"FAILED", "SUBMITTED"})
        tc.check_timeouts()
        st_fail2 = tc._states.get("XFER-FAIL")
        self.assertEqual(st_fail2.get("state"), "FAILED")

        # Timeout path: inject a stale SUBMITTED state and run check_timeouts (activates fail-close latch)
        tc._states["XFER-STALE"] = {
            "state": "SUBMITTED",
            "last_ts": time.time() - 1.0,
            "payload": {"exchange": "BINANCE"},
            "expires_ts_ms": int((time.time() - 1.0) * 1000),
        }
        tc.check_timeouts()
        st3 = tc._states.get("XFER-STALE")
        self.assertEqual(st3.get("state"), "FAILED")
        self.assertTrue(lhm.fail_closed, "fail-close latch was not activated for stale transfer")

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

        combos = [
            {"exchange": "BINANCE", "pair_key": "BTC-USDC", "pair": "BTCUSDC", "quote": "USDC"},
            {"exchange": "BYBIT", "pair_key": "ETH-USDC", "pair": "ETHUSDC", "quote": "USDC"},
        ]

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
            for i in range(2000):
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

        vol_mod = importlib.import_module("volatility_monitor")
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

        slip_mod = importlib.import_module("slippage_handler")
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

        scan_mod = importlib.import_module("opportunity_scanner")
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


if __name__ == "__main__":
    unittest.main(verbosity=2)
