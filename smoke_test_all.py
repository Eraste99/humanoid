#!/usr/bin/env python3
# smoke_test_all.py
from __future__ import annotations

import argparse
import asyncio
import contextlib
import dataclasses
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple
from urllib.request import urlopen, Request

# ------------------------- Imports projet (avec fallback) -------------------------

def _import_any(*candidates: str):
    last = None
    for name in candidates:
        try:
            module = __import__(name, fromlist=["*"])
            return module
        except Exception as e:
            last = e
    raise RuntimeError(f"Cannot import any of: {candidates}. Last error: {last}")

boot_mod = _import_any("modules.boot", "boot")
cfg_mod = _import_any("modules.bot_config", "bot_config")
obs_mod = _import_any("modules.obs_metrics", "obs_metrics")
payloads_mod = _import_any("contracts.payloads", "payloads")
errors_mod = _import_any("contracts.errors", "errors")

Boot = getattr(boot_mod, "Boot")
BotConfig = getattr(cfg_mod, "BotConfig")

EngineSubmitError = getattr(errors_mod, "EngineSubmitError", Exception)

# ------------------------- Utilitaires -------------------------

ART_DIR = Path("artifacts/smoke")
ART_DIR.mkdir(parents=True, exist_ok=True)

def now_ms() -> int:
    return int(time.time() * 1000)

@contextlib.contextmanager
def env_patch(overrides: Dict[str, str]):
    old = dict(os.environ)
    os.environ.update({k: str(v) for k, v in overrides.items() if v is not None})
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(old)

def http_get_text(url: str, timeout_s: float = 2.0) -> Tuple[int, str]:
    req = Request(url, headers={"User-Agent": "smoke_test_all/1.0"})
    with urlopen(req, timeout=timeout_s) as resp:
        body = resp.read().decode("utf-8", errors="replace")
        return int(resp.status), body

def http_get_json(url: str, timeout_s: float = 2.0) -> Tuple[int, Any]:
    code, txt = http_get_text(url, timeout_s=timeout_s)
    try:
        return code, json.loads(txt)
    except Exception:
        return code, {"_raw": txt}

async def wait_ready_http(host: str, port: int, timeout_s: float = 30.0) -> None:
    deadline = time.time() + timeout_s
    url = f"http://{host}:{port}/ready"
    last = None
    while time.time() < deadline:
        try:
            code, _ = http_get_text(url, timeout_s=1.5)
            if code == 200:
                return
            last = f"HTTP {code}"
        except Exception as e:
            last = repr(e)
        await asyncio.sleep(0.25)
    raise TimeoutError(f"/ready not OK within {timeout_s}s (last={last})")

def must_contain(hay: str, needle: str, label: str):
    if needle not in hay:
        raise AssertionError(f"{label}: missing '{needle}'")

def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True), encoding="utf-8")

# ------------------------- Harness Bot runner -------------------------

@dataclass
class RunningBot:
    cfg: Any
    boot: Any
    status_host: str
    status_port: int
    obs_port: Optional[int]
    status_server: Any | None
    obs_server: Any | None

async def start_bot(cfg: Any) -> RunningBot:
    boot = Boot(cfg)

    # Probes “ossature”
    try:
        obs_mod.start_loop_lag_probe()
    except Exception:
        pass
    try:
        obs_mod.start_time_skew_probe()
    except Exception:
        pass

    servers = {}
    try:
        servers = obs_mod.start_servers(boot=boot, cfg=cfg)
    except Exception:
        # On tolère si tes serveurs sont démarrés ailleurs (ex: bot_arbitrage.py),
        # mais le smoke vise justement à les prouver.
        servers = {"status": None, "obs": None}

    status_server = servers.get("status")
    obs_server = servers.get("obs")

    # Démarrage HTTP
    if obs_server is not None:
        await obs_server.start()
    if status_server is not None:
        await status_server.start()

    # Boot.start exige snapshot_hash + snapshot_dict (dans ton Boot actuel)
    snapshot_hash = None
    snapshot_dict = None
    try:
        snapshot_hash = cfg.snapshot_hash()
        snapshot_dict = cfg.snapshot_dict()
    except Exception:
        # fallback si ton BotConfig expose déjà cfg.snapshot_hash / cfg.snapshot_dict
        snapshot_hash = getattr(cfg, "snapshot_hash", None)
        snapshot_dict = getattr(cfg, "snapshot_dict", None)

    await boot.start(snapshot_hash=snapshot_hash, snapshot_dict=snapshot_dict)

    # Ports (issus cfg.obs)
    obs_cfg = getattr(cfg, "obs", None)
    host = getattr(obs_cfg, "status_host", "127.0.0.1") if obs_cfg else "127.0.0.1"
    s_port = int(getattr(obs_cfg, "status_port", 9110) if obs_cfg else 9110)
    obs_port = None
    if obs_cfg and getattr(obs_cfg, "obs_enable_9108", True):
        obs_port = int(getattr(obs_cfg, "obs_port", 9108))

    return RunningBot(
        cfg=cfg,
        boot=boot,
        status_host=host,
        status_port=s_port,
        obs_port=obs_port,
        status_server=status_server,
        obs_server=obs_server,
    )

async def stop_bot(rb: RunningBot) -> None:
    # Stop Boot
    try:
        await rb.boot.stop()
    except Exception:
        pass

    # Stop HTTP
    try:
        if rb.status_server is not None:
            await rb.status_server.stop()
    except Exception:
        pass
    try:
        if rb.obs_server is not None:
            await rb.obs_server.stop()
    except Exception:
        pass

# ------------------------- Scénarios -------------------------

@dataclass
class Scenario:
    name: str
    env: Dict[str, str]
    fn: Callable[[RunningBot], "asyncio.Future[None]"]

BASE_ENV = {
    # RC base
    "DEPLOYMENT_MODE": "EU_ONLY",
    "CAPITAL_PROFILE": "NANO",
    "MODE": "DRY_RUN",
    "PAIRS": "BTC-USDC,ETH-USDC",
    "ENABLED_EXCHANGES": "BINANCE,BYBIT",

    # HTTP status/metrics
    "STATUS_PORT": "9110",
    "OBS_ENABLE_9108": "1",
    "OBS_PORT": "9108",
    "EXPOSE_METRICS_ON_9110": "1",
}

MUST_HAVE_METRICS = [
    "event_loop_lag_ms",
    "time_skew_ms",
    "bot_startups_total",  # si présent dans ton obs_metrics
    "deployment_mode_info",
]

async def assert_http_surfaces(rb: RunningBot) -> None:
    await wait_ready_http(rb.status_host, rb.status_port, timeout_s=35.0)

    code, st = http_get_json(f"http://{rb.status_host}:{rb.status_port}/status", timeout_s=2.0)
    if code != 200:
        raise AssertionError(f"/status not 200: {code} body={st}")

    # /metrics peut être sur 9110 si EXPOSE_METRICS_ON_9110=1
    code, metrics = http_get_text(f"http://{rb.status_host}:{rb.status_port}/metrics", timeout_s=2.5)
    if code != 200:
        raise AssertionError(f"/metrics not 200 on status port: {code}")

    for m in MUST_HAVE_METRICS:
        must_contain(metrics, m, label="metrics surface")

async def scenario_1_dry_run(rb: RunningBot) -> None:
    await assert_http_surfaces(rb)

    # Dump utile
    code, st = http_get_json(f"http://{rb.status_host}:{rb.status_port}/status", timeout_s=2.0)
    write_json(ART_DIR / f"{rb.cfg.snapshot_hash() if hasattr(rb.cfg,'snapshot_hash') else 'cfg'}_status_dryrun.json", st)

async def scenario_2_micro_orders_dry(rb: RunningBot) -> None:
    await assert_http_surfaces(rb)

    engine = getattr(getattr(rb.boot, "ctx", None), "engine", None)
    if engine is None:
        raise AssertionError("Engine missing (boot.ctx.engine is None)")

    # 2a) submit bundle “valide” (DRY) + idempotence
    bundle_id = f"smoke-{now_ms()}"
    idem = f"smoke-idem-{bundle_id}"

    legs = [
        {"exchange": "BINANCE", "account_alias": "TT", "symbol": "BTC-USDC", "side": "BUY", "qty": 0.0001, "price": 50000.0, "type": "LIMIT", "tif": "IOC"},
        {"exchange": "BYBIT",   "account_alias": "TT", "symbol": "BTC-USDC", "side": "SELL","qty": 0.0001, "price": 50010.0, "type": "LIMIT", "tif": "IOC"},
    ]

    meta = {
        "branch": "TT",
        "capital_profile": getattr(getattr(rb.cfg, "g", None), "capital_profile", "NANO"),
        "deployment_mode": getattr(getattr(rb.cfg, "g", None), "deployment_mode", "EU_ONLY"),
        "idempotency_key": idem,
        "trace_id": bundle_id,
    }

    bundle = payloads_mod.make_submit_bundle(bundle_id=bundle_id, legs=legs, meta=meta)

    await engine.submit(bundle)

    # Duplicate → doit retourner un reason (pas de “silent loss”)
    try:
        await engine.submit(bundle)
        raise AssertionError("Expected duplicate bundle rejection, got success")
    except EngineSubmitError as exc:
        # “DUPLICATE_BUNDLE” attendu si ton engine est conforme
        msg = str(exc)
        if not msg:
            raise AssertionError("Duplicate rejection has empty reason (silent-ish)")
        # On tolère que le code exact varie, mais il doit être non vide + stable
        write_json(ART_DIR / f"{bundle_id}_duplicate_reject.json", {"reason": msg, "metadata": getattr(exc, "metadata", {})})

    # 2b) bundle invalide → reason
    bad_bundle = dict(bundle)
    bad_bundle["meta"] = dict(bad_bundle.get("meta", {}))
    bad_bundle["meta"].pop("idempotency_key", None)
    try:
        await engine.submit(bad_bundle)
        raise AssertionError("Expected invalid bundle rejection, got success")
    except EngineSubmitError as exc:
        msg = str(exc)
        if not msg:
            raise AssertionError("Invalid bundle rejection has empty reason")
        write_json(ART_DIR / f"{bundle_id}_invalid_reject.json", {"reason": msg, "metadata": getattr(exc, "metadata", {})})

async def scenario_3_pws_incident_sim(rb: RunningBot) -> None:
    await assert_http_surfaces(rb)

    pws = getattr(getattr(rb.boot, "ctx", None), "pws_hub", None)
    if pws is None:
        raise AssertionError("PrivateWSHub missing (boot.ctx.pws_hub is None)")

    attempts = {"n": 0}

    async def connect_coro():
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("simulated disconnect")
        await asyncio.sleep(0.15)

    task = asyncio.create_task(pws.reconnect_with_backoff("BINANCE", "SMOKE", connect_coro))
    try:
        await asyncio.sleep(1.0)
    finally:
        task.cancel()
        with contextlib.suppress(Exception):
            await task

    if attempts["n"] < 2:
        raise AssertionError(f"PWS reconnect loop did not retry enough (attempts={attempts['n']})")

async def scenario_4_rl_429_surge(rb: RunningBot) -> None:
    await assert_http_surfaces(rb)

    rl = getattr(getattr(rb.boot, "ctx", None), "rate_limiter", None)
    if rl is None:
        raise AssertionError("RateLimiter missing (boot.ctx.rate_limiter is None)")

    # On prouve “hedge > cancel > maker” au minimum via priorités + comportement buckets
    cfg_rl = getattr(rb.cfg, "rl", None)
    priorities = list(getattr(cfg_rl, "priorities", [])) if cfg_rl else []
    if priorities:
        # doit contenir hedge et maker
        if "hedge" not in [p.lower() for p in priorities] or "maker" not in [p.lower() for p in priorities]:
            raise AssertionError(f"RL_PRIORITIES missing hedge/maker: {priorities}")

    # Test pratique : maker très contraint, hedge plus “large”
    # (si ton RL est bucketisé par kind, hedge doit passer avant maker sous surcharge)
    maker = rl.bucket_for(exchange="BINANCE", kind="maker", sc_id="SMOKE")
    hedge = rl.bucket_for(exchange="BINANCE", kind="hedge", sc_id="SMOKE")

    async def one_acquire(bucket, name: str, timeout_s: float = 0.8) -> float:
        t0 = time.time()
        try:
            await asyncio.wait_for(bucket.acquire(), timeout=timeout_s)
            return time.time() - t0
        except Exception:
            return 999.0

    # On lance simultané : hedge doit être <= maker dans la majorité des configs raisonnables
    dt_h, dt_m = await asyncio.gather(one_acquire(hedge, "hedge"), one_acquire(maker, "maker"))
    write_json(ART_DIR / f"rl_timings_{now_ms()}.json", {"hedge_s": dt_h, "maker_s": dt_m, "priorities": priorities})

    if dt_h > dt_m + 0.05:
        raise AssertionError(f"RL priority not reflected (hedge slower than maker): hedge={dt_h:.3f}s maker={dt_m:.3f}s")

async def scenario_5_reb_transfers(rb: RunningBot) -> None:
    await assert_http_surfaces(rb)

    rm = getattr(getattr(rb.boot, "ctx", None), "rm", None)
    if rm is None:
        raise AssertionError("RiskManager missing (boot.ctx.rm is None)")

    # Si REB est activé, on exige qu’un composant REB soit présent (nom variable selon ta stack)
    enable_reb = bool(getattr(getattr(rb.cfg, "rm", None), "enable_reb", False))
    reb_obj = getattr(rm, "rebalancing_mgr", None) or getattr(rm, "reb", None)

    if enable_reb and reb_obj is None:
        raise AssertionError("REB enabled but no rebalancing manager attached on RM")

    # Si présent, on exige au minimum un status non-crash
    if reb_obj is not None and hasattr(reb_obj, "get_status"):
        st = reb_obj.get_status()
        write_json(ART_DIR / f"reb_status_{now_ms()}.json", st)

# ------------------------- Main runner -------------------------

def build_scenarios() -> list[Scenario]:
    return [
        Scenario(
            name="1-DRY_RUN-pipeline",
            env={
                **BASE_ENV,
                "MODE": "DRY_RUN",
                "CAPITAL_PROFILE": "NANO",
                "DEPLOYMENT_MODE": "EU_ONLY",
                "PAIRS": "BTC-USDC,ETH-USDC",
                "ENABLED_EXCHANGES": "BINANCE,BYBIT",
            },
            fn=scenario_1_dry_run,
        ),
        Scenario(
            name="2-micro-orders-DRY",
            env={
                **BASE_ENV,
                "MODE": "DRY_RUN",
                "CAPITAL_PROFILE": "NANO",
                "DEPLOYMENT_MODE": "EU_ONLY",
                "PAIRS": "BTC-USDC",
                "ENABLED_EXCHANGES": "BINANCE,BYBIT",
            },
            fn=scenario_2_micro_orders_dry,
        ),
        Scenario(
            name="3-PWS-incident-sim",
            env={
                **BASE_ENV,
                "MODE": "DRY_RUN",
                # On force private_ws ON même en DRY_RUN (sinon Boot ne le démarre pas)
                "FEATURE_SWITCHES": json.dumps({"private_ws": True, "engine_real": False, "balance_fetcher": False, "simulator": True}),
            },
            fn=scenario_3_pws_incident_sim,
        ),
        Scenario(
            name="4-429-surge-RL",
            env={
                **BASE_ENV,
                "MODE": "DRY_RUN",
                # maker hyper contraint, hedge permissif
                "RL_HARD_CAPS_RPS_BY_KIND": json.dumps({"maker": 0.2, "cancel": 1.0, "hedge": 5.0}),
                "RL_BURSTS_BY_KIND": json.dumps({"maker": 1, "cancel": 2, "hedge": 5}),
                "RL_PRIORITIES": "hedge,cancel,maker",
            },
            fn=scenario_4_rl_429_surge,
        ),
        Scenario(
            name="5-REB-transfers",
            env={
                **BASE_ENV,
                "MODE": "DRY_RUN",
                "ENABLE_REB": "1",
            },
            fn=scenario_5_reb_transfers,
        ),
    ]

async def run_one(s: Scenario, timeout_s: float) -> Tuple[bool, str]:
    tag = s.name.replace("/", "_")
    started = False
    rb: RunningBot | None = None

    with env_patch(s.env):
        cfg = BotConfig.from_env()

        # snapshot export (RC proof)
        try:
            h = cfg.snapshot_hash()
            snap = cfg.snapshot_dict()
        except Exception:
            h = f"nohash-{now_ms()}"
            snap = {"note": "snapshot_* not available on cfg"}
        write_json(ART_DIR / f"snapshot_{tag}_{h}.json", snap)

        try:
            rb = await asyncio.wait_for(start_bot(cfg), timeout=timeout_s)
            started = True
            await asyncio.wait_for(s.fn(rb), timeout=timeout_s)
            return True, "PASS"
        except Exception as e:
            # Dump status/metrics si possible
            if rb is not None:
                try:
                    code, st = http_get_json(f"http://{rb.status_host}:{rb.status_port}/status", timeout_s=1.2)
                    write_json(ART_DIR / f"FAIL_{tag}_status.json", {"code": code, "body": st})
                except Exception:
                    pass
                try:
                    code, mt = http_get_text(f"http://{rb.status_host}:{rb.status_port}/metrics", timeout_s=1.2)
                    (ART_DIR / f"FAIL_{tag}_metrics.txt").write_text(mt, encoding="utf-8")
                except Exception:
                    pass
            return False, f"FAIL: {type(e).__name__}: {e}"
        finally:
            if started and rb is not None:
                await stop_bot(rb)

async def main_async() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default="", help="Run only scenarios containing this substring")
    ap.add_argument("--timeout", type=float, default=60.0)
    args = ap.parse_args()

    scenarios = build_scenarios()
    if args.only:
        scenarios = [s for s in scenarios if args.only in s.name]
        if not scenarios:
            print(f"No scenarios match --only={args.only}")
            return 2

    print(f"Running {len(scenarios)} scenarios… artifacts in {ART_DIR}")
    ok_all = True
    for s in scenarios:
        t0 = time.time()
        ok, msg = await run_one(s, timeout_s=args.timeout)
        dt = time.time() - t0
        print(f"[{s.name}] {msg} ({dt:.2f}s)")
        ok_all = ok_all and ok

    return 0 if ok_all else 1

def main() -> int:
    try:
        return asyncio.run(main_async())
    except KeyboardInterrupt:
        return 130

if __name__ == "__main__":
    raise SystemExit(main())
