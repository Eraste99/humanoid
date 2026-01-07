# bot_arbitrage.py
# -*- coding: utf-8 -*-
"""
Main superviser — Arbitrage Bot (Tri-CEX)

- Gouvernance & supervision (pas de logique métier)
- Boot orchestrateur unique (Router/Scanner/RM/Engine/Sim/PrivWS…)
- CentralWatchdog (notify-only) + enfants watchdogs (events + snapshots)
- 3 modes de redémarrage: MANUAL (ACK Telegram), AUTO (intent CWD), WEBHOOK (escalade HMAC)
- WS soft-reload sous budget; aucun restart granulaire métier
- Observabilité main-scope (métriques/probes), arrêt propre (SIGINT/SIGTERM), anti-réentrance
"""

from __future__ import annotations

import asyncio, signal, time, hmac, hashlib, json, logging, os, contextlib, inspect
from typing import Any, Optional, Dict

# -----------------------------------------------------------------------------
# uvloop (best-effort)
# -----------------------------------------------------------------------------
def _maybe_install_uvloop() -> None:
    try:
        import uvloop  # type: ignore
        uvloop.install()
    except Exception:
        pass

# -----------------------------------------------------------------------------
# Imports projet (avec fallbacks)
# -----------------------------------------------------------------------------
try:
    from modules.bot_config import BotConfig, ConfigError
except Exception as e:
    raise RuntimeError("BotConfig introuvable (bot_config.py).") from e

try:
    from boot import Boot
except Exception as e:
    raise RuntimeError("Boot introuvable (boot.py).") from e

try:
    from modules.watchdog.central_watchdog import CentralWatchdog  # signature: CentralWatchdog(config: Optional[...]=None, service_name="bot")
except Exception:
    class CentralWatchdog:
        _IS_FALLBACK_NOOP = True
        def __init__(self, *a, **kw): pass
        def attach_telegram_from_env(self): pass
        def attach_webhook_from_env(self): pass
        def add_state_source(self, *a, **kw): pass
        def on_child_event(self, *a, **kw): pass
        async def run(self):
            while True: await asyncio.sleep(3600)
        # .on_full_restart sera posé par le main

# -----------------------------------------------------------------------------
# Obs (main scope) — best-effort
# -----------------------------------------------------------------------------
class _NoopMetrics:
    def set_deployment_info(self, *a, **kw): pass
    def guard_trigger(self, *a, **kw): pass
    def inc_ws_restart(self, *a, **kw): pass
    def set_budget_remaining(self, *a, **kw): pass
    def inc_full_restart_fail(self): pass

def _start_loop_lag_probe(): pass
def _start_time_skew_probe(): pass

try:
    # [OBS: imports superviseur]
    import os
    from modules.obs_metrics import (
        set_region, set_deployment_mode,
        start_time_skew_probe, start_loop_lag_probe,
        StatusHTTPServer, ObsServer,
        BOT_STARTUPS_TOTAL, BOT_STATE,MainMetrics
    )


    class _ObsFacade:
        def __init__(self):
            mm = MainMetrics()
            ret = mm.register()
            self._m = ret if ret is not None else mm

        def set_deployment_info(self, region: str, mode: str, profile: str):
            with contextlib.suppress(Exception): self._m.set_deployment_info(region, mode, profile)
        def guard_trigger(self, kind: str):
            with contextlib.suppress(Exception): self._m.guard_triggers_total.labels(kind=kind).inc()
        def inc_ws_restart(self, result: str):
            with contextlib.suppress(Exception): self._m.ws_restarts_total.labels(result=result).inc()
        def set_budget_remaining(self, kind: str, value: int):
            with contextlib.suppress(Exception): self._m.restart_budget_remaining.labels(kind=kind).set(value)
        def inc_full_restart_fail(self):
            with contextlib.suppress(Exception): self._m.full_restart_fail_total.inc()

    _metrics_factory = _ObsFacade
    _start_loop_lag_probe = start_loop_lag_probe
    _start_time_skew_probe = start_time_skew_probe

except Exception:
    _metrics_factory = _NoopMetrics  # fallback

# -----------------------------------------------------------------------------
# Alertes (best-effort, no-op si non dispo)
# -----------------------------------------------------------------------------
class _NoopAlerts:
    async def start(self): pass
    async def stop(self): pass
    async def notify_info(self, *a, **kw): pass
    async def notify_warn(self, *a, **kw): pass
    async def notify_crit(self, *a, **kw): pass
    async def prompt_ack(self, reason: str, timeout_s: int = 180) -> Any:
        class _R: ack = False
        return _R()
def _alerts_is_ok(alerts: Any) -> tuple[bool, str]:
    if alerts is None:
        return False, "alert_sink_missing"
    if isinstance(alerts, _NoopAlerts):
        return False, "alert_sink_noop"
    enabled = getattr(alerts, "_enabled", None)
    if enabled is not None and not bool(enabled):
        return False, "alert_sink_disabled"
    sink = getattr(alerts, "_sink", None)
    if sink is None and hasattr(alerts, "_sink"):
        return False, "alert_sink_missing"
    return True, "ok"
def _load_alert_dispatcher(cfg) -> Any:
    try:
        # Si un dispatcher projet existe, on l'utilise en priorité
        from modules.observability_pacer import AlertDispatcher  # type: ignore
        try:
            return AlertDispatcher(cfg)
        except TypeError:
            return AlertDispatcher()
    except Exception:
        # Fallback Telegram natif si token dispo via configuration
        tcfg = getattr(cfg, "telegram", None)
        if tcfg and tcfg.bot_token:
            try:
                return TelegramAlerts(cfg)
            except Exception:
                return _NoopAlerts()
        return _NoopAlerts()
    
# -----------------------------------------------------------------------------
# Alertes Telegram (long-poll, no-op si aiohttp absent ou token manquant)
# -----------------------------------------------------------------------------
class TelegramAlerts(_NoopAlerts):
    """
    Dispatcher minimal basé sur l'API Telegram (long-polling).
    - Envoi de messages: notify_info/warn/crit
    - /ack <code> : prompt_ack() se résout
    - register_command("/status"|"/restart"|...) : mapping commande → callback
    ENV attendus:
      TELEGRAM_BOT_TOKEN (obligatoire),
      TELEGRAM_ALLOWED_USER_IDS='[123,456]' ou '123,456' (facultatif),
      TELEGRAM_CHAT_ID_INFO/TELEGRAM_CHAT_ID_WARN/TELEGRAM_CHAT_ID_CRIT (facultatifs)
    """
    def __init__(self, cfg):
        try:
            import aiohttp  # noqa: F401
        except Exception:
            self._enabled = False
            return
        self._enabled = True
        tcfg = getattr(cfg, "telegram", None)
        self._token = "" if tcfg is None else tcfg.bot_token or ""
        allowed = [] if tcfg is None else tcfg.allowed_user_ids or []
        self._allowed = set(int(x) for x in allowed)
        self._chat_info = None if tcfg is None else tcfg.chat_id_info
        self._chat_warn = None if tcfg is None else tcfg.chat_id_warn
        self._chat_crit = None if tcfg is None else tcfg.chat_id_crit
        self._last_known_chat = None
        self._session = None
        self._task = None
        self._offset = None
        self._cmd_handlers = {}   # str -> async callable(ctx)
        self._ack_waiters = {}    # token(str) -> Future
        self._status_fn = None
        self._restart_fn = None

    @staticmethod
    def _parse_allowed(val: str):
        val = (val or "").strip()
        if not val:
            return set()
        try:
            import json as _json
            parsed = _json.loads(val)
            if isinstance(parsed, list):
                return set(int(x) for x in parsed)
        except Exception:
            pass
        try:
            return set(int(x.strip()) for x in val.split(",") if x.strip())
        except Exception:
            return set()

    def _allowed_user(self, user_id: int) -> bool:
        return (not self._allowed) or (int(user_id) in self._allowed)

    def _base(self) -> str:
        return f"https://api.telegram.org/bot{self._token}"

    async def _tg_post(self, path: str, **params):
        import aiohttp
        if not self._enabled or not self._token:
            return None
        async with aiohttp.ClientSession() as s:
            async with s.post(self._base()+path, json=params, timeout=aiohttp.ClientTimeout(total=60)) as r:
                try:
                    return await r.json()
                except Exception:
                    return None

    async def _tg_get(self, path: str, **params):
        import aiohttp
        if not self._enabled or not self._token:
            return None, []
        async with aiohttp.ClientSession() as s:
            async with s.get(self._base()+path, params=params, timeout=aiohttp.ClientTimeout(total=60)) as r:
                j = await r.json()
                return j, (j.get("result") or []) if isinstance(j, dict) else []

    async def _send_text(self, text: str, chat_id: str | int | None = None):
        if not self._enabled or not self._token:
            return
        cid = str(chat_id or self._chat_info or self._chat_warn or self._chat_crit or self._last_known_chat or "")
        if not cid:
            return
        await self._tg_post("/sendMessage", chat_id=cid, text=str(text))

    async def start(self):
        if not self._enabled or not self._token:
            return
        self._task = asyncio.create_task(self._loop(), name="telegram.loop")

    async def stop(self):
        if self._task:
            self._task.cancel()
            with contextlib.suppress(Exception):
                await self._task
        self._task = None

    async def notify_info(self, text: str, *_, **__):
        await self._send_text(f"ℹ️ {text}", chat_id=self._chat_info)

    async def notify_warn(self, text: str, *_, **__):
        await self._send_text(f"⚠️ {text}", chat_id=self._chat_warn)

    async def notify_crit(self, text: str, *_, **__):
        await self._send_text(f"🚨 {text}", chat_id=self._chat_crit)

    async def prompt_ack(self, reason: str, timeout_s: int = 180):
        class _R: pass
        ret = _R()
        ret.ack = False
        code = str(reason).strip().lstrip("#")
        msg = f"Confirmer le FULL RESTART avec /ack {code}"
        await self.notify_crit(msg)
        fut = asyncio.get_event_loop().create_future()
        self._ack_waiters[code] = fut
        try:
            await asyncio.wait_for(fut, timeout=timeout_s)
            ret.ack = True
        except Exception:
            ret.ack = False
        finally:
            self._ack_waiters.pop(code, None)
        return ret

    def register_command(self, cmd: str, handler):
        self._cmd_handlers[str(cmd).strip().lower()] = handler

    def bind_status(self, fn):
        self._status_fn = fn

    def bind_restart(self, fn):
        self._restart_fn = fn

    async def _loop(self):
        if not self._enabled or not self._token:
            return
        while True:
            try:
                j, updates = await self._tg_get("/getUpdates", timeout=60, offset=self._offset, allowed_updates=["message"])
                for upd in updates:
                    self._offset = int(upd.get("update_id", 0)) + 1
                    msg = upd.get("message") or {}
                    chat = msg.get("chat") or {}
                    frm  = msg.get("from") or {}
                    text = str(msg.get("text") or "")
                    chat_id = chat.get("id")
                    user_id = frm.get("id")
                    if chat_id: self._last_known_chat = chat_id
                    if not text: continue
                    if not self._allowed_user(int(user_id or 0)):
                        continue
                    lower = text.strip().lower()
                    if lower.startswith("/ack"):
                        toks = text.strip().split()
                        if len(toks) >= 2:
                            code = toks[1].lstrip("#")
                            fut = self._ack_waiters.get(code)
                            if fut and not fut.done():
                                fut.set_result(True)
                                await self._send_text("✅ ACK reçu, merci.", chat_id=chat_id)
                                continue
                    cmd = lower.split()[0]
                    h = self._cmd_handlers.get(cmd)
                    if h:
                        ctx = {"chat_id": chat_id, "user_id": user_id, "text": text}
                        try:
                            await h(ctx)
                        except Exception as e:
                            await self._send_text(f"Erreur: {e}", chat_id=chat_id)
                            continue
                    elif lower.startswith("/status") and self._status_fn:
                        try:
                            s = await self._status_fn()
                            summary = self._format_status(s)
                            await self._send_text(summary, chat_id=chat_id)
                        except Exception as e:
                            await self._send_text(f"Erreur status: {e}", chat_id=chat_id)
                    elif lower.startswith("/restart") and self._restart_fn:
                        try:
                            await self._restart_fn("manual-telegram")
                            await self._send_text("Requête de redémarrage envoyée.", chat_id=chat_id)
                        except Exception as e:
                            await self._send_text(f"Erreur restart: {e}", chat_id=chat_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(2.0)

    @staticmethod
    def _format_status(s: dict) -> str:
        try:
            flags = " | ".join([
                f"READY={'✅' if s.get('ready') else '❌'}",
                f"WS={'✅' if s.get('ws_ready') else '❌'}",
                f"ROUTER={'✅' if s.get('router_ready') else '❌'}",
                f"SCANNER={'✅' if s.get('scanner_ready') else '❌'}",
                f"RM={'✅' if s.get('rm_ready') else '❌'}",
                f"ENGINE={'✅' if s.get('engine_ready') else '❌'}",
                f"PRIVATE={'✅' if s.get('private_ready') else '❌'}",
                f"BALANCES={'✅' if s.get('balances_ready') else '❌'}",
                f"RPC={'✅' if s.get('rpc_ready') else '❌'}",
            ])
            pairs = s.get("router_l2_ready_pairs", "-")
            discovered = s.get("discovered", "-")
            actives = len(s.get("active_pairs") or [])
            return f"{flags}\nPairs L2 prêtes: {pairs} | Découvertes: {discovered} | Actives: {actives}"
        except Exception:
            return "Status indisponible."


# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
log = logging.getLogger("arbitrage.main")
_DEFAULT_LOG_LEVEL = "INFO"


def _configure_logging(cfg: Any) -> None:
    level = _DEFAULT_LOG_LEVEL
    try:
        level = getattr(cfg.obs, "log_level", _DEFAULT_LOG_LEVEL) or _DEFAULT_LOG_LEVEL
    except Exception:
        level = _DEFAULT_LOG_LEVEL

    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )



# -----------------------------------------------------------------------------
# Budgets & helpers
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Budgets & helpers
# -----------------------------------------------------------------------------
class RestartBudget:
    """Budget anti-tempête: cap/h, cooldown, mute après cap atteint."""
    def __init__(self, cap_per_hour: int, cooldown_s: int, mute_after_cap_s: int):
        self.cap = int(cap_per_hour)
        self.cooldown_s = int(cooldown_s)
        self.mute_after_cap_s = int(mute_after_cap_s)
        self._window_start = time.time()
        self._count = 0
        self._last_ts = 0.0
        self._muted_until = 0.0

    @staticmethod
    def _getint(val, default: int) -> int:
        """
        Convertit proprement en int :
          - None  -> default
          - ""    -> default
          - "0"   -> 0 (conservé)
          - "4"   -> 4
        Toute valeur illégale retombe sur default.
        """
        try:
            if val is None:
                return int(default)
            s = str(val).strip()
            if s == "":
                return int(default)
            return int(s)
        except Exception:
            return int(default)

    @classmethod
    def from_cfg(cls, cfg, kind: str) -> "RestartBudget":
        if kind == "ws":
            cap  = max(0, cls._getint(getattr(cfg, "WS_RELOAD_CAP_PER_HOUR", None), 4))
            cool = max(0, cls._getint(getattr(cfg, "WS_RELOAD_COOLDOWN_S", 20), 20))
            mute = max(0, cls._getint(getattr(cfg, "WS_RELOAD_MUTE_S", None), 1200))
        else:
            cap  = max(0, cls._getint(getattr(cfg, "FULL_RESTART_CAP_PER_HOUR", None), 2))
            cool = max(0, cls._getint(getattr(cfg, "FULL_RESTART_COOLDOWN_S", None), 60))
            mute = max(0, cls._getint(getattr(cfg, "FULL_RESTART_MUTE_S", None), 1800))
        return cls(cap, cool, mute)

    def remaining(self) -> int:
        now = time.time()
        if now - self._window_start >= 3600:
            self._window_start = now
            self._count = 0
        if now < self._muted_until:
            return 0
        return max(self.cap - self._count, 0)

    def allow(self) -> bool:
        now = time.time()
        if now - self._window_start >= 3600:
            self._window_start = now
            self._count = 0
        if now < self._muted_until:
            return False
        if now - self._last_ts < self.cooldown_s:
            return False
        if self._count >= self.cap:
            self._muted_until = now + self.mute_after_cap_s
            return False
        self._count += 1
        self._last_ts = now
        return True

def _cfg_str(cfg, name: str, default: str | None = None, *, strip: bool = True, allow_empty: bool = False) -> str | None:
    """
    Lit cfg.<name> de façon sûre et renvoie une str.
    - None -> default
    - ""   -> default (sauf allow_empty=True)
    """
    val = getattr(cfg, name, None)
    if val is None:
        return default
    s = str(val)
    if strip:
        s = s.strip()
    if s == "" and not allow_empty:
        return default
    return s

def _cfg_int(cfg, name: str, default: int = 0, *, min: int | None = None, max: int | None = None) -> int:
    """
    Lit un entier “robuste”:
      - None / "" => default
      - "0" => 0
      - "12.0" => 12
      - valeurs illégales => default
    Applique un clamp min/max si fournis.
    """
    val = getattr(cfg, name, None)
    if val is None:
        x = int(default)
    else:
        try:
            if isinstance(val, bool):
                x = 1 if val else 0
            elif isinstance(val, (int, float)):
                x = int(val)
            else:
                s = str(val).strip()
                if s == "":
                    x = int(default)
                else:
                    x = int(float(s))  # gère "12.0"
        except Exception:
            x = int(default)
    if min is not None and x < min:
        x = min
    if max is not None and x > max:
        x = max
    return x

def _cfg_bool(cfg, name: str, default: bool = False) -> bool:
    """
    Bool “tolérant”:
      - None => default
      - bool => tel quel
      - int/float => 0 => False, sinon True
      - str => true/false, yes/no, on/off, 1/0 (case/espaces ignorés)
      - ""  => default
    """
    val = getattr(cfg, name, None)
    if val is None:
        return bool(default)
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return val != 0
    s = str(val).strip().lower()
    if s == "":
        return bool(default)
    if s in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "f", "no", "n", "off"}:
        return False
    return bool(default)


def _hmac_headers(key: str, payload: Dict[str, Any]) -> Dict[str, str]:
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    sig  = hmac.new(key.encode("utf-8"), body.encode("utf-8"), hashlib.sha256).hexdigest()
    return {"Content-Type": "application/json", "X-Signature-SHA256": sig}

async def _post_json(url: str, headers: Dict[str, str], payload: Dict[str, Any]) -> int:
    try:
        import aiohttp  # type: ignore
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as s:
            async with s.post(url, headers=headers, json=payload) as r:
                return r.status
    except Exception:
        log.warning("Webhook non envoyé (aiohttp indisponible): %s", url)
        return 204

# -----------------------------------------------------------------------------
# ConnectivityGuard (WS soft-reload only, notify-only pour Router)
# -----------------------------------------------------------------------------
class ConnectivityGuard:
    """Supervision légère:
       - WS publics: soft-reload si DOWN (sous budget)
       - Router/chainage: notification seulement (aucune action locale)
    """
    def __init__(self, ws_client, router, engine, alerts, ws_budget, metrics, period_s: float = 3.0):
        self.ws = ws_client
        self.router = router
        self.engine = engine
        self.alerts = alerts
        self.ws_budget = ws_budget
        self.metrics = metrics
        self.period_s = period_s
        self._stopping = asyncio.Event()

    async def stop(self):
        self._stopping.set()

    async def _ws_is_healthy(self) -> bool:
        try:
            if self.ws is None:
                return False
            if hasattr(self.ws, "is_healthy"):
                fn = self.ws.is_healthy
                return await fn() if inspect.iscoroutinefunction(fn) else bool(fn())
            if hasattr(self.ws, "health"):
                fn = self.ws.health
                v = await fn() if inspect.iscoroutinefunction(fn) else fn()
                return bool(v if isinstance(v, bool) else v.get("ok", False))
            ts = float(getattr(self.ws, "last_msg_ts", 0.0) or 0.0)
            return (time.time() - ts) < 5.0
        except Exception:
            return False

    async def _ws_reload(self) -> bool:
        if not self.ws_budget.allow():
            self.metrics.guard_trigger("ws_reload_suppressed")
            await self.alerts.notify_warn("[Guard] WS reload supprimé (budget)")
            return False
        try:
            if hasattr(self.ws, "reload"):
                fn = self.ws.reload
                await fn() if inspect.iscoroutinefunction(fn) else await asyncio.to_thread(fn)
            elif hasattr(self.ws, "reconnect"):
                fn = self.ws.reconnect
                await fn() if inspect.iscoroutinefunction(fn) else await asyncio.to_thread(fn)
            else:
                raise RuntimeError("Aucune méthode reload/reconnect disponible")
            self.metrics.guard_trigger("ws_reload")
            self.metrics.inc_ws_restart("ok")
            await self.alerts.notify_info("[Guard] WS soft-reload déclenché")
            return True
        except Exception as e:
            logging.getLogger("arbitrage.main").exception("WS reload KO: %s", e)
            self.metrics.inc_ws_restart("ko")
            await self.alerts.notify_warn(f"[Guard] WS reload KO: {e}")
            return False

    async def run(self):
        logging.getLogger("arbitrage.main").info("[Guard] Démarré (period=%.1fs)", self.period_s)
        while not self._stopping.is_set():
            try:
                if not await self._ws_is_healthy():
                    await self._ws_reload()
            except Exception as e:
                logging.getLogger("arbitrage.main").warning("[Guard] boucle: %s", e)
            await asyncio.sleep(self.period_s)
        logging.getLogger("arbitrage.main").info("[Guard] Arrêté")

# -----------------------------------------------------------------------------
# Wiring enfants CWD (évènements + snapshots)
# -----------------------------------------------------------------------------
def _wire_cwd_children(cw, ctx) -> None:
    """Brancher events (register_event_sink) + snapshots (add_state_source)."""
    def _attach(name: str, obj):
        if obj is None: return
        wd = getattr(obj, "watchdog", None) or obj
        sink = getattr(wd, "register_event_sink", None)
        state_fn = getattr(wd, "state_fn", None)
        if callable(sink):
            with contextlib.suppress(Exception): sink(cw.on_child_event)
        if callable(state_fn):
            with contextlib.suppress(Exception): cw.add_state_source(name, state_fn)

    _attach("public_ws", getattr(ctx, "ws_client", None))
    _attach("router",    getattr(ctx, "router", None))
    _attach("scanner",   getattr(ctx, "scanner", None))
    _attach("risk_mgr",  getattr(ctx, "rm", None))
    _attach("engine",    getattr(ctx, "engine", None))
    _attach("simulator", getattr(ctx, "sim", None))
    _attach("private_ws",getattr(ctx, "priv", None))
    _attach("volatility",getattr(ctx, "volmon", None))
    _attach("slippage",  getattr(ctx, "slip_handler", None))
    _attach("balances",  getattr(ctx, "mbf", None))
    _attach("logger",    getattr(ctx, "logger_mgr", None))

# -----------------------------------------------------------------------------
# Arrêt propre du contexte
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Arrêt propre du contexte — délégué à Boot.stop() si disponible
# -----------------------------------------------------------------------------
async def graceful_stop_ctx(ctx, alerts, boot=None, timeout_s: float = 12.0) -> bool:
    """
    Arrêt global.
    Priorité: si `boot` est fourni et expose .stop(), on délègue (ordre inverse strict).
    Fallback: arrêt best-effort composant par composant.
    """
    log = logging.getLogger("arbitrage.main")

    # 1) Chemin privilégié: Boot.stop()
    try:
        if boot and hasattr(boot, "stop"):
            await boot.stop(timeout_s=timeout_s)
            with contextlib.suppress(Exception):
                await alerts.notify_info("[Main] Contexte arrêté proprement (Boot.stop)")
            return True
    except Exception as e:
        log.warning("[Main] Boot.stop() a échoué: %s — fallback legacy", e)

    # 2) Fallback legacy: best-effort, ordre approximativement inverse
    async def _stop_one(o):
        try:
            fn = getattr(o, "stop", None)
            if not fn:
                return
            if inspect.iscoroutinefunction(fn):
                await fn()
            else:
                await asyncio.to_thread(fn)
        except Exception as e:
            log.warning("Stop '%s' KO: %s", getattr(o, "__class__", type(o)).__name__, e)

    # Ordre inverse raisonnable
    names = (
        "rpc_server", "rpc_client",
        "scanner", "rm", "engine",
        "reconciler", "pws_hub", "balances",
        "router", "ws_public",
        "lhm",
    )
    for name in names:
        with contextlib.suppress(Exception):
            o = getattr(ctx, name, None)
            if o is not None:
                await asyncio.wait_for(_stop_one(o), timeout=timeout_s)

    with contextlib.suppress(Exception):
        await alerts.notify_info("[Main] Contexte arrêté proprement (fallback)")
    return True

# -----------------------------------------------------------------------------
# Arrêt supervision (Guard + CWD tasks)
# -----------------------------------------------------------------------------
async def _stop_supervision(guard, guard_task, cwd_task):
    with contextlib.suppress(Exception):
        if guard and hasattr(guard, "stop"):
            fn = guard.stop
            if inspect.iscoroutinefunction(fn): await fn()
            else: await asyncio.to_thread(fn)

    to_wait = []
    if isinstance(guard_task, asyncio.Task):
        with contextlib.suppress(Exception): guard_task.cancel()
        to_wait.append(guard_task)
    if isinstance(cwd_task, asyncio.Task):
        with contextlib.suppress(Exception): cwd_task.cancel()
        to_wait.append(cwd_task)
    if to_wait:
        with contextlib.suppress(Exception):
            await asyncio.gather(*to_wait, return_exceptions=True)

# -----------------------------------------------------------------------------
# CWD init (instance, wiring, sinks, task)
# -----------------------------------------------------------------------------
def _init_cwd(ctx):
    cwd = CentralWatchdog()           # signature correcte: pas d'arguments
    _wire_cwd_children(cwd, ctx)
    with contextlib.suppress(Exception): cwd.attach_telegram_from_env()  # sans arg
    with contextlib.suppress(Exception): cwd.attach_webhook_from_env()   # sans arg
    cwd_task = asyncio.create_task(cwd.run(), name="cwd.run")
    return cwd, cwd_task

def _wire_telegram_commands(alerts, state: Dict[str, Any]):
    """
    Mappe /status et /restart vers le CWD/Boot.
    - /status  → snapshot Boot.get_status()
    - /restart → cwd.on_full_restart("manual-telegram")
    Les callbacks lisent l'état courant à chaque invocation (pas de closure stale).
    """
    if not hasattr(alerts, "register_command"):
        return

    async def _cmd_status(ctx):
        boot = state.get("boot")
        if hasattr(boot, "get_status"):
            s = boot.get_status()
        else:
            s = {}
        try:
            await alerts.notify_info(TelegramAlerts._format_status(s))
        except Exception:
            pass

    async def _cmd_restart(ctx):
        cwd = state.get("cwd")
        if hasattr(cwd, "on_full_restart"):
            await cwd.on_full_restart("manual-telegram", ctx_hint={"via":"telegram","user_id":ctx.get("user_id")})
        else:
            await alerts.notify_warn("CWD.on_full_restart indisponible")

    alerts.register_command("/status", _cmd_status)
    alerts.register_command("/restart", _cmd_restart)

    # Raccourcis par défaut si implémentés
    if hasattr(alerts, "bind_status"):
        alerts.bind_status(lambda: (state.get("boot").get_status() if hasattr(state.get("boot"), "get_status") else {}))
    if hasattr(alerts, "bind_restart"):
        alerts.bind_restart(lambda reason: (state.get("cwd").on_full_restart(reason) if hasattr(state.get("cwd"), "on_full_restart") else None))

# -----------------------------------------------------------------------------
# Entrée principale
# -----------------------------------------------------------------------------
async def main() -> None:

    from modules.obs_metrics import StatusHTTPServer, ObsServer, start_loop_lag_probe, start_time_skew_probe


    # Config
    cfg = BotConfig.from_env()
    _configure_logging(cfg)
    region      = str(getattr(cfg, "POD_REGION", "EU")).upper()
    dep_mode    = str(getattr(cfg, "DEPLOYMENT_MODE", "EU_ONLY")).upper()
    cap_profile = str(getattr(cfg, "CAPITAL_PROFILE", "NANO")).upper()
    restart_mode = str(getattr(cfg, "RESTART_MODE", "HYBRID")).upper()
    mode_value = str(cfg.g.mode).upper()
    live_trading_armed = bool(cfg.g.live_trading_armed)
    raw = getattr(cfg, "TELEGRAM_REQUIRE_ACK", 1)

    if mode_value == "PROD" and not live_trading_armed:
        log.error("[Main] MODE=PROD sans LIVE_TRADING_ARMED — arrêt immédiat")
        raise ConfigError("CONFIG_SCHEMA_INVALID", "mode")
    if live_trading_armed and not bool(getattr(cfg.lhm, "ff_fail_closed_logging", False)):
        log.error("[Main] LIVE_TRADING_ARMED exige FF_FAIL_CLOSED_LOGGING — arrêt immédiat")
        raise ConfigError("CONFIG_SCHEMA_INVALID", "ff_fail_closed_logging")

    # DRY-RUN: ne pas bloquer sur ACK par défaut ; PROD: ACK requis par défaut
    _default_ack = (mode_value != "DRY_RUN")
    telegram_ack_required = _cfg_bool(cfg, "TELEGRAM_REQUIRE_ACK", _default_ack)

    esc_after_fails = _cfg_int(cfg, "RESTART_ESCALATE_AFTER_FAILS", 2)
    # (optionnel, si utilisé plus loin)
    esc_delay_s = _cfg_int(cfg, "RESTART_ESCALATE_DELAY_S", 30)
    # -- RESTART config robuste --
    restart_mode = _cfg_str(cfg, "RESTART_MODE", "LOCAL").upper()
    max_downtime_s = _cfg_int(cfg, "RESTART_MAX_DOWNTIME_S", 120)
    webhook_url = _cfg_str(cfg, "RESTART_WEBHOOK_URL", "")
    webhook_key = _cfg_str(cfg, "RESTART_WEBHOOK_HMAC_KEY", "")

    # Si WEBHOOK/HYBRID mais URL/KEY manquants → fallback LOCAL (safe)
    if restart_mode in ("WEBHOOK", "HYBRID") and (not webhook_url or not webhook_key):
        log.warning("[RESTART] Mode %s mais URL/KEY manquants → fallback LOCAL", restart_mode)
        restart_mode = "LOCAL"

    log.info("=== Arbitrage Bot — Main ===")
    log.info("Region=%s | Mode=%s | Profile=%s | RestartMode=%s", region, dep_mode, cap_profile, restart_mode)

    # Obs
    metrics = _metrics_factory()
    metrics.set_deployment_info(region, dep_mode, cap_profile)
    # State supervisé unique
    state: Dict[str, Any] = {
        "boot": None,
        "ctx": None,
        "guard": None,
        "guard_task": None,
        "cwd": None,
        "cwd_task": None,
        "metrics": metrics,
        "alerts": None,
        "ws_budget": None,
        "full_budget": None,
        "restart_gen": 0,
        "last_reason": None,
        "restart_lock": asyncio.Lock(),
        "consecutive_fail": 0,
        "first_down_ts": None,
    }

    # Fail-fast si CWD fallback en PROD
    if getattr(CentralWatchdog, "_IS_FALLBACK_NOOP", False):
        if mode_value not in ("DRY_RUN", "DEV"):
            log.critical("[Main] CentralWatchdog fallback/no-op en PROD — arrêt immédiat")
            raise SystemExit(2)
        else:
            log.warning("[Main] CentralWatchdog fallback/no-op en mode %s", mode_value)
            metrics.guard_trigger("degraded_supervisor")

    # Alertes
    alerts = _load_alert_dispatcher(cfg)
    state["alerts"] = alerts
    notification_ok = True
    notification_detail = "ok"
    try:
        await alerts.start()
    except Exception as exc:
        notification_ok = False
        notification_detail = f"alert_start_error:{exc}"
        log.exception("[Main] alert dispatcher start failed")
    if notification_ok:
        notification_ok, notification_detail = _alerts_is_ok(alerts)

    # Budgets
    ws_budget   = RestartBudget.from_cfg(cfg, kind="ws")
    full_budget = RestartBudget.from_cfg(cfg, kind="full")
    metrics.set_budget_remaining("ws", ws_budget.remaining())
    metrics.set_budget_remaining("full", full_budget.remaining())
    state["ws_budget"] = ws_budget
    state["full_budget"] = full_budget

    # Sink status → CWD courant
    boot_notify_cooldown_s = 30.0
    boot_notify_state = {"last": None, "last_ts": 0.0}
    def status_sink(payload):
        result = None
        cwd = state.get("cwd")
        if cwd and hasattr(cwd, "on_child_event"):
            with contextlib.suppress(Exception):
                result = cwd.on_child_event(payload)
        try:
            if payload.get("component") == "boot":
                status = str(payload.get("status") or "")
                if status in ("boot.ready", "boot.degraded"):
                    new_state = "READY" if status == "boot.ready" else "DEGRADED"
                    now = time.time()
                    if new_state != boot_notify_state["last"] and (
                            now - boot_notify_state["last_ts"]) >= boot_notify_cooldown_s:
                        reasons = payload.get("payload", {}) or {}
                        msg = f"[Boot] transition {new_state}"
                        if reasons:
                            msg = f"{msg} — {reasons}"
                        if new_state == "READY":
                            asyncio.create_task(alerts.notify_info(msg))
                        else:
                            asyncio.create_task(alerts.notify_warn(msg))
                        boot_notify_state["last"] = new_state
                        boot_notify_state["last_ts"] = now
        except Exception:
            pass
        return result

    # Boot (barrière READY)
    boot = Boot(cfg)
    boot.state["notification_ok"] = bool(notification_ok)
    boot.state["notify_state"] = {"ok": bool(notification_ok), "detail": notification_detail}
    if not notification_ok and mode_value == "PROD" and bool(cfg.g.live_trading_armed):
        boot._mark_degraded("BOOT_DEP_MISSING", where="notification")
    boot._status_sink = status_sink
    # [OBS-SUPERVISEUR INIT] — Observabilité côté superviseur (pas dans Boot)
    # Contexte & probes (idempotentes)
    region_cfg = getattr(cfg, "POD_REGION", "EU")
    mode_cfg = getattr(cfg, "DEPLOYMENT_MODE", "EU_ONLY")
    set_region(region_cfg)
    set_deployment_mode(mode_cfg)
    start_loop_lag_probe()
    start_time_skew_probe()

    # Politique: 9110 unique (ready/status/metrics), 9108 optionnel selon cfg.obs
    status_port = int(getattr(cfg.obs, "status_port", 9110))
    obs_enable_9108 = bool(getattr(cfg.obs, "enable_obs_port", False))
    obs_port = int(getattr(cfg.obs, "obs_port", 9108))
    include_metrics_on_9110 = bool(getattr(cfg.obs, "expose_metrics_on_status", True)) and not obs_enable_9108


    # Démarre UN seul StatusHTTPServer (et y branche le Boot)
    status_http = StatusHTTPServer(port=status_port, include_metrics=include_metrics_on_9110)
    status_http.set_boot(boot)
    await status_http.start()

    # Si on veut /metrics dédié sur 9108, on le démarre ici (sinon on n’a QUE 9110)
    obs = None
    if obs_enable_9108:
        obs = ObsServer(port=obs_port)
        await obs.start()

    # Signalisation d’état du bot (superviseur)
    BOT_STARTUPS_TOTAL.inc()
    BOT_STATE.set(1)  # STARTING
    ctx = await boot.run()
    await boot.boot_complete.wait()
    if not notification_ok and mode_value == "PROD" and bool(cfg.g.live_trading_armed):
        rm = getattr(ctx, "rm", None)
        if rm and hasattr(rm, "_set_trading_state"):
            rm._set_trading_state("BLOCKED", reason="TRADING_NOT_READY")
    BOT_STATE.set(2)  # READY
    log.info("[Boot] READY — contexte en ligne")
    state.update({
        "boot": boot,
        "ctx": ctx,
    })

    # Supervision: Guard + CWD
    guard = ConnectivityGuard(getattr(ctx,"ws_client",None), getattr(ctx,"router",None), getattr(ctx,"engine",None), alerts, ws_budget, metrics)
    guard_task = asyncio.create_task(guard.run(), name="guard.run")

    cwd, cwd_task = _init_cwd(ctx)
    state.update({
        "guard": guard,
        "guard_task": guard_task,
        "cwd": cwd,
        "cwd_task": cwd_task,
    })


    async def send_restart_webhook_hmac(reason: str, ctx_hint: dict | None = None) -> int:
        if not webhook_url or not webhook_key:
            log.warning("Webhook non configuré, escalade impossible")
            return 0
        payload = {
            "ts": int(time.time()),
            "reason": reason,
            "region": region,
            "mode": dep_mode,
            "profile": cap_profile,
            "fails": state.get("consecutive_fail", 0),
            "ctx": (ctx_hint or {}),
        }
        headers = _hmac_headers(webhook_key, payload)
        status = await _post_json(webhook_url, headers, payload)
        log.info("Webhook escalade → HTTP %s", status)
        return status

    async def request_full_restart(reason: str) -> bool:
        if state["restart_lock"].locked():
            metrics.guard_trigger("full_restart_suppressed")
            await alerts.notify_warn(f"[Main] Full restart SUPPRIMÉ (restart en cours) — {reason}")
            return False
        async with state["restart_lock"]:
            metrics.set_budget_remaining("full", full_budget.remaining())
            if not full_budget.allow():
                metrics.guard_trigger("full_restart_suppressed")
                await alerts.notify_warn(f"[Main] Full restart SUPPRIMÉ (budget épuisé). Raison: {reason}")
                return False
            state["restart_gen"] += 1
            state["last_reason"] = reason
            BOT_STATE.set(4)  # RESTARTING

            await alerts.notify_crit(f"[Main] Full restart DÉMARRÉ — {reason}")

            try:
                # 1) Stop supervision
                await _stop_supervision(state.get("guard"), state.get("guard_task"), state.get("cwd_task"))

                # 2) Arrêt propre du contexte
                await graceful_stop_ctx(state.get("ctx"), alerts, boot=state.get("boot"))

                # 3) Reboot → READY
                new_boot = Boot(cfg)
                new_boot._status_sink = status_sink
                new_ctx = await new_boot.run()
                await new_boot.boot_complete.wait()
                log.info("[Main] Boot2 READY")

                with contextlib.suppress(Exception):
                    status_http.set_boot(new_boot)

                # 4) Re-câbler supervision
                new_guard = ConnectivityGuard(
                    getattr(new_ctx, "ws_client", None),
                    getattr(new_ctx, "router", None),
                    getattr(new_ctx, "engine", None),
                    alerts, ws_budget, metrics
                )
                new_guard_task = asyncio.create_task(new_guard.run(), name="guard.run")

                new_cwd, new_cwd_task = _init_cwd(new_ctx)
                setattr(new_cwd, "on_full_restart", on_full_restart_intent)

                # 5) Switch refs (après succès)
                state.update({
                    "boot": new_boot,
                    "ctx": new_ctx,
                    "guard": new_guard,
                    "guard_task": new_guard_task,
                    "cwd": new_cwd,
                    "cwd_task": new_cwd_task,
                })

                state["consecutive_fail"] = 0
                state["first_down_ts"] = None
                await alerts.notify_info("[Main] Full restart OK")
                BOT_STATE.set(2)  # READY
                return True
            except Exception:
                metrics.inc_full_restart_fail()
                metrics.guard_trigger("full_restart_failed")
                BOT_STATE.set(3)  # DEGRADED
                log.exception("[Main] Full restart FAILED")
                return False
            finally:
                metrics.set_budget_remaining("full", full_budget.remaining())

    async def on_full_restart_intent(reason: str, ctx_hint: dict | None = None):
        if state.get("first_down_ts") is None:
            state["first_down_ts"] = time.time()

        # MANUAL/HYBRID avec ACK
        if restart_mode == "MANUAL" or (restart_mode == "HYBRID" and telegram_ack_required):
            inc_id = f"{int(time.time()) % 100000:05d}"
            await alerts.notify_crit(f"[CWD] Intent FULL_RESTART #{inc_id} — {reason}\nRépondre /ack {inc_id}")
            try:
                resp = await alerts.prompt_ack(reason=f"#{inc_id}", timeout_s=180)
                if not getattr(resp, "ack", False):
                    metrics.guard_trigger("full_restart_suppressed")
                    await alerts.notify_warn("[Main] Full restart non confirmé (ACK manquant)")
                    return
            except Exception:
                metrics.guard_trigger("full_restart_suppressed")
                await alerts.notify_warn("[Main] Full restart non confirmé (ACK KO)")
                return

        # AUTO/HYBRID
        ok = await request_full_restart(reason)
        if ok:
            return

        # Escalade orchestrateur si échecs/downtime
        downtime = time.time() - (state.get("first_down_ts") or time.time())
        state["consecutive_fail"] = state.get("consecutive_fail", 0) + 1
        if state["consecutive_fail"] >= esc_after_fails or downtime > max_downtime_s:
            await alerts.notify_crit("[Main] Escalade orchestrateur (webhook) — auto-restart insuffisant")
            status = await send_restart_webhook_hmac(reason, ctx_hint)
            if not (200 <= status < 300):
                metrics.inc_full_restart_fail()

    # callback CW
    setattr(cwd, "on_full_restart", on_full_restart_intent)
    # Commandes Telegram (si disponibles)
    try:
        _wire_telegram_commands(alerts, state)
    except Exception:
        pass

    # -------------------------------------------------------------------------
    # Signaux & shutdown
    # -------------------------------------------------------------------------
    stop_event = asyncio.Event()

    def _signal_handler(sig: int, _frame=None):
        log.warning("Signal reçu: %s — arrêt demandé...", sig)
        stop_event.set()

    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(s, _signal_handler, s)

    await stop_event.wait()

    # Shutdown propre
    BOT_STATE.set(5)  # STOPPING
    log.info("[Main] Arrêt demandé — shutdown…")
    with contextlib.suppress(Exception):
        await _stop_supervision(state.get("guard"), state.get("guard_task"), state.get("cwd_task"))
    await graceful_stop_ctx(state.get("ctx"), alerts, boot=state.get("boot"))
    # --- stop observability servers (dans l'ordre inverse du start) ---
    with contextlib.suppress(Exception):
        if status_http is not None:
            await status_http.stop()

    with contextlib.suppress(Exception):
        if obs is not None:
            await obs.stop()

    with contextlib.suppress(Exception):
        await alerts.stop()
    BOT_STATE.set(6)  # STOPPED
    log.info("[Main] Bye.")

# -----------------------------------------------------------------------------
# Exécutable
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    try:
        _maybe_install_uvloop()
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
