# central_watchdog.py
# -*- coding: utf-8 -*-
"""
Central Watchdog (CW) — notify-only orchestrator (industry-grade)
=================================================================

Design highlights
-----------------
- Notify-only: collecte, corrèle, propose un full restart (jamais d'action locale).
- Bus d'événements unifié: schéma normalisé, dédup/anti-spam, rate-limit, coalescence.
- Gouvernance duale: MANUEL (ACK Telegram) | AUTO (portes strictes) | HYBRID (AUTO sinon MANUEL).
- Intégrations optionnelles:
  * Telegram sink (notifs + ACK) — secrets via ENV (aucune clé en dur).
  * Webhook orchestrateur (HMAC, idempotence).
- Chemin critique: détection blocage WS/Router/Engine/Scanner/... → proposition de full restart coalescée.
- Status agrégé: état CW, métriques bus/sinks, gouvernance (locks/cooldown), enfants enregistrés.

Dépendances
-----------
- Aucune dépendance exotique. `requests` est facultatif (webhook/telegram); fallback no-op si absent.
- Le CW ne possède aucune connaissance des "modules surveillés". Les watchdogs enfants sont passifs
  et publient via `register_event_sink(cw.on_child_event)`. Le CW peut accepter n'importe quel enfant
  qui émet des événements de forme compatible.

Sécurité
--------
- Secrets lus depuis ENV: TELEGRAM_*, RESTART_WEBHOOK_* (jamais loggés).
- HMAC-SHA256 pour le webhook d'orchestrateur, idempotency key = correlation_id.
- Verrous, cooldown, circuit-breaker pour éviter les storm restarts.

Usage résumée
-------------
cw = CentralWatchdog(config=CWConfig(...))
cw.attach_telegram_from_env()          # optionnel (notifs + ACK)
cw.attach_webhook_from_env()           # optionnel (exécuteur externe)
for wd in watchdogs: wd.register_event_sink(cw.on_child_event)  # abonnement enfants
cw.start()  # non-bloquant; ou cw.run_forever() pour une boucle asynchrone simple

"""

from __future__ import annotations
import os, time, hmac, hashlib, json, threading, queue, uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

try:
    import requests  # facultatif
except Exception:  # pragma: no cover
    requests = None  # type: ignore


# =========================
# Événements & utilitaires
# =========================

Event = Dict[str, Any]
EventSink = Callable[[Event], None]

LEVELS = ("INFO", "WARN", "ERROR", "CRIT")
GOV_MODES = ("MANUAL", "AUTO", "HYBRID")


def now_ts() -> float:
    return time.time()


def _level_emoji(level: str) -> str:
    m = dict(INFO="ℹ️", WARN="⚠️", ERROR="❌", CRIT="🛑")
    return m.get(level.upper(), "•")


def _coerce_level(x: str) -> str:
    x = (x or "").upper()
    return x if x in LEVELS else "INFO"


# =========================
# Config & gouvernance
# =========================

@dataclass
class TelegramConfig:
    bot_token: Optional[str] = None
    chat_id_crit: Optional[str] = None
    chat_id_warn: Optional[str] = None
    chat_id_info: Optional[str] = None
    allowed_user_ids: List[int] = field(default_factory=list)
    ack_pin: Optional[str] = None
    rate_limit_rps: float = 1.0
    burst: int = 5


@dataclass
class WebhookConfig:
    url: Optional[str] = None
    hmac_secret: Optional[str] = None
    timeout_s: float = 5.0


@dataclass
class AutoPolicy:
    persist_s: int = 120          # durée CRIT corrélé
    block_s: int = 90             # durée blocage chemin critique
    cooldown_s: int = 600         # cooldown entre restarts
    lock_ttl_s: int = 900         # verrou de redémarrage
    max_restarts_per_hour: int = 2


@dataclass
class CWConfig:
    mode: str = "MANUAL"          # MANUAL | AUTO | HYBRID
    dedup_ttl_s: int = 10
    rate_limit_rps: float = 1.0
    rate_burst: int = 5
    reminder_every_s: int = 300   # rappel CRIT persistant (5 min)
    telegram: TelegramConfig = field(default_factory=TelegramConfig)
    webhook: WebhookConfig = field(default_factory=WebhookConfig)
    auto: AutoPolicy = field(default_factory=AutoPolicy)
    status_history_size: int = 500  # buffer d'événements pour audit


# =========================
# Token bucket (rate-limit)
# =========================

class TokenBucket:
    def __init__(self, rps: float, burst: int):
        self.rps = max(0.01, float(rps))
        self.capacity = max(1, int(burst))
        self.tokens = self.capacity
        self.last = now_ts()
        self.lock = threading.Lock()

    def allow(self) -> bool:
        with self.lock:
            t = now_ts()
            delta = t - self.last
            self.last = t
            self.tokens = min(self.capacity, self.tokens + delta * self.rps)
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return True
            return False


# =========================
# Dédup simple (TTL)
# =========================

class Deduper:
    def __init__(self, ttl_s: int):
        self.ttl = ttl_s
        self.mem: Dict[str, float] = {}
        self.lock = threading.Lock()

    def _key(self, e: Event) -> str:
        dims = []
        d = e.get("data", {}) or {}
        for k in ("component", "exchange", "pair", "tier", "stream", "shard"):
            v = d.get(k)
            if v is not None:
                dims.append(f"{k}={v}")
        return "|".join([
            _coerce_level(e.get("level", "")),
            str(e.get("watchdog", "")),
            str(e.get("type", "")),
            str(e.get("message", "")),
            ",".join(dims)
        ])

    def allow(self, e: Event) -> bool:
        k = self._key(e)
        t = now_ts()
        with self.lock:
            last = self.mem.get(k, 0.0)
            if t - last < self.ttl:
                return False
            self.mem[k] = t
            # GC simple
            if len(self.mem) > 2048:
                to_del = [kk for kk, vv in self.mem.items() if t - vv > self.ttl]
                for kk in to_del:
                    self.mem.pop(kk, None)
            return True


# =========================
# Telegram sink (optionnel)
# =========================

class TelegramSink:
    """
    Sink Telegram facultatif. Si `requests` ou `bot_token` manquent, no-op.
    Gère un petit rate-limit interne. Les ACK opérateur peuvent être gérés
    par un bot séparé (webhook) qui appelle cw.approve_restart(corr_id, by=...).
    Ici on implémente l'envoi "outbound" minimal.
    """
    def __init__(self, cfg: TelegramConfig):
        self.cfg = cfg
        self.enabled = bool(cfg.bot_token and (cfg.chat_id_crit or cfg.chat_id_warn or cfg.chat_id_info) and requests)
        self.bucket = TokenBucket(cfg.rate_limit_rps, cfg.burst)

    def _chat_for_level(self, level: str) -> Optional[str]:
        level = _coerce_level(level)
        if level in ("CRIT", "ERROR"):
            return self.cfg.chat_id_crit or self.cfg.chat_id_warn or self.cfg.chat_id_info
        if level == "WARN":
            return self.cfg.chat_id_warn or self.cfg.chat_id_info or self.cfg.chat_id_crit
        return self.cfg.chat_id_info or self.cfg.chat_id_warn or self.cfg.chat_id_crit

    def send(self, e: Event) -> None:
        if not self.enabled:
            return
        if not self.bucket.allow():
            return
        chat_id = self._chat_for_level(e.get("level", "INFO"))
        if not chat_id:
            return
        text = self._format(e)
        try:
            url = f"https://api.telegram.org/bot{self.cfg.bot_token}/sendMessage"
            payload = {"chat_id": chat_id, "text": text}
            if requests:
                requests.post(url, json=payload, timeout=5)
        except Exception:
            # On garde le CW robuste: aucun raise ici
            pass

    def _format(self, e: Event) -> str:
        lvl = _coerce_level(e.get("level", "INFO"))
        emoji = _level_emoji(lvl)
        wd = e.get("watchdog", "CW")
        typ = e.get("type", "")
        msg = e.get("message", "")
        d = e.get("data", {}) or {}
        dims = []
        for k in ("component", "exchange", "pair", "tier", "stream", "shard"):
            v = d.get(k)
            if v is not None:
                dims.append(f"{k}:{v}")
        dims_part = f"\n• dims: {'/'.join(dims)}" if dims else ""
        # 3 métriques max
        metrics = []
        for k in d:
            if k in ("component","exchange","pair","tier","stream","shard"):
                continue
            if len(metrics) >= 3:
                break
            v = d.get(k)
            if isinstance(v, (int, float, str, bool)):
                metrics.append(f"{k}={v}")
        metrics_part = f"\n• metrics: {', '.join(metrics)}" if metrics else ""
        intent_part = f"\n• intent: {e.get('intent')}" if e.get("intent") else ""
        corr_part = f"\n• corr: {e.get('correlation_id')}" if e.get("correlation_id") else ""
        when = e.get("ts") or now_ts()
        return f"[{emoji} {lvl}] {wd} • {typ} — {msg}{dims_part}{metrics_part}{intent_part}{corr_part}\n• when: {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(when))}Z"


# =========================
# Webhook orchestrateur
# =========================

class OrchestratorClient:
    def __init__(self, cfg: WebhookConfig):
        self.cfg = cfg

    def available(self) -> bool:
        return bool(self.cfg.url and self.cfg.hmac_secret and requests)

    def _sign(self, body: str) -> str:
        secret = (self.cfg.hmac_secret or "").encode("utf-8")
        return hmac.new(secret, body.encode("utf-8"), hashlib.sha256).hexdigest()

    def post_restart(self, service: str, reason: str, correlation_id: str, requested_by: str) -> Tuple[bool, Optional[str]]:
        if not self.available():
            return False, None
        payload = {
            "service": service,
            "reason": reason,
            "correlation_id": correlation_id,
            "requested_by": requested_by,
            "idempotency_key": correlation_id,
        }
        body = json.dumps(payload, separators=(",", ":"))
        try:
            headers = {
                "Content-Type": "application/json",
                "X-Signature": self._sign(body),
            }
            r = requests.post(self.cfg.url, data=body, headers=headers, timeout=self.cfg.timeout_s)  # type: ignore
            if r.status_code in (200, 201, 202):
                try:
                    data = r.json()
                    return True, str(data.get("exec_id", "unknown"))
                except Exception:
                    return True, "unknown"
            return False, None
        except Exception:
            return False, None


# =========================
# CW principal
# =========================

class CentralWatchdog:
    """
    Orchestrateur passif (notify-only).
    - Enfants: watchdogs quelconques → cw.on_child_event(event)
    - Sinks: Telegram (optionnel) + sinks custom → cw.register_event_sink(cb)
    - Gouvernance: MANUEL/AUTO/HYBRID avec verrous/cooldown/idempotence
    """
    def __init__(self, config: Optional[CWConfig] = None, service_name: str = "bot"):
        self.cfg = config or CWConfig()
        self.service_name = service_name
        self._sinks: List[EventSink] = []
        self._child_names: List[str] = []
        self._q: "queue.Queue[Event]" = queue.Queue()
        self._thread = threading.Thread(target=self._loop, name="cw-loop", daemon=True)
        self._stop = threading.Event()

        # bus helpers
        self._dedup = Deduper(self.cfg.dedup_ttl_s)
        self._bucket = TokenBucket(self.cfg.rate_limit_rps, self.cfg.rate_burst)

        # sinks optionnels
        self._telegram = TelegramSink(self.cfg.telegram) if self.cfg.telegram.bot_token else None
        self._orchestrator = OrchestratorClient(self.cfg.webhook)

        # gouvernance
        self._mode = self.cfg.mode if self.cfg.mode in GOV_MODES else "MANUAL"
        self._restart_lock_until = 0.0
        self._cooldown_until = 0.0
        self._restarts_window: List[float] = []  # timestamps des derniers restarts
        self._pending_corr: Optional[str] = None
        self._pending_since: float = 0.0

        # état chemin critique
        self._block_since: Optional[float] = None
        self._last_crit_since: Optional[float] = None

        # métriques locales
        self._history: List[Event] = []  # buffer circulaire
        self._sent_count = 0
        self._dropped_count = 0
        self._retry_count = 0

        # statut enfants
        self._children_status: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

    # ----- intégrations optionnelles via ENV -----

    def attach_telegram_from_env(self) -> None:
        # Si vous préférez piloter par ENV après init
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        if token:
            self.cfg.telegram.bot_token = token
            self.cfg.telegram.chat_id_crit = os.getenv("TELEGRAM_CHAT_ID_CRIT", self.cfg.telegram.chat_id_crit)
            self.cfg.telegram.chat_id_warn = os.getenv("TELEGRAM_CHAT_ID_WARN", self.cfg.telegram.chat_id_warn)
            self.cfg.telegram.chat_id_info = os.getenv("TELEGRAM_CHAT_ID_INFO", self.cfg.telegram.chat_id_info)
            allowed = os.getenv("TELEGRAM_ALLOWED_USER_IDS", "")
            if allowed:
                try:
                    self.cfg.telegram.allowed_user_ids = [int(x.strip()) for x in allowed.split(",") if x.strip()]
                except Exception:
                    pass
            self.cfg.telegram.ack_pin = os.getenv("TELEGRAM_ACK_PIN", self.cfg.telegram.ack_pin)
            self._telegram = TelegramSink(self.cfg.telegram)

    def attach_webhook_from_env(self) -> None:
        url = os.getenv("RESTART_WEBHOOK_URL")
        hmac_secret = os.getenv("RESTART_WEBHOOK_HMAC_KEY")
        if url and hmac_secret:
            self.cfg.webhook.url = url
            self.cfg.webhook.hmac_secret = hmac_secret
            t = os.getenv("RESTART_WEBHOOK_TIMEOUT_S")
            if t:
                try:
                    self.cfg.webhook.timeout_s = float(t)
                except Exception:
                    pass
            self._orchestrator = OrchestratorClient(self.cfg.webhook)

    # ----- wiring enfants / sinks -----

    def register_event_sink(self, sink: EventSink) -> None:
        self._sinks.append(sink)

    def register_child_name(self, name: str) -> None:
        # Purement informatif; l'abonnement réel se fait côté enfant: child.register_event_sink(cw.on_child_event)
        self._child_names.append(name)

    # ----- boucle interne -----

    def start(self) -> None:
        if not self._thread.is_alive():
            self._stop.clear()
            self._thread = threading.Thread(target=self._loop, name="cw-loop", daemon=True)
            self._thread.start()
            self.emit_event(type="cw", level="INFO", message="cw_started")

    def stop(self) -> None:
        self._stop.set()
        try:
            self._thread.join(timeout=2.0)
        except Exception:
            pass
        self.emit_event(type="cw", level="INFO", message="cw_stopped")

    def run_forever(self) -> None:
        self.start()
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            self.stop()

    # ----- émission / réception -----

    def on_child_event(self, event: Event) -> None:
        """Point d'entrée unique pour TOUS les watchdogs enfants."""
        # Normalisation minimale
        e = dict(event)
        e.setdefault("ts", now_ts())
        e["level"] = _coerce_level(e.get("level", "INFO"))
        e.setdefault("watchdog", e.get("watchdog") or "child")
        # wrap comme event enfant
        if not str(e.get("type", "")).startswith("child::"):
            e["type"] = "child::" + str(e.get("type", "event"))
        self._q.put(e)

    def emit_event(self, **kwargs: Any) -> None:
        """Émettre un event CW (non enfant)."""
        e: Event = {
            "ts": now_ts(),
            "watchdog": "CW",
            "type": kwargs.pop("type", "cw"),
            "level": _coerce_level(kwargs.pop("level", "INFO")),
            "message": kwargs.pop("message", ""),
            "data": kwargs.pop("data", {}) or {},
        }
        # champs facultatifs
        for k in ("intent", "correlation_id"):
            if k in kwargs:
                e[k] = kwargs[k]
        self._q.put(e)

    # ----- loop interne -----

    def _loop(self) -> None:
        last_reminder = 0.0
        while not self._stop.is_set():
            try:
                e = self._q.get(timeout=0.2)
            except queue.Empty:
                e = None
            if e:
                self._handle_event(e)
            # rappels CRIT persistants
            if self._pending_corr and (now_ts() - last_reminder) >= self.cfg.reminder_every_s:
                last_reminder = now_ts()
                self._remind_pending()
        # vidage file
        try:
            while True:
                e = self._q.get_nowait()
                self._handle_event(e)
        except queue.Empty:
            pass

    # ----- traitement événement -----

    def _handle_event(self, e: Event) -> None:
        t = e.get("type", "")
        lvl = _coerce_level(e.get("level", "INFO"))
        self._append_history(e)

        # Dédup + rate-limit (toujours)
        if not self._dedup.allow(e):
            self._dropped_count += 1
            return
        if not self._bucket.allow():
            self._dropped_count += 1
            return

        # Routage sinks (Telegram + externes)
        if self._telegram:
            self._telegram.send(e)
        for s in self._sinks:
            try:
                s(e)
                self._sent_count += 1
            except Exception:
                # Le CW ne tombe pas si un sink plante
                self._dropped_count += 1

        # Politique gouvernance: détecter intents / CRIT persistants / chemin critique
        self._update_critical_path(e)

        if e.get("intent") == "request_full_restart" or (lvl == "CRIT" and self._crit_persistent()):
            reason = self._derive_reason(e)
            self._propose_full_restart(reason)

        # Mise à jour status enfants (optionnel: si les enfants émettent un status event)
        if str(t).endswith("::status") and e.get("data", {}).get("component"):
            comp = e["data"]["component"]
            with self._lock:
                self._children_status[comp] = dict(e["data"])

    def _append_history(self, e: Event) -> None:
        self._history.append(e)
        if len(self._history) > self.cfg.status_history_size:
            self._history = self._history[-self.cfg.status_history_size :]

    # ----- chemin critique & persistance -----

    def _update_critical_path(self, e: Event) -> None:
        # Heuristique simple: si CRIT sur composants clés → blocage
        d = e.get("data", {}) or {}
        comp = d.get("component") or ""
        lvl = _coerce_level(e.get("level", "INFO"))
        crit_components = {"WebSocket", "Router", "Engine", "Scanner", "PrivateWS", "BalanceFetcher"}
        if lvl == "CRIT" and (comp in crit_components or "critical_path" in str(e.get("type", ""))):
            if not self._block_since:
                self._block_since = now_ts()
        # CRIT global: mémoire de début de persistance
        if lvl == "CRIT":
            if not self._last_crit_since:
                self._last_crit_since = now_ts()

    def _crit_persistent(self) -> bool:
        if self._last_crit_since is None:
            return False
        return (now_ts() - self._last_crit_since) >= self.cfg.auto.persist_s

    def _block_persistent(self) -> bool:
        if self._block_since is None:
            return False
        return (now_ts() - self._block_since) >= self.cfg.auto.block_s

    # ----- proposition & approbation -----

    def _derive_reason(self, e: Event) -> str:
        d = e.get("data", {}) or {}
        comp = d.get("component") or e.get("watchdog", "unknown")
        msg = e.get("message", "degradation")
        return f"{comp}: {msg}"

    def _propose_full_restart(self, reason: str) -> None:
        # si déjà en attente, ne pas reproposer
        if self._pending_corr:
            return
        corr = str(uuid.uuid4())
        self._pending_corr = corr
        self._pending_since = now_ts()
        # CW publie la demande (notify-only)
        self.emit_event(
            type="cw::policy",
            level="CRIT",
            message="full_restart_requested",
            data={"reason": reason},
            intent="request_full_restart",
            correlation_id=corr,
        )
        # Mode de gouvernance
        mode = self._mode
        if mode == "AUTO":
            self._auto_evaluate_and_maybe_approve(corr, reason)
        elif mode == "HYBRID":
            if not self._auto_evaluate_and_maybe_approve(corr, reason):
                # reste en attente pour ACK manuel
                pass
        # sinon MANUAL → attend ACK via Telegram/externe

    def _auto_evaluate_and_maybe_approve(self, corr: str, reason: str) -> bool:
        # Portes AUTO
        if not self._crit_persistent():
            return False
        if not self._block_persistent():
            return False
        if now_ts() < self._cooldown_until:
            return False
        if now_ts() < self._restart_lock_until:
            return False
        # fenêtre restarts/heure
        self._restarts_window = [t for t in self._restarts_window if now_ts() - t < 3600]
        if len(self._restarts_window) >= self.cfg.auto.max_restarts_per_hour:
            return False
        # orchestrateur prêt ?
        if not self._orchestrator.available():
            return False
        # Approuver
        self.emit_event(
            type="cw::policy",
            level="WARN",
            message="full_restart_approved",
            data={"by": "CW-auto"},
            correlation_id=corr,
        )
        self._execute_restart(corr, reason, requested_by="CW-auto")
        return True

    def approve_restart(self, correlation_id: str, by: str = "operator") -> None:
        # Validation basique: doit correspondre au pending en cours
        if not self._pending_corr or correlation_id != self._pending_corr:
            # rien à faire
            return
        # Cooldown/lock vérifiés côté exécution
        self.emit_event(
            type="cw::policy",
            level="WARN",
            message="full_restart_approved",
            data={"by": by},
            correlation_id=correlation_id,
        )
        self._execute_restart(correlation_id, f"approved by {by}", requested_by=by)

    def _execute_restart(self, corr: str, reason: str, requested_by: str) -> None:
        # Locks/cooldown
        if now_ts() < self._restart_lock_until or now_ts() < self._cooldown_until:
            self.emit_event(
                type="cw::policy",
                level="INFO",
                message="restart_throttled",
                data={"cooldown_until": int(self._cooldown_until), "lock_until": int(self._restart_lock_until)},
                correlation_id=corr,
            )
            return
        # verrou & cooldown immédiats
        self._restart_lock_until = now_ts() + self.cfg.auto.lock_ttl_s
        self._cooldown_until = now_ts() + self.cfg.auto.cooldown_s

        ok, exec_id = self._orchestrator.post_restart(
            service=self.service_name,
            reason=reason,
            correlation_id=corr,
            requested_by=requested_by,
        )
        if ok:
            self._restarts_window.append(now_ts())
            self.emit_event(
                type="cw::policy",
                level="INFO",
                message="full_restart_executed",
                data={"exec_id": exec_id, "cooldown_s": self.cfg.auto.cooldown_s},
                correlation_id=corr,
            )
            # reset états
            self._pending_corr = None
            self._pending_since = 0.0
            self._block_since = None
            self._last_crit_since = None
        else:
            # Orchestrateur indisponible → retombe MANUEL
            self.emit_event(
                type="cw::policy",
                level="ERROR",
                message="orchestrator_unavailable",
                data={},
                correlation_id=corr,
            )

    def _remind_pending(self) -> None:
        if not self._pending_corr:
            return
        self.emit_event(
            type="cw::policy",
            level="CRIT",
            message="full_restart_pending_reminder",
            data={"age_s": int(now_ts() - self._pending_since)},
            correlation_id=self._pending_corr,
        )

    # ----- statut -----

    def get_status(self) -> Dict[str, Any]:
        with self._lock:
            children = dict(self._children_status)
        return {
            "mode": self._mode,
            "cooldown_until": int(self._cooldown_until),
            "restart_lock_until": int(self._restart_lock_until),
            "pending_correlation_id": self._pending_corr,
            "block_since": int(self._block_since or 0),
            "last_crit_since": int(self._last_crit_since or 0),
            "sent_count": self._sent_count,
            "dropped_count": self._dropped_count,
            "retry_count": self._retry_count,
            "children": children,
            "history_tail": self._history[-10:],  # petit tail pour debug
        }
