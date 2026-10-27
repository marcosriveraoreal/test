"""Carga y validación de configuración desde variables de entorno."""
from __future__ import annotations

import logging
import os
import re
import socket
from dataclasses import dataclass
from typing import List

log = logging.getLogger(__name__)


class ConfigError(RuntimeError):
    pass


def _optional(name: str, default: str) -> str:
    return os.environ.get(name, "").strip() or default


def _optional_int(name: str, default: int) -> int:
    value = os.environ.get(name, "").strip()
    if not value:
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ConfigError(f"'{name}' debe ser un entero, recibido: '{value}'") from exc


def _optional_bool(name: str, default: bool) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on") \
        if os.environ.get(name, "").strip() else default


def _optional_list(name: str) -> List[str]:
    raw = os.environ.get(name, "").strip()
    return [x.strip() for x in raw.split(",") if x.strip()]


@dataclass(frozen=True)
class RabbitMQConfig:
    host: str
    port: int
    user: str
    password: str
    vhost: str
    # Exchange de email (notificaciones)
    exchange: str
    routing_key: str
    # Exchange del hub de monitorización
    hub_exchange: str
    hub_routing_key: str


@dataclass(frozen=True)
class AlertConfig:
    enabled: bool
    destinatarios: List[str]
    cc: str
    modelo_correo: int
    cuenta_correo: int
    auth_token: str
    # Cooldowns en segundos por tipo de alerta
    cooldown_died: int
    cooldown_oom: int
    cooldown_unhealthy: int
    cooldown_crashloop: int
    # Crashloop: N caídas en M segundos → alerta
    crashloop_threshold: int
    crashloop_window: int
    # Contenedores a ignorar (nombres, subcadenas)
    ignore_names: List[str]


@dataclass(frozen=True)
class LogMonitorConfig:
    enabled: bool
    patterns: List[str]   # raw pattern strings; compiled in LogMonitor.__init__
    cooldown: int         # segundos entre alertas por contenedor
    max_line_length: int  # truncar líneas largas en el detalle


@dataclass(frozen=True)
class AppConfig:
    host_name: str
    rabbitmq: RabbitMQConfig
    alert: AlertConfig
    log_monitor: LogMonitorConfig
    log_level: str
    state_dir: str
    hub_exchange: str
    hub_routing_key: str
    heartbeat_interval: int
    agent_api_port: int
    agent_api_token: str
    agent_api_url: str  # URL accesible desde el hub, ej: http://192.168.1.10:9092


def load_config() -> AppConfig:
    rabbitmq = RabbitMQConfig(
        host=_optional("RABBITMQ_HOST", "localhost"),
        port=_optional_int("RABBITMQ_PORT", 5672),
        user=_optional("RABBITMQ_USER", "guest"),
        password=_optional("RABBITMQ_PASSWORD", "guest"),
        vhost=_optional("RABBITMQ_VHOST", "/"),
        exchange=_optional("RABBITMQ_EXCHANGE", "notifications-exchange"),
        routing_key=_optional("RABBITMQ_ROUTING_KEY", "notification.it"),
        hub_exchange=_optional("HUB_EXCHANGE", "atalaya-monitor-exchange"),
        hub_routing_key=_optional("HUB_ROUTING_KEY", "monitor.events"),
    )

    alert = AlertConfig(
        enabled=_optional_bool("ALERT_ENABLED", True),
        destinatarios=_optional_list("ALERT_DESTINATARIOS"),
        cc=_optional("ALERT_CC", ""),
        modelo_correo=_optional_int("ALERT_MODELO_CORREO", 56),
        cuenta_correo=_optional_int("ALERT_CUENTA_CORREO", 1),
        auth_token=_optional("ALERT_AUTH_TOKEN", ""),
        cooldown_died=_optional_int("ALERT_COOLDOWN_DIED_SECONDS", 300),
        cooldown_oom=_optional_int("ALERT_COOLDOWN_OOM_SECONDS", 300),
        cooldown_unhealthy=_optional_int("ALERT_COOLDOWN_UNHEALTHY_SECONDS", 600),
        cooldown_crashloop=_optional_int("ALERT_COOLDOWN_CRASHLOOP_SECONDS", 1800),
        crashloop_threshold=_optional_int("ALERT_CRASHLOOP_THRESHOLD", 3),
        crashloop_window=_optional_int("ALERT_CRASHLOOP_WINDOW_SECONDS", 300),
        ignore_names=_optional_list("ALERT_IGNORE_CONTAINERS"),
    )

    _raw_patterns = _optional(
        "LOG_MONITOR_PATTERNS",
        "ERROR,CRITICAL,Exception,Traceback,FATAL,PANIC",
    )
    _valid_patterns: List[str] = []
    for _p in _raw_patterns.split(","):
        _p = _p.strip()
        if not _p:
            continue
        try:
            re.compile(_p)
            _valid_patterns.append(_p)
        except re.error as exc:
            log.warning("LOG_MONITOR_PATTERNS: patrón inválido '%s' ignorado: %s", _p, exc)

    log_monitor = LogMonitorConfig(
        enabled=_optional_bool("LOG_MONITOR_ENABLED", False),
        patterns=_valid_patterns,
        cooldown=_optional_int("LOG_MONITOR_COOLDOWN_SECONDS", 300),
        max_line_length=_optional_int("LOG_MONITOR_MAX_LINE_LENGTH", 200),
    )

    return AppConfig(
        host_name=_optional("HOST_NAME", socket.gethostname()),
        rabbitmq=rabbitmq,
        alert=alert,
        log_monitor=log_monitor,
        log_level=_optional("LOG_LEVEL", "INFO"),
        state_dir=_optional("STATE_DIR", "/app/state"),
        hub_exchange=_optional("HUB_EXCHANGE", "atalaya-monitor-exchange"),
        hub_routing_key=_optional("HUB_ROUTING_KEY", "monitor.events"),
        heartbeat_interval=_optional_int("HEARTBEAT_INTERVAL_SECONDS", 60),
        agent_api_port=_optional_int("AGENT_API_PORT", 9092),
        agent_api_token=_optional("AGENT_API_TOKEN", ""),
        agent_api_url=_optional("AGENT_API_URL", ""),
    )
