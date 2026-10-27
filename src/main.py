"""Atalaya Agent — punto de entrada."""
from __future__ import annotations

import logging
import os
import signal
import sys
import threading
from datetime import datetime, timezone

import re

from alert_engine import AlertEngine, AlertHistory
from config import load_config
from docker_monitor import DockerMonitor
from log_api import start_log_api
from log_monitor import LogMonitor
from docker_stats import collect_stats
from terminal_api import start_terminal_api
from logging_conf import setup_logging
from rabbit_publisher import publish

log = logging.getLogger(__name__)

_stop_event = threading.Event()
_monitor_ref:      "DockerMonitor | None" = None
_log_monitor_ref:  "LogMonitor | None"    = None


def _handle_signal(signum, _frame) -> None:
    log.info("Señal %s recibida, deteniendo agente...", signum)
    _stop_event.set()
    if _log_monitor_ref is not None:
        _log_monitor_ref.stop_all()
    if _monitor_ref is not None:
        _monitor_ref.stop()


def _write_heartbeat(state_dir: str) -> None:
    """Actualiza el fichero de heartbeat para el healthcheck."""
    try:
        os.makedirs(state_dir, exist_ok=True)
        path = os.path.join(state_dir, "heartbeat")
        with open(path, "w") as fh:
            fh.write(datetime.now(timezone.utc).isoformat())
    except Exception as exc:
        log.warning("No se pudo escribir heartbeat: %s", exc)


def _heartbeat_loop(config, monitor: DockerMonitor) -> None:
    """Hilo que publica heartbeats periódicos al hub."""
    log.info("Hilo de heartbeat iniciado (intervalo=%ds)", config.heartbeat_interval)
    while not _stop_event.is_set():
        try:
            containers = monitor.list_containers()
            stats_map  = collect_stats(monitor._client, containers)
            payload = {
                "type": "heartbeat",
                "host": config.host_name,
                "agent_url": config.agent_api_url,
                "agent_terminal_url": config.agent_terminal_url,
                "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "containers": [
                    {
                        "name": c["name"],
                        "id": c["id"],
                        "image": c["image"],
                        "status": c["status"],
                        "restart_count": c["restart_count"],
                        "started_at": c["started_at"],
                        "cpu_pct": stats_map.get(c["id"], {}).get("cpu_pct"),
                        "mem_pct": stats_map.get(c["id"], {}).get("mem_pct"),
                        "mem_mb": stats_map.get(c["id"], {}).get("mem_mb"),
                        "mem_limit_mb": stats_map.get(c["id"], {}).get("mem_limit_mb"),
                    }
                    for c in containers
                ],
            }
            publish(config.rabbitmq, payload)
            _write_heartbeat(config.state_dir)
            log.debug(
                "Heartbeat enviado: host=%s contenedores=%d",
                config.host_name,
                len(containers),
            )
        except Exception as exc:
            log.warning("Error en hilo de heartbeat: %s", exc)

        _stop_event.wait(config.heartbeat_interval)

    log.info("Hilo de heartbeat detenido.")


def _publish_alert(record, config) -> None:
    """Publica un AlertRecord al hub vía RabbitMQ."""
    publish(config.rabbitmq, {
        "type": "alert",
        "host": config.host_name,
        "timestamp": record.timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "container_name": record.container_name,
        "container_id": record.container_id,
        "image": record.image,
        "alert_type": record.alert_type,
        "severity": record.severity,
        "label": record.label,
        "detail": record.detail,
        "exit_code": record.exit_code,
    })


def _handle_event(
    event: dict,
    engine: AlertEngine,
    history: AlertHistory,
    config,
    log_monitor: "LogMonitor | None" = None,
) -> None:
    # Notificar al monitor de logs sobre el ciclo de vida del contenedor
    if log_monitor is not None:
        action = event.get("Action", "")
        attrs  = event.get("Actor", {}).get("Attributes", {})
        cid    = event.get("Actor", {}).get("ID", "")[:12]
        name   = attrs.get("name", cid)
        image  = attrs.get("image", "unknown")
        if action == "start":
            log_monitor.start_container(cid, name, image)
        elif action in ("die", "kill", "stop"):
            log_monitor.stop_container(cid)

    record = engine.process(event)
    if record is None:
        return

    history.add(record)
    log.warning(
        "[%s] %s — %s (exit=%s)",
        record.container_name,
        record.label,
        record.detail,
        record.exit_code if record.exit_code is not None else "—",
    )
    _publish_alert(record, config)


def main() -> int:
    global _monitor_ref, _log_monitor_ref

    config = load_config()
    setup_logging(config.log_level)

    log.info("Iniciando Atalaya Agent — host=%s", config.host_name)

    if config.alert.enabled and not config.alert.destinatarios:
        log.warning(
            "ALERT_ENABLED=true pero ALERT_DESTINATARIOS está vacío — no se enviarán emails."
        )

    alert_engine  = AlertEngine(config.alert)
    alert_history = AlertHistory(max_records=500)

    try:
        monitor = DockerMonitor(
            on_event=lambda e: _handle_event(
                e, alert_engine, alert_history, config, _log_monitor_ref
            ),
        )
    except Exception:
        log.error("No se pudo iniciar el monitor Docker. ¿Está montado el socket?")
        return 1

    _monitor_ref = monitor

    # ── Monitor de logs (opcional)
    lm_cfg = config.log_monitor
    if lm_cfg.enabled:
        if not lm_cfg.patterns:
            log.warning("LOG_MONITOR_ENABLED=true pero LOG_MONITOR_PATTERNS está vacío — monitor de logs desactivado.")
        else:
            compiled = [re.compile(p) for p in lm_cfg.patterns]
            log_monitor = LogMonitor(
                docker_client=monitor._client,
                patterns=compiled,
                cooldown=lm_cfg.cooldown,
                max_line_length=lm_cfg.max_line_length,
                on_alert=lambda rec: (
                    alert_history.add(rec),
                    _publish_alert(rec, config),
                ),
            )
            _log_monitor_ref = log_monitor
            log_monitor.start_all_running(monitor.list_containers())
            log.info(
                "Monitor de logs activo — patrones: %s — cooldown: %ds",
                ", ".join(lm_cfg.patterns),
                lm_cfg.cooldown,
            )
    else:
        log.info("Monitor de logs desactivado (LOG_MONITOR_ENABLED=false).")

    if config.agent_api_token:
        start_log_api(config.agent_api_port, config.agent_api_token, monitor._client)
        start_terminal_api(config.agent_terminal_port, config.agent_api_token, monitor._client)
    else:
        log.info("AGENT_API_TOKEN no configurado — API de logs y terminal desactivadas.")

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    hb_thread = threading.Thread(
        target=_heartbeat_loop,
        args=(config, monitor),
        daemon=True,
        name="heartbeat",
    )
    hb_thread.start()

    log.info("Escuchando eventos Docker y publicando al hub...")

    try:
        monitor.run()
    except SystemExit:
        pass
    finally:
        _stop_event.set()
        if _log_monitor_ref is not None:
            _log_monitor_ref.stop_all()
        monitor.stop()
        log.info("Atalaya Agent detenido.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
