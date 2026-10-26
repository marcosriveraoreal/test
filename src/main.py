"""Atalaya Agent — punto de entrada."""
from __future__ import annotations

import logging
import os
import signal
import sys
import threading
from datetime import datetime, timezone

from alert_engine import AlertEngine, AlertHistory
from config import load_config
from docker_monitor import DockerMonitor
from log_api import start_log_api
from logging_conf import setup_logging
from rabbit_publisher import publish

log = logging.getLogger(__name__)

_stop_event = threading.Event()
_monitor_ref: "DockerMonitor | None" = None


def _handle_signal(signum, _frame) -> None:
    log.info("Señal %s recibida, deteniendo agente...", signum)
    _stop_event.set()
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
            payload = {
                "type": "heartbeat",
                "host": config.host_name,
                "agent_url": config.agent_api_url,
                "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "containers": [
                    {
                        "name": c["name"],
                        "id": c["id"],
                        "image": c["image"],
                        "status": c["status"],
                        "restart_count": c["restart_count"],
                        "started_at": c["started_at"],
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


def _handle_event(event: dict, engine: AlertEngine, history: AlertHistory, config) -> None:
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

    # Publicar alerta al hub
    alert_payload = {
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
    }
    publish(config.rabbitmq, alert_payload)


def main() -> int:
    global _monitor_ref

    config = load_config()
    setup_logging(config.log_level)

    log.info("Iniciando Atalaya Agent — host=%s", config.host_name)

    if config.alert.enabled and not config.alert.destinatarios:
        log.warning(
            "ALERT_ENABLED=true pero ALERT_DESTINATARIOS está vacío — no se enviarán emails."
        )

    alert_engine = AlertEngine(config.alert)
    alert_history = AlertHistory(max_records=500)

    try:
        monitor = DockerMonitor(
            on_event=lambda e: _handle_event(e, alert_engine, alert_history, config),
        )
    except Exception:
        log.error("No se pudo iniciar el monitor Docker. ¿Está montado el socket?")
        return 1

    _monitor_ref = monitor

    if config.agent_api_token:
        start_log_api(config.agent_api_port, config.agent_api_token, monitor._client)
    else:
        log.info("AGENT_API_TOKEN no configurado — API de logs desactivada.")

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    # Hilo de heartbeat periódico
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
        monitor.stop()
        log.info("Atalaya Agent detenido.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
