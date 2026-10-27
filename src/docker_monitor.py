"""Escucha eventos del daemon Docker y los pasa al motor de alertas."""
from __future__ import annotations

import logging
import os
import threading
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

import docker
import docker.errors

log = logging.getLogger(__name__)


def _log_size(log_path: str) -> int:
    if not log_path:
        return 0
    try:
        return os.path.getsize(log_path)
    except OSError:
        return 0


def _parse_status(container) -> str:
    status = container.status  # running, exited, restarting, paused, dead, created
    health = container.attrs.get("State", {}).get("Health", {}).get("Status", "")
    if health == "unhealthy":
        return "unhealthy"
    if health == "healthy":
        return "healthy"
    return status


def _should_skip(container) -> bool:
    labels = container.labels or {}
    return labels.get("monitor.skip", "").lower() == "true"


def _container_info(container) -> dict:
    attrs = container.attrs
    state = attrs.get("State", {})
    return {
        "id": container.short_id,
        "name": container.name,
        "image": container.image.tags[0] if container.image.tags else attrs.get("Config", {}).get("Image", "unknown"),
        "status": _parse_status(container),
        "started_at": state.get("StartedAt", ""),
        "finished_at": state.get("FinishedAt", ""),
        "restart_count": attrs.get("RestartCount", 0),
        "exit_code": state.get("ExitCode", 0),
        "health": attrs.get("State", {}).get("Health", {}).get("Status", "none"),
        "log_size_bytes": _log_size(attrs.get("LogPath", "")),
    }


class DockerMonitor:
    """Accede al daemon Docker vía socket y distribuye eventos."""

    def __init__(self, on_event: Callable[[dict], None], ignore_self_id: Optional[str] = None):
        self._on_event = on_event
        self._ignore_self_id = ignore_self_id
        self._stop_event = threading.Event()
        try:
            self._client = docker.from_env()
            self._client.ping()
            log.info("Conexión con Docker daemon establecida.")
        except Exception as exc:
            log.error("No se pudo conectar con Docker daemon: %s", exc)
            raise

    def list_containers(self) -> List[dict]:
        try:
            containers = self._client.containers.list(all=True)
            result = []
            for c in containers:
                if _should_skip(c):
                    continue
                try:
                    result.append(_container_info(c))
                except Exception as exc:
                    log.debug("No se pudo obtener info de %s: %s", c.name, exc)
            return sorted(result, key=lambda x: x["name"])
        except Exception as exc:
            log.warning("Error listando contenedores: %s", exc)
            return []

    def run(self) -> None:
        log.info("Escuchando eventos Docker...")
        while not self._stop_event.is_set():
            try:
                for event in self._client.events(decode=True, filters={"type": "container"}):
                    if self._stop_event.is_set():
                        break
                    try:
                        self._on_event(event)
                    except Exception as exc:
                        log.warning("Error procesando evento Docker: %s", exc)
            except docker.errors.APIError as exc:
                log.error("Error de API Docker: %s — reintentando en 5s", exc)
                self._stop_event.wait(5)
            except Exception as exc:
                log.error("Error inesperado en listener Docker: %s — reintentando en 5s", exc)
                self._stop_event.wait(5)

    def stop(self) -> None:
        self._stop_event.set()
