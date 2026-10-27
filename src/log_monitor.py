"""Monitor de logs de contenedores Docker con detección de patrones."""
from __future__ import annotations

import logging
import re
import threading
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

log = logging.getLogger(__name__)


def _iter_lines(stream):
    """Yield decoded log lines from a Docker streaming generator.

    Handles Docker's 8-byte multiplexing frame header transparently.
    """
    buf = b""
    for chunk in stream:
        buf += chunk
        while b"\n" in buf:
            line, buf = buf.split(b"\n", 1)
            # Strip Docker multiplex header: byte[0] in {1,2}, bytes[1:4] == 0x000000
            if len(line) >= 8 and line[0] in (1, 2) and line[1:4] == b"\x00\x00\x00":
                line = line[8:]
            decoded = line.decode("utf-8", errors="replace").strip()
            if decoded:
                yield decoded
    if buf:
        if len(buf) >= 8 and buf[0] in (1, 2) and buf[1:4] == b"\x00\x00\x00":
            buf = buf[8:]
        decoded = buf.decode("utf-8", errors="replace").strip()
        if decoded:
            yield decoded


class LogMonitor:
    """Tails container logs in real-time and emits alerts on pattern matches.

    Spawns one daemon thread per running container. Threads are created on
    container start events and cleaned up on stop/die events. A per-container
    cooldown prevents alert spam when a pattern fires repeatedly.
    """

    def __init__(
        self,
        docker_client,
        patterns: List[re.Pattern],
        cooldown: int,
        max_line_length: int,
        on_alert: Callable,
    ) -> None:
        self._client        = docker_client
        self._patterns      = patterns
        self._cooldown      = cooldown
        self._max_line_len  = max_line_length
        self._on_alert      = on_alert

        self._lock        = threading.Lock()
        self._stop_events: Dict[str, threading.Event] = {}
        self._threads:     Dict[str, threading.Thread] = {}
        self._last_alert:  Dict[str, datetime]         = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start_all_running(self, containers: List[dict]) -> None:
        """Begin tailing all containers that are currently running."""
        for c in containers:
            if c.get("status") in ("running", "healthy", "unhealthy"):
                self.start_container(c["id"], c["name"], c["image"])

    def start_container(self, container_id: str, name: str, image: str) -> None:
        with self._lock:
            if container_id in self._threads:
                return
            stop_ev = threading.Event()
            self._stop_events[container_id] = stop_ev

        t = threading.Thread(
            target=self._tail_loop,
            args=(container_id, name, image, stop_ev),
            daemon=True,
            name=f"log-tail-{name}",
        )
        with self._lock:
            self._threads[container_id] = t
        t.start()
        log.debug("Log tail iniciado: %s", name)

    def stop_container(self, container_id: str) -> None:
        with self._lock:
            ev = self._stop_events.pop(container_id, None)
            self._threads.pop(container_id, None)
        if ev:
            ev.set()

    def stop_all(self) -> None:
        with self._lock:
            evs = list(self._stop_events.values())
            self._stop_events.clear()
            self._threads.clear()
        for ev in evs:
            ev.set()
        log.info("Log monitor detenido.")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _cooldown_ok(self, name: str) -> bool:
        last = self._last_alert.get(name)
        if last is None:
            return True
        return (datetime.now(timezone.utc) - last).total_seconds() >= self._cooldown

    def _record_sent(self, name: str) -> None:
        self._last_alert[name] = datetime.now(timezone.utc)

    def _match(self, line: str) -> Optional[re.Pattern]:
        for p in self._patterns:
            if p.search(line):
                return p
        return None

    def _tail_loop(
        self,
        container_id: str,
        name: str,
        image: str,
        stop_ev: threading.Event,
    ) -> None:
        try:
            container = self._client.containers.get(container_id)
        except Exception as exc:
            log.debug("No se pudo obtener contenedor %s: %s", name, exc)
            return

        try:
            # tail=0 → skip historical lines, only follow new output
            stream = container.logs(stream=True, follow=True, tail=0)
            for line in _iter_lines(stream):
                if stop_ev.is_set():
                    break

                matched = self._match(line)
                if matched is None:
                    continue

                with self._lock:
                    if not self._cooldown_ok(name):
                        continue
                    self._record_sent(name)

                detail = line[:self._max_line_len]
                if len(line) > self._max_line_len:
                    detail += "…"

                log.warning("[%s] Patrón detectado en logs: %s", name, detail[:120])
                self._emit(container_id, name, image, detail)

        except Exception as exc:
            if not stop_ev.is_set():
                log.debug("Log tail finalizado para %s: %s", name, exc)
        finally:
            with self._lock:
                self._stop_events.pop(container_id, None)
                self._threads.pop(container_id, None)

    def _emit(self, container_id: str, name: str, image: str, detail: str) -> None:
        from alert_engine import ALERT_LOG_ERROR, AlertRecord
        record = AlertRecord(
            container_name=name,
            container_id=container_id,
            image=image,
            alert_type=ALERT_LOG_ERROR,
            severity="warning",
            label="Error detectado en logs",
            detail=detail,
            exit_code=None,
        )
        try:
            self._on_alert(record)
        except Exception as exc:
            log.warning("Error emitiendo alerta de log: %s", exc)
