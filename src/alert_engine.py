"""Motor de alertas: evalúa eventos Docker, aplica cooldowns y detecta crashloops."""
from __future__ import annotations

import logging
import threading
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Deque, Dict, List, Optional

from config import AlertConfig

log = logging.getLogger(__name__)

ALERT_DIED      = "died"
ALERT_OOM       = "oom"
ALERT_UNHEALTHY = "unhealthy"
ALERT_HEALTHY   = "healthy"
ALERT_CRASHLOOP = "crashloop"
ALERT_STARTED   = "started"
ALERT_LOG_ERROR = "log_error"

_LABELS = {
    ALERT_DIED:      "Contenedor terminado inesperadamente",
    ALERT_OOM:       "Memoria agotada (OOM Kill)",
    ALERT_UNHEALTHY: "Healthcheck fallido",
    ALERT_HEALTHY:   "Contenedor recuperado",
    ALERT_CRASHLOOP: "Bucle de reinicios detectado",
    ALERT_STARTED:   "Contenedor iniciado",
    ALERT_LOG_ERROR: "Error detectado en logs",
}

_SEVERITY = {
    ALERT_DIED:      "error",
    ALERT_OOM:       "error",
    ALERT_UNHEALTHY: "warning",
    ALERT_HEALTHY:   "ok",
    ALERT_CRASHLOOP: "critical",
    ALERT_STARTED:   "info",
    ALERT_LOG_ERROR: "warning",
}


@dataclass
class AlertRecord:
    container_name: str
    container_id: str
    image: str
    alert_type: str
    severity: str
    label: str
    detail: str
    exit_code: Optional[int]
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def as_variables(self, host_name: str) -> List[str]:
        ts = self.timestamp.strftime("%d/%m/%Y %H:%M:%S")
        return [
            f"contenedor:{self.container_name}",
            f"imagen:{self.image}",
            f"tipo_alerta:{self.label}",
            f"severidad:{self.severity.upper()}",
            f"detalle:{self.detail}",
            f"codigo_salida:{self.exit_code if self.exit_code is not None else '-'}",
            f"fecha:{ts}",
            f"host:{host_name}",
        ]


class AlertHistory:
    """Historial en memoria de alertas, thread-safe."""

    def __init__(self, max_records: int = 500):
        self._lock = threading.Lock()
        self._records: Deque[AlertRecord] = deque(maxlen=max_records)

    def add(self, record: AlertRecord) -> None:
        with self._lock:
            self._records.append(record)

    def snapshot(self, limit: Optional[int] = None) -> List[AlertRecord]:
        with self._lock:
            records = list(self._records)
        records.reverse()
        return records[:limit] if limit else records


class AlertEngine:
    """Evalúa eventos Docker y decide si deben generar una alerta."""

    def __init__(self, config: AlertConfig):
        self._cfg = config
        self._lock = threading.Lock()
        # Último envío de alerta: container_name -> alert_type -> timestamp
        self._last_alert: Dict[str, Dict[str, datetime]] = {}
        # Historial de caídas para detección de crashloop: container_name -> deque[timestamp]
        self._crash_times: Dict[str, Deque[datetime]] = {}

    def _cooldown_for(self, alert_type: str) -> int:
        return {
            ALERT_DIED: self._cfg.cooldown_died,
            ALERT_OOM: self._cfg.cooldown_oom,
            ALERT_UNHEALTHY: self._cfg.cooldown_unhealthy,
            ALERT_CRASHLOOP: self._cfg.cooldown_crashloop,
        }.get(alert_type, 300)

    def _should_send(self, name: str, alert_type: str) -> bool:
        now = datetime.now(timezone.utc)
        cooldown = self._cooldown_for(alert_type)
        last = self._last_alert.get(name, {}).get(alert_type)
        if last is None:
            return True
        return (now - last).total_seconds() >= cooldown

    def _record_sent(self, name: str, alert_type: str) -> None:
        self._last_alert.setdefault(name, {})[alert_type] = datetime.now(timezone.utc)

    def _register_crash(self, name: str) -> bool:
        """Registra una caída y devuelve True si se detecta crashloop."""
        now = datetime.now(timezone.utc)
        times = self._crash_times.setdefault(name, deque())
        times.append(now)
        window = self._cfg.crashloop_window
        # Limpiar caídas fuera de la ventana
        while times and (now - times[0]).total_seconds() > window:
            times.popleft()
        return len(times) >= self._cfg.crashloop_threshold

    def _is_ignored(self, name: str) -> bool:
        return any(pattern in name for pattern in self._cfg.ignore_names)

    def process(self, event: dict) -> Optional[AlertRecord]:
        """Evalúa un evento Docker. Devuelve AlertRecord si hay que alertar."""
        if event.get("Type") != "container":
            return None

        action = event.get("Action", "")
        attrs = event.get("Actor", {}).get("Attributes", {})
        container_id = event.get("Actor", {}).get("ID", "")[:12]
        name = attrs.get("name", container_id)
        image = attrs.get("image", "unknown")

        if self._is_ignored(name):
            log.debug("Contenedor ignorado: %s", name)
            return None

        # Ignorar contenedores con label monitor.skip=true
        if attrs.get("monitor.skip", "").lower() == "true":
            return None

        with self._lock:
            return self._evaluate(action, attrs, container_id, name, image)

    def _evaluate(
        self, action: str, attrs: dict, container_id: str, name: str, image: str
    ) -> Optional[AlertRecord]:

        # --- Contenedor terminado ---
        if action == "die":
            exit_code = int(attrs.get("exitCode", 0))
            if exit_code == 0:
                return None  # parada limpia, no es un error

            is_crashloop = self._register_crash(name)

            if is_crashloop and self._should_send(name, ALERT_CRASHLOOP):
                self._record_sent(name, ALERT_CRASHLOOP)
                return AlertRecord(
                    container_name=name,
                    container_id=container_id,
                    image=image,
                    alert_type=ALERT_CRASHLOOP,
                    severity=_SEVERITY[ALERT_CRASHLOOP],
                    label=_LABELS[ALERT_CRASHLOOP],
                    detail=(
                        f"El contenedor ha caído {self._cfg.crashloop_threshold} veces "
                        f"en los últimos {self._cfg.crashloop_window}s."
                    ),
                    exit_code=exit_code,
                )

            if self._should_send(name, ALERT_DIED):
                self._record_sent(name, ALERT_DIED)
                return AlertRecord(
                    container_name=name,
                    container_id=container_id,
                    image=image,
                    alert_type=ALERT_DIED,
                    severity=_SEVERITY[ALERT_DIED],
                    label=_LABELS[ALERT_DIED],
                    detail=f"El contenedor terminó con código de salida {exit_code}.",
                    exit_code=exit_code,
                )

        # --- OOM Kill ---
        elif action == "oom":
            if self._should_send(name, ALERT_OOM):
                self._record_sent(name, ALERT_OOM)
                return AlertRecord(
                    container_name=name,
                    container_id=container_id,
                    image=image,
                    alert_type=ALERT_OOM,
                    severity=_SEVERITY[ALERT_OOM],
                    label=_LABELS[ALERT_OOM],
                    detail="El kernel mató el proceso por falta de memoria.",
                    exit_code=137,
                )

        # --- Healthcheck ---
        elif action == "health_status: unhealthy":
            if self._should_send(name, ALERT_UNHEALTHY):
                self._record_sent(name, ALERT_UNHEALTHY)
                return AlertRecord(
                    container_name=name,
                    container_id=container_id,
                    image=image,
                    alert_type=ALERT_UNHEALTHY,
                    severity=_SEVERITY[ALERT_UNHEALTHY],
                    label=_LABELS[ALERT_UNHEALTHY],
                    detail="El healthcheck del contenedor está fallando.",
                    exit_code=None,
                )

        elif action == "health_status: healthy":
            # Notificar recuperación solo si antes habíamos alertado por unhealthy
            prev = self._last_alert.get(name, {}).get(ALERT_UNHEALTHY)
            if prev is not None:
                self._last_alert[name].pop(ALERT_UNHEALTHY, None)
                return AlertRecord(
                    container_name=name,
                    container_id=container_id,
                    image=image,
                    alert_type=ALERT_HEALTHY,
                    severity=_SEVERITY[ALERT_HEALTHY],
                    label=_LABELS[ALERT_HEALTHY],
                    detail="El contenedor ha superado el healthcheck y está operativo.",
                    exit_code=None,
                )

        return None
