"""Recoge estadísticas de CPU y memoria de contenedores Docker en paralelo."""
from __future__ import annotations

import logging
import threading
from typing import Dict

log = logging.getLogger(__name__)


def _stats_one(docker_client, container_id: str, result: Dict[str, dict]) -> None:
    try:
        raw    = docker_client.api.stats(container_id, stream=False)
        cpu_s  = raw.get("cpu_stats", {})
        pcpu_s = raw.get("precpu_stats", {})

        cpu_delta = (
            cpu_s.get("cpu_usage", {}).get("total_usage", 0)
            - pcpu_s.get("cpu_usage", {}).get("total_usage", 0)
        )
        sys_delta = (
            cpu_s.get("system_cpu_usage", 0)
            - pcpu_s.get("system_cpu_usage", 0)
        )
        num_cpus = cpu_s.get("online_cpus") or len(
            cpu_s.get("cpu_usage", {}).get("percpu_usage", [None])
        )
        cpu_pct = round((cpu_delta / sys_delta) * num_cpus * 100, 1) if sys_delta > 0 else 0.0

        mem_s        = raw.get("memory_stats", {})
        cache        = mem_s.get("stats", {}).get("cache", 0)
        mem_usage    = max(0, mem_s.get("usage", 0) - cache)
        mem_limit    = mem_s.get("limit", 0)
        mem_pct      = round(mem_usage / mem_limit * 100, 1) if mem_limit > 0 else 0.0
        mem_mb       = round(mem_usage / 1024 / 1024, 1)
        mem_limit_mb = round(mem_limit / 1024 / 1024, 1)

        result[container_id] = {
            "cpu_pct":      cpu_pct,
            "mem_pct":      mem_pct,
            "mem_mb":       mem_mb,
            "mem_limit_mb": mem_limit_mb,
        }
    except Exception as exc:
        log.debug("Stats error %s: %s", container_id[:12], exc)


def collect_stats(docker_client, containers: list) -> Dict[str, dict]:
    """Recoge CPU y memoria de los contenedores activos en paralelo (timeout 5 s)."""
    running = [c for c in containers if c.get("status") in ("running", "healthy")]
    if not running:
        return {}

    result: Dict[str, dict] = {}
    threads = [
        threading.Thread(
            target=_stats_one,
            args=(docker_client, c["id"], result),
            daemon=True,
        )
        for c in running
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5.0)
    return result
