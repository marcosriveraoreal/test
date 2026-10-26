"""API HTTP ligera para servir logs de contenedores al hub."""
from __future__ import annotations

import logging
import threading

from flask import Flask, jsonify, request
from waitress import serve

log = logging.getLogger(__name__)

_app = Flask(__name__)
_token: str = ""
_docker_client = None


def _auth_ok() -> bool:
    auth = request.headers.get("Authorization", "")
    return bool(_token) and auth == f"Bearer {_token}"


@_app.route("/api/logs/<container_name>")
def get_logs(container_name: str):
    if not _auth_ok():
        return jsonify({"error": "No autorizado"}), 401
    lines = min(int(request.args.get("lines", 200)), 1000)
    try:
        container = _docker_client.containers.get(container_name)
        raw = container.logs(tail=lines, timestamps=True)
        decoded = raw.decode("utf-8", errors="replace")
        return jsonify({"container": container_name, "lines": decoded.splitlines()})
    except Exception as exc:
        code = 404 if "404" in str(exc) or "not found" in str(exc).lower() else 500
        return jsonify({"error": str(exc)}), code


@_app.route("/health")
def health():
    return jsonify({"status": "ok"})


def start_log_api(port: int, token: str, docker_client) -> None:
    global _token, _docker_client
    _token = token
    _docker_client = docker_client

    def _serve():
        serve(_app, host="0.0.0.0", port=port, threads=2, channel_timeout=30)

    threading.Thread(target=_serve, daemon=True, name="log-api").start()
    log.info("API de logs escuchando en puerto %d", port)
