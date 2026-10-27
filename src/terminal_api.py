"""WebSocket server para terminal interactiva en contenedores Docker."""
from __future__ import annotations

import asyncio
import json
import logging
import select as select_mod
import threading
from urllib.parse import parse_qs, urlparse

import websockets

log = logging.getLogger(__name__)


def _parse_path(raw_path: str) -> tuple[str, str]:
    """Devuelve (container_name, token) desde /terminal/<name>?token=..."""
    parsed = urlparse(raw_path)
    parts = parsed.path.strip("/").split("/")
    container = parts[1] if len(parts) >= 2 and parts[0] == "terminal" else ""
    token = parse_qs(parsed.query).get("token", [""])[0]
    return container, token


def _docker_reader(raw_sock, queue: asyncio.Queue, loop: asyncio.AbstractEventLoop) -> None:
    """Hilo: lee del socket Docker y encola datos para el WebSocket."""
    while True:
        try:
            r, _, _ = select_mod.select([raw_sock], [], [], 0.05)
            if r:
                data = raw_sock.recv(4096)
                if not data:
                    loop.call_soon_threadsafe(queue.put_nowait, None)
                    return
                loop.call_soon_threadsafe(queue.put_nowait, data)
        except Exception as exc:
            log.debug("docker_reader terminado: %s", exc)
            loop.call_soon_threadsafe(queue.put_nowait, None)
            return


async def _handle(websocket, auth_token: str, docker_client) -> None:
    req = getattr(websocket, "request", None)
    if req is not None:
        raw_path = req.path
    else:
        raw_path = getattr(websocket, "path", getattr(websocket, "resource_name", "/"))
    container_name, token = _parse_path(raw_path)

    log.info("Terminal: path=%r container=%r token_ok=%s", raw_path, container_name, token == auth_token)

    if not container_name:
        await websocket.send("\r\n\x1b[31mRuta inválida. Usa /terminal/<nombre>\x1b[0m\r\n")
        return

    if token != auth_token:
        await websocket.send("\r\n\x1b[31mNo autorizado.\x1b[0m\r\n")
        return

    # ── Abrir exec con TTY
    exec_id = None
    raw_sock = None
    for shell in ("/bin/bash", "/bin/sh"):
        try:
            eid = docker_client.api.exec_create(
                container_name, [shell],
                stdin=True, stdout=True, stderr=True, tty=True,
                environment={"TERM": "xterm-256color"},
            )
            esock = docker_client.api.exec_start(eid["Id"], socket=True, tty=True)
            exec_id = eid["Id"]
            raw_sock = esock._sock
            raw_sock.setblocking(False)
            break
        except Exception:
            continue

    if exec_id is None:
        await websocket.send(
            f"\r\n\x1b[31mNo se encontró ningún shell en '{container_name}'.\x1b[0m\r\n"
        )
        return

    loop = asyncio.get_running_loop()
    queue: asyncio.Queue = asyncio.Queue(maxsize=256)

    reader_thread = threading.Thread(
        target=_docker_reader, args=(raw_sock, queue, loop),
        daemon=True, name=f"term-reader-{container_name}",
    )
    reader_thread.start()

    # ── Docker → WebSocket
    async def docker_to_ws():
        while True:
            data = await queue.get()
            if data is None:
                await websocket.send("\r\n\x1b[33m[Proceso terminado]\x1b[0m\r\n")
                return
            try:
                await websocket.send(data.decode("utf-8", errors="replace"))
            except Exception:
                return

    # ── WebSocket → Docker
    async def ws_to_docker():
        async for msg in websocket:
            try:
                d = json.loads(msg)
                if d.get("t") == "r":
                    docker_client.api.exec_resize(
                        exec_id,
                        height=d.get("r", 24),
                        width=d.get("c", 80),
                    )
                elif d.get("t") == "i":
                    payload = d.get("d", "").encode("utf-8")
                    await loop.run_in_executor(None, _send_to_sock, raw_sock, payload)
            except (json.JSONDecodeError, TypeError):
                # Datos en crudo (fallback)
                payload = msg.encode("utf-8") if isinstance(msg, str) else msg
                await loop.run_in_executor(None, _send_to_sock, raw_sock, payload)
            except Exception:
                return

    tasks = [
        asyncio.create_task(docker_to_ws()),
        asyncio.create_task(ws_to_docker()),
    ]
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    try:
        raw_sock.close()
    except Exception:
        pass


def _send_to_sock(raw_sock, data: bytes) -> None:
    try:
        raw_sock.setblocking(True)
        raw_sock.sendall(data)
        raw_sock.setblocking(False)
    except Exception:
        pass


def start_terminal_api(port: int, token: str, docker_client) -> None:
    """Inicia el WebSocket server de terminal en un hilo daemon."""

    async def handler(websocket):
        remote = getattr(websocket, "remote_address", ("?", 0))
        log.info("Terminal WS conectado: %s:%s", remote[0], remote[1])
        try:
            await _handle(websocket, token, docker_client)
        except Exception as exc:
            log.warning("Terminal WS error: %s", exc)
        finally:
            log.info("Terminal WS desconectado: %s:%s", remote[0], remote[1])

    async def serve():
        async with websockets.serve(handler, "0.0.0.0", port):
            log.info("Terminal WebSocket API escuchando en puerto %d", port)
            await asyncio.Future()

    def run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(serve())
        except Exception as exc:
            log.error("Error en terminal API: %s", exc)

    threading.Thread(target=run, daemon=True, name="terminal-api").start()
