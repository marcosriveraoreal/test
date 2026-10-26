"""Configuración de logging: formato ISO8601, stdout, librerías externas a WARNING."""
from __future__ import annotations

import logging
import sys
from typing import Optional


def setup_logging(level: str = "INFO", extra_handler: Optional[logging.Handler] = None) -> None:
    fmt = logging.Formatter(
        fmt="%(asctime)s.%(msecs)03dZ %(levelname)-8s %(name)s - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    fmt.converter = __import__("time").gmtime

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(fmt)

    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    root.addHandler(handler)
    if extra_handler:
        extra_handler.setFormatter(fmt)
        root.addHandler(extra_handler)

    for noisy in ("urllib3", "waitress", "werkzeug", "pika"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
