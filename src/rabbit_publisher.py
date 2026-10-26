"""Publicador de mensajes de monitorización al exchange de atalaya-hub."""
from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

import pika

if TYPE_CHECKING:
    from config import RabbitMQConfig

log = logging.getLogger(__name__)


def publish(rabbitmq_cfg: "RabbitMQConfig", payload: dict) -> None:
    """Publica un mensaje JSON en el exchange de monitorización.

    Declara el exchange como topic/durable antes de publicar (idempotente).
    Nunca lanza excepción — registra warning en caso de error.
    """
    try:
        credentials = pika.PlainCredentials(rabbitmq_cfg.user, rabbitmq_cfg.password)
        params = pika.ConnectionParameters(
            host=rabbitmq_cfg.host,
            port=rabbitmq_cfg.port,
            virtual_host=rabbitmq_cfg.vhost,
            credentials=credentials,
            connection_attempts=2,
            retry_delay=2,
            socket_timeout=10,
        )
        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        channel.exchange_declare(
            exchange=rabbitmq_cfg.hub_exchange,
            exchange_type="topic",
            durable=True,
        )

        body = json.dumps(payload, default=str)
        channel.basic_publish(
            exchange=rabbitmq_cfg.hub_exchange,
            routing_key=rabbitmq_cfg.hub_routing_key,
            body=body,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,
            ),
        )
        connection.close()
        log.debug(
            "Mensaje publicado → exchange=%s key=%s type=%s",
            rabbitmq_cfg.hub_exchange,
            rabbitmq_cfg.hub_routing_key,
            payload.get("type", "?"),
        )
    except Exception as exc:
        log.warning("No se pudo publicar mensaje en RabbitMQ: %s", exc)
