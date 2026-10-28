# atalaya-monitor

Agente de monitorización Docker para Atalaya Mining. Corre en cada servidor, escucha eventos del daemon Docker, recoge métricas de contenedores y publica heartbeats y alertas al hub central vía RabbitMQ.

## Propósito

En cada servidor monitorizado:
1. Escucha eventos Docker en tiempo real (die, OOM, unhealthy, start…)
2. Envía heartbeats periódicos al hub con estado y métricas de todos los contenedores
3. Detecta y publica alertas: contenedor caído, OOM kill, healthcheck fallido, crashloop, error en logs
4. Expone API HTTP de logs y WebSocket de terminal para acceso remoto desde el hub

## Stack

- **Python 3.12** — sin frameworks pesados
- **docker SDK** — eventos y estadísticas del daemon Docker vía socket `/var/run/docker.sock`
- **pika** — publicación AMQP a RabbitMQ (heartbeats y alertas)
- **Flask + Waitress** — API HTTP de logs en `:9092`
- **websockets** — API WebSocket de terminal en `:9093`
- **Docker** — despliegue con `docker-compose.yml`; requiere `user: root` para acceder al socket

## Estructura

```
src/
  main.py            # arranque, señales SIGTERM/SIGINT, hilo de heartbeat
  config.py          # carga de variables de entorno (.env)
  docker_monitor.py  # DockerMonitor: lista contenedores, escucha eventos, log_size_bytes
  docker_stats.py    # recoge CPU% y RAM de todos los contenedores en paralelo
  alert_engine.py    # evalúa eventos → AlertRecord; cooldowns y detección de crashloop
  log_monitor.py     # LogMonitor: sigue logs de contenedores, detecta patrones regex
  log_api.py         # API HTTP /api/logs/<container> (Bearer token)
  terminal_api.py    # WebSocket de terminal con autenticación Bearer
  rabbit_publisher.py# publica mensajes AMQP al hub
  logging_conf.py    # formato ISO8601, stdout, waitress/werkzeug a WARNING
  config.py          # carga de .env; sin valores por defecto para credenciales
templates/
  email_container_alert.txt  # plantilla de email de alerta
docker-compose.yml   # despliegue en producción
.env.example         # plantilla de variables de entorno
```

## Tipos de alerta

| Tipo | Severidad | Descripción |
|---|---|---|
| `died` | error | Contenedor terminado con exit code ≠ 0 |
| `oom` | error | OOM kill por el kernel |
| `unhealthy` | warning | Healthcheck fallido |
| `healthy` | ok | Contenedor recuperado (healthcheck OK) |
| `crashloop` | critical | N caídas en M segundos (configurable) |
| `started` | info | Contenedor iniciado |
| `log_error` | warning | Patrón regex detectado en los logs |

## Variables de entorno clave

| Variable | Por defecto | Descripción |
|---|---|---|
| `HOST_NAME` | hostname del SO | Nombre del servidor tal como aparece en el hub |
| `RABBITMQ_HOST` | `localhost` | Host del broker RabbitMQ |
| `RABBITMQ_PORT` | `5672` | Puerto AMQP |
| `RABBITMQ_USER` | `guest` | Usuario AMQP |
| `RABBITMQ_PASSWORD` | `guest` | Contraseña AMQP |
| `RABBITMQ_VHOST` | `/` | Virtual host AMQP |
| `HUB_EXCHANGE` | `atalaya-monitor-exchange` | Exchange donde publica el agente |
| `HUB_ROUTING_KEY` | `monitor.events` | Routing key de heartbeats y alertas |
| `HEARTBEAT_INTERVAL_SECONDS` | `60` | Frecuencia de heartbeat |
| `AGENT_API_PORT` | `9092` | Puerto de la API HTTP de logs |
| `AGENT_API_TOKEN` | — | Token Bearer compartido con el hub |
| `AGENT_API_URL` | — | URL HTTP del agente vista desde el hub, ej: `http://172.16.11.42:9092` |
| `AGENT_TERMINAL_PORT` | `9093` | Puerto WebSocket de terminal |
| `AGENT_TERMINAL_URL` | — | URL WS del agente vista desde el navegador, ej: `ws://172.16.11.42:9093` |
| `ALERT_ENABLED` | `true` | Activar/desactivar alertas de email |
| `ALERT_DESTINATARIOS` | — | Emails destino separados por coma |
| `ALERT_COOLDOWN_DIED_SECONDS` | `300` | Cooldown entre alertas `died` del mismo contenedor |
| `ALERT_COOLDOWN_OOM_SECONDS` | `300` | Cooldown entre alertas OOM |
| `ALERT_COOLDOWN_UNHEALTHY_SECONDS` | `600` | Cooldown entre alertas unhealthy |
| `ALERT_COOLDOWN_CRASHLOOP_SECONDS` | `1800` | Cooldown entre alertas crashloop |
| `ALERT_CRASHLOOP_THRESHOLD` | `3` | Número de caídas para detectar crashloop |
| `ALERT_CRASHLOOP_WINDOW_SECONDS` | `300` | Ventana de tiempo para contar caídas |
| `ALERT_IGNORE_CONTAINERS` | — | Nombres (o subcadenas) de contenedores a ignorar |
| `LOG_MONITOR_ENABLED` | `false` | Activar seguimiento de logs en tiempo real |
| `LOG_MONITOR_PATTERNS` | `ERROR,CRITICAL,Exception,…` | Patrones regex separados por coma |
| `LOG_MONITOR_COOLDOWN_SECONDS` | `300` | Cooldown entre alertas de log del mismo contenedor |
| `LOG_LEVEL` | `INFO` | Nivel de log |
| `TZ` | — | Zona horaria del contenedor (ej: `Europe/Madrid`) |

## Datos que envía el agente al hub (heartbeat)

Cada heartbeat incluye, por contenedor:

- `name`, `id`, `image`, `status` — identidad y estado
- `restart_count` — total acumulado (el hub calcula el delta en 24h)
- `started_at` — timestamp de último inicio
- `cpu_pct`, `mem_pct`, `mem_mb`, `mem_limit_mb` — métricas en tiempo real
- `log_size_bytes` — tamaño del fichero de log JSON de Docker (requiere volumen `:ro`)

## Despliegue

```bash
# Primera vez: copiar y rellenar el .env
cp .env.example .env
# Editar .env con las credenciales reales

# Arrancar
docker compose up -d --build

# Ver logs
docker compose logs -f

# Parar
docker compose down
```

Acceso al agente desde el hub: `http://<IP-servidor>:9092`

## Requisitos del docker-compose.yml

```yaml
user: root                          # necesario para leer el socket Docker
volumes:
  - /var/run/docker.sock:/var/run/docker.sock          # obligatorio
  - /var/lib/docker/containers:/var/lib/docker/containers:ro  # para log_size_bytes
labels:
  monitor.skip: "true"              # el agente se excluye a sí mismo de las alertas
```

## Arquitectura de red

```
Servidor agente
  └─ docker.sock          → DockerMonitor (eventos + lista de contenedores)
  └─ docker stats API     → docker_stats.py (CPU/RAM en paralelo)
  └─ /var/lib/docker/...  → log_size_bytes (tamaño ficheros log JSON)
  └─ RabbitMQ (push)      → heartbeats + alertas al hub
  └─ :9092 (HTTP)         → API de logs (hub → agente, solo red interna)
  └─ :9093 (WS)           → terminal de contenedores (navegador → agente, solo red interna)
```

> **Limitación**: los puertos 9092 y 9093 solo son alcanzables desde la red interna.
> El hub en `hub.atalayamining.com` (Dokploy, internet) no puede llegar a IPs internas (172.16.x.x).
> Para habilitar logs/terminal desde internet se necesita Cloudflare Tunnel en cada servidor agente.

## Estado del proyecto

**Producción activa** en `test-dev` (172.16.11.42) y otros servidores.

Funcional:
- Heartbeats periódicos con CPU/RAM, restart_count, log_size_bytes
- Alertas: died, OOM, unhealthy, crashloop, log_error
- API de logs (:9092) y terminal WebSocket (:9093)

Pendiente:
- Logs y terminal desde `hub.atalayamining.com` requieren Cloudflare Tunnel en cada agente
- `AGENT_API_URL` y `AGENT_TERMINAL_URL` deben apuntar a la IP del propio servidor (no al hub)
