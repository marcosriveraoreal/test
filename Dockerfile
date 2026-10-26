FROM python:3.12-slim

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
        libffi-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Código fuente
COPY src/ ./src/

# Usuario sin privilegios (el docker-compose usa user: root para acceso al socket)
RUN useradd --create-home --uid 10001 monitor \
    && mkdir -p /app/state \
    && chown -R monitor:monitor /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src

CMD ["python", "/app/src/main.py"]
