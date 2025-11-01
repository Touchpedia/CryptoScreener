FROM python:3.11-slim

# system deps (agar psycopg/pgclient ki zarurat ho)
RUN apt-get update -y && apt-get install -y --no-install-recommends build-essential && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dependencies install
COPY requirements.txt /app/requirements.txt
ENV PIP_DEFAULT_TIMEOUT=240 \
    PIP_NO_CACHE_DIR=1

RUN pip install --upgrade pip && \
    pip install --progress-bar off --prefer-binary -r /app/requirements.txt

# App code
COPY . /app

# FastAPI port (compose command run karega)
EXPOSE 8000
