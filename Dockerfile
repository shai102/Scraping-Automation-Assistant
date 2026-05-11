# ── Build stage ──────────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

# Install build-time system packages (needed to compile some Python wheels)
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libffi-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements-docker.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements-docker.txt


# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM python:3.11-slim

LABEL maintainer="自动监控刮削助手"
LABEL description="媒体文件自动归档与元数据刮削工具"

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application source (web/dist is included to serve the frontend)
COPY . .

# ─── Persistent data directory ───────────────────────────────────────────────
# Everything that must survive container restarts lives here:
#   media_renamer.db   — SQLite database
#   renamer_config.json — user configuration
#   api_cache.json      — TMDB/BGM API cache
#   media_renamer.log   — error log
ENV DATA_DIR=/data
VOLUME ["/data"]

# ─── Port ────────────────────────────────────────────────────────────────────
ENV PORT=8090
EXPOSE 8090

# ─── Timezone ────────────────────────────────────────────────────────────────
ENV TZ=Asia/Shanghai

# ─── Entrypoint ──────────────────────────────────────────────────────────────
CMD ["python", "docker_main.py"]
