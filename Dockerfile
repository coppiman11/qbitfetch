FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# Avoids writing .pyc, makes logs unbuffered
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install lftp for fast SFTP transfers + openssh-client (needed by lftp for sftp://)
RUN apt-get update && apt-get install -y --no-install-recommends lftp openssh-client \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir --break-system-packages qbittorrent-api paramiko

# Copy scripts and assets
COPY qbit_sftp_pull.py /app/qbit_sftp_pull.py
COPY dashboard.py /app/dashboard.py
COPY icon.png /app/icon.png

# Create default download dir inside container (we'll mount a volume here)
RUN mkdir -p /data

# Default envs (override at runtime)
ENV LOCAL_DIR=/data
ENV POLL_SECONDS=30
ENV SKIP_IF_EXISTS=1
ENV SSH_KEEPALIVE=15
ENV DASHBOARD_PORT=8686

EXPOSE 8686

CMD ["bash", "-c", "umask 0000 && python /app/qbit_sftp_pull.py"]
