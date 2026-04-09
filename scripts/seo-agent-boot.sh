#!/usr/bin/env bash
# Girişte LaunchAgent tarafından çalıştırılır: Docker varsa compose, yoksa yerel venv + uvicorn.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
export PATH="/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:$PATH"

log() { echo "[seo-agent-boot] $(date '+%Y-%m-%d %H:%M:%S') $*" >&2; }

wait_for_docker() {
  local max="${1:-90}"
  local i
  for ((i = 1; i <= max; i++)); do
    if docker info >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

start_native() {
  if [[ ! -x "$ROOT/.venv/bin/python" ]]; then
    log "Yerel .venv yok. Önce: cd \"$ROOT\" && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt"
    exit 1
  fi
  log "Yerel uvicorn başlatılıyor (127.0.0.1:8012)…"
  exec "$ROOT/.venv/bin/python" -m uvicorn backend.main:app --host 127.0.0.1 --port 8012
}

# Açıkça yerel mod (Docker yüklü olsa bile)
if [[ "${SEO_AGENT_NATIVE:-}" == "1" ]] || [[ -f "$ROOT/.use-native-startup" ]]; then
  start_native
fi

if [[ -f "$ROOT/docker-compose.yml" ]] && command -v docker >/dev/null 2>&1; then
  if wait_for_docker; then
    log "docker compose up -d…"
    docker compose -f "$ROOT/docker-compose.yml" up -d --remove-orphans
    # LaunchAgent KeepAlive için süreç çıkmamalı: log akışı ön planda kalır (container'lar zaten restart: unless-stopped).
    log "Docker stack ayakta; app konteyner logları izleniyor (LaunchAgent işi canlı kalır)."
    exec docker compose -f "$ROOT/docker-compose.yml" logs -f --tail=20 app
  fi
  log "Docker daemon hazır değil; yerel sunucuya geçiliyor."
fi

start_native
