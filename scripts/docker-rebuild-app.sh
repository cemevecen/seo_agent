#!/usr/bin/env bash
# seo_agent app yansımasını .env + güncel kaynakla ayağa kaldırır (8012).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
docker compose build app
docker compose up -d --force-recreate app
