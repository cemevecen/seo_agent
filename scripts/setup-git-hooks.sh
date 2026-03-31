#!/usr/bin/env bash
# Bir kez çalıştır: git hook'larını bu repodaki scripts/git-hooks ile bağlar.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
chmod +x scripts/git-hooks/pre-commit 2>/dev/null || true
git config core.hooksPath scripts/git-hooks
echo "OK: core.hooksPath = scripts/git-hooks"
echo "     Pre-commit aktif. Test: git add bir dosya && git commit -m test"
