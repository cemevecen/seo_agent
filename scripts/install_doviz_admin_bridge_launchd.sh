#!/usr/bin/env bash
# macOS: VPN Mac köprüsü — sürekli daemon (bildirim/haber/virgül ~30 dk + Elle yenile)
# Kullanım: ./scripts/install_doviz_admin_bridge_launchd.sh
# Kaldırma: ./scripts/install_doviz_admin_bridge_launchd.sh --uninstall
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LABEL="com.cemevecen.doviz-admin-notification-bridge"
PLIST_NAME="${LABEL}.plist"
AGENTS_DIR="${HOME}/Library/LaunchAgents"
PLIST_DST="${AGENTS_DIR}/${PLIST_NAME}"
PY="${ROOT}/.venv/bin/python"
BRIDGE="${ROOT}/scripts/doviz_admin_notification_bridge.py"
LOG_DIR="${HOME}/Library/Logs"
OUT_LOG="${LOG_DIR}/doviz-admin-notification-bridge.log"
ERR_LOG="${LOG_DIR}/doviz-admin-notification-bridge.err"

uninstall() {
  launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null \
    || launchctl bootout "gui/$(id -u)" "$PLIST_DST" 2>/dev/null \
    || launchctl unload "$PLIST_DST" 2>/dev/null \
    || true
  rm -f "$PLIST_DST"
  echo "Kaldırıldı: $PLIST_DST"
  exit 0
}

WORKER_NAME=""
AUTO_JOBS=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --uninstall) uninstall ;;
    -h|--help)
      cat >&2 <<'USAGE'
Kullanım:
  ./scripts/install_doviz_admin_bridge_launchd.sh [--worker-name AD] [--no-auto-jobs]
  ./scripts/install_doviz_admin_bridge_launchd.sh --uninstall

  --worker-name AD   Panelde görünecek makine adı (ör. cem-office-mac / cem-home-mac).
                     Verilmezse bilgisayar adı + donanım soneki kullanılır.
  --no-auto-jobs     Zamanlı taramalar bu makinede koşmasın; yalnızca panel kuyruğu
                     işlensin (BRIDGE_AUTO_JOBS=0). İkincil Mac için uygundur.
USAGE
      exit 0
      ;;
    --worker-name) WORKER_NAME="${2:-}"; shift 2; continue ;;
    --worker-name=*) WORKER_NAME="${1#*=}"; shift; continue ;;
    --no-auto-jobs) AUTO_JOBS="0"; shift; continue ;;
    *) echo "Bilinmeyen argüman: $1" >&2; exit 1 ;;
  esac
done

[[ -x "$PY" ]] || { echo "Yok: $PY — önce .venv kurun" >&2; exit 1; }
[[ -f "$BRIDGE" ]] || { echo "Yok: $BRIDGE" >&2; exit 1; }
chmod +x "$BRIDGE" "$0" 2>/dev/null || true

# Playwright Firefox: ASC/CWV/Play/Moderation bu binary olmadan çalışmaz.
if ! "$PY" - <<'PYCHECK'
import json, sys
from pathlib import Path
try:
    import playwright
except Exception:
    sys.exit(1)
meta = Path(playwright.__file__).resolve().parent / "driver" / "package" / "browsers.json"
rev = ""
for entry in json.loads(meta.read_text(encoding="utf-8")).get("browsers", []):
    if entry.get("name") == "firefox":
        rev = str(entry.get("revision") or "")
sys.exit(0 if rev and (Path.home() / "Library/Caches/ms-playwright" / f"firefox-{rev}").exists() else 1)
PYCHECK
then
  echo "Playwright Firefox eksik — kuruluyor (~85 MB)…"
  "$PY" -m playwright install firefox || echo "Uyarı: playwright install firefox başarısız." >&2
fi

if [[ -f "$ROOT/.env" ]]; then
  if ! grep -q '^DOVIZ_ADMIN_EMAIL=.\+' "$ROOT/.env" 2>/dev/null; then
    echo "Uyarı: .env içinde DOVIZ_ADMIN_EMAIL dolu görünmüyor." >&2
  fi
  if ! grep -q '^DOVIZ_ADMIN_PASSWORD=.\+' "$ROOT/.env" 2>/dev/null; then
    echo "Uyarı: .env içinde DOVIZ_ADMIN_PASSWORD dolu görünmüyor." >&2
  fi
  if ! grep -q '^NOTIFICATION_INGEST_TOKEN=.\+' "$ROOT/.env" 2>/dev/null; then
    echo "Uyarı: .env içinde NOTIFICATION_INGEST_TOKEN dolu görünmüyor." >&2
  fi
fi

mkdir -p "$AGENTS_DIR" "$LOG_DIR"
launchctl bootout "gui/$(id -u)" "$PLIST_DST" 2>/dev/null || true
rm -f "$PLIST_DST"

# Opsiyonel env anahtarları (plist içine gömülür)
EXTRA_ENV=""
if [[ -n "$WORKER_NAME" ]]; then
  EXTRA_ENV+="    <key>BRIDGE_WORKER_NAME</key>"$'\n'"    <string>${WORKER_NAME}</string>"$'\n'
fi
if [[ -n "$AUTO_JOBS" ]]; then
  EXTRA_ENV+="    <key>BRIDGE_AUTO_JOBS</key>"$'\n'"    <string>${AUTO_JOBS}</string>"$'\n'
fi

# KeepAlive daemon: --daemon → ~30 dk auto (nt/news/virgul) + http://127.0.0.1:18765/sync
cat > "$PLIST_DST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>${PY}</string>
    <string>${BRIDGE}</string>
    <string>--daemon</string>
  </array>
  <key>WorkingDirectory</key>
  <string>${ROOT}</string>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
    <key>PYTHONUNBUFFERED</key>
    <string>1</string>
${EXTRA_ENV}  </dict>
  <key>StandardOutPath</key>
  <string>${OUT_LOG}</string>
  <key>StandardErrorPath</key>
  <string>${ERR_LOG}</string>
</dict>
</plist>
EOF

launchctl bootstrap "gui/$(id -u)" "$PLIST_DST"
launchctl enable "gui/$(id -u)/$LABEL" 2>/dev/null || true
launchctl kickstart -k "gui/$(id -u)/$LABEL" 2>/dev/null || true

echo "Kuruldu: $PLIST_DST"
echo "Repo: $ROOT"
echo "Worker adı: ${WORKER_NAME:-<bilgisayar adı + donanım soneki>}"
echo "Zamanlı taramalar: ${AUTO_JOBS:+kapalı (yalnızca panel kuyruğu)}${AUTO_JOBS:-açık (Railway kirası ile tekilleştirilir)}"
echo "Daemon: nt 30dk · news · virgul 04/07/13 · play/asc · 2×/gün gsc/policy/speed/noads · Elle → :18765/sync*"
echo "Log: $OUT_LOG"
echo "Kontrol: ./scripts/check_worker_mac.sh"
