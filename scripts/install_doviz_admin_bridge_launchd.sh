#!/usr/bin/env bash
# macOS: VPN’li bu Mac’ten Doviz admin → Railway bridge (her 15 dk).
# Kullanım:
#   1) .env içine DOVIZ_ADMIN_EMAIL, DOVIZ_ADMIN_PASSWORD, NOTIFICATION_INGEST_TOKEN
#   2) Aynı NOTIFICATION_INGEST_TOKEN’ı Railway Variables’a ekle
#   3) ./scripts/install_doviz_admin_bridge_launchd.sh
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

[[ "${1:-}" == "--uninstall" ]] && uninstall
[[ "${1:-}" == "-h" || "${1:-}" == "--help" ]] && {
  echo "Kullanım: $0 | $0 --uninstall" >&2
  exit 0
}

[[ -x "$PY" ]] || { echo "Yok: $PY — önce .venv kurun" >&2; exit 1; }
[[ -f "$BRIDGE" ]] || { echo "Yok: $BRIDGE" >&2; exit 1; }
chmod +x "$BRIDGE" "$0" 2>/dev/null || true

# Temel env kontrolü (değerleri yazdırma)
if [[ -f "$ROOT/.env" ]]; then
  # shellcheck disable=SC1091
  set -a
  # sadece ilgili satırları yokla
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

# StartInterval = 900 sn (15 dk); RunAtLoad = hemen bir kez
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
  </array>
  <key>WorkingDirectory</key>
  <string>${ROOT}</string>
  <key>RunAtLoad</key>
  <true/>
  <key>StartInterval</key>
  <integer>900</integer>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
    <key>PYTHONUNBUFFERED</key>
    <string>1</string>
  </dict>
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
echo "Aralık: 15 dk · log: $OUT_LOG"
echo "Railway Variables’a aynı NOTIFICATION_INGEST_TOKEN’ı eklemeyi unutmayın."
