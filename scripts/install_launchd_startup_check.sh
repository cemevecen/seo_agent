#!/usr/bin/env bash
# macOS: SEO Agent’ı oturum açılışında başlatır (LaunchAgent).
# Kullanım: ./scripts/install_launchd_startup_check.sh
# İzin hatası alırsan: bash ./scripts/install_launchd_startup_check.sh
# Kaldırma: ./scripts/install_launchd_startup_check.sh --uninstall
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Git klonunda +x kaybolabiliyor; kurulum öncesi script’leri çalıştırılabilir yap.
for _sf in "$ROOT/scripts/install_launchd_startup_check.sh" "$ROOT/scripts/seo-agent-service.sh" "$ROOT/scripts/seo-agent-boot.sh"; do
  [[ -f "$_sf" ]] && chmod +x "$_sf"
done
LABEL="com.cemevecen.seo-agent"
PLIST_NAME="${LABEL}.plist"
AGENTS_DIR="${HOME}/Library/LaunchAgents"
PLIST_DST="${AGENTS_DIR}/${PLIST_NAME}"
BOOT_SCRIPT="${ROOT}/scripts/seo-agent-boot.sh"
LOG_DIR="${HOME}/Library/Logs"
OUT_LOG="${LOG_DIR}/seo-agent-boot.log"
ERR_LOG="${LOG_DIR}/seo-agent-boot.err"

usage() {
  echo "Kullanım: $0 | $0 --uninstall" >&2
}

uninstall() {
  launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null \
    || launchctl bootout "gui/$(id -u)" "$PLIST_DST" 2>/dev/null \
    || launchctl unload "$PLIST_DST" 2>/dev/null \
    || true
  rm -f "$PLIST_DST"
  echo "Kaldırıldı: $PLIST_DST"
  exit 0
}

[[ "${1:-}" == "-h" || "${1:-}" == "--help" ]] && { usage; exit 0; }
[[ "${1:-}" == "--uninstall" ]] && uninstall

[[ -x "$BOOT_SCRIPT" ]] || chmod +x "$BOOT_SCRIPT"

mkdir -p "$AGENTS_DIR" "$LOG_DIR"

# bootout eski yükleme (varsa)
launchctl bootout "gui/$(id -u)" "$PLIST_DST" 2>/dev/null || true
rm -f "$PLIST_DST"

cat > "$PLIST_DST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>${BOOT_SCRIPT}</string>
  </array>
  <key>WorkingDirectory</key>
  <string>${ROOT}</string>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <dict>
    <key>SuccessfulExit</key>
    <false/>
  </dict>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
  </dict>
  <key>StandardOutPath</key>
  <string>${OUT_LOG}</string>
  <key>StandardErrorPath</key>
  <string>${ERR_LOG}</string>
  <key>ThrottleInterval</key>
  <integer>10</integer>
</dict>
</plist>
EOF

chmod 644 "$PLIST_DST"

if launchctl bootstrap "gui/$(id -u)" "$PLIST_DST" 2>/dev/null; then
  echo "Yüklendi (bootstrap): $PLIST_DST"
elif launchctl load -w "$PLIST_DST" 2>/dev/null; then
  echo "Yüklendi (load): $PLIST_DST"
else
  echo "HATA: launchctl ile plist yüklenemedi." >&2
  exit 1
fi

echo "İlk sağlık kontrolü (LaunchAgent bazen 10–20 sn gecikebilir)…"
_health_ok=""
for _i in $(seq 1 30); do
  if curl -sf --max-time 2 "http://127.0.0.1:8012/health" >/dev/null; then
    _health_ok=1
    break
  fi
  sleep 1
done
if [[ -n "$_health_ok" ]]; then
  curl -s --max-time 2 "http://127.0.0.1:8012/health"
  echo ""
  echo "Tamam: /health yanıt veriyor."
else
  echo "Uyarı: 30 sn içinde /health yanıt vermedi."
  echo "Loglar: $OUT_LOG ve $ERR_LOG"
  echo "Manuel: curl -s http://127.0.0.1:8012/health"
  echo "Durum: ${ROOT}/scripts/seo-agent-service.sh status"
fi
echo ""
echo "Terminale bağlı kalmadan çalışır; durum/stop: ${ROOT}/scripts/seo-agent-service.sh status"
