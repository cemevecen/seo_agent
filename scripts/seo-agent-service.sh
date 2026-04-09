#!/usr/bin/env bash
# SEO Agent arka plan servisi (LaunchAgent) — terminale bağlı kalmadan çalışır.
# Kurulum (bir kez): ./scripts/install_launchd_startup_check.sh
#
# Kullanım:
#   ./scripts/seo-agent-service.sh status | start | stop | restart | logs
set -euo pipefail

LABEL="com.cemevecen.seo-agent"
PLIST="${HOME}/Library/LaunchAgents/${LABEL}.plist"
OUT_LOG="${HOME}/Library/Logs/seo-agent-boot.log"
ERR_LOG="${HOME}/Library/Logs/seo-agent-boot.err"
UID_NUM="$(id -u)"

cmd="${1:-status}"

health() {
  curl -sf --max-time 2 "http://127.0.0.1:8012/health" && echo "" && return 0
  return 1
}

case "$cmd" in
  status)
    if [[ -f "$PLIST" ]]; then
      echo "LaunchAgent plist: $PLIST"
      launchctl print "gui/${UID_NUM}/${LABEL}" 2>/dev/null | head -20 || echo "(launchctl print başarısız — plist yüklü olmayabilir)"
    else
      echo "Kurulu değil: $PLIST"
      echo "Kur: $(dirname "$0")/install_launchd_startup_check.sh"
    fi
    echo ""
    if health; then
      echo "/health: OK"
    else
      echo "/health: yanıt yok (sunucu kapalı veya henüz ayağa kalkmadı)"
    fi
    ;;
  start)
    if [[ ! -f "$PLIST" ]]; then
      echo "Önce kurulum: $(dirname "$0")/install_launchd_startup_check.sh" >&2
      exit 1
    fi
    launchctl kickstart -k "gui/${UID_NUM}/${LABEL}" 2>/dev/null || \
      launchctl bootstrap "gui/${UID_NUM}" "$PLIST" 2>/dev/null || \
      launchctl load -w "$PLIST"
    sleep 2
    health || true
    ;;
  stop)
    # KeepAlive + SuccessfulExit=false: düzgün kapanışta yeniden başlatılmaz.
    launchctl kill TERM "gui/${UID_NUM}/${LABEL}" 2>/dev/null || true
    sleep 1
    if health 2>/dev/null; then
      echo "Hâlâ yanıt veriyor; süreç kill edilemedi (manuel: lsof -i :8012)."
    else
      echo "Durduruldu (port 8012 serbest olmalı)."
    fi
    ;;
  restart)
    launchctl kickstart -k "gui/${UID_NUM}/${LABEL}" 2>/dev/null || {
      "$0" start
    }
    sleep 2
    health || true
    ;;
  logs)
    echo "=== stdout: $OUT_LOG ==="
    tail -n 80 "$OUT_LOG" 2>/dev/null || echo "(dosya yok)"
    echo ""
    echo "=== stderr: $ERR_LOG ==="
    tail -n 80 "$ERR_LOG" 2>/dev/null || echo "(dosya yok)"
    ;;
  -h|--help|help)
    echo "Kullanım: $0 status|start|stop|restart|logs"
    echo "Kurulum: $(dirname "$0")/install_launchd_startup_check.sh"
    ;;
  *)
    echo "Bilinmeyen komut: $cmd" >&2
    exit 1
    ;;
esac
