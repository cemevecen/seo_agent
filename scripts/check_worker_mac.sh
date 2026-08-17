#!/usr/bin/env bash
# Bu Mac panel kuyruğunun sağlıklı bir worker'ı mı? (ofis + ev Mac'inde aynı script)
# Kullanım: ./scripts/check_worker_mac.sh
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LABEL="com.cemevecen.doviz-admin-notification-bridge"
PLIST="${HOME}/Library/LaunchAgents/${LABEL}.plist"
LOG="${HOME}/Library/Logs/doviz-admin-notification-bridge.log"
PORT="${NOTIFICATION_BRIDGE_PORT:-18765}"
PY="${ROOT}/.venv/bin/python"
problems=0

note_ok()   { printf "  ✓ %s\n" "$1"; }
note_bad()  { printf "  ✗ %s\n" "$1"; problems=$((problems + 1)); }
note_warn() { printf "  ! %s\n" "$1"; }

echo "== Makine: $(scutil --get ComputerName 2>/dev/null || hostname)"
echo "== Repo:   ${ROOT}"

echo "-- 1) daemon"
RUNNING_CMD="$(ps -eo command= | grep "[d]oviz_admin_notification_bridge.py" | head -1)"
if [[ -z "$RUNNING_CMD" ]]; then
  note_bad "bridge daemon çalışmıyor → ./scripts/install_doviz_admin_bridge_launchd.sh"
else
  RUNNING_PATH="$(awk '{for(i=1;i<=NF;i++) if ($i ~ /doviz_admin_notification_bridge\.py$/) print $i}' <<<"$RUNNING_CMD" | head -1)"
  if [[ "$RUNNING_PATH" == "${ROOT}/scripts/doviz_admin_notification_bridge.py" ]]; then
    note_ok "daemon bu repodan çalışıyor"
  else
    note_bad "daemon BAŞKA kopyadan çalışıyor: ${RUNNING_PATH} → ./scripts/install_doviz_admin_bridge_launchd.sh"
  fi
fi

echo "-- 2) LaunchAgent"
if [[ -f "$PLIST" ]]; then
  if grep -q "${ROOT}/scripts/doviz_admin_notification_bridge.py" "$PLIST"; then
    note_ok "plist bu repoyu gösteriyor"
  else
    note_bad "plist başka yolu gösteriyor: $(grep -o '/Users/[^<]*doviz_admin_notification_bridge.py' "$PLIST" | head -1)"
  fi
  WNAME="$(grep -A1 BRIDGE_WORKER_NAME "$PLIST" | tail -1 | sed 's/.*<string>\(.*\)<\/string>.*/\1/')"
  [[ -n "$WNAME" ]] && note_ok "worker adı: ${WNAME}" || note_warn "BRIDGE_WORKER_NAME yok — otomatik ad kullanılacak (--worker-name ile sabitleyebilirsin)"
else
  note_bad "plist yok → ./scripts/install_doviz_admin_bridge_launchd.sh"
fi

echo "-- 3) çalışan sürüm"
CODE="$(curl -s -o /dev/null -m 5 -w '%{http_code}' "http://127.0.0.1:${PORT}/status" 2>/dev/null)"
if [[ "$CODE" == "200" ]]; then
  note_ok "bridge :${PORT} yanıt veriyor (güncel sürüm)"
elif [[ "$CODE" == "404" ]]; then
  note_bad "bridge eski sürüm çalıştırıyor (/status yok) → yeniden kur"
else
  note_bad "bridge :${PORT} yanıt vermiyor (HTTP '${CODE:-yok}')"
fi
if [[ -f "$LOG" ]] && grep -q "Uzaktan tarama kuyru" "$LOG"; then
  note_ok "panel kuyruğu (claim loop) log'da görünüyor"
else
  note_warn "log'da claim loop satırı yok — daemon yeni başladıysa normal"
fi

echo "-- 4) Playwright Firefox"
if [[ -x "$PY" ]] && "$PY" - <<'PYCHECK'
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
  note_ok "playwright firefox revizyonu yerinde"
else
  note_bad "playwright firefox eksik → ${PY} -m playwright install firefox"
fi

echo "-- 5) .env anahtarları (yalnızca var/yok)"
for key in NOTIFICATION_INGEST_TOKEN DOVIZ_ADMIN_EMAIL DOVIZ_ADMIN_PASSWORD VIRGUL_EMAIL VIRGUL_PASSWORD; do
  if grep -q "^${key}=.\+" "${ROOT}/.env" 2>/dev/null; then
    note_ok "${key} var"
  else
    note_warn "${key} YOK — bu makine ilgili işi alamaz (kuyruk diğer Mac'e yönlendirir)"
  fi
done

echo "-- 6) bu makinenin kuyruğa bildirdiği kabiliyet"
if [[ -x "$PY" ]]; then
  # Daemon'un gördüğü adı raporla (plist'teki BRIDGE_WORKER_NAME)
  [[ -n "${WNAME:-}" ]] && export BRIDGE_WORKER_NAME="$WNAME"
  (cd "$ROOT" && "$PY" - <<'PYREADY'
import importlib.util
spec = importlib.util.spec_from_file_location("bridge", "scripts/doviz_admin_notification_bridge.py")
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
ready = mod._worker_readiness()
bad = {k: v for k, v in ready.items() if v != "ready"}
print(f"  worker adı : {mod._worker_name()}")
print(f"  zamanlı iş : {'açık' if mod.BRIDGE_AUTO_JOBS else 'kapalı'}")
print(f"  hazır      : {sum(1 for v in ready.values() if v == 'ready')}/{len(ready)} iş")
print(f"  eksik      : {bad or 'yok'}")
PYREADY
  ) 2>/dev/null || note_warn "kabiliyet raporu alınamadı"
fi

echo
if [[ "$problems" -eq 0 ]]; then
  echo "SONUÇ: bu Mac sağlıklı bir worker."
else
  echo "SONUÇ: ${problems} sorun var — yukarıdaki ✗ satırlarını uygula."
fi
exit 0
