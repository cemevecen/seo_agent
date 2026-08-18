#!/usr/bin/env bash
# Yeni bir Mac'i bu projenin worker'ı yap — tek komut, tekrar çalıştırmaya güvenli.
#
#   ./scripts/setup_mac.sh              # venv + bağımlılıklar + playwright + köprü
#   ./scripts/setup_mac.sh --no-bridge  # yalnızca çalışma ortamı (LaunchAgent kurma)
#   ./scripts/setup_mac.sh --with-server# ayrıca 127.0.0.1:8012 yerel paneli
#
# Neden gerekiyor: köprü kurulum betiği `.venv`'in hazır olmasını bekliyor ve
# yoksa çıkıyor. Ofis Mac'inde venv hiç yoktu; sonuç olarak playwright de
# kurulmuyor, warm-up elle çalıştırılınca "No module named 'playwright'"
# veriyordu. Bu betik o boşluğu kapatır.
#
# Gizli bilgi kurmaz: .env ve Keychain kayıtları makineye özeldir, onları
# ayrıca sen girersin (bkz. betik sonundaki özet).
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

WITH_BRIDGE=1
WITH_SERVER=0
for arg in "$@"; do
  case "$arg" in
    --no-bridge) WITH_BRIDGE=0 ;;
    --with-server) WITH_SERVER=1 ;;
    -h|--help) sed -n '2,12p' "$0"; exit 0 ;;
    *) echo "Bilinmeyen seçenek: $arg" >&2; exit 2 ;;
  esac
done

PY="${ROOT}/.venv/bin/python"
problems=0
step()  { printf "\n== %s\n" "$1"; }
ok()    { printf "   ✓ %s\n" "$1"; }
bad()   { printf "   ✗ %s\n" "$1"; problems=$((problems + 1)); }
warn()  { printf "   ! %s\n" "$1"; }

echo "Makine: $(scutil --get ComputerName 2>/dev/null || hostname)"
echo "Repo  : ${ROOT}"

# ── 1) Sanal ortam ───────────────────────────────────────────────────────────
step "1) Python sanal ortamı"
if [[ -x "$PY" ]]; then
  ok "zaten var — $($PY -V 2>&1)"
else
  BASE_PY="$(command -v python3.14 || command -v python3 || true)"
  if [[ -z "$BASE_PY" ]]; then
    bad "python3 bulunamadı. Homebrew: brew install python@3.14"
    exit 1
  fi
  echo "   venv kuruluyor ($BASE_PY)…"
  if "$BASE_PY" -m venv "${ROOT}/.venv"; then
    ok "kuruldu — $($PY -V 2>&1)"
  else
    bad "venv kurulamadı"
    exit 1
  fi
fi

# ── 2) Bağımlılıklar ─────────────────────────────────────────────────────────
step "2) Bağımlılıklar (requirements.txt)"
"$PY" -m pip install --quiet --upgrade pip >/dev/null 2>&1 || true
if "$PY" -m pip install --quiet -r "${ROOT}/requirements.txt"; then
  ok "kuruldu / güncel"
else
  bad "pip install başarısız — çıktıyı görmek için: $PY -m pip install -r requirements.txt"
fi

# ── 3) Playwright tarayıcısı ────────────────────────────────────────────────
step "3) Playwright + Firefox"
if "$PY" -c "import playwright" 2>/dev/null; then
  ok "playwright modülü var"
else
  bad "playwright modülü kurulamadı (2. adıma bak)"
fi
# Firefox indirilmemişse çek; kuruluysa hızlıca geçer.
if "$PY" -m playwright install firefox >/dev/null 2>&1; then
  ok "firefox hazır"
else
  warn "playwright install firefox başarısız — ağ/proxy kontrol et"
fi

# ── 3b) geckodriver (kalıcı pencere için) ───────────────────────────────────
# Ayrık pencere (köprü yeniden başlasa da yaşayan oturum) geckodriver ister.
# Selenium Manager ilk kullanımda indirir; burada peşinen indirilir ki ilk
# tarama sessizce eski davranışa düşmesin.
step "3b) geckodriver (kalıcı oturum penceresi)"
GECKO_PATH="$("$PY" - <<'PYEOF' 2>/dev/null
try:
    from selenium.webdriver.common.selenium_manager import SeleniumManager
    out = SeleniumManager().binary_paths(["--browser", "firefox"])
    print((out or {}).get("driver_path") or "")
except Exception:
    print("")
PYEOF
)"
if [[ -n "$GECKO_PATH" && -x "$GECKO_PATH" ]]; then
  ok "hazır — $GECKO_PATH"
else
  warn "indirilemedi — kalıcı pencere devre dışı kalır (ağ/proxy kontrol et)"
fi

# ── 4) Köprü LaunchAgent ────────────────────────────────────────────────────
if [[ "$WITH_BRIDGE" == "1" ]]; then
  step "4) Köprü LaunchAgent (zamanlanmış scrape'ler)"
  if [[ -f "${ROOT}/scripts/install_doviz_admin_bridge_launchd.sh" ]]; then
    chmod +x "${ROOT}/scripts/install_doviz_admin_bridge_launchd.sh" 2>/dev/null || true
    if bash "${ROOT}/scripts/install_doviz_admin_bridge_launchd.sh"; then
      ok "köprü kuruldu / yeniden yüklendi"
    else
      bad "köprü kurulumu başarısız"
    fi
  else
    bad "install_doviz_admin_bridge_launchd.sh bulunamadı"
  fi
else
  step "4) Köprü LaunchAgent — atlandı (--no-bridge)"
fi

# ── 5) Yerel panel (opsiyonel) ──────────────────────────────────────────────
if [[ "$WITH_SERVER" == "1" ]]; then
  step "5) Yerel panel 127.0.0.1:8012"
  if bash "${ROOT}/scripts/install_launchd_startup_check.sh"; then
    ok "panel servisi kuruldu"
  else
    bad "panel servisi kurulamadı"
  fi
fi

# ── 6) Teşhis ───────────────────────────────────────────────────────────────
step "6) Teşhis"
"$PY" "${ROOT}/scripts/scrape_login_warmup.py" --doctor --no-browser || problems=$((problems + 1))

# ── Özet ────────────────────────────────────────────────────────────────────
cat <<'SUMMARY'

────────────────────────────────────────────────────────────
Makineye özel olan ve bu betiğin KURMADIĞI iki şey:

  1) .env  — gizli ayarlar (SMTP, ingest token, DB). Diğer Mac'ten
     güvenli biçimde kopyala; repoya girmez.

  2) Keychain kimlikleri — parolayı yalnızca sen girersin:
       security add-generic-password -U -s seo-agent-asc    -a "APPLE-ID" -w
       security add-generic-password -U -s seo-agent-google -a "GOOGLE-EPOSTA" -w

Kurulum sonrası tam kontrol:
  ./scripts/check_worker_mac.sh
  .venv/bin/python scripts/scrape_login_warmup.py --doctor
────────────────────────────────────────────────────────────
SUMMARY

if [[ "$problems" -gt 0 ]]; then
  echo "SONUÇ: ${problems} sorun var — yukarıdaki ✗ satırlarına bak"
  exit 1
fi
echo "SONUÇ: kurulum tamam"
