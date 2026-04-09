#!/usr/bin/env bash
# Yerel SEO Agent (varsayılan 8012) GET smoke testi — curl takılmasın diye --max-time kullanır.
# Kullanım: ./scripts/smoke-test-local.sh
#          BASE=http://127.0.0.1:8013 ./scripts/smoke-test-local.sh
set -euo pipefail

BASE="${BASE:-http://127.0.0.1:8012}"
MAXT="${SMOKE_MAX_TIME:-30}"
# Dashboard / GA4 liste / mağaza benchmark yavaş olabilir
SLOW="${SMOKE_SLOW_TIME:-90}"

echo "BASE=$BASE  (SMOKE_MAX_TIME=${MAXT}s, SMOKE_SLOW_TIME=${SLOW}s)"
echo ""

fail=0
http_ok() {
  local c="$1"
  [[ "$c" =~ ^[23][0-9]{2}$ ]]
}

check() {
  local name="$1"
  local url="$2"
  local mt="${3:-$MAXT}"
  local code
  code=$(curl -sS -L -o /dev/null --max-time "$mt" -w "%{http_code}" "$url" 2>/dev/null) || code="000"
  code=$(printf '%s' "$code" | tr -d '\r\n')
  [[ ${#code} -ne 3 ]] && code="000"
  if http_ok "$code"; then
    printf "OK  %3s  %s\n" "$code" "$name"
  else
    printf "BAD %3s  %s  (%s)\n" "$code" "$name" "$url"
    fail=$((fail + 1))
  fi
}

# 200 veya 404 kabul (bazı build'lerde rota yok)
check_200_or_404() {
  local name="$1"
  local url="$2"
  local mt="${3:-$MAXT}"
  local code
  code=$(curl -sS -L -o /dev/null --max-time "$mt" -w "%{http_code}" "$url" 2>/dev/null) || code="000"
  code=$(printf '%s' "$code" | tr -d '\r\n')
  [[ ${#code} -ne 3 ]] && code="000"
  if [[ "$code" == "200" || "$code" == "404" ]]; then
    printf "OK  %3s  %s  (200 veya 404)\n" "$code" "$name"
  else
    printf "BAD %3s  %s  (%s)\n" "$code" "$name" "$url"
    fail=$((fail + 1))
  fi
}

echo "=== Temel ==="
check "GET /" "$BASE/" "$SLOW"
check "GET /health" "$BASE/health"
check "GET /favicon.ico" "$BASE/favicon.ico"

echo ""
echo "=== Sayfalar (HTML) ==="
check "GET /alerts" "$BASE/alerts"
check "GET /settings" "$BASE/settings"
check "GET /settings/site-list" "$BASE/settings/site-list"
check "GET /settings/alert-thresholds" "$BASE/settings/alert-thresholds"
check "GET /ai" "$BASE/ai"
check "GET /ga4" "$BASE/ga4"
check "GET /ga4/site-list" "$BASE/ga4/site-list" "$SLOW"
check "GET /app" "$BASE/app"
check "GET /search-console" "$BASE/search-console"
check "GET /search-console/site-list" "$BASE/search-console/site-list"
check "GET /search-console/health" "$BASE/search-console/health"
check "GET /external" "$BASE/external"
check "GET /public-sites" "$BASE/public-sites"
check "GET /external/site-list" "$BASE/external/site-list"
check "GET /public-sites/site-list" "$BASE/public-sites/site-list"
check_200_or_404 "GET /design/lighthouse-minimal-options" "$BASE/design/lighthouse-minimal-options"

echo ""
echo "=== Admin (okuma) ==="
check "GET /admin/db-size" "$BASE/admin/db-size"
check "GET /admin/sc-scope-stats" "$BASE/admin/sc-scope-stats"

echo ""
echo "=== API (JSON) ==="
check "GET /api/sites" "$BASE/api/sites"
check "GET /api/alerts" "$BASE/api/alerts"
check "GET /api/app/intel?product=doviz&period=30" "$BASE/api/app/intel?product=doviz&period=30"
check "GET /api/app/aso?product=doviz" "$BASE/api/app/aso?product=doviz"
# benchmark: parametresiz 400 beklenir — ayrı satırda kontrol
bc=$(curl -sS -o /dev/null --max-time "$MAXT" -w "%{http_code}" "$BASE/api/app/aso/benchmark" 2>/dev/null) || bc="000"
bc=$(printf '%s' "$bc" | tr -d '\r\n')
[[ ${#bc} -ne 3 ]] && bc="000"
printf "%s  GET /api/app/aso/benchmark (beklenen: 400 pair_required) -> %s\n" "$([[ "$bc" == "400" ]] && echo OK || echo '!!')" "$bc"
[[ "$bc" == "400" ]] || fail=$((fail + 1))

bc2=$(curl -sS -o /dev/null --max-time "$SLOW" -w "%{http_code}" "$BASE/api/app/aso/benchmark?period=30&android_packages=com.foo.app,com.bar.app" 2>/dev/null) || bc2="000"
bc2=$(printf '%s' "$bc2" | tr -d '\r\n')
[[ ${#bc2} -ne 3 ]] && bc2="000"
if http_ok "$bc2"; then
  printf "OK  %3s  GET /api/app/aso/benchmark (2 fake android packages)\n" "$bc2"
else
  printf "BAD %3s  GET /api/app/aso/benchmark (2 fake android packages)\n" "$bc2"
  fail=$((fail + 1))
fi

echo ""
echo "=== Yanlış yol (404 beklenir) ==="
nf=$(curl -sS -o /dev/null --max-time "$MAXT" -w "%{http_code}" "$BASE/alerts/health" 2>/dev/null || echo "000")
printf "%s  GET /alerts/health -> %s (beklenen 404)\n" "$([[ "$nf" == "404" ]] && echo OK || echo '!!')" "$nf"
[[ "$nf" == "404" ]] || fail=$((fail + 1))

echo ""
if [[ "$fail" -eq 0 ]]; then
  echo "Özet: Tüm kontroller geçti (benchmark 400 ve alerts/health 404 dahil)."
  exit 0
else
  echo "Özet: $fail uyarı/başarısız satır — yukarıdaki BAD/!! satırlarına bak."
  exit 1
fi
