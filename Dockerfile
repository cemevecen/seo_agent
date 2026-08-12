# Yerel / docker-compose ile uyumlu minimal imaj (Railway Nixpacks’tan bağımsız).
FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

ARG GIT_COMMIT=
ENV GIT_COMMIT=${GIT_COMMIT}

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev \
    # Playwright Firefox sistem bağımlılıkları
    libgtk-3-0 libdbus-glib-1-2 libxt6 libx11-xcb1 libasound2 \
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 libxkbcommon0 \
    libxcomposite1 libxdamage1 libxfixes3 libxrandr2 libgbm1 \
    libpango-1.0-0 libpangocairo-1.0-0 libcairo2 libcairo-gobject2 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Tarama tarayıcısı: yalnızca Firefox (Chrome/Chromium kurulmaz)
RUN playwright install firefox \
    && find /root/.cache/ms-playwright -name "*.map" -delete || true

COPY . .

# Build sonrası geçici dosyaları temizle (runtime'ı etkilemez)
RUN rm -rf /tmp/* /var/tmp/* /var/cache/apt/* || true

EXPOSE 8012

# Railway/Render gibi ortamlarda PORT dinamik verilir. JSON-array CMD'de shell expansion
# çalışmadığı için sh -c kullanıyoruz (PORT yoksa 8012).
CMD ["sh", "-c", "exec uvicorn backend.main:app --host 0.0.0.0 --port ${PORT:-8012}"]
