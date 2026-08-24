# Yerel / docker-compose ile uyumlu minimal imaj (Railway Nixpacks’tan bağımsız).
# Playwright/Firefox/Selenium YOK — browser scrape yalnız Mac bridge (masrafsız yerel).
FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

ARG GIT_COMMIT=
ENV GIT_COMMIT=${GIT_COMMIT}

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Playwright/Firefox/Selenium: imaja girmez (platform Darwin marker + Mac bridge).
# Acil bulut debug: ALLOW_BROWSER_SCRAPE_ON_RAILWAY=1 + requirements-mac + elle install.

COPY . .

RUN rm -rf /tmp/* /var/tmp/* /var/cache/apt/* || true

EXPOSE 8012

CMD ["sh", "-c", "exec uvicorn backend.main:app --host 0.0.0.0 --port ${PORT:-8012}"]
