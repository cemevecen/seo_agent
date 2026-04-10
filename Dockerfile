# Yerel / docker-compose ile uyumlu minimal imaj (Railway Nixpacks’tan bağımsız).
FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev \
    # Chromium (Playwright) bağımlılıkları
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 libxkbcommon0 \
    libxcomposite1 libxdamage1 libxfixes3 libxrandr2 libgbm1 libasound2 \
    libpango-1.0-0 libpangocairo-1.0-0 libcairo2 libcairo-gobject2 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Playwright Chromium tarayıcısını yükle (sistem bağımlılıkları zaten mevcut)
RUN playwright install chromium

COPY . .

EXPOSE 8012

CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8012"]
