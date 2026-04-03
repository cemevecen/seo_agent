FROM python:3.12-slim

WORKDIR /app

# Sistem bağımlılıkları (psycopg binary için libpq)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Önce sadece requirements kopyala (layer cache için)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Uygulama kodunu kopyala
COPY backend/ ./backend/
COPY templates/ ./templates/
COPY static/ ./static/
COPY run_server.py .

# Docker içinde 0.0.0.0 dinle, port 8012
ENV APP_HOST=0.0.0.0

EXPOSE 8012

CMD ["python", "-m", "uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8012"]
