FROM python:3.11-slim

# Make logs flush immediately
ENV PYTHONUNBUFFERED=1

# System deps (kept minimal)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl && rm -rf /var/lib/apt/lists/*

# App
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Fly sets $PORT; default to 8080 for local build/run
ENV PORT=8080

# Single process so APScheduler doesn't double-run
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080", "--proxy-headers", "--forwarded-allow-ips", "*"]
