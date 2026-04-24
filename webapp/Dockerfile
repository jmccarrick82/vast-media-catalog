# ─── Stage 1: Build React frontend ──────────────────────────────────────
FROM node:20-slim AS frontend-build

WORKDIR /app/frontend
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm install
COPY frontend/ ./
RUN npm run build

# ─── Stage 2: Python + Flask runtime (no Trino — runs as separate container) ─
FROM python:3.11-slim

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY backend/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY backend/ /app/backend/
COPY --from=frontend-build /app/frontend/dist /app/frontend/dist

# c2patool — bundled so the /api/packages/<id>/renditions/<rid>/c2pa
# endpoint can live-verify signed renditions on demand. Same binary
# the media-packager function uses.
COPY c2patool /usr/local/bin/c2patool
RUN chmod +x /usr/local/bin/c2patool

EXPOSE 3000

CMD ["gunicorn", "-b", "0.0.0.0:3000", "-w", "2", "--chdir", "/app/backend", "app:app"]
