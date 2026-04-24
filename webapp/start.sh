#!/bin/bash
set -e

# ── Inject VAST credentials into Trino connector config ──
CONFIG_PATH="${CONFIG_PATH:-/app/config.json}"

if [ -f "$CONFIG_PATH" ]; then
    VAST_ENDPOINT=$(python3 -c "import json; c=json.load(open('$CONFIG_PATH')); print(c['vast']['endpoint'])")
    VAST_ACCESS_KEY=$(python3 -c "import json; c=json.load(open('$CONFIG_PATH')); print(c['vast']['access_key'])")
    VAST_SECRET_KEY=$(python3 -c "import json; c=json.load(open('$CONFIG_PATH')); print(c['vast']['secret_key'])")

    export VAST_ENDPOINT VAST_ACCESS_KEY VAST_SECRET_KEY

    sed -i "s|\${ENV:VAST_ENDPOINT}|${VAST_ENDPOINT}|g" /etc/trino/catalog/vast.properties
    sed -i "s|\${ENV:VAST_ACCESS_KEY}|${VAST_ACCESS_KEY}|g" /etc/trino/catalog/vast.properties
    sed -i "s|\${ENV:VAST_SECRET_KEY}|${VAST_SECRET_KEY}|g" /etc/trino/catalog/vast.properties

    echo "Trino connector configured for: ${VAST_ENDPOINT}"
fi

# ── Start Trino in background ──
echo "Starting Trino..."
/usr/lib/trino/bin/launcher run &
TRINO_PID=$!

# ── Wait for Trino to be ready ──
echo "Waiting for Trino to start on port 8080..."
for i in $(seq 1 60); do
    if curl -sf http://localhost:8080/v1/info > /dev/null 2>&1; then
        echo "Trino is ready."
        break
    fi
    sleep 2
done

# ── Start Flask/Gunicorn in foreground ──
echo "Starting Flask on port 3000..."
exec gunicorn -b 0.0.0.0:3000 -w 2 --chdir /app/backend app:app
