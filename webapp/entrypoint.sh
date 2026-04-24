#!/bin/bash
set -e

# Read config.json and inject VAST credentials into Trino connector config
CONFIG_PATH="${CONFIG_PATH:-/app/config.json}"

if [ -f "$CONFIG_PATH" ]; then
    VAST_ENDPOINT=$(python3 -c "import json; c=json.load(open('$CONFIG_PATH')); print(c['vast']['endpoint'])")
    VAST_ACCESS_KEY=$(python3 -c "import json; c=json.load(open('$CONFIG_PATH')); print(c['vast']['access_key'])")
    VAST_SECRET_KEY=$(python3 -c "import json; c=json.load(open('$CONFIG_PATH')); print(c['vast']['secret_key'])")

    export VAST_ENDPOINT VAST_ACCESS_KEY VAST_SECRET_KEY

    # Template Trino connector config
    sed -i "s|\${ENV:VAST_ENDPOINT}|${VAST_ENDPOINT}|g" /etc/trino/catalog/vast.properties
    sed -i "s|\${ENV:VAST_ACCESS_KEY}|${VAST_ACCESS_KEY}|g" /etc/trino/catalog/vast.properties
    sed -i "s|\${ENV:VAST_SECRET_KEY}|${VAST_SECRET_KEY}|g" /etc/trino/catalog/vast.properties
fi

exec supervisord -n -c /etc/supervisor/conf.d/supervisord.conf
