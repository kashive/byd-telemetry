#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER="${DOCKER:-$(command -v docker 2>/dev/null || echo "/usr/local/bin/docker")}"
MQTT_USER="${MQTT_USER:-byd}"
MQTT_PASS="${MQTT_PASS:-byd-atto3-ktm-2026}"

cd "$SCRIPT_DIR"

echo "==> Backing up live telemetry.db before running mock E2E tests..."
"$DOCKER" exec byd-telemetry python3 -c "import sqlite3; c=sqlite3.connect('/data/telemetry.db'); c.execute('PRAGMA wal_checkpoint(TRUNCATE);'); c.close()" 2>/dev/null || true
"$DOCKER" exec byd-telemetry sh -c "cp -f /data/telemetry.db /data/telemetry.db.bak 2>/dev/null || true"
"$DOCKER" exec byd-telemetry python3 -c "import sqlite3; c=sqlite3.connect('/data/telemetry.db'); c.execute('UPDATE notification_settings SET ntfy_enabled = 0, telegram_enabled = 0;'); c.commit(); c.close()" 2>/dev/null || true
trap '
  "$DOCKER" exec byd-telemetry python3 -c "
import paho.mqtt.client as mqtt
c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
c.username_pw_set(\"'"$MQTT_USER"'\", \"'"$MQTT_PASS"'\")
c.connect(\"mosquitto\", 1883)
c.loop_start()
for sub in (\"availability\", \"state\", \"trip\", \"charge_session\"):
    c.publish(f\"byd-trip-stats/Atto3/{sub}\", \"\", qos=1, retain=True).wait_for_publish()
c.loop_stop()
c.disconnect()
" 2>/dev/null || true
  "$DOCKER" exec byd-telemetry sh -c "if [ -f /data/telemetry.db.bak ]; then rm -f /data/telemetry.db-wal /data/telemetry.db-shm && mv -f /data/telemetry.db.bak /data/telemetry.db; fi"
  "$DOCKER" restart byd-telemetry >/dev/null
' EXIT

echo "==> Syncing latest app, UI & test suite into byd-telemetry container..."
"$DOCKER" cp "$SCRIPT_DIR/app.py" byd-telemetry:/app/app.py
"$DOCKER" cp "$SCRIPT_DIR/static/index.html" byd-telemetry:/app/static/index.html
"$DOCKER" cp "$SCRIPT_DIR/tests" byd-telemetry:/app/
"$DOCKER" restart byd-telemetry >/dev/null
sleep 2

echo "==> Running E2E Pipeline + Playwright Browser UI Test Suite..."
"$DOCKER" exec \
  -e PYTHONPATH=/app/tests \
  -e MQTT_HOST=mosquitto \
  -e MQTT_PORT=1883 \
  -e MQTT_USER="$MQTT_USER" \
  -e MQTT_PASS="$MQTT_PASS" \
  -e BASE_URL=http://localhost:8080 \
  -e SCREENSHOT_DIR=/data/test-screenshots \
  byd-telemetry \
  pytest -v /app/tests/test_e2e_pipeline.py /app/tests/test_ui_e2e.py
