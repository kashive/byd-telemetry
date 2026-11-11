# Contributing to BYD Telemetry

Thank you for your interest in improving BYD Telemetry! We welcome issues, bug reports, feature requests, and pull requests.

## Architecture Overview

- **Backend (`byd-telemetry/app.py`):** Single-file FastAPI + paho-mqtt + sqlite3 service. It ingests MQTT packets, segments trips, maintains rollups, and serves both REST endpoints and real-time WebSockets.
- **Frontend (`byd-telemetry/static/index.html`):** Single-page application using vanilla JavaScript, pre-compiled Tailwind CSS, Leaflet for maps, and Chart.js for telemetry charts. No node/npm build step required.
- **MQTT Broker (`mosquitto`):** Eclipse Mosquitto broker running TCP listener on port 1883 and WebSocket listener on port 9001.

## Local Development Workflow

1. Clone the repository and configure `.env`:
   ```bash
   cp .env.example .env
   ```
2. Start the Docker stack:
   ```bash
   docker compose up -d
   ```
3. Open `http://localhost:8080` in your web browser.

### Simulating Car Drives

You don't need a real car to test! Run the included drive simulator:
```bash
python3 byd-telemetry/simulate_kathmandu_trip.py
```
This publishes authentic live telemetry and trip packets over MQTT to verify map movement, elevation/speed graphs, and trip completion logic.

## Running Tests

Automated end-to-end tests test MQTT ingestion, SQLite persistence, and UI rendering:
```bash
./byd-telemetry/run_e2e_tests.sh
```

## Pull Request Guidelines

- Ensure `python3 -m py_compile byd-telemetry/app.py` passes without syntax errors.
- Keep the database migrations additive in `init_db()`.
- Avoid adding heavy external frontend dependencies without packaging or vendoring them in `static/vendor/`.
- Never commit private databases (`*.db`), vehicle credentials, or personal GPS logs.
