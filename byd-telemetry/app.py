import asyncio
import csv
import io
import json
import logging
import math
import os
import sqlite3
import threading
import time
import urllib.request
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import paho.mqtt.client as mqtt
from fastapi import FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("byd-telemetry")

DB_PATH = os.environ.get("DB_PATH", "/data/telemetry.db")
MQTT_HOST = os.environ.get("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USER = os.environ.get("MQTT_USER", "byd")
MQTT_PASS = os.environ.get("MQTT_PASS", "byd-atto3-ktm-2026")
DEFAULT_NTFY_TOPIC = os.environ.get("DEFAULT_NTFY_TOPIC", "byd-telemetry-alerts")

# Nepal Standard Time (UTC+05:45)
NPT = timezone(timedelta(hours=5, minutes=45))
KATHMANDU_LAT = 27.7172
KATHMANDU_LON = 85.3240

db_lock = threading.Lock()
ws_clients: List[WebSocket] = []
main_loop: Optional[asyncio.AbstractEventLoop] = None
last_processed_datetime: Optional[str] = None
last_state_rx_wall_ms: int = 0

PERSISTENT_SENSOR_KEYS = (
    "lat",
    "lon",
    "alt",
    "heading",
    "odometer_km",
    "total_discharge_kwh",
    "soc",
    "soh",
    "range_km",
    "hv_voltage",
    "pack_temp_c",
    "cell_min_v",
    "cell_max_v",
    "ext_temp_c",
    "cabin_temp_c",
    "tire_lf_psi",
    "tire_rf_psi",
    "tire_lr_psi",
    "tire_rr_psi",
    "tire_lf_c",
    "tire_rf_c",
    "tire_lr_c",
    "tire_rr_c",
)


def get_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_db() -> None:
    with db_lock:
        conn = get_db()
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS vehicle_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                device_id TEXT,
                availability TEXT DEFAULT 'offline',
                updated_at_ms INTEGER,
                state_json TEXT
            );

            CREATE TABLE IF NOT EXISTS trips (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                byd_trip_id INTEGER UNIQUE,
                device_id TEXT,
                status TEXT DEFAULT 'active',
                start_time_ms INTEGER NOT NULL,
                end_time_ms INTEGER NOT NULL,
                duration_s INTEGER DEFAULT 0,
                start_lat REAL,
                start_lon REAL,
                end_lat REAL,
                end_lon REAL,
                start_odo REAL,
                end_odo REAL,
                gps_distance_km REAL DEFAULT 0.0,
                distance_km REAL DEFAULT 0.0,
                start_soc REAL,
                end_soc REAL,
                soc_used REAL DEFAULT 0.0,
                start_discharge_kwh REAL,
                end_discharge_kwh REAL,
                energy_kwh REAL DEFAULT 0.0,
                regen_kwh REAL DEFAULT 0.0,
                efficiency_wh_km REAL DEFAULT 0.0,
                efficiency_kwh_100km REAL DEFAULT 0.0,
                avg_speed_kmh REAL DEFAULT 0.0,
                max_speed_kmh REAL DEFAULT 0.0,
                trip_score INTEGER,
                cost REAL,
                currency TEXT,
                point_count INTEGER DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_trips_start_time ON trips(start_time_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_trips_status ON trips(status);

            CREATE TABLE IF NOT EXISTS telemetry_points (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trip_id INTEGER,
                timestamp_ms INTEGER NOT NULL,
                iso_time TEXT,
                lat REAL,
                lon REAL,
                alt REAL,
                heading REAL,
                speed_kmh REAL DEFAULT 0.0,
                odometer_km REAL,
                gear TEXT,
                drive_mode TEXT,
                regen_mode TEXT,
                engine_power_kw REAL DEFAULT 0.0,
                total_discharge_kwh REAL,
                soc REAL,
                soh REAL,
                range_km REAL,
                hv_voltage REAL,
                hv_current REAL,
                aux_12v_voltage REAL,
                pack_temp_c REAL,
                cell_min_v REAL,
                cell_max_v REAL,
                ext_temp_c REAL,
                cabin_temp_c REAL,
                tire_lf_psi REAL,
                tire_rf_psi REAL,
                tire_lr_psi REAL,
                tire_rr_psi REAL,
                tire_lf_c REAL,
                tire_rf_c REAL,
                tire_lr_c REAL,
                tire_rr_c REAL,
                car_on INTEGER DEFAULT 1,
                is_charging INTEGER DEFAULT 0,
                raw_json TEXT,
                FOREIGN KEY(trip_id) REFERENCES trips(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_points_trip ON telemetry_points(trip_id, timestamp_ms ASC);
            CREATE INDEX IF NOT EXISTS idx_points_time ON telemetry_points(timestamp_ms DESC);

            CREATE TABLE IF NOT EXISTS charge_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                byd_session_id INTEGER UNIQUE,
                device_id TEXT,
                started_at_ms INTEGER,
                ended_at_ms INTEGER,
                duration_s INTEGER,
                soc_start_pct REAL,
                soc_end_pct REAL,
                kwh_added REAL,
                avg_kw REAL,
                peak_kw REAL,
                cost REAL,
                currency TEXT,
                raw_json TEXT
            );

            CREATE TABLE IF NOT EXISTS notification_settings (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                ntfy_enabled INTEGER DEFAULT 1,
                ntfy_topic TEXT DEFAULT 'byd-telemetry-alerts',
                telegram_enabled INTEGER DEFAULT 0,
                telegram_bot_token TEXT DEFAULT '',
                telegram_chat_id TEXT DEFAULT '',
                notify_trip_start INTEGER DEFAULT 1,
                notify_trip_end INTEGER DEFAULT 1
            );

            INSERT OR IGNORE INTO notification_settings (
                id, ntfy_enabled, ntfy_topic, telegram_enabled, telegram_bot_token, telegram_chat_id, notify_trip_start, notify_trip_end
            ) VALUES (1, 1, 'byd-telemetry-alerts', 0, '', '', 1, 1);

            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                trip_id INTEGER,
                title TEXT NOT NULL,
                body TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                lat REAL,
                lon REAL
            );
            """
        )
        _reconcile_trips_consistency(conn)
        conn.commit()
        conn.close()


def _reconcile_trips_consistency(conn: sqlite3.Connection) -> None:
    """Ensures all trips have duration_s and metrics consistent with their start/end timestamps and telemetry_points."""
    rows = conn.execute("SELECT * FROM trips WHERE status != 'fragment'").fetchall()
    for r in rows:
        tid = r["id"]
        pts_meta = conn.execute(
            """
            SELECT
                COUNT(*) AS cnt,
                MIN(timestamp_ms) AS min_ts,
                MAX(timestamp_ms) AS max_ts,
                MIN(odometer_km) AS min_odo,
                MAX(odometer_km) AS max_odo,
                MIN(total_discharge_kwh) AS min_dis,
                MAX(total_discharge_kwh) AS max_dis
            FROM telemetry_points
            WHERE trip_id = ?
            """,
            (tid,),
        ).fetchone()

        cnt = int(pts_meta["cnt"] or 0) if pts_meta else 0
        stored_span_ms = int(r["end_time_ms"] or 0) - int(r["start_time_ms"] or 0)
        if cnt > 0 and r["byd_trip_id"] is not None and stored_span_ms > (int(pts_meta["max_ts"]) - int(pts_meta["min_ts"])) + 300_000:
            # Car-summary trip whose GPS only partially covers the drive (patchy connectivity, see
            # _absorb_fragments): keep the summary's times/distance/energy, only sync the point count.
            if int(r["point_count"] or 0) != cnt:
                conn.execute("UPDATE trips SET point_count = ? WHERE id = ?", (cnt, tid))
            continue
        if cnt > 0:
            start_ms = int(pts_meta["min_ts"])
            end_ms = int(pts_meta["max_ts"])
            dur_s = max(1, (end_ms - start_ms) // 1000)

            first_soc_row = conn.execute(
                "SELECT soc FROM telemetry_points WHERE trip_id = ? AND soc IS NOT NULL ORDER BY timestamp_ms ASC LIMIT 1",
                (tid,),
            ).fetchone()
            last_soc_row = conn.execute(
                "SELECT soc FROM telemetry_points WHERE trip_id = ? AND soc IS NOT NULL ORDER BY timestamp_ms DESC LIMIT 1",
                (tid,),
            ).fetchone()
            s_soc = round(float(first_soc_row["soc"]), 1) if first_soc_row else r["start_soc"]
            e_soc = round(float(last_soc_row["soc"]), 1) if last_soc_row else r["end_soc"]
            soc_used = round(max(0.0, float(s_soc) - float(e_soc)), 1) if (s_soc is not None and e_soc is not None) else float(r["soc_used"] or 0.0)

            dist_km = float(r["distance_km"] or 0.0)
            energy_kwh = float(r["energy_kwh"] or 0.0)
            eff_wh_km = float(r["efficiency_wh_km"] or 0.0)
            eff_kwh_100 = float(r["efficiency_kwh_100km"] or 0.0)
            avg_speed = float(r["avg_speed_kmh"] or 0.0)

            min_odo = pts_meta["min_odo"]
            max_odo = pts_meta["max_odo"]
            gps_km = float(r["gps_distance_km"] or 0.0)
            if min_odo is not None and max_odo is not None and float(max_odo) >= float(min_odo):
                odo_delta = round(float(max_odo) - float(min_odo), 2)
                telemetry_dist = round(max(odo_delta, gps_km), 2)
                # If distance_km was overwritten by a multi-trip summary (> 0.5 km above both odometer and GPS)
                if dist_km > telemetry_dist + 0.5:
                    dist_km = telemetry_dist
                    min_dis = pts_meta["min_dis"]
                    max_dis = pts_meta["max_dis"]
                    if min_dis is not None and max_dis is not None and float(max_dis) >= float(min_dis):
                        energy_kwh = round(float(max_dis) - float(min_dis), 3)
                    eff_wh_km = round(energy_kwh * 1000.0 / dist_km, 1) if dist_km >= 0.05 else 0.0
                    eff_kwh_100 = round(energy_kwh * 100.0 / dist_km, 2) if dist_km >= 0.05 else 0.0
                    avg_speed = round(dist_km / (dur_s / 3600.0), 1) if dur_s > 5 else avg_speed

            conn.execute(
                """
                UPDATE trips SET
                    start_time_ms = ?,
                    end_time_ms = ?,
                    duration_s = ?,
                    point_count = ?,
                    start_soc = ?,
                    end_soc = ?,
                    soc_used = ?,
                    distance_km = ?,
                    energy_kwh = ?,
                    efficiency_wh_km = ?,
                    efficiency_kwh_100km = ?,
                    avg_speed_kmh = ?
                WHERE id = ?
                """,
                (
                    start_ms,
                    end_ms,
                    dur_s,
                    cnt,
                    s_soc,
                    e_soc,
                    soc_used,
                    dist_km,
                    energy_kwh,
                    eff_wh_km,
                    eff_kwh_100,
                    avg_speed,
                    tid,
                ),
            )
        else:
            start_ms = int(r["start_time_ms"] or 0)
            end_ms = int(r["end_time_ms"] or 0)
            if end_ms >= start_ms and start_ms > 0:
                dur_s = max(1, (end_ms - start_ms) // 1000)
                if int(r["duration_s"] or 0) != dur_s:
                    conn.execute("UPDATE trips SET duration_s = ? WHERE id = ?", (dur_s, tid))


def haversine_km(lat1: Optional[float], lon1: Optional[float], lat2: Optional[float], lon2: Optional[float]) -> float:
    if not lat1 or not lon1 or not lat2 or not lon2:
        return 0.0
    if abs(lat1) < 0.01 or abs(lon1) < 0.01 or abs(lat2) < 0.01 or abs(lon2) < 0.01:
        return 0.0
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    dist = r * c
    # Guard against GPS jumps > 2 km in a single second
    return dist if dist < 2.0 else 0.0


def parse_iso_ms(iso_str: Optional[str]) -> int:
    if not iso_str:
        return int(time.time() * 1000)
    try:
        s = iso_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return int(dt.timestamp() * 1000)
    except Exception:
        return int(time.time() * 1000)


def normalize_gear(val: Any) -> str:
    if val is None:
        return "P"
    s = str(val).strip().upper()
    mapping = {
        "1": "P",
        "2": "R",
        "3": "N",
        "4": "D",
        "0": "P",
        "P": "P",
        "R": "R",
        "N": "N",
        "D": "D",
        "PARK": "P",
        "DRIVE": "D",
        "REVERSE": "R",
        "NEUTRAL": "N",
    }
    return mapping.get(s, s)


def broadcast_ws(message: Dict[str, Any]) -> None:
    if main_loop is None or not ws_clients:
        return

    async def _send_all():
        dead = []
        data = json.dumps(message)
        for ws in list(ws_clients):
            try:
                await ws.send_text(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            if ws in ws_clients:
                ws_clients.remove(ws)

    asyncio.run_coroutine_threadsafe(_send_all(), main_loop)


last_trip_start_notif_ms: int = 0


def format_npt_str(ms: int) -> str:
    dt = datetime.fromtimestamp(ms / 1000.0, tz=NPT)
    return dt.strftime("%I:%M %p NPT (%b %d)")


def _send_external_push(
    settings: Dict[str, Any],
    title: str,
    body: str,
    lat: Optional[float] = None,
    lon: Optional[float] = None,
) -> None:
    # 1. ntfy.sh Push Notification (Android / iOS / Web) via UTF-8 JSON API
    if settings.get("ntfy_enabled") and settings.get("ntfy_topic"):
        topic = str(settings["ntfy_topic"]).strip()
        if topic:
            try:
                actions = [
                    {
                        "action": "view",
                        "label": "Open Dashboard",
                        "url": "http://anjans-macbook-pro:8080",
                    }
                ]
                if lat and lon and abs(lat) > 0.01:
                    actions.append(
                        {
                            "action": "view",
                            "label": "Google Maps",
                            "url": f"https://maps.google.com/?q={lat:.5f},{lon:.5f}",
                        }
                    )
                ntfy_payload = json.dumps(
                    {
                        "topic": topic,
                        "title": title,
                        "message": body,
                        "priority": 4,
                        "tags": ["red_car", "zap"],
                        "actions": actions,
                    }
                ).encode("utf-8")
                req = urllib.request.Request(
                    "https://ntfy.sh",
                    data=ntfy_payload,
                    headers={"Content-Type": "application/json; charset=utf-8"},
                    method="POST",
                )
                urllib.request.urlopen(req, timeout=8).read()
            except Exception as exc:
                logger.warning("ntfy push failed: %s", exc)

    # 2. Optional Telegram Bot Notification
    if (
        settings.get("telegram_enabled")
        and settings.get("telegram_bot_token")
        and settings.get("telegram_chat_id")
    ):
        token = str(settings["telegram_bot_token"]).strip()
        chat_id = str(settings["telegram_chat_id"]).strip()
        if token and chat_id:
            try:
                map_link = (
                    f"\n📍 https://maps.google.com/?q={lat:.5f},{lon:.5f}"
                    if (lat and lon and abs(lat) > 0.01)
                    else ""
                )
                payload = json.dumps(
                    {
                        "chat_id": chat_id,
                        "text": f"*{title}*\n{body}{map_link}\n🔗 http://anjans-macbook-pro:8080",
                        "parse_mode": "Markdown",
                    }
                ).encode("utf-8")
                req = urllib.request.Request(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    data=payload,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                urllib.request.urlopen(req, timeout=8).read()
            except Exception as exc:
                logger.warning("Telegram push failed: %s", exc)


def dispatch_notification(
    conn: sqlite3.Connection,
    event_type: str,
    title: str,
    body: str,
    trip_id: Optional[int] = None,
    lat: Optional[float] = None,
    lon: Optional[float] = None,
    force_external: bool = False,
) -> Dict[str, Any]:
    now_ms = int(time.time() * 1000)
    cur = conn.execute(
        """
        INSERT INTO notifications (event_type, trip_id, title, body, created_at_ms, lat, lon)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (event_type, trip_id, title, body, now_ms, lat, lon),
    )
    notif_id = cur.lastrowid
    s_row = conn.execute("SELECT * FROM notification_settings WHERE id = 1").fetchone()
    settings = dict(s_row) if s_row else {}

    notif_obj = {
        "id": notif_id,
        "event_type": event_type,
        "trip_id": trip_id,
        "title": title,
        "body": body,
        "created_at_ms": now_ms,
        "lat": lat,
        "lon": lon,
    }
    broadcast_ws({"type": "notification", "notification": notif_obj})

    # Never send external push notifications for synthetic E2E mock device ("Atto3")
    is_mock_trip = False
    if trip_id is not None:
        t_row = conn.execute("SELECT device_id FROM trips WHERE id = ?", (trip_id,)).fetchone()
        if t_row and t_row["device_id"] == "Atto3":
            is_mock_trip = True

    should_push = not is_mock_trip and (
        force_external
        or (
            (event_type == "trip_started" and settings.get("notify_trip_start", 1))
            or (event_type == "trip_completed" and settings.get("notify_trip_end", 1))
        )
    )
    if should_push:
        threading.Thread(
            target=_send_external_push,
            args=(settings, title, body, lat, lon),
            daemon=True,
        ).start()

    return notif_obj


def handle_state_message(device_id: str, payload: Dict[str, Any], is_retain: bool) -> None:
    global last_processed_datetime, last_state_rx_wall_ms
    last_state_rx_wall_ms = int(time.time() * 1000)

    iso_dt = payload.get("current_datetime")
    now_ms = parse_iso_ms(iso_dt)

    lat = payload.get("location_latitude") or payload.get("latitude") or payload.get("lat")
    lon = payload.get("location_longitude") or payload.get("longitude") or payload.get("lon")
    alt = payload.get("location_altitude") or payload.get("altitude") or payload.get("alt")
    heading = payload.get("heading")

    speed_kmh = float(payload.get("speed") or 0.0)
    odometer_km = payload.get("odometer")
    odometer_km = float(odometer_km) if odometer_km is not None and float(odometer_km) > 0 else None

    gear = normalize_gear(payload.get("gear"))
    drive_mode = str(payload.get("drive_mode") or "Normal")
    regen_mode = str(payload.get("regen_mode") or "Standard")

    engine_power_kw = float(payload.get("engine_power") or 0.0)
    total_discharge = payload.get("total_discharge")
    total_discharge_kwh = float(total_discharge) if total_discharge is not None and float(total_discharge) > 0 else None

    soc = payload.get("soc") if payload.get("soc") is not None else payload.get("soc_panel")
    soc = float(soc) if soc is not None else None
    soh = payload.get("statistic_soh") or payload.get("soh")
    soh = float(soh) if soh is not None else None
    range_km = payload.get("electric_driving_range_km")
    range_km = float(range_km) if range_km is not None else None

    hv_voltage = payload.get("battery_total_voltage")
    hv_current = payload.get("battery_total_current")
    aux_12v = payload.get("battery_12v_voltage")
    pack_temp = payload.get("battery_pack_temp")
    cell_min = payload.get("cell_voltage_min")
    cell_max = payload.get("cell_voltage_max")
    ext_temp = payload.get("ext_temp")
    cabin_temp = payload.get("cabin_temp")

    tire_lf_psi = payload.get("tyre_pressure_left_front_psi")
    tire_rf_psi = payload.get("tyre_pressure_right_front_psi")
    tire_lr_psi = payload.get("tyre_pressure_left_rear_psi")
    tire_rr_psi = payload.get("tyre_pressure_right_rear_psi")

    tire_lf_c = payload.get("tyre_temperature_left_front_c")
    tire_rf_c = payload.get("tyre_temperature_right_front_c")
    tire_lr_c = payload.get("tyre_temperature_left_rear_c")
    tire_rr_c = payload.get("tyre_temperature_right_rear_c")

    car_on = bool(payload.get("car_on", True))
    is_charging = bool(payload.get("is_charging", False))

    normalized_state = {
        "device_id": device_id,
        "timestamp_ms": now_ms,
        "current_datetime": iso_dt or datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).isoformat(),
        "lat": float(lat) if lat else None,
        "lon": float(lon) if lon else None,
        "alt": float(alt) if alt else None,
        "heading": float(heading) if heading is not None else None,
        "speed_kmh": round(speed_kmh, 1),
        "odometer_km": round(odometer_km, 1) if odometer_km else None,
        "gear": gear,
        "drive_mode": drive_mode,
        "regen_mode": regen_mode,
        "engine_power_kw": round(engine_power_kw, 2),
        "total_discharge_kwh": round(total_discharge_kwh, 3) if total_discharge_kwh else None,
        "soc": round(soc, 1) if soc is not None else None,
        "soh": round(soh, 1) if soh is not None else None,
        "range_km": round(range_km, 1) if range_km is not None else None,
        "hv_voltage": float(hv_voltage) if hv_voltage is not None else None,
        "hv_current": float(hv_current) if hv_current is not None else None,
        "aux_12v_voltage": float(aux_12v) if aux_12v is not None else None,
        "pack_temp_c": float(pack_temp) if pack_temp is not None else None,
        "cell_min_v": float(cell_min) if cell_min is not None else None,
        "cell_max_v": float(cell_max) if cell_max is not None else None,
        "ext_temp_c": float(ext_temp) if ext_temp is not None else None,
        "cabin_temp_c": float(cabin_temp) if cabin_temp is not None else None,
        "tire_lf_psi": float(tire_lf_psi) if tire_lf_psi is not None else None,
        "tire_rf_psi": float(tire_rf_psi) if tire_rf_psi is not None else None,
        "tire_lr_psi": float(tire_lr_psi) if tire_lr_psi is not None else None,
        "tire_rr_psi": float(tire_rr_psi) if tire_rr_psi is not None else None,
        "tire_lf_c": float(tire_lf_c) if tire_lf_c is not None else None,
        "tire_rf_c": float(tire_rf_c) if tire_rf_c is not None else None,
        "tire_lr_c": float(tire_lr_c) if tire_lr_c is not None else None,
        "tire_rr_c": float(tire_rr_c) if tire_rr_c is not None else None,
        "car_on": car_on,
        "is_charging": is_charging,
        "charging_power_kw": float(payload.get("charging_power") or 0.0),
        "car_locked": bool(payload.get("car_locked", False)),
        "any_door_opened": bool(payload.get("any_door_opened", False)),
        "wifi_ssid": payload.get("wifi_ssid"),
    }

    with db_lock:
        conn = get_db()
        try:
            # Merge with last-known non-null sensor values so parked 30s pings do not wipe out
            # fields that the car only reports while awake (ext_temp_c, pack_temp_c, hv_voltage, etc.)
            prev_vs = conn.execute("SELECT device_id, state_json FROM vehicle_state WHERE id = 1").fetchone()
            if prev_vs and prev_vs["state_json"] and prev_vs["device_id"] == device_id:
                try:
                    prev_state = json.loads(prev_vs["state_json"])
                    for k in PERSISTENT_SENSOR_KEYS:
                        if normalized_state.get(k) is None and prev_state.get(k) is not None:
                            normalized_state[k] = prev_state[k]
                except Exception:
                    pass

            odometer_km = normalized_state["odometer_km"]
            total_discharge_kwh = normalized_state["total_discharge_kwh"]
            soc = normalized_state["soc"]
            range_km = normalized_state["range_km"]

            conn.execute(
                """
                INSERT INTO vehicle_state (id, device_id, availability, updated_at_ms, state_json)
                VALUES (1, ?, 'online', ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    device_id = excluded.device_id,
                    availability = 'online',
                    updated_at_ms = excluded.updated_at_ms,
                    state_json = excluded.state_json
                """,
                (device_id, now_ms, json.dumps(normalized_state)),
            )

            # Avoid re-ingesting retained duplicate state packet on server restart
            if is_retain and iso_dt and iso_dt == last_processed_datetime:
                conn.commit()
                return
            if iso_dt:
                last_processed_datetime = iso_dt

            active_trip = conn.execute(
                "SELECT * FROM trips WHERE status = 'active' AND device_id = ? ORDER BY id DESC LIMIT 1",
                (device_id,),
            ).fetchone()

            # Check if an active trip timed out (> 5 mins gap or car switched off)
            if active_trip:
                gap_ms = now_ms - int(active_trip["end_time_ms"])
                if not car_on or is_charging or gap_ms > 300_000:
                    _finalize_trip(conn, active_trip["id"])
                    active_trip = None

            # Starting a new trip requires Drive (D), Reverse (R), or moving > 0.5 km/h;
            # continuing an active trip also includes Neutral (N) at traffic stops.
            if active_trip:
                is_driving = car_on and not is_charging and (gear in ("D", "R", "N") or speed_kmh > 0.5)
            else:
                is_driving = car_on and not is_charging and (gear in ("D", "R") or speed_kmh > 0.5)

            trip_id = None
            if is_driving:
                if not active_trip:
                    cursor = conn.execute(
                        """
                        INSERT INTO trips (
                            device_id, status, start_time_ms, end_time_ms, duration_s,
                            start_lat, start_lon, end_lat, end_lon,
                            start_odo, end_odo, gps_distance_km, distance_km,
                            start_soc, end_soc, soc_used,
                            start_discharge_kwh, end_discharge_kwh, energy_kwh, regen_kwh,
                            avg_speed_kmh, max_speed_kmh, point_count
                        ) VALUES (?, 'active', ?, ?, 0, ?, ?, ?, ?, ?, ?, 0.0, 0.0, ?, ?, 0.0, ?, ?, 0.0, 0.0, ?, ?, 1)
                        """,
                        (
                            device_id,
                            now_ms,
                            now_ms,
                            normalized_state["lat"],
                            normalized_state["lon"],
                            normalized_state["lat"],
                            normalized_state["lon"],
                            odometer_km,
                            odometer_km,
                            soc,
                            soc,
                            total_discharge_kwh,
                            total_discharge_kwh,
                            speed_kmh,
                            speed_kmh,
                        ),
                    )
                    trip_id = cursor.lastrowid
                    global last_trip_start_notif_ms
                    now_wall_ms = int(time.time() * 1000)
                    if not is_retain and (now_wall_ms - last_trip_start_notif_ms) > 60_000:
                        last_trip_start_notif_ms = now_wall_ms
                        soc_txt = f"{soc:.1f}%" if soc is not None else "—"
                        rng_txt = f" ({range_km:.0f} km)" if range_km is not None else ""
                        odo_txt = f"{odometer_km:,.1f} km" if odometer_km is not None else "—"
                        dispatch_notification(
                            conn,
                            event_type="trip_started",
                            title=f"BYD Atto 3 — Trip #{trip_id} Started",
                            body=f"{format_npt_str(now_ms)} • Gear {gear} ({drive_mode}) • Battery: {soc_txt}{rng_txt} • Odometer: {odo_txt}",
                            trip_id=trip_id,
                            lat=normalized_state["lat"],
                            lon=normalized_state["lon"],
                        )
                else:
                    trip_id = active_trip["id"]
                    prev_point = conn.execute(
                        "SELECT timestamp_ms, lat, lon, engine_power_kw FROM telemetry_points WHERE trip_id = ? ORDER BY timestamp_ms DESC LIMIT 1",
                        (trip_id,),
                    ).fetchone()

                    dt_hours = 0.0
                    step_gps_km = 0.0
                    if prev_point:
                        dt_ms = max(0, now_ms - int(prev_point["timestamp_ms"]))
                        if dt_ms < 60_000:
                            dt_hours = dt_ms / 3_600_000.0
                        step_gps_km = haversine_km(
                            prev_point["lat"],
                            prev_point["lon"],
                            normalized_state["lat"],
                            normalized_state["lon"],
                        )

                    new_gps_km = float(active_trip["gps_distance_km"] or 0.0) + step_gps_km
                    start_odo = active_trip["start_odo"] or odometer_km
                    end_odo = odometer_km or active_trip["end_odo"]
                    odo_delta = (float(end_odo) - float(start_odo)) if (start_odo and end_odo and end_odo >= start_odo) else 0.0
                    # Use whichever is more granular (GPS distance within sub-km resolution, reconciled with odometer)
                    distance_km = max(odo_delta, new_gps_km)

                    start_soc = active_trip["start_soc"] if active_trip["start_soc"] is not None else soc
                    end_soc = soc if soc is not None else active_trip["end_soc"]
                    soc_used = max(0.0, float(start_soc) - float(end_soc)) if (start_soc is not None and end_soc is not None) else 0.0

                    start_dis = active_trip["start_discharge_kwh"] or total_discharge_kwh
                    end_dis = total_discharge_kwh or active_trip["end_discharge_kwh"]

                    regen_add = abs(engine_power_kw) * dt_hours if engine_power_kw < 0 else 0.0
                    new_regen_kwh = float(active_trip["regen_kwh"] or 0.0) + regen_add

                    if start_dis and end_dis and end_dis >= start_dis:
                        energy_kwh = float(end_dis) - float(start_dis)
                    else:
                        net_step_kwh = engine_power_kw * dt_hours
                        energy_kwh = max(0.0, float(active_trip["energy_kwh"] or 0.0) + net_step_kwh)
                        if energy_kwh == 0.0 and soc_used > 0:
                            # BYD Atto 3 Extended Range ~60.48 kWh pack fallback
                            energy_kwh = round(soc_used * 0.6048, 2)

                    duration_s = max(1, int((now_ms - int(active_trip["start_time_ms"])) / 1000))
                    avg_speed = (distance_km / (duration_s / 3600.0)) if duration_s > 5 else speed_kmh
                    max_speed = max(float(active_trip["max_speed_kmh"] or 0.0), speed_kmh)
                    eff_wh_km = (energy_kwh * 1000.0 / distance_km) if distance_km >= 0.05 else 0.0
                    eff_kwh_100 = (energy_kwh * 100.0 / distance_km) if distance_km >= 0.05 else 0.0
                    new_count = int(active_trip["point_count"] or 0) + 1

                    conn.execute(
                        """
                        UPDATE trips SET
                            end_time_ms = ?,
                            duration_s = ?,
                            start_lat = COALESCE(start_lat, ?),
                            start_lon = COALESCE(start_lon, ?),
                            end_lat = COALESCE(?, end_lat),
                            end_lon = COALESCE(?, end_lon),
                            start_odo = COALESCE(start_odo, ?),
                            end_odo = COALESCE(?, end_odo),
                            gps_distance_km = ?,
                            distance_km = ?,
                            start_soc = COALESCE(start_soc, ?),
                            end_soc = COALESCE(?, end_soc),
                            soc_used = ?,
                            start_discharge_kwh = COALESCE(start_discharge_kwh, ?),
                            end_discharge_kwh = COALESCE(?, end_discharge_kwh),
                            energy_kwh = ?,
                            regen_kwh = ?,
                            efficiency_wh_km = ?,
                            efficiency_kwh_100km = ?,
                            avg_speed_kmh = ?,
                            max_speed_kmh = ?,
                            point_count = ?
                        WHERE id = ?
                        """,
                        (
                            now_ms,
                            duration_s,
                            normalized_state["lat"],
                            normalized_state["lon"],
                            normalized_state["lat"],
                            normalized_state["lon"],
                            odometer_km,
                            odometer_km,
                            round(new_gps_km, 3),
                            round(distance_km, 2),
                            soc,
                            soc,
                            round(soc_used, 1),
                            total_discharge_kwh,
                            total_discharge_kwh,
                            round(energy_kwh, 3),
                            round(new_regen_kwh, 3),
                            round(eff_wh_km, 1),
                            round(eff_kwh_100, 2),
                            round(avg_speed, 1),
                            round(max_speed, 1),
                            new_count,
                            trip_id,
                        ),
                    )
            elif active_trip and gear == "P" and speed_kmh < 0.5:
                # Car is in Park with power still on; record the transition to Park once
                # so prolonged idling in P does not bloat telemetry_points.
                last_pt = conn.execute(
                    "SELECT gear, speed_kmh FROM telemetry_points WHERE trip_id = ? ORDER BY timestamp_ms DESC LIMIT 1",
                    (active_trip["id"],),
                ).fetchone()
                if not last_pt or last_pt["gear"] != "P" or float(last_pt["speed_kmh"] or 0.0) >= 0.5:
                    trip_id = active_trip["id"]
                    duration_s = max(1, int((now_ms - int(active_trip["start_time_ms"])) // 1000))
                    new_count = int(active_trip["point_count"] or 0) + 1
                    conn.execute(
                        """
                        UPDATE trips SET
                            end_time_ms = ?,
                            duration_s = ?,
                            end_lat = COALESCE(?, end_lat),
                            end_lon = COALESCE(?, end_lon),
                            end_odo = COALESCE(?, end_odo),
                            end_soc = COALESCE(?, end_soc),
                            end_discharge_kwh = COALESCE(?, end_discharge_kwh),
                            point_count = ?
                        WHERE id = ?
                        """,
                        (
                            now_ms,
                            duration_s,
                            normalized_state["lat"],
                            normalized_state["lon"],
                            odometer_km,
                            soc,
                            total_discharge_kwh,
                            new_count,
                            trip_id,
                        ),
                    )

            if trip_id is not None:
                conn.execute(
                    """
                    INSERT INTO telemetry_points (
                        trip_id, timestamp_ms, iso_time, lat, lon, alt, heading,
                        speed_kmh, odometer_km, gear, drive_mode, regen_mode,
                        engine_power_kw, total_discharge_kwh, soc, soh, range_km,
                        hv_voltage, hv_current, aux_12v_voltage, pack_temp_c,
                        cell_min_v, cell_max_v, ext_temp_c, cabin_temp_c,
                        tire_lf_psi, tire_rf_psi, tire_lr_psi, tire_rr_psi,
                        tire_lf_c, tire_rf_c, tire_lr_c, tire_rr_c,
                        car_on, is_charging, raw_json
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?,
                        ?, ?, ?, ?,
                        ?, ?, ?, ?,
                        ?, ?, ?, ?,
                        ?, ?, ?, ?,
                        ?, ?, ?
                    )
                    """,
                    (
                        trip_id,
                        now_ms,
                        normalized_state["current_datetime"],
                        normalized_state["lat"],
                        normalized_state["lon"],
                        normalized_state["alt"],
                        normalized_state["heading"],
                        normalized_state["speed_kmh"],
                        normalized_state["odometer_km"],
                        normalized_state["gear"],
                        normalized_state["drive_mode"],
                        normalized_state["regen_mode"],
                        normalized_state["engine_power_kw"],
                        normalized_state["total_discharge_kwh"],
                        normalized_state["soc"],
                        normalized_state["soh"],
                        normalized_state["range_km"],
                        normalized_state["hv_voltage"],
                        normalized_state["hv_current"],
                        normalized_state["aux_12v_voltage"],
                        normalized_state["pack_temp_c"],
                        normalized_state["cell_min_v"],
                        normalized_state["cell_max_v"],
                        normalized_state["ext_temp_c"],
                        normalized_state["cabin_temp_c"],
                        normalized_state["tire_lf_psi"],
                        normalized_state["tire_rf_psi"],
                        normalized_state["tire_lr_psi"],
                        normalized_state["tire_rr_psi"],
                        normalized_state["tire_lf_c"],
                        normalized_state["tire_rf_c"],
                        normalized_state["tire_lr_c"],
                        normalized_state["tire_rr_c"],
                        1 if car_on else 0,
                        1 if is_charging else 0,
                        json.dumps(payload),
                    ),
                )

            conn.commit()
        finally:
            conn.close()

    broadcast_ws({"type": "telemetry", "state": normalized_state, "trip_id": trip_id})


def _finalize_trip(conn: sqlite3.Connection, trip_id: int) -> None:
    row = conn.execute("SELECT * FROM trips WHERE id = ?", (trip_id,)).fetchone()
    if not row:
        return
    if float(row["distance_km"] or 0.0) < 0.2 or int(row["point_count"] or 0) < 4:
        has_points = conn.execute(
            "SELECT 1 FROM telemetry_points WHERE trip_id = ? AND lat IS NOT NULL LIMIT 1", (trip_id,)
        ).fetchone()
        if has_points:
            # Too short to be a trip on its own, but on a flaky car connection this can be the only
            # GPS we got for a real drive (the rest of the trip is split off by the 5-min gap rule).
            # Keep it as a hidden 'fragment' so the car's end-of-trip summary can absorb its points
            # (see _absorb_fragments). Fragments are excluded from trip lists and mileage rollups.
            conn.execute("UPDATE trips SET status = 'fragment' WHERE id = ?", (trip_id,))
        else:
            conn.execute("DELETE FROM telemetry_points WHERE trip_id = ?", (trip_id,))
            conn.execute("DELETE FROM trips WHERE id = ?", (trip_id,))
    else:
        pts_meta = conn.execute(
            "SELECT COUNT(*) AS cnt, MIN(timestamp_ms) AS min_ts, MAX(timestamp_ms) AS max_ts FROM telemetry_points WHERE trip_id = ?",
            (trip_id,),
        ).fetchone()
        start_ms = int(pts_meta["min_ts"]) if (pts_meta and pts_meta["min_ts"]) else int(row["start_time_ms"])
        end_ms = int(pts_meta["max_ts"]) if (pts_meta and pts_meta["max_ts"]) else int(row["end_time_ms"])
        dur_s = max(1, (end_ms - start_ms) // 1000)
        pt_cnt = int(pts_meta["cnt"]) if (pts_meta and pts_meta["cnt"]) else int(row["point_count"] or 0)

        conn.execute(
            """
            UPDATE trips SET
                status = 'completed',
                start_time_ms = ?,
                end_time_ms = ?,
                duration_s = ?,
                point_count = ?
            WHERE id = ?
            """,
            (start_ms, end_ms, dur_s, pt_cnt, trip_id),
        )
        dist_km = float(row["distance_km"] or 0.0)
        dur_m = max(1, dur_s // 60)
        eff_wh = float(row["efficiency_wh_km"] or 0.0)
        s_soc = row["start_soc"]
        e_soc = row["end_soc"]
        soc_span = f"{s_soc:.1f}% → {e_soc:.1f}%" if (s_soc is not None and e_soc is not None) else "—"
        dispatch_notification(
            conn,
            event_type="trip_completed",
            title=f"BYD Atto 3 — Trip #{trip_id} Completed ({dist_km:.2f} km)",
            body=f"Duration: {dur_m}m • SoC: {soc_span} • Efficiency: {eff_wh:.0f} Wh/km",
            trip_id=trip_id,
            lat=row["end_lat"],
            lon=row["end_lon"],
        )


FRAGMENT_MATCH_SLACK_MS = 120_000
FRAGMENT_RETENTION_MS = 7 * 24 * 3600 * 1000


def _absorb_fragments(conn: sqlite3.Connection, device_id: str, target_id: int, window_start_ms: int, window_end_ms: int) -> int:
    """Move telemetry points of hidden 'fragment' trips inside [window_start_ms, window_end_ms] into target_id.

    Used when the car's end-of-trip summary arrives: the GPS samples the server did receive during a
    patchy-connectivity drive are attached to the summary trip so its route can still be drawn.
    The summary's distance/energy figures stay authoritative; only route-derived fields are refreshed.
    """
    if not window_start_ms or not window_end_ms or window_end_ms < window_start_ms:
        return 0
    frags = conn.execute(
        """
        SELECT id FROM trips
        WHERE status = 'fragment' AND device_id = ? AND id != ?
          AND start_time_ms <= ? AND end_time_ms >= ?
        """,
        (device_id, target_id, window_end_ms + FRAGMENT_MATCH_SLACK_MS, window_start_ms - FRAGMENT_MATCH_SLACK_MS),
    ).fetchall()
    if not frags:
        return 0
    frag_ids = [int(f["id"]) for f in frags]
    marks = ",".join("?" for _ in frag_ids)
    conn.execute(f"UPDATE telemetry_points SET trip_id = ? WHERE trip_id IN ({marks})", (target_id, *frag_ids))
    conn.execute(f"DELETE FROM trips WHERE id IN ({marks})", frag_ids)

    pts = conn.execute(
        "SELECT timestamp_ms, lat, lon, odometer_km FROM telemetry_points WHERE trip_id = ? ORDER BY timestamp_ms ASC",
        (target_id,),
    ).fetchall()
    geo = [p for p in pts if p["lat"] is not None and p["lon"] is not None and abs(float(p["lat"])) > 0.01]
    gps_km = sum(haversine_km(a["lat"], a["lon"], b["lat"], b["lon"]) for a, b in zip(geo, geo[1:]))
    odos = [float(p["odometer_km"]) for p in pts if p["odometer_km"] is not None]
    trip = conn.execute("SELECT start_time_ms, end_time_ms FROM trips WHERE id = ?", (target_id,)).fetchone()
    start_ms = min(int(trip["start_time_ms"] or pts[0]["timestamp_ms"]), int(pts[0]["timestamp_ms"]))
    end_ms = max(int(trip["end_time_ms"] or pts[-1]["timestamp_ms"]), int(pts[-1]["timestamp_ms"]))
    conn.execute(
        """
        UPDATE trips SET
            start_time_ms = ?,
            end_time_ms = ?,
            duration_s = ?,
            start_lat = COALESCE(start_lat, ?),
            start_lon = COALESCE(start_lon, ?),
            end_lat = COALESCE(end_lat, ?),
            end_lon = COALESCE(end_lon, ?),
            start_odo = COALESCE(start_odo, ?),
            end_odo = COALESCE(end_odo, ?),
            gps_distance_km = MAX(COALESCE(gps_distance_km, 0), ?),
            point_count = ?
        WHERE id = ?
        """,
        (
            start_ms,
            end_ms,
            max(1, (end_ms - start_ms) // 1000),
            geo[0]["lat"] if geo else None,
            geo[0]["lon"] if geo else None,
            geo[-1]["lat"] if geo else None,
            geo[-1]["lon"] if geo else None,
            min(odos) if odos else None,
            max(odos) if odos else None,
            round(gps_km, 3),
            len(pts),
            target_id,
        ),
    )
    logger.info("Attached %d fragment trip(s) %s (%d points) to trip #%s", len(frag_ids), frag_ids, len(pts), target_id)
    return len(frag_ids)


def _prune_old_fragments(conn: sqlite3.Connection) -> None:
    """Drop fragments that no car trip summary claimed within FRAGMENT_RETENTION_MS."""
    cutoff = int(time.time() * 1000) - FRAGMENT_RETENTION_MS
    old = conn.execute("SELECT id FROM trips WHERE status = 'fragment' AND end_time_ms < ?", (cutoff,)).fetchall()
    for r in old:
        conn.execute("DELETE FROM telemetry_points WHERE trip_id = ?", (r["id"],))
        conn.execute("DELETE FROM trips WHERE id = ?", (r["id"],))
    if old:
        conn.commit()


def handle_trip_summary_message(device_id: str, payload: Dict[str, Any]) -> None:
    byd_trip_id = payload.get("trip_id")
    started_at_ms = int(payload.get("started_at_ms") or parse_iso_ms(payload.get("started_at")))
    ended_at_ms = int(payload.get("ended_at_ms") or parse_iso_ms(payload.get("ended_at")))
    span_duration_s = (
        max(1, (ended_at_ms - started_at_ms) // 1000)
        if ended_at_ms > started_at_ms
        else int(payload.get("duration_s") or 0)
    )
    distance_km = float(payload.get("distance_km") or 0.0)
    energy_kwh = float(payload.get("energy_kwh") or 0.0)
    eff_kwh_100 = float(payload.get("efficiency_kwh_per_100km") or (energy_kwh * 100.0 / distance_km if distance_km > 0 else 0.0))
    eff_wh_km = eff_kwh_100 * 10.0
    avg_speed = float(payload.get("avg_speed_kmh") or 0.0)
    soc_start = payload.get("soc_start_pct")
    soc_end = payload.get("soc_end_pct")
    soc_used = max(0.0, float(soc_start) - float(soc_end)) if (soc_start is not None and soc_end is not None) else 0.0
    trip_score = payload.get("trip_score")
    cost = payload.get("cost")
    currency = payload.get("currency")

    with db_lock:
        conn = get_db()
        try:
            existing = None
            if byd_trip_id is not None:
                existing = conn.execute("SELECT * FROM trips WHERE byd_trip_id = ?", (byd_trip_id,)).fetchone()

            if not existing:
                # Match most recent active or recently closed trip within 10 minutes or overlapping the summary window
                existing = conn.execute(
                    """
                    SELECT * FROM trips
                    WHERE byd_trip_id IS NULL
                      AND status != 'fragment'
                      AND (
                        ABS(end_time_ms - ?) < 600000
                        OR (end_time_ms >= ? AND start_time_ms <= ?)
                      )
                    ORDER BY id DESC LIMIT 1
                    """,
                    (ended_at_ms, started_at_ms, ended_at_ms),
                ).fetchone()

            if existing:
                was_active = existing["status"] == "active"
                existing_pts = int(existing["point_count"] or 0)
                existing_span_s = max(1, (int(existing["end_time_ms"]) - int(existing["start_time_ms"])) // 1000)
                # Always keep duration_s consistent with the trip's actual start_time_ms and end_time_ms
                final_duration_s = existing_span_s if existing_pts > 0 else (span_duration_s or existing_span_s)

                # If the car APK rolled multiple separate server trips into one cumulative summary
                # (summary started > 5 mins before this trip's start_time_ms), do not overwrite
                # this single leg's telemetry-measured distance, energy, speed, or SoC.
                is_multi_trip_rollup = (
                    existing_pts > 0
                    and started_at_ms > 0
                    and (int(existing["start_time_ms"]) - started_at_ms) > 300_000
                )

                if is_multi_trip_rollup:
                    conn.execute(
                        """
                        UPDATE trips SET
                            byd_trip_id = COALESCE(?, byd_trip_id),
                            status = 'completed',
                            duration_s = ?,
                            trip_score = COALESCE(?, trip_score),
                            cost = COALESCE(?, cost),
                            currency = COALESCE(?, currency)
                        WHERE id = ?
                        """,
                        (
                            byd_trip_id,
                            final_duration_s,
                            trip_score,
                            cost,
                            currency,
                            existing["id"],
                        ),
                    )
                else:
                    conn.execute(
                        """
                        UPDATE trips SET
                            byd_trip_id = COALESCE(?, byd_trip_id),
                            status = 'completed',
                            duration_s = ?,
                            distance_km = CASE WHEN ? > 0 THEN ? ELSE distance_km END,
                            energy_kwh = CASE WHEN ? > 0 THEN ? ELSE energy_kwh END,
                            efficiency_wh_km = CASE WHEN ? > 0 THEN ? ELSE efficiency_wh_km END,
                            efficiency_kwh_100km = CASE WHEN ? > 0 THEN ? ELSE efficiency_kwh_100km END,
                            avg_speed_kmh = CASE WHEN ? > 0 THEN ? ELSE avg_speed_kmh END,
                            start_soc = COALESCE(start_soc, ?),
                            end_soc = COALESCE(end_soc, ?),
                            soc_used = CASE WHEN start_soc IS NOT NULL AND end_soc IS NOT NULL THEN soc_used WHEN ? > 0 THEN ? ELSE soc_used END,
                            trip_score = COALESCE(?, trip_score),
                            cost = COALESCE(?, cost),
                            currency = COALESCE(?, currency)
                        WHERE id = ?
                        """,
                        (
                            byd_trip_id,
                            final_duration_s,
                            distance_km,
                            round(distance_km, 2),
                            energy_kwh,
                            round(energy_kwh, 3),
                            eff_wh_km,
                            round(eff_wh_km, 1),
                            eff_kwh_100,
                            round(eff_kwh_100, 2),
                            avg_speed,
                            round(avg_speed, 1),
                            soc_start,
                            soc_end,
                            soc_used,
                            round(soc_used, 1),
                            trip_score,
                            cost,
                            currency,
                            existing["id"],
                        ),
                    )
                if was_active:
                    final_dist = (
                        float(existing["distance_km"] or 0.0)
                        if is_multi_trip_rollup
                        else (round(distance_km, 2) if distance_km > 0 else float(existing["distance_km"] or 0.0))
                    )
                    final_dur_m = max(1, final_duration_s // 60)
                    final_eff = (
                        float(existing["efficiency_wh_km"] or 0.0)
                        if is_multi_trip_rollup
                        else (round(eff_wh_km, 1) if eff_wh_km > 0 else float(existing["efficiency_wh_km"] or 0.0))
                    )
                    s_soc = existing["start_soc"] if existing["start_soc"] is not None else soc_start
                    e_soc = existing["end_soc"] if existing["end_soc"] is not None else soc_end
                    soc_span = f"{float(s_soc):.1f}% → {float(e_soc):.1f}%" if (s_soc is not None and e_soc is not None) else "—"
                    dispatch_notification(
                        conn,
                        event_type="trip_completed",
                        title=f"BYD Atto 3 — Trip #{existing['id']} Completed ({final_dist:.2f} km)",
                        body=f"Duration: {final_dur_m}m • SoC: {soc_span} • Efficiency: {final_eff:.0f} Wh/km",
                        trip_id=existing["id"],
                        lat=existing["end_lat"],
                        lon=existing["end_lon"],
                    )
                if not is_multi_trip_rollup:
                    # The summary describes exactly this trip: attach any GPS fragments from its window.
                    _absorb_fragments(conn, device_id, int(existing["id"]), started_at_ms, ended_at_ms)
            else:
                cursor = conn.execute(
                    """
                    INSERT INTO trips (
                        byd_trip_id, device_id, status, start_time_ms, end_time_ms, duration_s,
                        distance_km, start_soc, end_soc, soc_used, energy_kwh,
                        efficiency_wh_km, efficiency_kwh_100km, avg_speed_kmh,
                        trip_score, cost, currency, point_count
                    ) VALUES (?, ?, 'completed', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                    """,
                    (
                        byd_trip_id,
                        device_id,
                        started_at_ms,
                        ended_at_ms,
                        span_duration_s,
                        round(distance_km, 2),
                        soc_start,
                        soc_end,
                        round(soc_used, 1),
                        round(energy_kwh, 3),
                        round(eff_wh_km, 1),
                        round(eff_kwh_100, 2),
                        round(avg_speed, 1),
                        trip_score,
                        cost,
                        currency,
                    ),
                )
                # Car-only trip (server saw no full trip): recover whatever GPS fragments we did get.
                _absorb_fragments(conn, device_id, int(cursor.lastrowid), started_at_ms, ended_at_ms)
            conn.commit()
        finally:
            conn.close()

    broadcast_ws({"type": "trip_completed", "summary": payload})


def handle_charge_summary_message(device_id: str, payload: Dict[str, Any]) -> None:
    session_id = payload.get("session_id")
    started_at_ms = payload.get("started_at_ms") or parse_iso_ms(payload.get("started_at"))
    ended_at_ms = payload.get("ended_at_ms") or parse_iso_ms(payload.get("ended_at"))
    duration_s = payload.get("duration_s")
    soc_start = payload.get("soc_start_pct")
    soc_end = payload.get("soc_end_pct")
    kwh_added = payload.get("kwh_added") or payload.get("received_kwh")
    avg_kw = payload.get("avg_kw")
    peak_kw = payload.get("peak_kw")
    cost = payload.get("cost")
    currency = payload.get("currency")

    with db_lock:
        conn = get_db()
        try:
            conn.execute(
                """
                INSERT INTO charge_sessions (
                    byd_session_id, device_id, started_at_ms, ended_at_ms, duration_s,
                    soc_start_pct, soc_end_pct, kwh_added, avg_kw, peak_kw, cost, currency, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(byd_session_id) DO UPDATE SET
                    ended_at_ms = excluded.ended_at_ms,
                    soc_end_pct = excluded.soc_end_pct,
                    kwh_added = excluded.kwh_added,
                    avg_kw = excluded.avg_kw,
                    peak_kw = excluded.peak_kw
                """,
                (
                    session_id,
                    device_id,
                    started_at_ms,
                    ended_at_ms,
                    duration_s,
                    soc_start,
                    soc_end,
                    kwh_added,
                    avg_kw,
                    peak_kw,
                    cost,
                    currency,
                    json.dumps(payload),
                ),
            )
            conn.commit()
        finally:
            conn.close()


def on_mqtt_connect(client: mqtt.Client, userdata: Any, flags: Any, rc: Any, properties: Any = None) -> None:
    logger.info("Connected to Mosquitto MQTT broker (%s:%s) with result=%s", MQTT_HOST, MQTT_PORT, rc)
    client.subscribe("byd-trip-stats/#", qos=1)
    client.subscribe("byd/#", qos=1)


def on_mqtt_message(client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage) -> None:
    topic = msg.topic
    raw = msg.payload.decode("utf-8", errors="replace").strip()
    parts = topic.split("/")

    # Ignore Home Assistant discovery bursts
    if topic.startswith("homeassistant/"):
        return

    device_id = parts[1] if len(parts) >= 2 else "Atto3"
    subtopic = parts[2] if len(parts) >= 3 else "state"

    if subtopic == "availability":
        with db_lock:
            conn = get_db()
            try:
                conn.execute(
                    """
                    INSERT INTO vehicle_state (id, device_id, availability, updated_at_ms)
                    VALUES (1, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET availability = excluded.availability
                    """,
                    (device_id, raw.lower(), int(time.time() * 1000)),
                )
                conn.commit()
            finally:
                conn.close()
        broadcast_ws({"type": "availability", "device_id": device_id, "availability": raw.lower()})
        return

    try:
        payload = json.loads(raw)
    except Exception:
        return

    if not isinstance(payload, dict):
        return

    if subtopic == "trip" or payload.get("event") == "trip":
        handle_trip_summary_message(device_id, payload)
    elif subtopic == "charge_session" or payload.get("event") == "charge_session":
        handle_charge_summary_message(device_id, payload)
    else:
        handle_state_message(device_id, payload, bool(msg.retain))


def start_mqtt_background() -> mqtt.Client:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="byd-telemetry-server")
    if MQTT_USER:
        client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.on_connect = on_mqtt_connect
    client.on_message = on_mqtt_message

    def _connect_loop():
        while True:
            try:
                client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
                client.loop_forever()
            except Exception as exc:
                logger.warning("MQTT connection retry in 3s: %s", exc)
                time.sleep(3)

    t = threading.Thread(target=_connect_loop, daemon=True)
    t.start()
    return client


@asynccontextmanager
async def lifespan(app: FastAPI):
    global main_loop
    main_loop = asyncio.get_running_loop()
    init_db()
    start_mqtt_background()
    yield


app = FastAPI(title="BYD Atto 3 Telemetry, Mileage & Trip Server", lifespan=lifespan)


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    ws_clients.append(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        if ws in ws_clients:
            ws_clients.remove(ws)


def _cleanup_stale_active_trips(conn: sqlite3.Connection) -> None:
    """Auto-finalize an active trip if no MQTT state packet has arrived for > 5 minutes."""
    now_wall = int(time.time() * 1000)
    if last_state_rx_wall_ms > 0 and (now_wall - last_state_rx_wall_ms) <= 300_000:
        return
    active_rows = conn.execute("SELECT id, end_time_ms FROM trips WHERE status = 'active'").fetchall()
    changed = False
    for r in active_rows:
        if (now_wall - int(r["end_time_ms"] or 0)) > 300_000:
            _finalize_trip(conn, r["id"])
            changed = True
    if changed:
        conn.commit()


@app.get("/api/overview")
def get_overview() -> Dict[str, Any]:
    now_npt = datetime.now(NPT)
    today_start_ms = int(now_npt.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    week_start_ms = int((now_npt - timedelta(days=7)).timestamp() * 1000)
    month_start_ms = int(now_npt.replace(day=1, hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)

    with db_lock:
        conn = get_db()
        try:
            _cleanup_stale_active_trips(conn)
            _prune_old_fragments(conn)
            vs_row = conn.execute("SELECT * FROM vehicle_state WHERE id = 1").fetchone()
            state = json.loads(vs_row["state_json"]) if (vs_row and vs_row["state_json"]) else None
            availability = vs_row["availability"] if vs_row else "offline"

            active_trip_row = conn.execute("SELECT * FROM trips WHERE status = 'active' ORDER BY id DESC LIMIT 1").fetchone()
            active_trip = dict(active_trip_row) if active_trip_row else None
            active_route = []
            if active_trip:
                pts = conn.execute(
                    "SELECT lat, lon, speed_kmh, engine_power_kw, soc, timestamp_ms FROM telemetry_points WHERE trip_id = ? AND lat IS NOT NULL AND lon IS NOT NULL ORDER BY timestamp_ms ASC",
                    (active_trip["id"],),
                ).fetchall()
                active_route = [dict(p) for p in pts]

            today_row = conn.execute(
                "SELECT COALESCE(SUM(distance_km), 0) AS km, COALESCE(SUM(energy_kwh), 0) AS kwh, COUNT(*) AS count FROM trips WHERE start_time_ms >= ? AND status != 'fragment'",
                (today_start_ms,),
            ).fetchone()

            week_row = conn.execute(
                "SELECT COALESCE(SUM(distance_km), 0) AS km, COALESCE(SUM(energy_kwh), 0) AS kwh, COUNT(*) AS count FROM trips WHERE start_time_ms >= ? AND status != 'fragment'",
                (week_start_ms,),
            ).fetchone()

            month_row = conn.execute(
                "SELECT COALESCE(SUM(distance_km), 0) AS km, COALESCE(SUM(energy_kwh), 0) AS kwh, COUNT(*) AS count FROM trips WHERE start_time_ms >= ? AND status != 'fragment'",
                (month_start_ms,),
            ).fetchone()

            total_row = conn.execute(
                """
                SELECT
                    COALESCE(SUM(distance_km), 0) AS km,
                    COALESCE(SUM(energy_kwh), 0) AS kwh,
                    COALESCE(SUM(regen_kwh), 0) AS regen_kwh,
                    COALESCE(SUM(duration_s), 0) AS duration_s,
                    MAX(end_odo) AS max_odo,
                    COUNT(*) AS count
                FROM trips
                WHERE status != 'fragment'
                """
            ).fetchone()

            # Build 14-day daily mileage & efficiency chart series in Nepal time (+05:45 = +20700s)
            daily_rows = conn.execute(
                """
                SELECT
                     strftime('%Y-%m-%d', (start_time_ms / 1000) + 20700, 'unixepoch') AS day,
                     ROUND(SUM(distance_km), 2) AS distance_km,
                     ROUND(SUM(energy_kwh), 3) AS energy_kwh,
                     COUNT(*) AS trips_count
                FROM trips
                WHERE start_time_ms >= ? AND status != 'fragment'
                GROUP BY day
                ORDER BY day ASC
                """,
                (int((now_npt - timedelta(days=30)).timestamp() * 1000),),
            ).fetchall()

            daily_series = []
            for r in daily_rows:
                d_km = float(r["distance_km"] or 0.0)
                d_kwh = float(r["energy_kwh"] or 0.0)
                wh_km = round(d_kwh * 1000.0 / d_km, 1) if d_km > 0.1 else 0.0
                daily_series.append(
                    {
                        "day": r["day"],
                        "distance_km": d_km,
                        "energy_kwh": d_kwh,
                        "wh_per_km": wh_km,
                        "trips_count": r["trips_count"],
                    }
                )

            total_km = float(total_row["km"] or 0.0)
            total_kwh = float(total_row["kwh"] or 0.0)
            avg_wh_per_km = round(total_kwh * 1000.0 / total_km, 1) if total_km > 0.1 else 0.0
            avg_km_per_kwh = round(total_km / total_kwh, 2) if total_kwh > 0.05 else 0.0
            odometer_km = (
                (state.get("odometer_km") if state else None)
                or total_row["max_odo"]
                or 0.0
            )

            charges = [
                dict(c)
                for c in conn.execute(
                    "SELECT * FROM charge_sessions ORDER BY ended_at_ms DESC LIMIT 10"
                ).fetchall()
            ]

            return {
                "availability": availability,
                "state": state,
                "active_trip": active_trip,
                "active_route": active_route,
                "mileage": {
                    "odometer_km": round(float(odometer_km), 1),
                    "today_km": round(float(today_row["km"]), 2),
                    "today_kwh": round(float(today_row["kwh"]), 2),
                    "today_trips": int(today_row["count"]),
                    "week_km": round(float(week_row["km"]), 2),
                    "week_kwh": round(float(week_row["kwh"]), 2),
                    "week_trips": int(week_row["count"]),
                    "month_km": round(float(month_row["km"]), 2),
                    "month_kwh": round(float(month_row["kwh"]), 2),
                    "month_trips": int(month_row["count"]),
                    "lifetime_tracked_km": round(total_km, 2),
                    "lifetime_kwh": round(total_kwh, 2),
                    "lifetime_regen_kwh": round(float(total_row["regen_kwh"] or 0.0), 2),
                    "lifetime_duration_s": int(total_row["duration_s"] or 0),
                    "total_trips": int(total_row["count"]),
                    "avg_wh_per_km": avg_wh_per_km,
                    "avg_km_per_kwh": avg_km_per_kwh,
                },
                "daily_series": daily_series,
                "recent_charges": charges,
            }
        finally:
            conn.close()


@app.get("/api/trips")
def list_trips(
    limit: int = Query(100, ge=1, le=500),
    search: Optional[str] = None,
) -> Dict[str, Any]:
    with db_lock:
        conn = get_db()
        try:
            _cleanup_stale_active_trips(conn)
            if search and search.strip():
                q = f"%{search.strip()}%"
                rows = conn.execute(
                    """
                    SELECT * FROM trips
                    WHERE status != 'fragment' AND (
                          CAST(id AS TEXT) LIKE ?
                       OR CAST(byd_trip_id AS TEXT) LIKE ?
                       OR status LIKE ?
                       OR CAST(distance_km AS TEXT) LIKE ?
                    )
                    ORDER BY start_time_ms DESC LIMIT ?
                    """,
                    (q, q, q, q, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM trips WHERE status != 'fragment' ORDER BY start_time_ms DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            trips = [dict(r) for r in rows]
            return {"trips": trips}
        finally:
            conn.close()


@app.get("/api/trips/{trip_id}")
def get_trip_detail(trip_id: int) -> Dict[str, Any]:
    with db_lock:
        conn = get_db()
        try:
            trip = conn.execute("SELECT * FROM trips WHERE id = ?", (trip_id,)).fetchone()
            if not trip:
                raise HTTPException(status_code=404, detail="Trip not found")
            points = conn.execute(
                """
                SELECT
                    id, timestamp_ms, iso_time, lat, lon, alt, heading,
                    speed_kmh, odometer_km, gear, drive_mode, regen_mode,
                    engine_power_kw, total_discharge_kwh, soc, soh, range_km,
                    hv_voltage, hv_current, aux_12v_voltage, pack_temp_c,
                    cell_min_v, cell_max_v, ext_temp_c, cabin_temp_c,
                    tire_lf_psi, tire_rf_psi, tire_lr_psi, tire_rr_psi
                FROM telemetry_points
                WHERE trip_id = ?
                ORDER BY timestamp_ms ASC
                """,
                (trip_id,),
            ).fetchall()
            return {
                "trip": dict(trip),
                "points": [dict(p) for p in points],
            }
        finally:
            conn.close()


@app.delete("/api/trips/{trip_id}")
def delete_trip(trip_id: int) -> Dict[str, Any]:
    with db_lock:
        conn = get_db()
        try:
            conn.execute("DELETE FROM telemetry_points WHERE trip_id = ?", (trip_id,))
            conn.execute("DELETE FROM trips WHERE id = ?", (trip_id,))
            conn.commit()
            return {"deleted": trip_id}
        finally:
            conn.close()


@app.get("/api/trips/{trip_id}/export.gpx")
def export_trip_gpx(trip_id: int) -> Response:
    with db_lock:
        conn = get_db()
        try:
            trip = conn.execute("SELECT * FROM trips WHERE id = ?", (trip_id,)).fetchone()
            if not trip:
                raise HTTPException(status_code=404, detail="Trip not found")
            points = conn.execute(
                "SELECT * FROM telemetry_points WHERE trip_id = ? AND lat IS NOT NULL AND lon IS NOT NULL ORDER BY timestamp_ms ASC",
                (trip_id,),
            ).fetchall()
        finally:
            conn.close()

    gpx_lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<gpx version="1.1" creator="BYD Atto 3 Telemetry Server" xmlns="http://www.topografix.com/GPX/1/1">',
        "  <trk>",
        f"    <name>BYD Atto 3 Trip #{trip_id} ({trip['distance_km']} km)</name>",
        "    <trkseg>",
    ]
    for p in points:
        ele = f"<ele>{p['alt']}</ele>" if p["alt"] is not None else ""
        time_tag = f"<time>{p['iso_time']}</time>" if p["iso_time"] else ""
        gpx_lines.append(f'      <trkpt lat="{p["lat"]}" lon="{p["lon"]}">{ele}{time_tag}</trkpt>')
    gpx_lines.extend(["    </trkseg>", "  </trk>", "</gpx>"])

    return Response(
        content="\n".join(gpx_lines),
        media_type="application/gpx+xml",
        headers={"Content-Disposition": f'attachment; filename="byd-atto3-trip-{trip_id}.gpx"'},
    )


@app.get("/api/trips/{trip_id}/export.csv")
def export_trip_csv(trip_id: int) -> Response:
    with db_lock:
        conn = get_db()
        try:
            trip = conn.execute("SELECT * FROM trips WHERE id = ?", (trip_id,)).fetchone()
            if not trip:
                raise HTTPException(status_code=404, detail="Trip not found")
            points = conn.execute(
                """
                SELECT
                    iso_time, timestamp_ms, lat, lon, alt, heading,
                    speed_kmh, odometer_km, gear, drive_mode, regen_mode,
                    engine_power_kw, soc, range_km, hv_voltage, hv_current,
                    aux_12v_voltage, pack_temp_c, ext_temp_c, cabin_temp_c,
                    tire_lf_psi, tire_rf_psi, tire_lr_psi, tire_rr_psi
                FROM telemetry_points
                WHERE trip_id = ?
                ORDER BY timestamp_ms ASC
                """,
                (trip_id,),
            ).fetchall()
        finally:
            conn.close()

    buf = io.StringIO()
    writer = csv.writer(buf)
    cols = [
        "iso_time", "timestamp_ms", "lat", "lon", "alt", "heading",
        "speed_kmh", "odometer_km", "gear", "drive_mode", "regen_mode",
        "engine_power_kw", "soc", "range_km", "hv_voltage", "hv_current",
        "aux_12v_voltage", "pack_temp_c", "ext_temp_c", "cabin_temp_c",
        "tire_lf_psi", "tire_rf_psi", "tire_lr_psi", "tire_rr_psi",
    ]
    writer.writerow(cols)
    for p in points:
        writer.writerow([p[c] for c in cols])

    return Response(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="byd-atto3-trip-{trip_id}.csv"'},
    )


app.mount("/static", StaticFiles(directory="/app/static"), name="static")


@app.get("/apk", response_class=HTMLResponse)
def apk_download_page():
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>BYD Atto 3 — Wireless APK Installer</title>
  <style>
    body {
      background: #070b14; color: #f8fafc; font-family: -apple-system, BlinkMacSystemFont, sans-serif;
      display: flex; align-items: center; justify-content: center; min-height: 100vh; margin: 0; padding: 24px;
    }
    .card {
      max-width: 640px; width: 100%; background: #0f172a; border: 1px solid #1e293b;
      border-radius: 20px; padding: 36px; text-align: center; box-shadow: 0 20px 50px rgba(0,0,0,0.5);
    }
    h1 { margin: 0 0 12px; font-size: 28px; color: #38bdf8; }
    p { color: #94a3b8; font-size: 16px; line-height: 1.6; margin-bottom: 28px; }
    .btn {
      display: block; width: 100%; box-sizing: border-box; padding: 22px;
      background: linear-gradient(135deg, #0284c7, #38bdf8); color: #041019;
      font-size: 22px; font-weight: 800; text-decoration: none; border-radius: 14px;
      margin-bottom: 18px;
    }
    .steps {
      text-align: left; background: #131c31; border-radius: 12px; padding: 18px 22px;
      font-size: 15px; color: #cbd5e1; line-height: 1.7;
    }
    code { background: #1e293b; color: #38bdf8; padding: 2px 6px; border-radius: 5px; }
  </style>
</head>
<body>
  <div class="card">
    <h1>BYD Trip Stats (v2.18.0 DiLink 3)</h1>
    <p>Tap the button below on your BYD Atto 3 browser to download the APK directly over Wi-Fi without a USB drive.</p>
    <a class="btn" href="/apk/byd-trip-stats.apk" download="byd-trip-stats.apk">⬇ Download byd-trip-stats.apk (22.4 MB)</a>
    <div class="steps">
      <strong>After downloading on the car screen:</strong>
      <ol>
        <li>Open the car's <strong>File Manager</strong> → <strong>Download</strong> folder.</li>
        <li>Tap <code>byd-trip-stats.apk</code> to install (if blocked, move/copy it into <code>Internal Storage/third party apps/</code> first).</li>
        <li>If prompted for a password, enter <code>BYD612525</code> or <code>20211231</code>.</li>
      </ol>
    </div>
  </div>
</body>
</html>"""


@app.get("/apk/byd-trip-stats.apk")
def download_apk():
    apk_path = "/data/byd-trip-stats.apk"
    if not os.path.exists(apk_path):
        raise HTTPException(status_code=404, detail="APK file not found in /data/byd-trip-stats.apk")
    return FileResponse(
        apk_path,
        media_type="application/vnd.android.package-archive",
        filename="byd-trip-stats.apk",
    )


@app.get("/api/notifications")
def get_notifications() -> Dict[str, Any]:
    with db_lock:
        conn = get_db()
        try:
            s_row = conn.execute("SELECT * FROM notification_settings WHERE id = 1").fetchone()
            items = [
                dict(r)
                for r in conn.execute(
                    "SELECT * FROM notifications ORDER BY created_at_ms DESC LIMIT 25"
                ).fetchall()
            ]
            return {
                "settings": dict(s_row) if s_row else {},
                "notifications": items,
            }
        finally:
            conn.close()


@app.post("/api/notifications/settings")
async def save_notification_settings(req: Request) -> Dict[str, Any]:
    body = await req.json()
    with db_lock:
        conn = get_db()
        try:
            conn.execute(
                """
                UPDATE notification_settings SET
                    ntfy_enabled = ?,
                    ntfy_topic = ?,
                    telegram_enabled = ?,
                    telegram_bot_token = ?,
                    telegram_chat_id = ?,
                    notify_trip_start = ?,
                    notify_trip_end = ?
                WHERE id = 1
                """,
                (
                    1 if body.get("ntfy_enabled", True) else 0,
                    str(body.get("ntfy_topic") or "byd-atto3-anjan-ktm-8532").strip(),
                    1 if body.get("telegram_enabled", False) else 0,
                    str(body.get("telegram_bot_token") or "").strip(),
                    str(body.get("telegram_chat_id") or "").strip(),
                    1 if body.get("notify_trip_start", True) else 0,
                    1 if body.get("notify_trip_end", True) else 0,
                ),
            )
            conn.commit()
            s_row = conn.execute("SELECT * FROM notification_settings WHERE id = 1").fetchone()
            return {"ok": True, "settings": dict(s_row)}
        finally:
            conn.close()


@app.post("/api/notifications/test")
def test_trip_start_notification() -> Dict[str, Any]:
    with db_lock:
        conn = get_db()
        try:
            st_row = conn.execute("SELECT state_json FROM vehicle_state WHERE id = 1").fetchone()
            state = json.loads(st_row["state_json"]) if (st_row and st_row["state_json"]) else {}
            soc = state.get("soc")
            rng = state.get("range_km")
            odo = state.get("odometer_km")
            lat = state.get("lat") or KATHMANDU_LAT
            lon = state.get("lon") or KATHMANDU_LON
            soc_txt = f"{float(soc):.1f}%" if soc is not None else "85.7%"
            rng_txt = f" ({float(rng):.0f} km)" if rng is not None else ""
            odo_txt = f"{float(odo):,.1f} km" if odo is not None else "—"
            now_ms = int(time.time() * 1000)

            notif = dispatch_notification(
                conn,
                event_type="trip_started",
                title="BYD Atto 3 — Trip Started (Test)",
                body=f"{format_npt_str(now_ms)} • Gear D (Eco) • Battery: {soc_txt}{rng_txt} • Odometer: {odo_txt}",
                trip_id=None,
                lat=lat,
                lon=lon,
                force_external=True,
            )
            conn.commit()
            return {"ok": True, "notification": notif}
        finally:
            conn.close()


@app.get("/", response_class=HTMLResponse)
def index():
    return FileResponse(
        "/app/static/index.html",
        headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"},
    )
