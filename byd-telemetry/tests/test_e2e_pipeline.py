"""
End-to-End Backend, MQTT, SQLite & API Pipeline Tests for BYD Atto 3 Telemetry Server.
"""
import csv
import io
import json
import os
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

import httpx
import paho.mqtt.client as mqtt
import pytest

from mock_stream_generator import (
    DEVICE_ID,
    MQTT_HOST,
    MQTT_PASS,
    MQTT_PORT,
    MQTT_USER,
    generate_route_samples,
    seed_multi_day_dataset,
)

BASE_URL = os.environ.get("BASE_URL", "http://localhost:8080")


@pytest.fixture(scope="module")
def mqtt_pub():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="pytest-e2e-publisher")
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()
    yield client
    client.loop_stop()
    client.disconnect()


def clear_all_trips():
    with httpx.Client(base_url=BASE_URL, timeout=10.0) as client:
        res = client.get("/api/trips?limit=500")
        assert res.status_code == 200
        for t in res.json().get("trips", []):
            client.delete(f"/api/trips/{t['id']}")


def test_01_live_driving_stream_lifecycle(mqtt_pub):
    """
    Verifies that streaming live state packets creates an active trip when in Drive (D),
    accumulates Kathmandu GPS route points, odometer/GPS mileage, and regen energy,
    and automatically finalizes the trip when the car parks and powers off.
    """
    clear_all_trips()

    start_dt = datetime.now(timezone.utc) - timedelta(minutes=12)
    samples, summary = generate_route_samples(
        "naxal_to_jhamsikhel",
        start_time=start_dt,
        start_odo=5000.0,
        start_soc=80.0,
        start_discharge_kwh=800.0,
        steps_per_segment=3,
        step_seconds=15,
    )

    # 1. Stream the driving samples (all except the final parked sample)
    for sample in samples[:-1]:
        info = mqtt_pub.publish(
            f"byd-trip-stats/{DEVICE_ID}/state",
            json.dumps(sample),
            qos=1,
            retain=False,
        )
        info.wait_for_publish()
        time.sleep(0.02)

    time.sleep(0.3)

    # Verify the trip is currently ACTIVE in /api/overview
    with httpx.Client(base_url=BASE_URL, timeout=10.0) as http:
        ov = http.get("/api/overview").json()
        assert ov["availability"] == "online"
        assert ov["active_trip"] is not None, "Expected an active trip while car_on=True and gear=D"
        active_id = ov["active_trip"]["id"]
        assert ov["active_trip"]["status"] == "active"
        assert ov["active_trip"]["distance_km"] > 2.5
        assert len(ov["active_route"]) >= 15

        # 2. Send the final parked & powered-off sample (`car_on: False`, `gear: "P"`)
        info = mqtt_pub.publish(
            f"byd-trip-stats/{DEVICE_ID}/state",
            json.dumps(samples[-1]),
            qos=1,
            retain=True,
        )
        info.wait_for_publish()
        time.sleep(0.3)

        # Verify the active trip was automatically finalized into 'completed'
        ov_after = http.get("/api/overview").json()
        assert ov_after["active_trip"] is None
        detail = http.get(f"/api/trips/{active_id}").json()
        assert detail["trip"]["status"] == "completed"
        assert detail["trip"]["distance_km"] >= 3.0
        assert len(detail["points"]) == len(samples) - 1


def test_02_parked_30s_pings_do_not_create_phantom_trips(mqtt_pub):
    """
    When parked (`car_on: false`), BYD Trip Stats publishes a state ping every 30s with
    cached GPS coordinates. Verifies that these update the 12V battery & lock status
    without creating phantom trips or increasing mileage.
    """
    with httpx.Client(base_url=BASE_URL, timeout=10.0) as http:
        before_trips = http.get("/api/trips").json()["trips"]
        before_count = len(before_trips)
        before_km = http.get("/api/overview").json()["mileage"]["today_km"]

        for i in range(3):
            parked_ping = {
                "current_datetime": (datetime.now(timezone.utc) + timedelta(seconds=i + 1)).isoformat(),
                "soc": 78.8,
                "battery_12v_voltage": round(13.15 - i * 0.02, 2),
                "battery_total_voltage": 401.2,
                "speed": 0.0,
                "gear": "P",
                "odometer": 5004.5,
                "engine_power": 0.0,
                "car_on": False,
                "car_locked": True,
                "is_charging": False,
                "location_latitude": 27.6815,
                "location_longitude": 85.3048,
                "location_altitude": 1308.0,
            }
            info = mqtt_pub.publish(f"byd-trip-stats/{DEVICE_ID}/state", json.dumps(parked_ping), qos=1)
            info.wait_for_publish()
            time.sleep(0.05)

        time.sleep(0.25)
        after_trips = http.get("/api/trips").json()["trips"]
        ov = http.get("/api/overview").json()

        assert len(after_trips) == before_count, "Parked pings must never create phantom trips"
        assert ov["mileage"]["today_km"] == before_km
        assert ov["state"]["aux_12v_voltage"] == 13.11
        assert ov["state"]["car_locked"] is True
        assert ov["state"]["ext_temp_c"] is not None, "Parked pings omitting ext_temp must preserve last known ext_temp_c"
        assert ov["state"]["pack_temp_c"] is not None, "Parked pings omitting pack_temp must preserve last known pack_temp_c"


def test_03_trip_summary_reconciliation_and_offline_queue(mqtt_pub):
    """
    Verifies that publishing a `byd-trip-stats/Atto3/trip` summary reconciles with the
    completed trip and that re-publishing the retained summary is idempotent (no duplicate trip).
    """
    with httpx.Client(base_url=BASE_URL, timeout=10.0) as http:
        trips = http.get("/api/trips").json()["trips"]
        assert len(trips) >= 1
        latest_trip = trips[0]

        summary_payload = {
            "event": "trip",
            "trip_id": 77701,
            "started_at_ms": latest_trip["start_time_ms"],
            "ended_at_ms": latest_trip["end_time_ms"],
            "duration_s": 960,
            "distance_km": 6.85,
            "energy_kwh": 0.98,
            "efficiency_kwh_per_100km": 14.31,
            "avg_speed_kmh": 25.7,
            "soc_start_pct": 80.0,
            "soc_end_pct": 78.4,
            "trip_score": 95,
        }

        # Publish twice (simulating retained replay on broker reconnect)
        for _ in range(2):
            info = mqtt_pub.publish(
                f"byd-trip-stats/{DEVICE_ID}/trip",
                json.dumps(summary_payload),
                qos=1,
                retain=True,
            )
            info.wait_for_publish()
            time.sleep(0.15)

        updated_trips = http.get("/api/trips").json()["trips"]
        assert len(updated_trips) == len(trips), "Retained trip summary replay must not duplicate trips"
        reconciled = updated_trips[0]
        assert reconciled["byd_trip_id"] == 77701
        assert reconciled["distance_km"] == 6.85
        assert reconciled["trip_score"] == 95
        expected_dur_s = max(1, (reconciled["end_time_ms"] - reconciled["start_time_ms"]) // 1000)
        assert reconciled["duration_s"] == expected_dur_s, "Trip duration_s must match start_time_ms -> end_time_ms span"

        # Verify server-side search filter on /api/trips
        searched = http.get("/api/trips?search=77701").json()["trips"]
        assert len(searched) == 1 and searched[0]["byd_trip_id"] == 77701


def test_04_multi_day_mileage_and_exports(mqtt_pub):
    """
    Seeds the multi-day Kathmandu dataset (Budhanilkantha descent, Kirtipur climb, Naxal cross-town)
    and verifies mileage aggregations, daily series, GPX XML export, and CSV export.
    """
    clear_all_trips()
    seed_multi_day_dataset(delay_s=0.005)
    time.sleep(0.4)

    with httpx.Client(base_url=BASE_URL, timeout=10.0) as http:
        ov = http.get("/api/overview").json()
        m = ov["mileage"]
        assert m["total_trips"] == 3
        assert m["today_km"] > 3.0
        assert m["week_km"] > m["today_km"]
        assert m["avg_wh_per_km"] > 50.0
        assert m["avg_km_per_kwh"] > 2.0
        assert len(ov["daily_series"]) >= 2
        assert len(ov["recent_charges"]) >= 2

        trips = http.get("/api/trips").json()["trips"]
        assert len(trips) == 3
        trip_id = trips[0]["id"]

        # Verify GPX Export
        gpx_res = http.get(f"/api/trips/{trip_id}/export.gpx")
        assert gpx_res.status_code == 200
        root = ET.fromstring(gpx_res.text)
        ns = {"gpx": "http://www.topografix.com/GPX/1/1"}
        trkpts = root.findall(".//gpx:trkpt", ns)
        assert len(trkpts) > 20
        first_lat = float(trkpts[0].attrib["lat"])
        assert 27.60 < first_lat < 27.85, "GPX track point must be in Kathmandu Valley"

        # Verify CSV Export
        csv_res = http.get(f"/api/trips/{trip_id}/export.csv")
        assert csv_res.status_code == 200
        reader = list(csv.reader(io.StringIO(csv_res.text)))
        assert reader[0][0] == "iso_time"
        assert "engine_power_kw" in reader[0]
        assert len(reader) > 20
