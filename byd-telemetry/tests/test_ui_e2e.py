"""
Comprehensive Browser UI End-to-End Tests using Playwright (Chromium).
Verifies real-time WebSocket DOM streaming, Kathmandu Leaflet Map, Mileage & Efficiency Charts,
Trip History Filtering, and Trip Detail Route Scrubbing.
"""
import json
import os
import re
import time
from datetime import datetime, timezone

import httpx
import paho.mqtt.client as mqtt
import pytest
from playwright.sync_api import Page, expect

from mock_stream_generator import (
    DEVICE_ID,
    MQTT_HOST,
    MQTT_PASS,
    MQTT_PORT,
    MQTT_USER,
    seed_multi_day_dataset,
)

BASE_URL = os.environ.get("BASE_URL", "http://localhost:8080")
SCREENSHOT_DIR = os.environ.get("SCREENSHOT_DIR", "/data/test-screenshots")


@pytest.fixture(scope="module", autouse=True)
def seed_data_for_ui():
    os.makedirs(SCREENSHOT_DIR, exist_ok=True)
    with httpx.Client(base_url=BASE_URL, timeout=10.0) as client:
        for t in client.get("/api/trips?limit=500").json().get("trips", []):
            client.delete(f"/api/trips/{t['id']}")
    seed_multi_day_dataset(delay_s=0.005)
    time.sleep(0.5)


@pytest.fixture(scope="module")
def mqtt_live():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="playwright-live-streamer")
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()
    yield client
    client.loop_stop()
    client.disconnect()


def test_ui_01_live_map_and_realtime_websocket_streaming(page: Page, mqtt_live):
    """
    Opens the dashboard, verifies the Leaflet Kathmandu map and car marker,
    then streams live DRIVE and REGEN packets via MQTT and asserts the UI gauges
    and map coordinates update live via WebSocket without page reload.
    """
    page.set_viewport_size({"width": 1440, "height": 900})
    page.goto(BASE_URL, wait_until="networkidle")

    # Verify header & MQTT connected badge
    expect(page.locator("#conn-badge")).to_contain_text("MQTT Connected")
    expect(page.locator("#live-map")).to_be_visible()
    expect(page.locator("#live-map .car-marker-wrap")).to_be_visible()

    # Wait briefly for WebSocket connection to establish
    page.wait_for_timeout(400)

    # 1. Stream a live DRIVE packet at Durbar Marg (44.5 km/h, +28.4 kW)
    drive_packet = {
        "current_datetime": datetime.now(timezone.utc).isoformat(),
        "soc": 81.2,
        "soc_panel": 81,
        "battery_12v_voltage": 13.85,
        "battery_total_voltage": 401.5,
        "battery_total_current": 70.7,
        "electric_driving_range_km": 341.0,
        "total_discharge": 745.2,
        "speed": 44.5,
        "gear": "D",
        "odometer": 4850.5,
        "engine_power": 28.4,
        "car_on": True,
        "car_locked": False,
        "is_charging": False,
        "drive_mode": "Sport",
        "regen_mode": "High",
        "location_latitude": 27.71251,
        "location_longitude": 85.31821,
        "location_altitude": 1304.0,
        "heading": 175.0,
        "ext_temp": 24.0,
        "cabin_temp": 21.0,
        "soh": 100,
        "battery_pack_temp": 27.8,
        "cell_voltage_min": 3.295,
        "cell_voltage_max": 3.314,
        "tyre_pressure_left_front_psi": 36.5,
        "tyre_pressure_right_front_psi": 36.4,
        "tyre_pressure_left_rear_psi": 36.6,
        "tyre_pressure_right_rear_psi": 36.5,
        "tyre_temperature_left_front_c": 29,
        "tyre_temperature_right_front_c": 29,
        "tyre_temperature_left_rear_c": 28,
        "tyre_temperature_right_rear_c": 28,
    }
    mqtt_live.publish(f"byd-trip-stats/{DEVICE_ID}/state", json.dumps(drive_packet), qos=1).wait_for_publish()

    # Assert WebSocket live DOM update without reload
    expect(page.locator("#live-speed")).to_have_text("44.5", timeout=5000)
    expect(page.locator("#live-gear")).to_have_text("D")
    expect(page.locator("#live-soc")).to_have_text("81.2")
    expect(page.locator("#live-power")).to_have_text("28.4")
    expect(page.locator("#live-power-tag")).to_have_text("DRIVE")
    expect(page.locator("#live-coords-text")).to_contain_text("27.71251° N, 85.31821° E")
    expect(page.locator("#cell-delta-mv")).to_have_text("19 mV")
    expect(page.locator("#tire-lf-psi")).to_have_text("36.5")

    # 2. Stream a live REGENERATIVE BRAKING packet (-14.6 kW)
    regen_packet = dict(drive_packet)
    regen_packet["current_datetime"] = datetime.now(timezone.utc).isoformat()
    regen_packet["speed"] = 26.0
    regen_packet["engine_power"] = -14.6
    regen_packet["location_latitude"] = 27.71120
    regen_packet["location_longitude"] = 85.31790
    mqtt_live.publish(f"byd-trip-stats/{DEVICE_ID}/state", json.dumps(regen_packet), qos=1).wait_for_publish()

    expect(page.locator("#live-speed")).to_have_text("26.0", timeout=5000)
    expect(page.locator("#live-power")).to_have_text("-14.6")
    expect(page.locator("#live-power-tag")).to_have_text("REGEN")

    # Finalize the short live test drive so trips stay clean
    park_packet = dict(regen_packet)
    park_packet["current_datetime"] = datetime.now(timezone.utc).isoformat()
    park_packet["speed"] = 0.0
    park_packet["gear"] = "P"
    park_packet["engine_power"] = 0.3
    park_packet["car_on"] = False
    park_packet["car_locked"] = True
    mqtt_live.publish(f"byd-trip-stats/{DEVICE_ID}/state", json.dumps(park_packet), qos=1).wait_for_publish()
    page.wait_for_timeout(300)

    page.screenshot(path=os.path.join(SCREENSHOT_DIR, "01_live_map_view.png"), full_page=True)


def test_ui_02_mileage_and_efficiency_dashboard(page: Page):
    """
    Switches to the Mileage & Efficiency tab and verifies the Odometer, Today/7-Day/Monthly
    mileage cards, Chart.js datasets, and Recorded Charging Sessions table.
    """
    page.set_viewport_size({"width": 1440, "height": 900})
    page.goto(BASE_URL, wait_until="networkidle")

    page.click("#tab-btn-mileage")
    expect(page.locator("#view-mileage")).to_be_visible()
    expect(page.locator("#view-live")).to_be_hidden()

    # Verify Mileage cards have valid numbers
    odo_text = page.locator("#mil-odo").inner_text()
    assert odo_text != "—" and float(odo_text.replace(",", "")) > 4700

    week_km = float(page.locator("#mil-week-km").inner_text())
    assert week_km > 10.0, f"Expected >10 km weekly mileage from seeded Kathmandu trips, got {week_km}"

    wh_km = float(page.locator("#mil-wh-km").inner_text())
    assert 50.0 < wh_km < 300.0

    # Verify Chart.js instances rendered multi-day data
    km_chart_len = page.evaluate("() => dailyKmChart && dailyKmChart.data.datasets[0].data.length")
    eff_chart_len = page.evaluate("() => dailyEffChart && dailyEffChart.data.datasets[0].data.length")
    assert km_chart_len >= 2
    assert eff_chart_len >= 2

    # Verify Charging Sessions table
    expect(page.locator("#charges-table-body")).to_contain_text("kWh")
    page.wait_for_timeout(300)
    page.screenshot(path=os.path.join(SCREENSHOT_DIR, "02_mileage_efficiency_view.png"), full_page=True)


def test_ui_03_trip_history_and_search_filter(page: Page):
    """
    Switches to the Trip History tab, verifies all recorded trips are listed with
    distance, odometer span, SoC used, and efficiency, and tests search filtering.
    """
    page.set_viewport_size({"width": 1440, "height": 900})
    page.goto(BASE_URL, wait_until="networkidle")

    page.click("#tab-btn-trips")
    expect(page.locator("#view-trips")).to_be_visible()

    rows = page.locator("#trips-table-body tr")
    row_count = rows.count()
    assert row_count >= 3, f"Expected at least 3 trips in history table, got {row_count}"

    page.screenshot(path=os.path.join(SCREENSHOT_DIR, "03_trip_history_view.png"), full_page=True)

    # Filter by non-existent string and verify empty state
    page.fill("#trip-search", "nonexistent-trip-xyz")
    expect(page.locator("#trips-table-body")).to_contain_text("No matching trips found.")

    # Clear filter and verify rows return
    page.fill("#trip-search", "")
    assert page.locator("#trips-table-body tr").count() == row_count


def test_ui_04_trip_detail_route_map_and_chart_scrubbing(page: Page):
    """
    Clicks a trip to open the Trip Detail Page, verifies route polylines on the Kathmandu map,
    toggles between Speed and Power/Regen coloring, and tests interactive chart hover scrubbing.
    """
    page.set_viewport_size({"width": 1440, "height": 1050})
    page.goto(BASE_URL, wait_until="networkidle")

    page.click("#tab-btn-trips")
    page.locator("#trips-table-body tr").first.click()

    expect(page.locator("#view-trip-detail")).to_be_visible()
    expect(page.locator("#detail-title")).to_contain_text("km in Kathmandu")
    expect(page.locator("#detail-export-gpx")).to_have_attribute("href", re.compile(r"^/api/trips/\d+/export\.gpx$"))

    # Wait for Leaflet detailMap fitBounds & polyline drawing
    page.wait_for_timeout(400)
    layer_count = page.evaluate("() => detailRouteGroup.getLayers().length")
    assert layer_count > 15, f"Expected >15 route polyline segments on detail map, got {layer_count}"

    # Switch route color mode from Speed to Power / Regen
    page.click("#color-by-power")
    mode = page.evaluate("() => routeColorMode")
    assert mode == "power"

    # Test interactive chart hover scrubbing (`highlightPointOnDetailMap`)
    initial_latlng = page.evaluate("() => detailHoverMarker.getLatLng()")
    page.evaluate("() => highlightPointOnDetailMap(10)")
    scrubbed_latlng = page.evaluate("() => detailHoverMarker.getLatLng()")

    assert (initial_latlng["lat"], initial_latlng["lng"]) != (scrubbed_latlng["lat"], scrubbed_latlng["lng"]), (
        "Hover marker on Kathmandu map must move along the route when scrubbing the chart"
    )
    expect(page.locator("#detail-hover-readout")).to_contain_text("Speed:")
    expect(page.locator("#detail-hover-readout")).to_contain_text("Power:")

    page.screenshot(path=os.path.join(SCREENSHOT_DIR, "04_trip_detail_view.png"), full_page=True)
