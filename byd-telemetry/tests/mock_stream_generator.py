#!/usr/bin/env python3
"""
Realistic BYD Atto 3 Telemetry & Physics Stream Generator for Kathmandu Valley.

Models authentic BYD Atto 3 (60.48 kWh Blade LFP Battery, 150 kW Front Motor) behavior:
- Speed- & grade-dependent motor power draw (+kW) and regenerative braking (-kW)
- HV pack voltage sag under load (~404V nominal, sagging with current draw)
- Cell min/max voltage spread (mV) increasing under high current load
- Tire pressure (PSI) & temperature (°C) warming up over a drive
- Multi-day Kathmandu trips + live real-time streaming mode + parked 30s pings + AC charging
"""
import argparse
import json
import math
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

import paho.mqtt.client as mqtt

MQTT_HOST = os.environ.get("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USER = os.environ.get("MQTT_USER", "byd")
MQTT_PASS = os.environ.get("MQTT_PASS", "byd-atto3-ktm-2026")
DEVICE_ID = os.environ.get("DEVICE_ID", "Atto3")

# Realistic Kathmandu routes: (lat, lon, altitude_m, target_speed_kmh)
ROUTES: Dict[str, Dict[str, Any]] = {
    "budhanilkantha_descent": {
        "name": "Budhanilkantha -> Maharajgunj -> Lazimpat -> Durbar Marg (Downhill Regen Commute)",
        "drive_mode": "Eco",
        "regen_mode": "High",
        "waypoints": [
            (27.7782, 85.3621, 1445.0, 22.0),  # Budhanilkantha Temple
            (27.7654, 85.3562, 1398.0, 38.0),  # Golfutar downhill (heavy regen)
            (27.7521, 85.3490, 1358.0, 42.0),  # Bansbari
            (27.7398, 85.3358, 1335.0, 30.0),  # Maharajgunj Narayan Gopal Chowk
            (27.7285, 85.3242, 1318.0, 26.0),  # Panipokhari
            (27.7210, 85.3210, 1310.0, 34.0),  # Lazimpat
            (27.7125, 85.3182, 1303.0, 18.0),  # Durbar Marg
            (27.7102, 85.3175, 1302.0, 0.0),   # Parked at Durbar Marg
        ],
    },
    "naxal_to_jhamsikhel": {
        "name": "Naxal -> Sundhara -> Tripureshwor -> Pulchowk -> Jhamsikhel (City Cross-Town)",
        "drive_mode": "Normal",
        "regen_mode": "High",
        "waypoints": [
            (27.7196, 85.3305, 1312.0, 20.0),  # Naxal Bhatbhateni
            (27.7138, 85.3225, 1305.0, 35.0),  # Kamalpokhari / Jamal
            (27.7058, 85.3145, 1298.0, 28.0),  # Ratnapark
            (27.6998, 85.3131, 1294.0, 16.0),  # Sundhara traffic slow-down
            (27.6941, 85.3142, 1289.0, 36.0),  # Tripureshwor
            (27.6882, 85.3168, 1285.0, 24.0),  # Thapathali Bagmati Bridge
            (27.6844, 85.3159, 1296.0, 40.0),  # Kupondole climb
            (27.6776, 85.3125, 1314.0, 32.0),  # Pulchowk Labim Mall
            (27.6815, 85.3048, 1308.0, 0.0),   # Jhamsikhel
        ],
    },
    "ring_road_kirtipur_climb": {
        "name": "Kalimati -> Kalanki -> Balkhu -> Kirtipur TU Gate (Steep Climb & Ring Road)",
        "drive_mode": "Sport",
        "regen_mode": "Standard",
        "waypoints": [
            (27.6985, 85.2990, 1292.0, 28.0),  # Kalimati
            (27.6938, 85.2815, 1305.0, 48.0),  # Kalanki Underpass
            (27.6845, 85.2975, 1288.0, 52.0),  # Balkhu Ring Road
            (27.6812, 85.2885, 1325.0, 44.0),  # Kirtipur uphill climb
            (27.6768, 85.2782, 1378.0, 35.0),  # Kirtipur steep section
            (27.6735, 85.2730, 1392.0, 0.0),   # Kirtipur hilltop parked
        ],
    },
}


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def generate_route_samples(
    route_key: str,
    start_time: datetime,
    start_odo: float,
    start_soc: float,
    start_discharge_kwh: float,
    steps_per_segment: int = 5,
    step_seconds: int = 15,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    route = ROUTES[route_key]
    waypoints = route["waypoints"]
    drive_mode = route["drive_mode"]
    regen_mode = route["regen_mode"]

    interpolated = []
    for i in range(len(waypoints) - 1):
        lat1, lon1, alt1, spd1 = waypoints[i]
        lat2, lon2, alt2, spd2 = waypoints[i + 1]
        dy, dx = lat2 - lat1, lon2 - lon1
        heading = round((math.degrees(math.atan2(dx, dy)) + 360) % 360, 1)
        for s in range(steps_per_segment):
            t = s / float(steps_per_segment)
            interpolated.append(
                (
                    lat1 + (lat2 - lat1) * t,
                    lon1 + (lon2 - lon1) * t,
                    alt1 + (alt2 - alt1) * t,
                    spd1 + (spd2 - spd1) * t,
                    heading,
                )
            )
    last_wp = waypoints[-1]
    interpolated.append((last_wp[0], last_wp[1], last_wp[2], 0.0, 180.0))

    samples: List[Dict[str, Any]] = []
    cum_dist_km = 0.0
    cum_energy_kwh = 0.0
    dt_hours = step_seconds / 3600.0

    for idx, (lat, lon, alt, spd, hdg) in enumerate(interpolated):
        is_last = idx == len(interpolated) - 1
        ts = start_time + timedelta(seconds=idx * step_seconds)

        if idx > 0:
            prev_lat, prev_lon, prev_alt, prev_spd, _ = interpolated[idx - 1]
            step_km = haversine_km(prev_lat, prev_lon, lat, lon)
            cum_dist_km += step_km
            alt_diff_m = alt - prev_alt
            accel = (spd - prev_spd) / max(1.0, step_seconds)
        else:
            step_km = 0.0
            alt_diff_m = 0.0
            accel = 0.2

        # Physics-based motor power model (rolling + aero + grade + acceleration + 0.6kW HVAC/aux)
        if is_last or spd < 0.5:
            power_kw = 0.45
        else:
            base_traction_kw = 0.12 * spd + 0.00045 * (spd ** 2)
            grade_kw = (alt_diff_m / max(1.0, step_seconds)) * 17.5  # 1750 kg Atto 3 mass * g
            accel_kw = accel * 18.0
            power_kw = round(max(-35.0, min(95.0, base_traction_kw + grade_kw + accel_kw + 0.6)), 2)

        step_kwh = power_kw * dt_hours
        cum_energy_kwh += step_kwh

        # Battery SoC (60.48 kWh usable pack)
        soc = round(max(5.0, min(100.0, start_soc - (cum_energy_kwh / 60.48) * 100.0)), 1)
        odo = round(start_odo + cum_dist_km, 2)
        total_dis = round(start_discharge_kwh + max(0.0, cum_energy_kwh), 3)

        # HV pack voltage sag under load & cell spread
        open_circuit_v = 396.0 + (soc / 100.0) * 14.0
        hv_voltage = round(open_circuit_v - (power_kw * 0.06), 1)
        hv_current = round((power_kw * 1000.0) / hv_voltage, 1)
        cell_avg_v = hv_voltage / 120.0
        spread_v = 0.008 + abs(power_kw) * 0.00025
        cell_min_v = round(cell_avg_v - spread_v / 2.0, 3)
        cell_max_v = round(cell_avg_v + spread_v / 2.0, 3)

        # Tire warm-up over the trip
        warmup = min(1.0, idx / 30.0)
        tire_temp = int(24 + warmup * 5)
        tire_psi_base = 35.8 + warmup * 0.6

        payload = {
            "current_datetime": ts.isoformat(),
            "soc": soc,
            "soc_panel": round(soc),
            "battery_12v_voltage": 13.82 if not is_last else 13.22,
            "battery_total_voltage": hv_voltage,
            "battery_total_current": hv_current,
            "electric_driving_range_km": round(soc * 4.2, 1),
            "total_discharge": total_dis,
            "speed": round(spd, 1),
            "gear": "P" if is_last else "D",
            "odometer": odo,
            "engine_power": power_kw,
            "engine_speed_front": int(spd * 185),
            "is_charging": False,
            "charging_power": 0.0,
            "car_on": not is_last,
            "car_locked": is_last,
            "any_door_opened": False,
            "drive_mode": drive_mode,
            "regen_mode": regen_mode,
            "energy_mode": "—",
            "location_latitude": round(lat, 6),
            "location_longitude": round(lon, 6),
            "location_altitude": round(alt, 1),
            "heading": hdg,
            "ext_temp": 23.0,
            "cabin_temp": 21.5,
            "soh": 100,
            "statistic_soh": 100.0,
            "available_power": 150.0,
            "battery_remain_power_ev": round(soc * 0.6048, 1),
            "battery_pack_temp": round(25.5 + warmup * 3.2, 1),
            "battery_cell_temp_min": round(25.0 + warmup * 2.8, 1),
            "battery_cell_temp_max": round(26.2 + warmup * 3.6, 1),
            "cell_voltage_min": cell_min_v,
            "cell_voltage_max": cell_max_v,
            "tyre_pressure_left_front_psi": round(tire_psi_base + 0.1, 2),
            "tyre_pressure_right_front_psi": round(tire_psi_base, 2),
            "tyre_pressure_left_rear_psi": round(tire_psi_base + 0.2, 2),
            "tyre_pressure_right_rear_psi": round(tire_psi_base + 0.15, 2),
            "tyre_temperature_left_front_c": tire_temp,
            "tyre_temperature_right_front_c": tire_temp,
            "tyre_temperature_left_rear_c": tire_temp - 1,
            "tyre_temperature_right_rear_c": tire_temp - 1,
        }
        samples.append(payload)

    duration_s = (len(interpolated) - 1) * step_seconds
    end_time = start_time + timedelta(seconds=duration_s)
    net_kwh = round(max(0.15, cum_energy_kwh), 3)
    dist_km = round(max(0.2, cum_dist_km), 2)
    eff_100km = round((net_kwh * 100.0) / dist_km, 2)
    avg_spd = round(dist_km / max(0.01, duration_s / 3600.0), 1)

    summary = {
        "event": "trip",
        "trip_id": int(start_time.timestamp()) % 100000,
        "started_at": start_time.isoformat(),
        "ended_at": end_time.isoformat(),
        "started_at_ms": int(start_time.timestamp() * 1000),
        "ended_at_ms": int(end_time.timestamp() * 1000),
        "closed_at": end_time.isoformat(),
        "closed_at_ms": int(end_time.timestamp() * 1000),
        "duration_s": duration_s,
        "distance_km": dist_km,
        "energy_kwh": net_kwh,
        "efficiency_kwh_per_100km": eff_100km,
        "fleet_avg_kwh_per_100km": 14.2,
        "avg_speed_kmh": avg_spd,
        "soc_start_pct": start_soc,
        "soc_end_pct": samples[-1]["soc"],
        "soc_source": "bms",
        "trip_score": 92 if eff_100km < 15.0 else 84,
    }
    return samples, summary


def seed_multi_day_dataset(
    mqtt_host: str = MQTT_HOST,
    mqtt_port: int = MQTT_PORT,
    mqtt_user: str = MQTT_USER,
    mqtt_pass: str = MQTT_PASS,
    delay_s: float = 0.01,
) -> None:
    """Publishes 3 realistic Kathmandu trips across different days + 2 charging sessions."""
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="byd-e2e-seeder")
    client.username_pw_set(mqtt_user, mqtt_pass)
    client.connect(mqtt_host, mqtt_port, keepalive=60)
    client.loop_start()

    client.publish(f"byd-trip-stats/{DEVICE_ID}/availability", "online", qos=1, retain=True)

    now = datetime.now(timezone.utc)
    schedule = [
        ("budhanilkantha_descent", now - timedelta(days=2, hours=3), 4795.0, 90.0, 735.0),
        ("ring_road_kirtipur_climb", now - timedelta(days=1, hours=2), 4804.2, 88.2, 736.1),
        ("naxal_to_jhamsikhel", now - timedelta(minutes=25), 4813.8, 84.5, 738.4),
    ]

    for route_key, start_dt, odo, soc, dis in schedule:
        samples, summary = generate_route_samples(route_key, start_dt, odo, soc, dis)
        for sample in samples:
            info = client.publish(
                f"byd-trip-stats/{DEVICE_ID}/state",
                json.dumps(sample),
                qos=1,
                retain=True,
            )
            info.wait_for_publish()
            if delay_s > 0:
                time.sleep(delay_s)

        info = client.publish(
            f"byd-trip-stats/{DEVICE_ID}/trip",
            json.dumps(summary),
            qos=1,
            retain=True,
        )
        info.wait_for_publish()
        time.sleep(0.05)

    # Publish 2 AC charging sessions (7kW BYD home wallbox)
    charges = [
        {
            "event": "charge_session",
            "session_id": 201,
            "started_at_ms": int((now - timedelta(days=2)).timestamp() * 1000),
            "ended_at_ms": int((now - timedelta(days=2) + timedelta(hours=3)).timestamp() * 1000),
            "duration_s": 10800,
            "soc_start_pct": 54.0,
            "soc_end_pct": 88.2,
            "kwh_added": 20.68,
            "avg_kw": 6.6,
            "peak_kw": 6.9,
        },
        {
            "event": "charge_session",
            "session_id": 202,
            "started_at_ms": int((now - timedelta(hours=6)).timestamp() * 1000),
            "ended_at_ms": int((now - timedelta(hours=3, minutes=30)).timestamp() * 1000),
            "duration_s": 9000,
            "soc_start_pct": 60.0,
            "soc_end_pct": 84.5,
            "kwh_added": 14.82,
            "avg_kw": 6.5,
            "peak_kw": 6.8,
        },
    ]
    for ch in charges:
        info = client.publish(
            f"byd-trip-stats/{DEVICE_ID}/charge_session",
            json.dumps(ch),
            qos=1,
            retain=True,
        )
        info.wait_for_publish()

    client.loop_stop()
    client.disconnect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BYD Atto 3 Mock Telemetry Streamer")
    parser.add_argument("--delay", type=float, default=0.02, help="Delay in seconds between MQTT packets")
    args = parser.parse_args()
    seed_multi_day_dataset(delay_s=args.delay)
    print("Multi-day Kathmandu telemetry dataset streamed successfully.")
