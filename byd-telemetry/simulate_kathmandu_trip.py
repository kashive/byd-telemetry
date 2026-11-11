#!/usr/bin/env python3
"""
Simulates a realistic BYD Atto 3 drive across Kathmandu Valley:
Route: Naxal (Bhatbhateni) -> Durbar Marg -> Sundhara -> Tripureshwor -> Thapathali Bridge -> Pulchowk -> Jhamsikhel
Publishes authentic `byd-trip-stats/Atto3/state` and `byd-trip-stats/Atto3/trip` MQTT packets.
"""
import json
import math
import os
import time
from datetime import datetime, timedelta, timezone

import paho.mqtt.client as mqtt

MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USER = os.environ.get("MQTT_USER", "byd")
MQTT_PASS = os.environ.get("MQTT_PASS", "byd-atto3-ktm-2026")
DEVICE_ID = "Atto3"

# Waypoints along a real Kathmandu route (Lat, Lon, Altitude m, Target Speed km/h, Engine Power kW)
WAYPOINTS = [
    (27.7196, 85.3305, 1312, 18.0, 12.5),  # Naxal Bhatbhateni start
    (27.7172, 85.3278, 1309, 28.0, 18.2),  # Naxal -> Nagpokhari
    (27.7138, 85.3225, 1305, 34.0, 22.0),  # Jamal / Durbar Marg north
    (27.7105, 85.3177, 1302, 24.0, -6.5),  # Durbar Marg (regen braking before roundabout)
    (27.7058, 85.3145, 1298, 31.0, 16.4),  # Ratnapark / Bir Hospital
    (27.6998, 85.3131, 1294, 22.0, -8.2),  # Sundhara / Dharahara (regen)
    (27.6941, 85.3142, 1289, 38.0, 26.5),  # Tripureshwor Chowk
    (27.6915, 85.3178, 1286, 29.0, 14.0),  # Thapathali
    (27.6882, 85.3168, 1285, 25.0, -4.5),  # Bagmati Bridge crossing into Lalitpur
    (27.6844, 85.3159, 1295, 42.0, 34.0),  # Kupondole uphill climb
    (27.6805, 85.3145, 1308, 36.0, 21.5),  # Pulchowk Damkal
    (27.6776, 85.3125, 1314, 30.0, 15.0),  # Pulchowk Labim Mall
    (27.6789, 85.3075, 1311, 20.0, -5.0),  # Turning into Jhamsikhel
    (27.6815, 85.3048, 1308, 0.0, 0.5),    # Jhamsikhel destination (Parked)
]


def interpolate_points(waypoints, steps_per_segment=4):
    pts = []
    for i in range(len(waypoints) - 1):
        lat1, lon1, alt1, spd1, pwr1 = waypoints[i]
        lat2, lon2, alt2, spd2, pwr2 = waypoints[i + 1]
        dy = lat2 - lat1
        dx = lon2 - lon1
        heading = (math.degrees(math.atan2(dx, dy)) + 360) % 360
        for s in range(steps_per_segment):
            t = s / float(steps_per_segment)
            pts.append(
                (
                    lat1 + (lat2 - lat1) * t,
                    lon1 + (lon2 - lon1) * t,
                    alt1 + (alt2 - alt1) * t,
                    spd1 + (spd2 - spd1) * t,
                    pwr1 + (pwr2 - pwr1) * t,
                    round(heading, 1),
                )
            )
    last = waypoints[-1]
    pts.append((last[0], last[1], last[2], 0.0, 0.4, 290.0))
    return pts


def run_simulation():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="byd-ktm-simulator")
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()

    client.publish(f"byd-trip-stats/{DEVICE_ID}/availability", "online", qos=1, retain=True)

    points = interpolate_points(WAYPOINTS, steps_per_segment=4)
    total_steps = len(points)
    start_odo = 4820.4
    total_dist_km = 6.8
    start_soc = 84.0
    end_soc = 82.4
    start_discharge = 742.10
    total_energy_kwh = 0.96

    base_time = datetime.now(timezone.utc) - timedelta(minutes=18)

    for idx, (lat, lon, alt, spd, pwr, hdg) in enumerate(points):
        frac = idx / float(max(1, total_steps - 1))
        ts = base_time + timedelta(seconds=idx * 20)
        is_last = idx == total_steps - 1

        odo = round(start_odo + total_dist_km * frac, 2)
        soc = round(start_soc - (start_soc - end_soc) * frac, 1)
        discharge = round(start_discharge + total_energy_kwh * frac, 3)

        payload = {
            "current_datetime": ts.isoformat(),
            "soc": soc,
            "soc_panel": round(soc),
            "battery_12v_voltage": 13.78 if not is_last else 13.25,
            "battery_total_voltage": round(404.2 - frac * 1.4, 1),
            "battery_total_current": round((pwr * 1000.0) / 403.5, 1),
            "electric_driving_range_km": round(soc * 4.2, 1),
            "total_discharge": discharge,
            "speed": round(spd, 1),
            "gear": "P" if is_last else "D",
            "odometer": odo,
            "engine_power": round(pwr, 2),
            "engine_speed_front": int(spd * 185),
            "is_charging": False,
            "charging_power": 0.0,
            "car_on": not is_last,
            "car_locked": is_last,
            "any_door_opened": False,
            "drive_mode": "Normal",
            "regen_mode": "High",
            "energy_mode": "—",
            "location_latitude": round(lat, 6),
            "location_longitude": round(lon, 6),
            "location_altitude": round(alt, 1),
            "heading": hdg,
            "ext_temp": 22.5,
            "cabin_temp": 21.0,
            "soh": 100,
            "statistic_soh": 100.0,
            "available_power": 150.0,
            "battery_remain_power_ev": round(soc * 0.6048, 1),
            "battery_pack_temp": 26.4,
            "battery_cell_temp_min": 25.8,
            "battery_cell_temp_max": 27.1,
            "cell_voltage_min": 3.298,
            "cell_voltage_max": 3.312,
            "tyre_pressure_left_front_psi": 36.2,
            "tyre_pressure_right_front_psi": 36.1,
            "tyre_pressure_left_rear_psi": 36.4,
            "tyre_pressure_right_rear_psi": 36.3,
            "tyre_temperature_left_front_c": 28,
            "tyre_temperature_right_front_c": 28,
            "tyre_temperature_left_rear_c": 27,
            "tyre_temperature_right_rear_c": 27,
        }

        info = client.publish(
            f"byd-trip-stats/{DEVICE_ID}/state",
            json.dumps(payload),
            qos=1,
            retain=True,
        )
        info.wait_for_publish()
        time.sleep(0.05)

    # Publish completed trip summary packet (`byd-trip-stats/Atto3/trip`)
    end_time = base_time + timedelta(seconds=(total_steps - 1) * 20)
    trip_summary = {
        "event": "trip",
        "trip_id": 101,
        "started_at": base_time.isoformat(),
        "ended_at": end_time.isoformat(),
        "started_at_ms": int(base_time.timestamp() * 1000),
        "ended_at_ms": int(end_time.timestamp() * 1000),
        "closed_at": end_time.isoformat(),
        "closed_at_ms": int(end_time.timestamp() * 1000),
        "duration_s": (total_steps - 1) * 20,
        "distance_km": total_dist_km,
        "energy_kwh": total_energy_kwh,
        "efficiency_kwh_per_100km": round(total_energy_kwh * 100.0 / total_dist_km, 2),
        "fleet_avg_kwh_per_100km": 14.5,
        "avg_speed_kmh": round(total_dist_km / (((total_steps - 1) * 20) / 3600.0), 1),
        "soc_start_pct": start_soc,
        "soc_end_pct": end_soc,
        "soc_source": "bms",
        "trip_score": 94,
    }
    info = client.publish(
        f"byd-trip-stats/{DEVICE_ID}/trip",
        json.dumps(trip_summary),
        qos=1,
        retain=True,
    )
    info.wait_for_publish()

    # Publish a sample charging session summary (`byd-trip-stats/Atto3/charge_session`)
    charge_summary = {
        "event": "charge_session",
        "session_id": 12,
        "started_at_ms": int((base_time - timedelta(hours=5)).timestamp() * 1000),
        "ended_at_ms": int((base_time - timedelta(hours=2)).timestamp() * 1000),
        "duration_s": 10800,
        "soc_start_pct": 52.0,
        "soc_end_pct": 84.0,
        "kwh_added": 19.35,
        "avg_kw": 6.45,
        "peak_kw": 6.80,
    }
    info = client.publish(
        f"byd-trip-stats/{DEVICE_ID}/charge_session",
        json.dumps(charge_summary),
        qos=1,
        retain=True,
    )
    info.wait_for_publish()

    client.loop_stop()
    client.disconnect()
    print("Simulated Kathmandu drive (Naxal -> Jhamsikhel, 6.8 km) published successfully!")


if __name__ == "__main__":
    run_simulation()
