#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ADB="${ADB:-$(command -v adb 2>/dev/null || echo "$SCRIPT_DIR/platform-tools/adb")}"
APK_PATH="${APK_PATH:-$SCRIPT_DIR/byd-trip-stats.apk}"
MAC_IP=$(ipconfig getifaddr en0 2>/dev/null || ipconfig getifaddr en1 2>/dev/null || hostname -I 2>/dev/null | awk '{print $1}' || echo "127.0.0.1")

echo "============================================================"
echo "  BYD Atto 3 — In-Car App Installer (USB or Wi-Fi)"
echo "============================================================"
echo "💡 Tip: If both this computer & the car are on the same Wi-Fi/Hotspot,"
echo "   you can also open the car's Browser and go to:"
echo "   👉 http://${MAC_IP}:8080/apk"
echo "------------------------------------------------------------"

if [ ! -f "$APK_PATH" ]; then
  echo "⚠️  APK not found at $APK_PATH"
  echo "    Run ./download_apk.sh to fetch the latest byd-trip-stats release,"
  echo "    or download manually from: https://github.com/angoikon/byd-trip-stats/releases"
  exit 1
fi

if ! command -v "$ADB" >/dev/null 2>&1 && [ ! -x "$ADB" ]; then
  echo "❌ ADB executable not found ($ADB). Please install Android platform-tools (e.g. brew install android-platform-tools or apt install adb)."
  exit 1
fi

if [ $# -ge 1 ]; then
  CAR_IP="$1"
  echo "==> Connecting to BYD Atto 3 over Wi-Fi at ${CAR_IP}:5555..."
  echo "    (Watch the car screen and tap 'Allow' on the Debugging prompt!)"
  "$ADB" connect "${CAR_IP}:5555"
  echo "==> Installing byd-trip-stats.apk onto ${CAR_IP}:5555..."
  "$ADB" -s "${CAR_IP}:5555" install -r "$APK_PATH"
  echo "✅ Successfully installed BYD Trip Stats on your BYD Atto 3!"
  exit 0
fi

echo "==> Checking for USB-connected BYD Atto 3..."
"$ADB" start-server
DEVICES=$("$ADB" devices | grep -v "List of devices" | grep -v "^$" || true)

if [ -n "$DEVICES" ]; then
  echo "==> Found connected device:"
  echo "$DEVICES"
  echo "==> Installing byd-trip-stats.apk..."
  "$ADB" install -r "$APK_PATH"
  echo "✅ Successfully installed BYD Trip Stats on your BYD Atto 3!"
else
  echo "No USB cable connection detected."
  echo ""
  echo "To install over Wi-Fi / Hotspot with ADB, pass the car's IP address:"
  echo "  ./wireless_adb_install.sh <CAR_IP_ADDRESS>"
fi
