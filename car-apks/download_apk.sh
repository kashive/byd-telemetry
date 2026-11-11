#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_FILE="$SCRIPT_DIR/byd-trip-stats.apk"

echo "==> Fetching latest BYD Trip Stats release from GitHub (angoikon/byd-trip-stats)..."

LATEST_TAG=$(curl -sSL "https://api.github.com/repos/angoikon/byd-trip-stats/releases/latest" | grep '"tag_name":' | head -n 1 | cut -d '"' -f 4 || echo "v2.18.0")

if [ -z "$LATEST_TAG" ]; then
  LATEST_TAG="v2.18.0"
fi

echo "==> Latest release tag: ${LATEST_TAG}"
DOWNLOAD_URL="https://github.com/angoikon/byd-trip-stats/releases/download/${LATEST_TAG}/byd-trip-stats.apk"

echo "==> Downloading from ${DOWNLOAD_URL}..."
if curl -sSL -f -L -o "$TARGET_FILE" "$DOWNLOAD_URL"; then
  echo "✅ Downloaded successfully to: $TARGET_FILE"
else
  echo "⚠️  Direct tag download failed. Trying fallback release link..."
  curl -sSL -L -o "$TARGET_FILE" "https://github.com/angoikon/byd-trip-stats/releases/download/v2.18.0/byd-trip-stats.apk"
  echo "✅ Downloaded to: $TARGET_FILE"
fi
chmod +x "$TARGET_FILE" 2>/dev/null || true
