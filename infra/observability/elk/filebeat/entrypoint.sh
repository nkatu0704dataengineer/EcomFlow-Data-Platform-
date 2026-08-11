#!/bin/bash
# ==========================================
# Filebeat Entrypoint - EcomFlow
# ==========================================
# Purpose: Bootstrap script to solve Windows Docker bind mount permission issue
#
# Problem:
#   Windows bind mounts expose files with 777 permissions inside containers.
#   Filebeat security check refuses to start with world-writable config files.
#
# Solution:
#   1. Copy read-only source config to internal filesystem
#   2. Set strict owner-only permissions (600)
#   3. Launch Filebeat using internal config
#
# This script does NOT contain:
#   - Log parsing logic (belongs in filebeat.yml)
#   - Business logic (belongs in Logstash)
#   - Application logic (belongs in Airflow/Databricks)
#
# It exists ONLY to solve the runtime permission problem.
# ==========================================

set -e  # Exit immediately on error
set -u  # Exit on undefined variable

# ==========================================
# Configuration
# ==========================================

# Source: Read-only mounted config from Windows host
SOURCE_CONFIG="/tmp/filebeat.yml"

# Destination: Internal filesystem with strict permissions
DEST_CONFIG="/usr/share/filebeat/filebeat.yml"

# ==========================================
# Validation
# ==========================================

echo "[Filebeat Bootstrap] Starting..."

# Verify source config exists
if [ ! -f "${SOURCE_CONFIG}" ]; then
    echo "[ERROR] Source config not found: ${SOURCE_CONFIG}"
    echo "[ERROR] Expected docker-compose to mount: ./filebeat/filebeat.yml:/tmp/filebeat.yml:ro"
    exit 1
fi

echo "[Filebeat Bootstrap] Source config found: ${SOURCE_CONFIG}"

# ==========================================
# Copy and Secure
# ==========================================

# Copy source to destination
cp "${SOURCE_CONFIG}" "${DEST_CONFIG}"
echo "[Filebeat Bootstrap] Config copied to: ${DEST_CONFIG}"

# Set strict permissions (owner read/write only)
chmod 600 "${DEST_CONFIG}"
chown root:root "${DEST_CONFIG}"
echo "[Filebeat Bootstrap] Permissions set: 600 (owner read/write only)"

# Verify final permissions
PERMS=$(stat -c "%a" "${DEST_CONFIG}" 2>/dev/null || stat -f "%A" "${DEST_CONFIG}")
echo "[Filebeat Bootstrap] Final permissions: ${PERMS}"

if [ "${PERMS}" != "600" ]; then
    echo "[WARNING] Permissions are ${PERMS}, expected 600"
    echo "[WARNING] Filebeat may still reject the config"
fi

# ==========================================
# Launch Filebeat
# ==========================================

echo "[Filebeat Bootstrap] Launching Filebeat..."
echo "[Filebeat Bootstrap] Config: ${DEST_CONFIG}"
echo "[Filebeat Bootstrap] Command: filebeat -e -c ${DEST_CONFIG}"
echo "========================================="

# Use exec so Filebeat becomes PID 1
# This ensures proper signal handling and container lifecycle
exec filebeat -e -c "${DEST_CONFIG}"