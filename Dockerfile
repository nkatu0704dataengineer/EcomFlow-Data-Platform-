FROM astrocrpublic.azurecr.io/runtime:3.2-5

# ============================================================
# EcomFlow Observability - Enable Airflow StatsD Metrics
# ============================================================
# Purpose:
#   Configure Airflow to send metrics to Telegraf (TIG stack)
#   for monitoring and visualization in Grafana.
#
# Architecture:
#   Airflow Container
#      ↓ UDP :8125 (StatsD protocol)
#   host.docker.internal (Windows host)
#      ↓
#   Telegraf Container (TIG stack)
#      ↓
#   InfluxDB → Grafana
# ============================================================

# Enable StatsD metrics collection
ENV AIRFLOW__METRICS__STATSD_ON=True

# StatsD server address
# host.docker.internal = Docker Desktop special DNS name for host machine
ENV AIRFLOW__METRICS__STATSD_HOST=host.docker.internal

# StatsD server port (matches Telegraf's listening port)
ENV AIRFLOW__METRICS__STATSD_PORT=8125

# Metric prefix (all metrics will start with "airflow.")
ENV AIRFLOW__METRICS__STATSD_PREFIX=airflow

# Optional: Enable additional metric types
# Uncomment if you want more detailed metrics
# ENV AIRFLOW__METRICS__STATSD_ALLOW_LIST=*
