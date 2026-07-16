"""Centralized Databricks job registry for EcomFlow.

This module is the single source of truth for Databricks job IDs used by Airflow
and other orchestration components. It keeps the runtime configuration lightweight
and makes it easy to extend the registry for future jobs such as Silver, Gold,
streaming, or ML workloads.
"""

from __future__ import annotations

from typing import Final

BRONZE_JOB_NAME: Final[str] = "EcomFlow Bronze Pipeline"
BRONZE_JOB_ID: Final[int] = 590069787429591

_JOB_REGISTRY: Final[dict[str, int]] = {
    "bronze": BRONZE_JOB_ID,
    BRONZE_JOB_NAME.casefold(): BRONZE_JOB_ID,
}


def get_databricks_job_registry() -> dict[str, int]:
    """Return a copy of the Databricks job registry."""
    return dict(_JOB_REGISTRY)


def get_job_id(job_key: str) -> int:
    """Return the Databricks job ID for the provided logical job key."""
    normalized_key = job_key.strip().casefold()
    if normalized_key in _JOB_REGISTRY:
        return _JOB_REGISTRY[normalized_key]

    raise ValueError(f"Unknown Databricks job key: {job_key}")


def get_bronze_job_id() -> int:
    """Return the Databricks job ID for the Bronze runner job."""
    return get_job_id("bronze")


__all__ = [
    "BRONZE_JOB_ID",
    "BRONZE_JOB_NAME",
    "get_bronze_job_id",
    "get_databricks_job_registry",
    "get_job_id",
]
