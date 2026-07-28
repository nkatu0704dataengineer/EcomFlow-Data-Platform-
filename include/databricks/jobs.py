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

SILVER_JOB_NAME: Final[str] = "EcomFlow Silver Pipeline"
SILVER_JOB_ID: Final[int] = 392046771671346

GOLD_JOB_NAME: Final[str] = "EcomFlow Gold Pipeline"
GOLD_JOB_ID: Final[int] = 178669601725968

_JOB_REGISTRY: Final[dict[str, int]] = {
    "bronze": BRONZE_JOB_ID,
    BRONZE_JOB_NAME.casefold(): BRONZE_JOB_ID,
    "silver": SILVER_JOB_ID,
    SILVER_JOB_NAME.casefold(): SILVER_JOB_ID,
    "gold": GOLD_JOB_ID,
    GOLD_JOB_NAME.casefold(): GOLD_JOB_ID,
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

def get_silver_job_id() -> int:
    """Return the Databricks job ID for the Silver runner job."""
    return get_job_id("silver")
def get_gold_job_id() -> int:
    """Return the Databricks job ID for the Gold runner job."""
    return get_job_id("gold")


__all__ = [
    # Job IDs
    "BRONZE_JOB_ID",
    "SILVER_JOB_ID",
    "GOLD_JOB_ID",

    # Job Names
    "BRONZE_JOB_NAME",
    "SILVER_JOB_NAME",
    "GOLD_JOB_NAME",

    # Helper Functions
    "get_bronze_job_id",
    "get_silver_job_id",
    "get_gold_job_id",
    "get_databricks_job_registry",
    "get_job_id",
]
