"""
EcomFlow Bronze dataset registry.

Defines all datasets belonging to the Bronze layer.

Each dataset provides the runtime configuration required by the
Databricks Bronze Job and is consumed by the Airflow orchestration layer.

This module contains configuration only and must not contain
business logic or Spark processing.
"""

BRONZE_LAYER = "bronze"
BRONZE_CATALOG = "ecomflow"
BRONZE_SCHEMA = "ecom_bronze_v2"
BRONZE_VOLUME = "pipeline_data"

BRONZE_DATASETS = [
    {
        "task_id": "users",
        "layer": BRONZE_LAYER,
        "catalog_name": BRONZE_CATALOG,
        "schema_name": BRONZE_SCHEMA,
        "table_name": "users",
        "volume_name": BRONZE_VOLUME,
    },
    {
        "task_id": "products",
        "layer": BRONZE_LAYER,
        "catalog_name": BRONZE_CATALOG,
        "schema_name": BRONZE_SCHEMA,
        "table_name": "products",
        "volume_name": BRONZE_VOLUME,
    },
    {
        "task_id": "orders",
        "layer": BRONZE_LAYER,
        "catalog_name": BRONZE_CATALOG,
        "schema_name": BRONZE_SCHEMA,
        "table_name": "orders",
        "volume_name": BRONZE_VOLUME,
    },
    {
        "task_id": "brands",
        "layer": BRONZE_LAYER,
        "catalog_name": BRONZE_CATALOG,
        "schema_name": BRONZE_SCHEMA,
        "table_name": "brands",
        "volume_name": BRONZE_VOLUME,
    },
    {
        "task_id": "cart_items",
        "layer": BRONZE_LAYER,
        "catalog_name": BRONZE_CATALOG,
        "schema_name": BRONZE_SCHEMA,
        "table_name": "cart_items",
        "volume_name": BRONZE_VOLUME,
    },
    {
        "task_id": "carts",
        "layer": BRONZE_LAYER,
        "catalog_name": BRONZE_CATALOG,
        "schema_name": BRONZE_SCHEMA,
        "table_name": "carts",
        "volume_name": BRONZE_VOLUME,
    },
    {
        "task_id": "categories",
        "layer": BRONZE_LAYER,
        "catalog_name": BRONZE_CATALOG,
        "schema_name": BRONZE_SCHEMA,
        "table_name": "categories",
        "volume_name": BRONZE_VOLUME,
    },
    {
        "task_id": "employees",
        "layer": BRONZE_LAYER,
        "catalog_name": BRONZE_CATALOG,
        "schema_name": BRONZE_SCHEMA,
        "table_name": "employees",
        "volume_name": BRONZE_VOLUME,
    },
    {
        "task_id": "order_items",
        "layer": BRONZE_LAYER,
        "catalog_name": BRONZE_CATALOG,
        "schema_name": BRONZE_SCHEMA,
        "table_name": "order_items",
        "volume_name": BRONZE_VOLUME,
    },
    {
        "task_id": "payments",
        "layer": BRONZE_LAYER,
        "catalog_name": BRONZE_CATALOG,
        "schema_name": BRONZE_SCHEMA,
        "table_name": "payments",
        "volume_name": BRONZE_VOLUME,
    },
    {
        "task_id": "product_sales",
        "layer": BRONZE_LAYER,
        "catalog_name": BRONZE_CATALOG,
        "schema_name": BRONZE_SCHEMA,
        "table_name": "product_sales",
        "volume_name": BRONZE_VOLUME,
    },
    {
        "task_id": "program_sales",
        "layer": BRONZE_LAYER,
        "catalog_name": BRONZE_CATALOG,
        "schema_name": BRONZE_SCHEMA,
        "table_name": "program_sales",
        "volume_name": BRONZE_VOLUME,
    },
    {
        "task_id": "reviews",
        "layer": BRONZE_LAYER,
        "catalog_name": BRONZE_CATALOG,
        "schema_name": BRONZE_SCHEMA,
        "table_name": "reviews",
        "volume_name": BRONZE_VOLUME,
    },
    {
        "task_id": "sub_products",
        "layer": BRONZE_LAYER,
        "catalog_name": BRONZE_CATALOG,
        "schema_name": BRONZE_SCHEMA,
        "table_name": "sub_products",
        "volume_name": BRONZE_VOLUME,
    },
    {
        "task_id": "subsidiaries",
        "layer": BRONZE_LAYER,
        "catalog_name": BRONZE_CATALOG,
        "schema_name": BRONZE_SCHEMA,
        "table_name": "subsidiaries",
        "volume_name": BRONZE_VOLUME,
    },
    {
        "task_id": "user_behaviors",
        "layer": BRONZE_LAYER,
        "catalog_name": BRONZE_CATALOG,
        "schema_name": BRONZE_SCHEMA,
        "table_name": "user_behaviors",
        "volume_name": BRONZE_VOLUME,
    },
]