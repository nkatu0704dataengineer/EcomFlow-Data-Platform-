# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Setup Python Path
import sys
from pathlib import Path

# Add package root to Python path
package_root = Path("/Workspace/Users/tumaxpro99@gmail.com/EcomFlow-Data-Platform-")
if str(package_root) not in sys.path:
    sys.path.insert(0, str(package_root))

print(f"Package root added to path: {package_root}")

# COMMAND ----------

# DBTITLE 1,Run All Silver Pipelines
import time
from datetime import datetime

# Silver pipeline modules in dependency order
SILVER_MODULES = [
    "Brands",
    "Customers",
    "Categories",
    "Employees",
    "products",
    "Subsidiaries",
    "sub_product",
    "program_sales",
    "product_sales",
    "cart_items",
    "carts",
    "order_items",
    "orders",
    "payments",
    "reviews",
    "product_scores",
    "Behaviors",
]

print("=" * 80)
print("SILVER PIPELINE STARTED")
print("=" * 80)
print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Total Modules: {len(SILVER_MODULES)}")
print("=" * 80)
print()

start_time = time.time()
successful = []
failed = []

for idx, module_name in enumerate(SILVER_MODULES, 1):
    module_start = time.time()
    
    print(f"[{idx}/{len(SILVER_MODULES)}] Running {module_name}...")
    print("-" * 80)
    
    try:
        # Execute module by running its code with access to global spark session
        module_file = f"/Workspace/Users/tumaxpro99@gmail.com/EcomFlow-Data-Platform-/notebooks/silver_job_notebook/{module_name}.py"
        
        # Read and execute with globals() to give access to spark session
        with open(module_file, 'r') as f:
            module_code = f.read()
        
        exec(module_code, globals())
        
        module_duration = time.time() - module_start
        successful.append(module_name)
        
        print(f"✓ Completed {module_name} in {module_duration:.2f}s")
        print()
        
    except Exception as e:
        module_duration = time.time() - module_start
        failed.append((module_name, str(e)))
        
        print(f"✗ FAILED: {module_name} after {module_duration:.2f}s")
        print(f"Error: {str(e)}")
        print()
        
        # Fail fast - stop pipeline immediately
        print("=" * 80)
        print("PIPELINE FAILED")
        print("=" * 80)
        print(f"Failed Module: {module_name}")
        print(f"Error: {str(e)}")
        print("=" * 80)
        
        raise Exception(f"Silver pipeline failed at module: {module_name}") from e

total_duration = time.time() - start_time

# Summary
print("=" * 80)
print("SILVER PIPELINE COMPLETED")
print("=" * 80)
print(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Total Duration: {total_duration:.2f}s")
print(f"Successful Modules: {len(successful)}/{len(SILVER_MODULES)}")
print(f"Failed Modules: {len(failed)}")
print("=" * 80)

if successful:
    print("\nSuccessful Modules:")
    for module in successful:
        print(f"  ✓ {module}")

if failed:
    print("\nFailed Modules:")
    for module, error in failed:
        print(f"  ✗ {module}: {error}")

print("=" * 80)

# COMMAND ----------

