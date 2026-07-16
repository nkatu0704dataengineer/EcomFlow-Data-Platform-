# EcomFlow Architecture Refactoring Report
**Date:** 2026-07-10  
**Status:** Complete  
**Tests Passing:** 5/5 ✅

---

## Bối cảnh ban đầu

EcomFlow được thiết kế theo hướng kiến trúc Databricks-centric:

```
Airflow → DatabricksSubmitRunOperator → Databricks Job → Unity Catalog → Spark → CSV → MinIO
```

Kiến trúc này tự nó không có vấn đề, nhưng trong thực tế triển khai gặp phải những ràng buộc không ngờ:

- **Databricks Free Edition** chỉ hỗ trợ Serverless, không hỗ trợ Existing Cluster
- **Databricks Connect** không khả dụng trong môi trường Free
- **Airflow phải build toàn bộ payload SubmitRun** với cluster config, libraries, python task definition — tất cả phụ thuộc vào hạ tầng Databricks
- **Framework bị coupling** với Databricks SDK, Unity Catalog, Delta Lake — khó để refactor hoặc thay đổi backend sau này
- **Over-engineering quá sớm**: Dự án vẫn ở giai đoạn prototyping nhưng kiến trúc đã yêu cầu các công nghệ enterprise phức tạp
- **Bug phát sinh chỉ vì hạ tầng**: Thay vì tập trung vào logic ETL, team phải debug hạ tầng Databricks

## Quyết định refactor

Sau khi đánh giá, nhóm đưa ra nhận định: **"Chúng tôi over-engineering quá sớm."**

Thay vì cố gắng fix hạ tầng Databricks, quyết định được đưa ra là:

1. **Simplify ngay lập tức**: Bỏ Databricks ra khỏi critical path
2. **Defer Databricks**: Giữ tích hợp Databricks cho sau này khi kiến trúc và logic đã ổn định
3. **Focus on framework**: Tập trung xây dựng framework ETL mạnh mẽ, độc lập với platform

Mục tiêu: Một kiến trúc đơn giản hơn nhưng **vẫn có khả năng mở rộng** về Databricks, Delta Lake, Kafka sau này mà **không cần viết lại code**.

## Kiến trúc mới

```
Generated CSV (data/bronze/*.csv)
        ↓
Spark Job (local execution)
        ↓
Framework (reader → validator → writer → uploader → metadata)
        ↓
Silver CSV
        ↓
MinIO
        ↓
Airflow (orchestration only)
```

### Nguyên tắc thiết kế

**Separation of Concerns (SoC):**
- **Airflow**: Chỉ orchestration, scheduling, error handling
- **Spark**: Chỉ data processing engine, không biết Airflow
- **Framework**: Chỉ ETL business logic, không biết Databricks, Airflow, hay Unity Catalog
- **Reader**: Chỉ đọc CSV (không phải Delta Table)
- **Writer**: Chỉ ghi CSV
- **Uploader**: Chỉ upload lên MinIO
- **Validator**: Chỉ validate data quality
- **Metadata**: Chỉ emit metadata

**Không hard-code, dễ migration:**
- Framework vẫn nhận `catalog_name`, `schema_name`, `table_name` làm parameter (dù hiện tại không dùng)
- Có thể thay đổi input source từ CSV → Delta Table chỉ bằng cách update Reader
- Có thể thay đổi output destination từ MinIO → Data Lakehouse chỉ bằng cách update Writer
- Không có tight coupling giữa các layer

## Module/File Refactoring Decision Matrix

### Giữ nguyên (KEEP)
- `include/framework/spark/validator.py` — Logic validate dataframe, không phụ thuộc platform
- `include/framework/spark/writer.py` — Logic ghi CSV, dễ extend để ghi Delta sau
- `include/framework/spark/uploader.py` — MinIO uploader, logic vẫn hợp lệ
- `include/framework/spark/metadata.py` — Metadata generation, không phụ thuộc backend
- `include/framework/spark/models/` — Data models, pure data structures
- `include/config/airflow_config.py` — Airflow configuration, vẫn cần
- `include/config/constants.py` — Project constants, vẫn hợp lệ (mặc dù Databricks constants)
- `include/config/bronze.py` — Dataset configuration, ở dạng agnostic (không bound vào Delta/UC)
- `dags/common/callbacks.py`, `notifications.py`, `operators.py` — Reusable Airflow helpers
- `tests/unit/test_uploader.py` — Framework test, vẫn hợp lệ

### Sửa (MODIFY)
- `include/framework/spark/reader.py`:
  - **Trước**: `read_delta_table(spark, catalog, schema, table)` — đọc từ Delta Lake + Unity Catalog
  - **Sau**: `read_csv(spark, input_path)` — đọc từ local CSV
  - **Lý do**: Loại bỏ dependency Databricks, tăng tính portable
  - **Backward compatibility**: Giữ wrapper `read_delta_table()` với parameter `input_path` để dễ migration sau

- `include/framework/spark/pipeline.py`:
  - **Trước**: Gọi `read_delta_table()` với catalog/schema/table, yêu cầu bắt buộc tất cả parameter
  - **Sau**: Gọi `read_csv()`, parameter catalog/schema/table trở thành optional (vẫn chấp nhận để dễ migrate)
  - **Lý do**: Thích ứng với CSV input, nhưng vẫn giữ signature tương thích để sau này dễ add back Databricks

- `dags/bronze/bronze_dag.py`:
  - **Trước**: "Orchestrate Bronze Databricks jobs via the Bronze Task Group"
  - **Sau**: "Bronze DAG for local Spark execution"
  - **Lý do**: Update documentation để phản ánh kiến trúc mới

- `dags/bronze/bronze_task_groups.py`:
  - **Trước**: Gọi `create_databricks_submit_tasks()` từ `dags/common/databricks_task_group.py`
  - **Sau**: Gọi `create_spark_tasks()` từ `dags/common/spark_task_group.py`
  - **Lý do**: Switch từ SubmitRunOperator sang local Spark execution

- `include/config/databricks.py`:
  - **Trước**: Khó khăn, nhiều env var liên quan Cluster, Libraries, Job config
  - **Sau**: Simplified, chỉ giữ connection ID (`get_databricks_conn_id()`)
  - **Lý do**: Giảm complexity, chỉ giữ cái cần thiết cho future integration

### Tạo mới (CREATE)
- `dags/common/spark_task_group.py`:
  - **Mục đích**: Thay thế `databricks_task_group.py`, provide `create_spark_tasks()`
  - **Logic**: Tạo list `PythonOperator` tasks, mỗi task gọi framework pipeline qua local Spark
  - **Design**: Minimal, lightweight, dễ extend cho Silver/Gold layers

- `include/databricks/jobs.py`:
  - **Mục đích**: Centralized job registry cho future Databricks integration
  - **Content**: Constants như `BRONZE_JOB_ID`, helper functions `get_bronze_job_id()`
  - **Design**: Lightweight, không hard-code, extensible cho Silver/Gold/ML jobs
  - **Rationale**: Khi return to Databricks later, có centralized place để manage job IDs

- `include/databricks/__init__.py`:
  - **Mục đích**: Package marker, documentation về Databricks integration
  - **Content**: Docstring giải thích rằng package này intentionally lightweight

- `tests/unit/test_databricks_jobs.py`:
  - **Mục đích**: Unit test cho job registry
  - **Coverage**: Test job ID retrieval, registry copy, unknown key error handling

### Xóa/Deprecated (DELETE/DEPRECATE)
- `dags/common/databricks_task_group.py`:
  - **Xóa vì**: Không còn dùng `DatabricksSubmitRunOperator`, logic moved to `spark_task_group.py`
  - **Lưu ý**: Giữ trong git history, dễ revert nếu cần

- Databricks SDK dependencies từ workflow:
  - **Xóa vì**: Framework không dùng SDK đúng cách, chỉ Airflow dùng nếu trigger Databricks later
  - **Lưu ý**: Vẫn giữ trong `requirements.txt` (optional), chỉ không dùng trong code

- Legacy SubmitRun payload builders:
  - `build_bronze_job_config()` — Xóa vì không còn cần build JSON payload
  - `get_new_cluster_spec()` — Xóa vì không submit cluster config
  - `get_use_existing_cluster()`, `get_databricks_libraries()` — Xóa vì deprecated

## Kiến trúc mới chi tiết

### Layer 1: Airflow Orchestration
```python
# dags/bronze/bronze_dag.py
START → bronze_task_group() → END

# dags/bronze/bronze_task_groups.py
bronze_task_group() → [create_spark_tasks(BRONZE_DATASETS)]

# dags/common/spark_task_group.py
create_spark_tasks() → [PythonOperator(python_callable=_run_bronze_dataset)]
```

**Responsibility**: Scheduling, task dependency, error handling, retries  
**Technology**: Airflow core, PythonOperator  
**Coupling**: Minimal, dùng standard Airflow patterns

### Layer 2: Spark Execution
```python
# Airflow task invoke PythonOperator
_run_bronze_dataset(dataset, input_path, ...)
    ↓
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName(...).getOrCreate()
    ↓
run_pipeline(spark, layer, table_name, bucket_name, input_path)
```

**Responsibility**: Create Spark context, manage Spark lifecycle  
**Technology**: PySpark 4.0  
**Coupling**: Standard Spark patterns, không tight-coupling vào framework

### Layer 3: Framework (ETL Business Logic)
```python
# include/framework/spark/pipeline.py
def run_pipeline(spark, layer, table_name, bucket_name, input_path):
    dataframe = read_csv(input_path)
    validation_result = validate_dataframe(dataframe)
    csv_path = write_csv(dataframe, layer, table_name)
    object_path = upload_directory_to_minio(csv_path, bucket_name)
    metadata = generate_metadata(dataframe, validation_result, ...)
    return PipelineResult(...)
```

**Responsibility**: Orchestrate reader → validator → writer → uploader → metadata  
**Technology**: PySpark DataFrame operations, MinIO SDK, dataclass models  
**Coupling**: Zero coupling to Databricks, Airflow, or specific storage backend

### Input/Output
```
Input:  data/bronze/{table_name}.csv (local file system)
Output: tmp/bronze/{table_name}/{timestamp}/ (local CSV)
Upload: MinIO bucket (configurable)
```

## Những cải tiến đạt được

### 1. **Giảm Complexity**
- Trước: 8 environment variables liên quan cluster config, libraries
- Sau: 1 environment variable cho Databricks connection ID
- **Result**: Dễ setup, ít configuration error

### 2. **Tăng Flexibility**
- Reader có thể switch CSV → Parquet → Delta với 1 dòng code
- Writer có thể switch CSV → Parquet → Delta Lake
- Uploader có thể switch MinIO → S3 → ADLS
- Framework không phải change

### 3. **Framework decoupled từ Orchestration**
- Framework không import từ Airflow
- Framework không import Databricks SDK
- Framework chỉ biết Spark DataFrame
- **Result**: Có thể test framework standalone, dễ reuse ngoài Airflow

### 4. **Testing đơn giản hơn**
- Không cần mock Databricks API
- Không cần mock Airflow context
- Mock chỉ cần: Spark DataFrame, MinIO client
- **Result**: Unit tests chạy nhanh, tin cậy

### 5. **Roadmap rõ ràng**
- Databricks integration không bị loại bỏ, chỉ postpone
- Code được thiết kế để easy migration:
  - Chỉ cần update Reader module từ CSV → Delta
  - Chỉ cần update Writer module từ CSV → Delta Lake
  - Framework logic không đổi
- **Result**: Khi mua Databricks Pro/Enterprise, reintegration không phải rewrite

## Những điểm cần lưu ý (Critical Considerations)

### 1. **Spark execution environment**
- Hiện tại, local Spark execution yêu cầu PySpark 4.0 cài sẵn
- Có 2 option:
  - A) Docker container chạy Spark worker
  - B) Airflow worker machine có PySpark cài sẵn
  - C) Airflow trigger external Spark cluster (future)
- **Action**: Cần quyết định execution environment cho production

### 2. **Input data location**
- Hiện tại: `data/bronze/{table_name}.csv` (hardcoded path)
- Cần rõ: Dữ liệu CSV được generate từ đâu?
- Option:
  - A) Generated trước đó, upload vào MinIO, Spark download từ MinIO
  - B) Generated ở local filesystem (dev/test only)
  - C) Generated từ HTTP endpoint, Spark fetch
- **Action**: Định rõ data ingestion flow

### 3. **Error handling & retry**
- Framework throw exception nếu CSV không tồn tại
- Framework throw exception nếu validation fail
- Airflow PythonOperator có built-in retry logic
- **Action**: Configure retry policy trong DAG (already in `airflow_config.py`)

### 4. **Backward compatibility**
- Reader vẫn accept `catalog_name`, `schema_name`, `table_name` parameter
- Pipeline vẫn accept `catalog_name`, `schema_name` (dù không dùng)
- **Lý do**: Khi return to Databricks, dễ add back code mà không break signature
- **Lưu ý**: Signature flexibility có cost (parameter clutter), cân nhắc sau

## Roadmap tiếp theo

### Phase 1 (Current)
- ✅ Local Spark execution
- ✅ CSV input/output
- ✅ MinIO uploader
- ✅ Validation & metadata
- **Goal**: Proof of concept, demonstrate framework works without Databricks

### Phase 2 (Short-term: 1-2 sprints)
- Setup local data ingestion (CSV generation)
- Run end-to-end pipeline locally
- Add Silver, Gold DAGs (follow Bronze pattern)
- Implement data quality dashboards
- **Goal**: Working pipeline for all 3 layers

### Phase 3 (Medium-term: 1-2 months)
- Migrate to cloud infrastructure (AWS/Azure/GCP)
- Setup MinIO or S3 object storage
- Run on Airflow cloud (Managed Airflow / MWAA)
- Implement monitoring & alerting
- **Goal**: Production-ready pipeline

### Phase 4 (Long-term: Reintegrate Databricks)
- Once project stabilizes and budget allows:
  - Setup Databricks workspace (Standard edition)
  - Update Reader to read from Delta Lake instead of CSV
  - Update Writer to write to Delta Lake
  - Framework logic stays the same
  - Migrate Silver/Gold to run on Databricks
- **Goal**: Leverage Databricks for scale without breaking framework

---

## Tóm tắt kiến trúc sau refactor

```
┌─────────────────────────────────────────────────────────────┐
│                     AIRFLOW DAG Layer                       │
│  (bronze_dag.py → bronze_task_group.py → spark_task_group.py)
│  Responsibility: Orchestration, Scheduling, Error handling  │
└──────────────────────┬──────────────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────────────────┐
│               SPARK EXECUTION Layer (Local)                 │
│  (Spark SQL, DataFrame operations)                          │
│  Responsibility: Execute distributed compute                │
└──────────────────────┬──────────────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────────────────┐
│                  FRAMEWORK Layer (ETL)                      │
│  (reader → validator → writer → uploader → metadata)        │
│  Responsibility: Business logic, data quality, audit trail  │
└──────────────────────┬──────────────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────────────────┐
│           STORAGE Layer (CSV/MinIO/Databases)               │
│  (MinIO object storage)                                     │
│  Responsibility: Persistent data storage                    │
└─────────────────────────────────────────────────────────────┘
```

**Key principle**: Each layer is independently testable, replaceable, and scalable.

---

**Conclusion**: EcomFlow architecture refactor successfully shifted the project from an over-engineered Databricks-centric design to a lightweight, portable, framework-first approach. The new architecture maintains flexibility for future Databricks integration while immediately reducing operational complexity and improving time-to-value.
