# EcomFlow TIG Observability - Monitoring Strategy (G4)

**Version**: 1.0  
**Date**: 2024-08-14  
**Status**: Active

---

## Purpose

This document defines the monitoring strategy for the EcomFlow data platform, establishing what metrics to track, how to organize them, and what operational decisions they support.

**Principle**: Monitor real system behavior to detect abnormalities and support operational decisions, not to create decorative visualizations.

---

## Monitoring Architecture

```
Airflow Orchestration
   ↓ StatsD metrics
Telegraf (Collector)
   ↓ InfluxDB Line Protocol
InfluxDB (Storage)
   ↓ Flux queries
Grafana (Visualization)
   ↓
Operational Decisions
```

---

## Metric Categories

### A. Airflow Health

**Purpose**: Ensure Airflow scheduler and executor are functioning correctly.

**Key Questions**:
- Is the scheduler alive?
- Are tasks being executed?
- Are failures increasing?
- Is the executor saturated?

**Metrics**:

| Metric | Type | Source | Purpose |
|--------|------|--------|---------|
| `airflow.scheduler.heartbeat` | Counter | Scheduler | Verify scheduler is alive |
| `airflow.scheduler.critical_section_duration` | Timer (p50/p95/p99) | Scheduler | Detect scheduler performance degradation |
| `airflow.executor.open_slots` | Gauge | Executor | Track available capacity |
| `airflow.executor.queued_tasks` | Gauge | Executor | Identify task backlog |
| `airflow.executor.running_tasks` | Gauge | Executor | Monitor active workload |
| `airflow.task.success` | Counter | Tasks | Track successful completions |
| `airflow.task.failed` | Counter | Tasks | Identify failures |
| `airflow.task_instance_created-<operator>` | Counter | Tasks | Track task creation by operator |

**Alert Thresholds** (Recommendations):
- Scheduler heartbeat missing for >60 seconds → CRITICAL
- Task failure rate >10% over 5 minutes → WARNING
- Task failure rate >25% over 5 minutes → CRITICAL
- Queued tasks >100 for >10 minutes → WARNING
- Executor open slots = 0 for >5 minutes → WARNING

---

### B. Pipeline Performance

**Purpose**: Understand execution performance and identify slow tasks.

**Key Questions**:
- How long do tasks typically take?
- Are execution times increasing?
- Which tasks are slowest?
- What are p95/p99 latencies?

**Metrics**:

| Metric | Type | Source | Purpose |
|--------|------|--------|---------|
| `airflow.dag_processing.total_parse_time` | Timer (p50/p95/p99) | DAG Processor | Measure DAG parsing overhead |
| `airflow.task.duration.<dag>.<task>` | Timer (p50/p90/p95/p99/p99.9) | Tasks | Track individual task durations |
| `airflow.dagrun.duration.success.<dag>` | Timer (p50/p95) | DAG Runs | Measure end-to-end pipeline time |
| `airflow.dagrun.duration.failed.<dag>` | Timer (p50/p95) | DAG Runs | Understand failure scenarios |

**Percentile Interpretation**:
- **p50 (Median)**: Typical performance
- **p90**: Most tasks complete within this time
- **p95**: SLA boundary (only 5% slower)
- **p99**: Slow tasks (potential bottlenecks)
- **p99.9**: Outliers (infrastructure issues)

**Alert Thresholds** (Recommendations):
- Task p95 duration >2x historical baseline → WARNING
- Task p99 duration >5x historical baseline → WARNING
- DAG run duration p95 >SLA threshold → CRITICAL

---

### C. Data Pipeline Health

**Purpose**: Monitor data quality and pipeline correctness.

**Key Questions**:
- Are pipelines completing successfully?
- Is data fresh?
- Are validation rules passing?

**Metrics** (if implemented):

| Metric | Type | Source | Purpose |
|--------|------|--------|---------|
| `airflow.dagrun.success.<dag>` | Counter | DAG Runs | Track successful pipeline runs |
| `airflow.dagrun.failed.<dag>` | Counter | DAG Runs | Identify failed pipelines |
| `airflow.task.sla_miss` | Counter | Tasks | Detect SLA violations |

**Note**: Data-specific metrics (records processed, data freshness, validation failures) depend on custom instrumentation in DAG code. Not available by default from Airflow StatsD.

**Recommendations**:
- Instrument critical pipelines with custom metrics
- Track row counts, data quality checks, and freshness
- Emit as additional StatsD metrics from task code

---

### D. TIG Infrastructure Health

**Purpose**: Ensure the observability stack itself is functioning.

**Key Questions**:
- Is Telegraf receiving metrics?
- Is InfluxDB accepting writes?
- Is Grafana accessible?
- Are metrics being dropped?

**Metrics**:

| Metric | Type | Source | Purpose |
|--------|------|--------|---------|
| `telegraf.internal_gather` | Timer | Telegraf | Monitor Telegraf collection performance |
| `telegraf.internal_write` | Timer | Telegraf | Track InfluxDB write latency |
| `telegraf.metrics_written` | Counter | Telegraf | Verify metrics are being written |
| `telegraf.metrics_dropped` | Counter | Telegraf | Detect dropped metrics |
| InfluxDB HTTP /health endpoint | HTTP | InfluxDB | Verify InfluxDB availability |
| Grafana HTTP / endpoint | HTTP | Grafana | Verify Grafana availability |

**Alert Thresholds** (Recommendations):
- Telegraf metrics dropped >0 → WARNING
- InfluxDB write latency p95 >500ms → WARNING
- InfluxDB /health returns non-200 → CRITICAL
- Grafana / returns non-200 → CRITICAL

---

## Dashboard Architecture

### 1. EcomFlow Overview Dashboard

**Audience**: Operations team, management  
**Purpose**: High-level operational health  
**Refresh**: Every 10 seconds  

**Panels**:
- **Row 1: Health Summary** (Stat panels)
  - Tasks Succeeded (last hour)
  - Tasks Failed (last hour) - RED if >0
  - Queued Tasks (current)
  - Running Tasks (current)

- **Row 2: Task Trends** (Time series)
  - Task Success/Failure Rate (stacked area chart)
  - Task Duration p50/p95 (line chart)

- **Row 3: Executor Status** (Time series)
  - Executor Capacity (open slots, queued tasks, running tasks)

**Key Insight**: "Is the platform healthy right now?"

---

### 2. Airflow Dashboard

**Audience**: Data engineers, SREs  
**Purpose**: Deep-dive into Airflow internals  
**Refresh**: Every 30 seconds  

**Panels**:
- **Row 1: Scheduler Health**
  - Scheduler Heartbeat (time series)
  - Critical Section Duration p50/p95/p99 (time series)

- **Row 2: DAG Processing**
  - DAG Parse Time p50/p95/p99 (time series)
  - DAG File Processing Count (time series)

- **Row 3: Task Execution**
  - Task Success/Failure (stacked bar chart, per DAG)
  - Task Duration by DAG (table, sorted by p95)

- **Row 4: Executor Details**
  - Open Slots (gauge)
  - Queued Tasks (stat)
  - Running Tasks (stat)

**Key Insight**: "Why is Airflow slow or failing?"

---

### 3. Pipeline Performance Dashboard

**Audience**: Data engineers, analytics team  
**Purpose**: Optimize pipeline execution  
**Refresh**: Every 1 minute  

**Panels**:
- **Row 1: DAG Run Duration**
  - DAG Run Duration p50/p95 by DAG (table)
  - DAG Run Duration Trend (time series, filterable by DAG)

- **Row 2: Task Performance**
  - Slowest Tasks (table: DAG, Task, p95 duration, p99 duration)
  - Task Duration Distribution (histogram)

- **Row 3: Performance Percentiles**
  - Task Duration Percentiles (time series: p50, p90, p95, p99, p99.9)
  - Identify p99.9 spikes (outliers)

**Key Insight**: "Which pipelines/tasks are slow and getting slower?"

---

### 4. TIG Infrastructure Dashboard

**Audience**: SREs, platform team  
**Purpose**: Monitor the monitoring stack  
**Refresh**: Every 1 minute  

**Panels**:
- **Row 1: Telegraf Health**
  - Metrics Written (counter, time series)
  - Metrics Dropped (counter, time series) - RED if >0
  - Write Latency p95 (time series)

- **Row 2: InfluxDB Health**
  - InfluxDB /health Status (stat panel: UP/DOWN)
  - Write Request Rate (time series)
  - Query Duration p95 (if available)

- **Row 3: Grafana Health**
  - Grafana / Status (stat panel: UP/DOWN)
  - Dashboard Load Time (if available)

**Key Insight**: "Is our monitoring infrastructure reliable?"

---

## Monitoring Principles

### 1. Business Realism > Technical Complexity

Measure what matters to operations, not what is technically interesting.

**Example**:
- ✅ **Good**: Task failure rate (operational impact)
- ❌ **Bad**: Python GC pause time (technical curiosity)

### 2. Observe Real System Behavior

Only monitor metrics that actually exist or can be reliably derived.

**Example**:
- ✅ **Good**: `airflow.task.success` (emitted by Airflow)
- ❌ **Bad**: "Data quality score" (not emitted, requires custom instrumentation)

### 3. Avoid Premature Aggregation

Preserve raw metrics; aggregate in Grafana queries when needed.

**Example**:
- ✅ **Good**: Query `airflow.task.duration` with Flux aggregations
- ❌ **Bad**: Pre-aggregate in Telegraf processors

### 4. Every Panel Has a Purpose

If a panel does not answer an operational question, remove it.

**Test**: "What decision would I make based on this panel?"

### 5. Simple, Explainable Queries

Prefer understandable Flux queries over complex transformations.

**Example**:
```flux
// ✅ Good: Clear, understandable
from(bucket: "airflow_metrics")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "airflow.task.success")
  |> aggregateWindow(every: 1m, fn: sum)
```

```flux
// ❌ Bad: Over-engineered
from(bucket: "airflow_metrics")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement =~ /^airflow\.task\..*/)
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> map(fn: (r) => ({ r with _value: r.success / (r.success + r.failed) }))
  // ... 10 more transformations
```

---

## Metric Naming Conventions

Airflow emits metrics with this structure:

```
<prefix>.<component>.<metric_name>[.<tag>]
```

**Examples**:
- `airflow.scheduler.heartbeat`
- `airflow.task.success`
- `airflow.task.duration.<dag_id>.<task_id>`
- `airflow.executor.open_slots`

**Preservation Strategy**:
- Keep original metric names in InfluxDB
- No renaming in Telegraf processors
- Use Flux queries to filter/transform as needed

---

## Alert Strategy (Future - G6)

Alerting is **NOT implemented in G4/G5**.

When implementing alerts (G6), follow these rules:

### Alert Types

1. **Availability Alerts** (immediate notification)
   - Scheduler heartbeat stopped
   - InfluxDB down
   - Grafana down

2. **Performance Alerts** (5-10 min window)
   - Task duration p95 >2x baseline
   - Executor saturated (queued tasks >threshold)

3. **Error Rate Alerts** (5 min window)
   - Task failure rate >10%
   - Metrics dropped by Telegraf

### Alert Channels (Future)
- Slack: For WARNING-level alerts
- PagerDuty: For CRITICAL-level alerts
- Email: For daily/weekly summary reports

---

## Dashboard Variables

Each dashboard should support these variables:

| Variable | Type | Purpose |
|----------|------|---------|
| `$interval` | Auto | Query aggregation window (based on time range) |
| `$dag_id` | Query | Filter by specific DAG |
| `$task_id` | Query | Filter by specific task |
| `$executor` | Query | Filter by executor type |

**Implementation**: Use Grafana template variables with Flux queries to populate dropdowns.

---

## Data Retention Policy

**Current**: Infinite retention (InfluxDB default)

**Recommendation**:
- **High-resolution data** (10s interval): Retain 30 days
- **Downsampled data** (1h interval): Retain 1 year
- **Monthly aggregates**: Retain forever

**Implementation** (Future):
```flux
// Downsample task: Run daily
// Aggregate 10s data → 1h averages
from(bucket: "airflow_metrics")
  |> range(start: -2d, stop: -1d)
  |> aggregateWindow(every: 1h, fn: mean)
  |> to(bucket: "airflow_metrics_1h")
```

---

## Success Criteria

G4/G5 is successful when:

1. ✅ All 4 dashboards are provisioned and load correctly
2. ✅ Each panel displays real metrics (no "No Data" errors)
3. ✅ Dashboard variables work (filter by DAG, task, etc.)
4. ✅ Queries execute in <1 second for default time range (last 1 hour)
5. ✅ Operational questions are answered:
   - "Is Airflow healthy?" → EcomFlow Overview
   - "Why is this DAG slow?" → Airflow Dashboard
   - "Which tasks are bottlenecks?" → Pipeline Performance
   - "Is monitoring working?" → TIG Infrastructure

---

## Next Steps

### Immediate (G5):
1. ✅ Complete `ecomflow-overview.json`
2. ✅ Complete `airflow.json`
3. ⬜ Create `pipeline-performance.json`
4. ⬜ Create `tig-infrastructure.json`
5. ⬜ Test all dashboards with real metrics

### Future (G6):
1. Implement alert rules in Grafana
2. Configure notification channels (Slack, PagerDuty)
3. Set up on-call rotation
4. Establish escalation policies

### Future (Optimization):
1. Implement data downsampling (30d → 1y)
2. Add custom metrics from DAG code (data quality, row counts)
3. Create per-team dashboards (Analytics, ML, Ops)

---

**Document Owner**: Data Platform Team  
**Review Cadence**: Quarterly  
**Last Updated**: 2024-08-14
