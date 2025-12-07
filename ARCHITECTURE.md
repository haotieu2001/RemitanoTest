## Task 2 Documentation - Data Model Design

### 1. Data Model Overview

We implement a **medallion architecture** with three layers:

* **Bronze (Staging):** Raw data ingestion and basic cleaning.
* **Silver (Intermediate):** Business logic and transformations.
* **Gold (Marts):** Analytics-ready data for business consumption.

**Data Flow:**

```
CSV files → Staging models (cleaning, type casting) → Intermediate models (business logic, USD calculation, KYC history) → Marts models (star schema for analytics)
```

---

### 2. Architecture Choice: Star Schema

**Why Star Schema?**

* **BI Tool Optimization:** Simple dimensional model with minimal joins, ideal for Tableau/Power BI.
* **Query Performance:** Pre-aggregated fact tables and denormalized dimensions for fast queries.
* **Maintainability:** Clear separation between facts and dimensions, easy to extend.
* **Business Readability:** Intuitive structure for non-technical stakeholders.

**Gold Layer Structure:**

* **Fact Table:** `fct_transactions` - All transaction details with USD values and historical KYC.
* **Dimension Table:** `dim_users` - User profiles with KYC history and transaction statistics.
* **Aggregate Tables:** `agg_daily_transactions`, `agg_monthly_volume` - Pre-calculated metrics.
* **Specialized View:** `transactions_with_historical_kyc` - Direct answer to business question #2.

---

### 3. Solution for Historical KYC Tracking (Requirement #3)

**Problem:** Need KYC level at transaction time, not the current KYC level.

**Solution:** Slowly Changing Dimensions (SCD) Type 2 via **dbt snapshots**.

**Three-Step Implementation:**

1. **Change Capture:** `dbt snapshots` automatically track every KYC change with timestamps.
2. **Time-Ranged History:** Transform snapshot data into contiguous time ranges for each KYC version (`valid_from`, `valid_to`).
3. **Temporal Joins:** Match transactions to KYC levels valid at the exact transaction timestamp.

**Business Benefits:**

* **Regulatory Compliance:** Accurate historical reporting for audits.
* **Data Integrity:** No distortion of historical metrics by current states.
* **Analytical Accuracy:** Correct attribution in time-series analysis.

---

## Task 3 - Architecture & Storage Decisions

### 1. Data Warehouse Selection

**Selected Platform:** Databricks Lakehouse

**Why Databricks?**

* ✅ **Unified Platform:** Combines data warehousing (SQL analytics) and data lake (raw storage) in one platform.
* ✅ **Delta Lake Architecture:** ACID transactions, time travel, schema enforcement, open format.
* ✅ **Cost Efficiency:** Separated storage (cheap object storage) and compute (elastic scaling).
* ✅ **Enterprise Features:** Unity Catalog for governance, fine-grained security, compliance ready.
* ✅ **Team Collaboration:** Notebooks, SQL editor, Git integration for different team roles.

---

### 2. Materialization Strategy

**Philosophy:** Balance data freshness, query performance, and infrastructure cost.

**Strategy by Layer:**

* **Staging (Bronze):** `table` - Full refresh daily for data quality; source of truth.
* **Intermediate (Silver):** Mix of `table` and `incremental` - Persist complex logic; use incremental for large datasets.
* **Gold/Marts:** `table` with partitions - BI performance critical; star schema needs persisted, partitioned tables.
* **Snapshots:** `table` (dbt managed) - SCD Type 2 requires historical tracking.
* **Tests:** `ephemeral` - Lightweight validation, no persistence required.

**Databricks Optimizations:**

* Use **Delta Lake** format for reliability and time travel.
* **Partition** tables by date for time-series workloads.
* Apply **Z-ordering** (where appropriate) to improve join performance.
* Use efficient **MERGE** strategies for incremental loads.

---

### 3. Orchestration Strategy

**Selected Tool:** Apache Airflow

**Why Airflow?**

* **Mature:** Proven at scale with a large community.
* **Python-native:** Easy to extend with custom logic and operators.
* **Rich UI:** Built-in monitoring and troubleshooting interfaces.
* **Flexible:** Handles complex scheduling and dependency graphs.
* **Robust:** Advanced error handling and retry capabilities.

**Daily Pipeline Design:**

* **Schedule:** `02:00 AM UTC` daily (after market close, off-peak).
* **Dependencies:** Sequential orchestration with parallel branches for independent tasks.
* **Error Handling:** 3 retries with exponential backoff; task-specific recovery strategies.
* **Monitoring:** Dashboard tracking freshness, quality, runtime, and performance metrics.

**Pipeline Steps:**

1. Data ingestion from sources.
2. Load raw data via `dbt seed`.
3. Build staging (bronze) layer.
4. Create KYC snapshots (SCD Type 2).
5. Build intermediate (silver) layer with business logic.
6. Build analytics (gold) marts (star schema).
7. Run data quality tests.
8. Send notifications and update BI systems.

**Key Features:**

* **SLAs:** Target 4-hour total runtime maximum.
* **Alerting:** Critical alerts (PagerDuty) and warnings (Slack).
* **Monitoring:** Freshness, quality, volume, performance, and cost metrics.
* **Governance:** Role-based access, row-level security, and data lineage tracking.

---
