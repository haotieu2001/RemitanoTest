## 📌 Project Purpose

This project implements an end‑to‑end **crypto exchange analytics
pipeline**, supporting: 
1. Transaction USD value computation using hourly USDT rates.
2. Historical KYC tracking using SCD Type‑2 snapshots.
3. Analytics marts (daily, monthly, user dimension, fact tables).


------------------------------------------------------------------------

## 📁 Repository Structure (from packed file)

    remitano/
      analyses/
      macros/
      models/
        intermediate/
        marts/
        staging/
      snapshots/
      tests/
    .gitignore
    ARCHITECTURE.md
    get_rates.py
    requirements.txt

### Key Components

#### **Staging Layer (Bronze)**

-   Cleans raw CSV data
-   Type‑casts timestamps
-   Ensures referential integrity

Files: - `stg_users.sql` - `stg_transactions.sql` - `stg_rates.sql`

#### **Intermediate Layer (Silver)**

Implements business logic: - USD conversion using USDT rates - Full KYC
history construction from snapshots - Rate normalization

Files: - `int_transactions_with_usd.sql` - `int_usdt_rates.sql` -
`int_user_kyc_history.sql`

#### **Marts Layer (Gold)**

Analytics‑optimized, star‑schema models: - `fct_transactions` --- fact
table with historical KYC - `dim_users` --- enriched user dimension -
Aggregated marts (daily & monthly)

Files: - `agg_daily_transactions.sql` - `agg_monthly_volume.sql` -
`transactions_with_historical_kyc.sql`

#### **Snapshots**

Files: - `snapshot_users.sql` --- Implements SCD‑2 KYC tracking


------------------------------------------------------------------------

## 📄 Architecture

For deeper architectural explanations (star schema, orchestration,
Databricks strategy), see:  👉 **ARCHITECTURE.md**


------------------------------------------------------------------------

## 📦 Other Files

-   `get_rates.py` --- fetches crypto market data.
-   `requirements.txt` --- Python package list.
-   `dbt_project.yml` --- dbt configuration.
-   `packages.yml` --- dbt packages configuration.

------------------------------------------------------------------------