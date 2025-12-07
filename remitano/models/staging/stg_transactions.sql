-- models/staging/stg_transactions.sql
{{
    config(
        materialized='table',
        unique_key='txn_id',
        tags=['staging', 'transactions']
    )
}}

with source as (
    select * from {{ ref('transactions') }}
),

cleaned as (
    select
        trim(cast(txn_id as string)) as txn_id,
        trim(cast(user_id as string)) as user_id,
        trim(status) as status,
        trim(source_currency) as source_currency,
        trim(destination_currency) as destination_currency,
        cast(created_at as timestamp) as created_at,
        cast(source_amount as decimal(38, 10)) as source_amount,
        cast(destination_amount as decimal(38, 10)) as destination_amount,
        current_timestamp() as loaded_at
    from source
)

select * from cleaned