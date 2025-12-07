{{
    config(
        materialized='table',
        unique_key='txn_id',
        tags=['marts', 'fact', 'transactions']
    )
}}

with transactions_usd as (
    select * from {{ ref('int_transactions_with_usd') }}
),

user_kyc_history as (
    select * from {{ ref('int_user_kyc_history') }}
),

transactions_with_kyc as (
    select
        t.*,
        uk.kyc_level as historical_kyc_level,
        uk.kyc_version,
        row_number() over (partition by t.txn_id order by uk.valid_from desc) as kyc_rank
    from transactions_usd t
    left join user_kyc_history uk
        on t.user_id = uk.user_id
        and t.created_at >= uk.valid_from
        and t.created_at < uk.valid_to
    where uk.kyc_level is not null
),

final as (
    select
        txn_id,
        user_id,
        status,
        source_currency,
        destination_currency,
        created_at,
        source_amount,
        destination_amount,
        usd_value_estimated,
        usd_calculation_method,
        historical_kyc_level,
        kyc_version,
        -- Date dimensions for easier grouping
        date(created_at) as transaction_date,
        year(created_at) as transaction_year,
        month(created_at) as transaction_month,
        quarter(created_at) as transaction_quarter,
        date_trunc('month', created_at) as transaction_month_start,
        date_trunc('quarter', created_at) as transaction_quarter_start
    from transactions_with_kyc
    where kyc_rank = 1  -- Get the most recent kyc level valid at transaction time
)

select * from final