{{
    config(
        materialized='table',
        tags=['intermediate', 'transactions', 'usd']
    )
}}

with transactions as (
    select
        t.txn_id,
        t.user_id,
        t.status,
        t.source_currency,
        t.destination_currency,
        t.created_at,
        t.source_amount,
        t.destination_amount,
        -- Match to hour start
        -- Transaction at '2025-05-20 00:49:01' → '2025-05-20 00:00:00'
        date_trunc('hour', t.created_at) as transaction_hour
    from {{ ref('stg_transactions') }} t
),

rates as (
    select
        hour_start,
        base_currency,
        effective_usdt_rate
    from {{ ref('int_usdt_rates') }}
),

transactions_with_rates as (
    select
        t.*,
        -- Get source currency rate to USDT
        src_rates.effective_usdt_rate as source_to_usdt_rate,
        -- Get destination currency rate to USDT
        dest_rates.effective_usdt_rate as destination_to_usdt_rate
    from transactions t
    left join rates src_rates
        on t.transaction_hour = src_rates.hour_start
        and t.source_currency = src_rates.base_currency
    left join rates dest_rates
        on t.transaction_hour = dest_rates.hour_start
        and t.destination_currency = dest_rates.base_currency
),

calculated_usd as (
    select
        *,
        -- Calculate USD value based on source amount
        case
            when source_to_usdt_rate is not null then
                source_amount * source_to_usdt_rate
            when destination_to_usdt_rate is not null then
                destination_amount * destination_to_usdt_rate
            else null
        end as usd_value,
        -- Handle cases where direct rate is not available
        case
            when source_currency = '{{ var("usdt_currency") }}' then source_amount
            when destination_currency = '{{ var("usdt_currency") }}' then destination_amount
            else null
        end as direct_usdt_amount
    from transactions_with_rates
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
        coalesce(usd_value, direct_usdt_amount) as usd_value_estimated,
        case
            when usd_value is not null then 'calculated_via_rate'
            when direct_usdt_amount is not null then 'direct_usdt'
            else 'unknown'
        end as usd_calculation_method
    from calculated_usd
)

select * from final