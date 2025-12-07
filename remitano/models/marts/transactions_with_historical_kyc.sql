-- models/marts/transactions_with_historical_kyc.sql
{{
    config(
        materialized='table',
        tags=['marts', 'transactions', 'kyc']
    )
}}

select
    t.txn_id,
    t.user_id,
    t.status,
    t.source_currency,
    t.destination_currency,
    t.created_at,
    t.usd_value_estimated,
    t.historical_kyc_level,
    u.current_kyc_level,
    case
        when t.historical_kyc_level = u.current_kyc_level then 'unchanged'
        else 'changed'
    end as kyc_change_status,
    t.kyc_version
from {{ ref('fct_transactions') }} t
join {{ ref('dim_users') }} u on t.user_id = u.user_id
where t.status = 'completed'