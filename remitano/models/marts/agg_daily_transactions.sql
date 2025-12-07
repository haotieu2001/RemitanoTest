{{
    config(
        materialized='table',
        tags=['marts', 'aggregation', 'daily']
    )
}}

select
    transaction_date,
    source_currency,
    destination_currency,
    count(*) as transaction_count,
    count(case when status = 'completed' then 1 end) as completed_transactions,
    sum(case when status = 'completed' then usd_value_estimated else 0 end) as total_usd_volume,
    avg(case when status = 'completed' then usd_value_estimated end) as avg_usd_per_transaction
from {{ ref('fct_transactions') }}
group by 1, 2, 3