{{
    config(
        materialized='table',
        tags=['marts', 'aggregation', 'monthly']
    )
}}

select
    transaction_month_start as month,
    year(transaction_month_start) as year,
    month(transaction_month_start) as month_number,
    quarter(transaction_month_start) as quarter,
    sum(usd_value_estimated) as total_usd_volume,
    count(*) as total_transactions,
    count(distinct user_id) as active_users,
    avg(usd_value_estimated) as avg_transaction_size
from {{ ref('fct_transactions') }}
where status = 'completed'
group by 1, 2, 3, 4