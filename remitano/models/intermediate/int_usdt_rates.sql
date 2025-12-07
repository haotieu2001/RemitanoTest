{{
    config(
        materialized='table',
        tags=['intermediate', 'rates', 'usdt']
    )
}}

with rates as (
    select
        -- Use open_time (hour start)
        open_time as hour_start,
        base_currency,      -- ADA, SHIB, BTC, etc.
        quote_currency,     -- USDT (filtered below)
        -- Use the CLOSE price 
        close_rate,         -- 0.7898000000000001 ADA/USDT
        -- Calculate inverse rate: USDT/ADA = 1/0.7898 = 1.265
        case
            when quote_currency = '{{ var("usdt_currency") }}' then 
                1 / close_rate
            else null
        end as usdt_to_base_rate
    from {{ ref('stg_rates') }}
    where quote_currency = '{{ var("usdt_currency") }}'  -- Only USDT pairs
)

select
    hour_start,
    base_currency,
    close_rate,
    usdt_to_base_rate,
    coalesce(usdt_to_base_rate, 1) as effective_usdt_rate
from rates
