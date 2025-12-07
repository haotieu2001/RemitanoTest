-- models/staging/stg_rates.sql (UPDATED)
{{
    config(
        materialized='table',
        tags=['staging', 'rates']
    )
}}

with source as (
    select * from {{ ref('rates') }}
),

cleaned as (
    select
        -- Keep open_time as is (hour start)
        cast(open_time as timestamp) as open_time,
        --  keep close_time for reference
        cast(close_time as timestamp) as close_time,
        cast(open as decimal(38, 10)) as open_rate,
        cast(high as decimal(38, 10)) as high_rate,
        cast(low as decimal(38, 10)) as low_rate,
        cast(close as decimal(38, 10)) as close_rate,
        -- Trading volume
        cast(volume as decimal(38, 10)) as volume,     
        trim(symbol) as symbol,
        trim(base_currency) as base_currency,
        trim(quote_currency) as quote_currency,      
        -- Audit
        current_timestamp() as loaded_at
    from source
)

select * from cleaned