{{
    config(
        materialized='table',
        unique_key='user_id',
        tags=['staging', 'users']
    )
}}

with source as (
    select * from {{ ref('users') }}
),

cleaned as (
    select
        trim(cast(user_id as string)) as user_id,
        cast(kyc_level as integer) as kyc_level,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        current_timestamp() as loaded_at
    from source
)

select * from cleaned