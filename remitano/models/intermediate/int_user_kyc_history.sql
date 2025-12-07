{{
    config(
        materialized='table',
        tags=['intermediate', 'users', 'kyc']
    )
}}

with user_snapshot as (
    select
        user_id,
        kyc_level,
        updated_at as valid_from,
        coalesce(
            lead(updated_at) over (partition by user_id order by updated_at),
            '9999-12-31'::timestamp
        ) as valid_to,
        dbt_valid_from as snapshot_valid_from,
        dbt_valid_to as snapshot_valid_to
    from {{ ref('snapshot_users') }}
    where dbt_valid_to is null or dbt_valid_to > '1900-01-01'
),

final as (
    select
        user_id,
        kyc_level,
        valid_from,
        valid_to,
        case
            when valid_to = '9999-12-31'::timestamp then true
            else false
        end as is_current,
        row_number() over (partition by user_id order by valid_from) as kyc_version
    from user_snapshot
)

select * from final