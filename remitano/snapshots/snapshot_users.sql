{% snapshot snapshot_users %}

{{
    config(
        target_database='workspace',
        target_schema='rmt_snapshots',
        unique_key='user_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

select
    user_id,
    kyc_level,
    created_at,
    updated_at,
    loaded_at
from {{ ref('stg_users') }}

{% endsnapshot %}