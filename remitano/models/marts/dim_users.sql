-- models/marts/dim_users.sql
{{
    config(
        materialized='table',
        unique_key='user_id',
        tags=['marts', 'dimension', 'users']
    )
}}

with current_users as (
    select
        user_id,
        kyc_level as current_kyc_level,
        created_at as user_created_at,
        updated_at as user_last_updated
    from {{ ref('stg_users') }}
),

kyc_history as (
    select
        user_id,
        count(*) as total_kyc_changes,
        min(valid_from) as first_kyc_date,
        max(valid_from) as last_kyc_change_date
    from {{ ref('int_user_kyc_history') }}
    group by 1
),

user_transactions as (
    select
        user_id,
        count(*) as total_transactions,
        count(case when status = 'completed' then 1 end) as completed_transactions,
        min(created_at) as first_transaction_date,
        max(created_at) as last_transaction_date,
        sum(case when status = 'completed' then usd_value_estimated else 0 end) as total_usd_volume
    from {{ ref('fct_transactions') }}
    group by 1
)

select
    cu.user_id,
    cu.current_kyc_level,
    cu.user_created_at,
    cu.user_last_updated,
    coalesce(kh.total_kyc_changes, 0) as total_kyc_changes,
    kh.first_kyc_date,
    kh.last_kyc_change_date,
    coalesce(ut.total_transactions, 0) as total_transactions,
    coalesce(ut.completed_transactions, 0) as completed_transactions,
    ut.first_transaction_date,
    ut.last_transaction_date,
    coalesce(ut.total_usd_volume, 0) as total_usd_volume,
    case
        when ut.total_transactions > 0 then 'active'
        else 'inactive'
    end as user_status
from current_users cu
left join kyc_history kh on cu.user_id = kh.user_id
left join user_transactions ut on cu.user_id = ut.user_id