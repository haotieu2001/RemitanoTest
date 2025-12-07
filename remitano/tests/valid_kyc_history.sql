-- tests/valid_kyc_history.sql
select *
from {{ ref('fct_transactions') }}
where historical_kyc_level is null
and created_at > '2024-01-01'