select *
from {{ ref('fct_transactions') }}
where usd_value_estimated < 0