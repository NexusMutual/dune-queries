select
  cast(date(burn_block_time) as varchar) as burn_block_date,
  substr(concat('0', cast(pool_id as varchar)), -2, 2) as pool_id, -- formatting necessary for sorting in pivot table
  count(distinct token_id) as affected_tokens,
  count(distinct staker) as affected_stakers,
  sum(abs(burn_delta)) as burn_total
from query_5666869 -- stake burn impact
group by 1, 2
order by 1, 2
