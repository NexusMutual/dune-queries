with pricing_setup (id, time_period, time_price_ratio, deductible, deductible_price_ratio) as (
  values
  (1, '3 days', 1.10, 0.025, 1.00*0.325/0.30),
  (2, '7 days', 1.00, 0.05, 1.00)
)

select time_period, time_price_ratio, '|', deductible, deductible_price_ratio
from pricing_setup
order by id
