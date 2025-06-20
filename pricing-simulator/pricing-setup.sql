with pricing_setup (id, time_period, deductible, time_price_ratio, deductible_price_ratio) as (
  values
  (1, '7 days', 0.05, 1.00, 1.00),
  (2, '7 days', 0.025, 1.00, 1.00*0.325/0.30),
  (3, '3 days', 0.05, 1.10, 1.00),
  (4, '3 days', 0.025, 1.10, 1.00*0.325/0.30)
)

select time_period, deductible, '|', time_price_ratio, deductible_price_ratio, '|', time_price_ratio * deductible_price_ratio as price_ratio
from pricing_setup
order by id
