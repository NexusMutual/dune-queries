select
  n,
  0.1 * n as depeg,
  1 - 0.1 * n / 100.0000 as price,
  normal_cdf(0, 1.4, ln(0.1 * n)) as p_no_depeg,
  case
    when n < 10 then 0.01
    when n = 10 then 0.05
    when n <= 50 then 0.05 + (0.0025 * (n - 10))
    else least(0.15 + (0.005 * (n - 50)), 1.00)
  end as p_perm_depeg
from unnest(sequence(1, 225)) as t(n)
order by 1
