select
  (a * 10000 + b) as n,
  rand() as r_depeg,
  rand() as r_recovery
from unnest(sequence(0, 100 - 1)) as t1(a)
  cross join unnest(sequence(1, 10000)) as t2(b)
order by 1
