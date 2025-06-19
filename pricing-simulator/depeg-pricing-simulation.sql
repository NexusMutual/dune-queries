with

depeg_probability_inputs as (
  select
    n,
    depeg,
    price,
    p_no_depeg,
    p_perm_depeg
  from query_5302618 -- depeg probability inputs
  where depeg in (10, 15, 20)
),

sequence_randomness as (
  select
    (a * 10000 + b) as n,
    rand() as r_depeg,
    rand() as r_recovery
  from unnest(sequence(0, 100 - 1)) as t1(a)
    cross join unnest(sequence(1, 10000)) as t2(b)
  where (a * 10000 + b) <= 1000
),

simulation_results as (
  select
    sr.n,
    sr.r_depeg,
    sr.r_recovery,
    if(sr.r_depeg > dpi_10.p_no_depeg, 1, 0) as depeg_10,
    if(sr.r_depeg > dpi_15.p_no_depeg, 1, 0) as depeg_15,
    if(sr.r_depeg > dpi_20.p_no_depeg, 1, 0) as depeg_20,
    if(sr.r_recovery < dpi_10.p_perm_depeg, 1, 0) as perm_depeg_10,
    if(sr.r_recovery < dpi_15.p_perm_depeg, 1, 0) as perm_depeg_15,
    if(sr.r_recovery < dpi_20.p_perm_depeg, 1, 0) as perm_depeg_20
  from sequence_randomness sr
    inner join depeg_probability_inputs dpi_10 on dpi_10.depeg = 10
    inner join depeg_probability_inputs dpi_15 on dpi_15.depeg = 15
    inner join depeg_probability_inputs dpi_20 on dpi_20.depeg = 20
)

select
  n,
  r_depeg,
  r_recovery,
  depeg_10,
  depeg_15,
  depeg_20,
  perm_depeg_10,
  perm_depeg_15,
  perm_depeg_20,
  depeg_10 * perm_depeg_10 as loss_event_10,
  depeg_15 * perm_depeg_15 as loss_event_15,
  depeg_20 * perm_depeg_20 as loss_event_20
from simulation_results
order by 1
