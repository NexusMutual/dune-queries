with simulation_results as (

  with

  depeg_probability_inputs as (
    select
      n,
      0.1 * n as depeg,
      1 - 0.1 * n / 100.0000 as price,
      normal_cdf({{mu}}, {{sigma}}, ln(0.1 * n)) as p_no_depeg,
      case
        when n < 10 then 0.01
        when n = 10 then 0.05
        when n <= 50 then 0.05 + (0.0025 * (n - 10))
        else least(0.15 + (0.005 * (n - 50)), 1.00)
      end as p_perm_depeg
    from unnest(sequence(1, 225)) as t(n)
    where (0.1 * n) in (10, 15, 20)
  ),

  sequence_randomness as (
    select
      (a * 10000 + b) as n,
      rand() as r_depeg,
      rand() as r_recovery
    from unnest(sequence(0, 10000 - 1)) as t1(a)
      cross join unnest(sequence(1, 10000)) as t2(b)
    where (a * 10000 + b) <= {{simulation size}} -- max = 100M with current setup
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
  ),

  simulation_probability_results as (
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
  )

  select
    1.0000 * sum(depeg_10) / count(depeg_10) as p_depeg_10,
    1.0000 * sum(depeg_15) / count(depeg_15) as p_depeg_15,
    1.0000 * sum(depeg_20) / count(depeg_20) as p_depeg_20,
    1.0000 * sum(perm_depeg_10) / count(perm_depeg_10) as p_perm_depeg_10,
    1.0000 * sum(perm_depeg_15) / count(perm_depeg_15) as p_perm_depeg_15,
    1.0000 * sum(perm_depeg_20) / count(perm_depeg_20) as p_perm_depeg_20,
    1.0000 * sum(loss_event_10) / count(loss_event_10) as p_loss_event_10,
    1.0000 * sum(loss_event_15) / count(loss_event_15) as p_loss_event_15,
    1.0000 * sum(loss_event_20) / count(loss_event_20) as p_loss_event_20
  from simulation_probability_results

),

pricing_setup (id, time_period, deductible, time_price_ratio, deductible_price_ratio) as (
  values
  (1, '7 days', 0.05, 1.00, 1.00),
  (2, '7 days', 0.025, 1.00, 1.00*0.325/0.30),
  (3, '3 days', 0.05, 1.10, 1.00),
  (4, '3 days', 0.025, 1.10, 1.00*0.325/0.30)
)

select
  '10%' as depeg_pct,
  (0.02 * sr.p_loss_event_10 / sr.p_loss_event_20) * ps_7d_50pct.time_price_ratio * ps_7d_50pct.deductible_price_ratio as deductible_7d_50pct,
  (0.02 * sr.p_loss_event_10 / sr.p_loss_event_20) * ps_3d_50pct.time_price_ratio * ps_3d_50pct.deductible_price_ratio as deductible_3d_50pct,
  (0.02 * sr.p_loss_event_10 / sr.p_loss_event_20) * ps_7d_25pct.time_price_ratio * ps_7d_25pct.deductible_price_ratio as deductible_7d_25pct,
  (0.02 * sr.p_loss_event_10 / sr.p_loss_event_20) * ps_3d_25pct.time_price_ratio * ps_3d_25pct.deductible_price_ratio as deductible_3d_25pct
from simulation_results sr
  inner join pricing_setup ps_7d_50pct on ps_7d_50pct.id = 1
  inner join pricing_setup ps_7d_25pct on ps_7d_25pct.id = 2
  inner join pricing_setup ps_3d_50pct on ps_3d_50pct.id = 3
  inner join pricing_setup ps_3d_25pct on ps_3d_25pct.id = 4
union all
select
  '15%' as depeg_pct,
  (0.02 * sr.p_loss_event_15 / sr.p_loss_event_20) * ps_7d_50pct.time_price_ratio * ps_7d_50pct.deductible_price_ratio as deductible_7d_50pct,
  (0.02 * sr.p_loss_event_15 / sr.p_loss_event_20) * ps_3d_50pct.time_price_ratio * ps_3d_50pct.deductible_price_ratio as deductible_3d_50pct,
  (0.02 * sr.p_loss_event_15 / sr.p_loss_event_20) * ps_7d_25pct.time_price_ratio * ps_7d_25pct.deductible_price_ratio as deductible_7d_25pct,
  (0.02 * sr.p_loss_event_15 / sr.p_loss_event_20) * ps_3d_25pct.time_price_ratio * ps_3d_25pct.deductible_price_ratio as deductible_3d_25pct
from simulation_results sr
  inner join pricing_setup ps_7d_50pct on ps_7d_50pct.id = 1
  inner join pricing_setup ps_7d_25pct on ps_7d_25pct.id = 2
  inner join pricing_setup ps_3d_50pct on ps_3d_50pct.id = 3
  inner join pricing_setup ps_3d_25pct on ps_3d_25pct.id = 4
union all
select
  '20%' as depeg_pct,
  (0.02 * sr.p_loss_event_20 / sr.p_loss_event_20) * ps_7d_50pct.time_price_ratio * ps_7d_50pct.deductible_price_ratio as deductible_7d_50pct,
  (0.02 * sr.p_loss_event_20 / sr.p_loss_event_20) * ps_3d_50pct.time_price_ratio * ps_3d_50pct.deductible_price_ratio as deductible_3d_50pct,
  (0.02 * sr.p_loss_event_20 / sr.p_loss_event_20) * ps_7d_25pct.time_price_ratio * ps_7d_25pct.deductible_price_ratio as deductible_7d_25pct,
  (0.02 * sr.p_loss_event_20 / sr.p_loss_event_20) * ps_3d_25pct.time_price_ratio * ps_3d_25pct.deductible_price_ratio as deductible_3d_25pct
from simulation_results sr
  inner join pricing_setup ps_7d_50pct on ps_7d_50pct.id = 1
  inner join pricing_setup ps_7d_25pct on ps_7d_25pct.id = 2
  inner join pricing_setup ps_3d_50pct on ps_3d_50pct.id = 3
  inner join pricing_setup ps_3d_25pct on ps_3d_25pct.id = 4
order by 1 desc
