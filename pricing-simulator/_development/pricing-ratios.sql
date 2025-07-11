with simulation_results as (

  with

  depeg_probability_inputs as (
    select
      n,
      0.1 * n as depeg,
      1 - 0.1 * n / 100.0000 as price,
      normal_cdf({{mu}}, {{sigma}}, ln(0.1 * n)) as p_no_depeg,
      normal_cdf({{mu}}, 1.0, ln(0.1 * n)) as p_no_depeg_baseline,
      case
        when n < 10 then 0.01
        when n = 10 then 0.05
        when n <= 50 then 0.05 + (0.0025 * (n - 10))
        else least(0.15 + (0.005 * (n - 50)), 1.00)
      end as p_perm_depeg
      --p_perm_depeg_baseline ??
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
      if(sr.r_depeg > dpi_10.p_no_depeg_baseline, 1, 0) as depeg_10_baseline,
      if(sr.r_depeg > dpi_15.p_no_depeg_baseline, 1, 0) as depeg_15_baseline,
      if(sr.r_depeg > dpi_20.p_no_depeg_baseline, 1, 0) as depeg_20_baseline,
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
      -- depeg results for given sigma
      depeg_10,
      depeg_15,
      depeg_20,
      perm_depeg_10,
      perm_depeg_15,
      perm_depeg_20,
      depeg_10 * perm_depeg_10 as loss_event_10,
      depeg_15 * perm_depeg_15 as loss_event_15,
      depeg_20 * perm_depeg_20 as loss_event_20,
      -- baseline results
      depeg_10_baseline,
      depeg_15_baseline,
      depeg_20_baseline,
      depeg_10_baseline * perm_depeg_10 as loss_event_10_baseline,
      depeg_15_baseline * perm_depeg_15 as loss_event_15_baseline,
      depeg_20_baseline * perm_depeg_20 as loss_event_20_baseline
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
    1.0000 * sum(loss_event_20) / count(loss_event_20) as p_loss_event_20,
    1.0000 * sum(loss_event_10_baseline) / count(loss_event_10_baseline) as p_loss_event_10_baseline,
    1.0000 * sum(loss_event_15_baseline) / count(loss_event_15_baseline) as p_loss_event_15_baseline,
    1.0000 * sum(loss_event_20_baseline) / count(loss_event_20_baseline) as p_loss_event_20_baseline
  from simulation_probability_results

),

ratios as (
  select
    p_loss_event_10 / p_loss_event_20 as ratio_10,
    p_loss_event_10_baseline / p_loss_event_20_baseline as ratio_10_baseline,
    p_loss_event_15 / p_loss_event_20 as ratio_15,
    p_loss_event_15_baseline / p_loss_event_20_baseline as ratio_15_baseline,
    p_loss_event_20 / p_loss_event_20 as ratio_20,
    p_loss_event_20_baseline / p_loss_event_20_baseline as ratio_20_baseline
  from simulation_results
)

select
  '10%' as depeg_pct,
  0.02 * ratio_10 * ratio_10_baseline / ratio_10_baseline as p_event,
  ratio_10 * ratio_10_baseline / ratio_10_baseline as ratio
from ratios
union all
select
  '15%' as depeg_pct,
  0.02 * ratio_15 * ratio_15_baseline / ratio_15_baseline as p_event,
  ratio_15 * ratio_15_baseline / ratio_15_baseline as ratio
from ratios
union all
select
  '20%' as depeg_pct,
  0.02 * ratio_20 * ratio_20_baseline / ratio_20_baseline as p_event,
  ratio_20 * ratio_20_baseline / ratio_20_baseline as ratio
from simulation_results
order by 1
