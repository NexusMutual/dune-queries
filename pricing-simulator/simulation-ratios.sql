/*
select 'chosen ratio', 1.55, 1.40, 1.00 union all
select 'pricing ratio'
*/

with simulation_results as (
  select
    p_loss_event_10,
    p_loss_event_15,
    p_loss_event_20  
  from query_5310092 -- simulation results
)

select
  'Pr (loss event)' as metric,
  p_loss_event_10 as ratio_10,
  p_loss_event_15 as ratio_15,
  p_loss_event_20 as ratio_20  
from simulation_results
union all
select
  'pricing ratio' as metric,
  p_loss_event_10 / p_loss_event_20 as ratio_10,
  p_loss_event_15 / p_loss_event_20 as ratio_15,
  p_loss_event_20 / p_loss_event_20 as ratio_20 
from simulation_results
