select
  100.0000 * sum(depeg_10) / count(depeg_10) as p_depeg_10,
  100.0000 * sum(depeg_15) / count(depeg_15) as p_depeg_15,
  100.0000 * sum(depeg_20) / count(depeg_20) as p_depeg_20,
  100.0000 * sum(perm_depeg_10) / count(perm_depeg_10) as p_perm_depeg_10,
  100.0000 * sum(perm_depeg_15) / count(perm_depeg_15) as p_perm_depeg_15,
  100.0000 * sum(perm_depeg_20) / count(perm_depeg_20) as p_perm_depeg_20,
  100.0000 * sum(loss_event_10) / count(loss_event_10) as p_loss_event_10,
  100.0000 * sum(loss_event_15) / count(loss_event_15) as p_loss_event_15,
  100.0000 * sum(loss_event_20) / count(loss_event_20) as p_loss_event_20
from query_5304368 -- depeg pricing simulation
