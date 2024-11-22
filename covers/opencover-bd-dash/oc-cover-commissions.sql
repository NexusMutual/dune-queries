select
  cover_month,
  commission_destination,
  eth_commission,
  usd_commission
from query_3926339 -- bd-cover-commissions
where commission_destination = 'OpenCover'
order by 1, 2
