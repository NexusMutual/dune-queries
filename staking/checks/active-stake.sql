select
  pool_id,
  sum(total_staked_nxm) as total_staked_nxm
--from nexusmutual_ethereum.staked_per_token_tranche
from query_5226858 -- staked nxm per token & tranche - base
where token_tranche_rn = 1 -- current tranche
  and block_date = current_date -- today's stake
group by 1
order by 1
