with

prices as (
  select
    block_date,
    avg_eth_usd_price,
    avg_usdc_usd_price
  from query_4627588 -- Capital Pool - base root
),

capital_pool as (
  select
    date_trunc('month', block_date) as block_month,
    capital_pool,
    lag(capital_pool, 1) over (order by block_date) as capital_pool_prev,
    --eth,
    --lag(eth, 1) over (order by block_date) as eth_prev,
    steth,
    lag(steth, 1) over (order by block_date) as steth_prev,
    reth,
    lag(reth, 1) over (order by block_date) as reth_prev,
    nxmty,
    lag(nxmty, 1) over (order by block_date) as nxmty_prev,
    aweth,
    lag(aweth, 1) over (order by block_date) as aweth_prev,
    debt_usdc,
    lag(debt_usdc, 1) over (order by block_date) as debt_usdc_prev,
    dai,
    lag(dai, 1) over (order by block_date) as dai_prev,
    usdc,
    lag(usdc, 1) over (order by block_date) as usdc_prev,
    cover_re,
    lag(cover_re, 1) over (order by block_date) as cover_re_prev
  from (
    select
      block_date,
      avg_capital_pool_eth_total as capital_pool,
      --eth_total as eth,
      steth_total as steth,
      avg_reth_eth_total as reth,
      nxmty_eth_total as nxmty,
      aave_collateral_weth_total as aweth,
      avg_aave_debt_usdc_eth_total as debt_usdc,
      avg_dai_eth_total as dai,
      avg_usdc_eth_total as usdc,
      avg_cover_re_usdc_eth_total as cover_re,
      row_number() over (partition by date_trunc('month', block_date) order by block_date desc) as rn
    from query_4627588 -- Capital Pool - base root
  ) t
  where t.rn = 1
  order by 1 desc
  limit 12 -- 12 months rolling
),

kiln_rewards as (
  select
    date_trunc('month', seq_date) as block_month,
    kiln_rewards,
    lag(kiln_rewards, 1) over (order by seq_date) as kiln_rewards_prev
  from (
    select
      seq_date,
      kiln_rewards,
      row_number() over (partition by date_trunc('month', seq_date) order by seq_date desc) as rn
    from query_4830965 -- kiln rewards
  ) t
  where rn = 1
),

prices_start_end as (
  select
    date_trunc('month', block_date) as block_month,
    lag(avg_eth_usd_price, 1) over (order by block_date) as eth_usd_price_start, -- start: last of the previous month
    avg_eth_usd_price as eth_usd_price_end -- end: last of the month
  from (
    select
      block_date,
      avg_eth_usd_price,
      row_number() over (partition by date_trunc('month', block_date) order by block_date desc) as rn
    from prices
  ) t
  where rn = 1
),

stables_fx_impact as (
  select
    s.block_month,
    -- stables expressed in ETH
    s.dai_prev * p.eth_usd_price_start / p.eth_usd_price_end - s.dai_prev as dai_fx_change,
    s.usdc_prev * p.eth_usd_price_start / p.eth_usd_price_end - s.usdc_prev as usdc_fx_change,
    s.cover_re_prev * p.eth_usd_price_start / p.eth_usd_price_end - s.cover_re_prev as cover_re_fx_change,
    s.debt_usdc_prev * p.eth_usd_price_start / p.eth_usd_price_end - s.debt_usdc_prev as debt_usdc_fx_change
  from capital_pool s
    inner join prices_start_end p on s.block_month = p.block_month
),

aweth_collateral as (
  select
    date_trunc('month', block_time) as block_month,
    sum(if(transaction_type = 'deposit', amount)) as aweth_deposit_eth,
    sum(if(transaction_type = 'deposit', amount_usd)) as aweth_deposit_usd,
    sum(if(transaction_type = 'withdraw', amount)) as aweth_withdraw_eth,
    sum(if(transaction_type = 'withdraw', amount_usd)) as aweth_withdraw_usd
  from aave_ethereum.supply
  where block_time >= timestamp '2024-05-23'
    and coalesce(on_behalf_of, depositor) = 0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e -- Advisory Board multisig
    and version = '3'
    and symbol = 'WETH'
  group by 1
),

usdc_debt as (
  select
    date_trunc('month', b.block_time) as block_month,
    sum(if(b.transaction_type = 'borrow', b.amount) / p.avg_eth_usd_price) as debt_usdc_borrow_eth,
    sum(if(b.transaction_type = 'borrow', b.amount_usd)) as debt_usdc_borrow_usd,
    -1 * sum(if(b.transaction_type = 'repay', b.amount) / p.avg_eth_usd_price) as debt_usdc_repay_eth,
    -1 * sum(if(b.transaction_type = 'repay', b.amount_usd)) as debt_usdc_repay_usd
  from aave_ethereum.borrow b
    inner join prices p on date_trunc('day', b.block_time) = p.block_date
  where b.block_time >= timestamp '2024-05-23'
    and coalesce(b.on_behalf_of, b.borrower) = 0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e -- Advisory Board multisig
    and b.version = '3'
    and b.symbol = 'USDC'
  group by 1
),

steth_sales as (
  select
    block_time,
    block_date,
    fill_type,
    sell_amount,
    buy_amount,
    fill_amount,
    tx_hash
  from nexusmutual_ethereum.swap_order_closed
  where sell_token_symbol = 'stETH'
    and buy_token_symbol = 'ETH'
    and fill_type <> 'no fill'
),

steth_sales_adjusted as (
  select
    block_time,
    block_date,
    fill_type,
    sell_amount - fill_amount as sell_amount,
    buy_amount,
    fill_amount,
    tx_hash
  from steth_sales
  where fill_type = 'funds returned'
),

steth_sales_agg as (
  select
    date_trunc('month', s.block_date) as block_month,
    -1 * sum(coalesce(sa.sell_amount, s.sell_amount)) as sell_amount,
    sum(s.fill_amount) as fill_amount,
    -1 * sum(coalesce(sa.sell_amount, s.sell_amount) - s.fill_amount) as disposal_loss
  from steth_sales s
    left join steth_sales_adjusted sa on sa.block_time = s.block_time and sa.tx_hash = s.tx_hash
  where s.fill_type <> 'funds returned'
  group by 1
),

capital_pool_enriched as (
  select
    cp.block_month,
    cp.capital_pool,
    cp.capital_pool_prev,
    -- eth denomiated assets
    --cp.eth,
    --cp.eth_prev,
    cp.steth,
    cp.steth_prev,
    coalesce(s.sell_amount, 0) as steth_sale,
    cp.reth,
    cp.reth_prev,
    cp.nxmty + coalesce(kr.kiln_rewards, 0) as nxmty,
    cp.nxmty_prev + coalesce(kr.kiln_rewards_prev, 0) as nxmty_prev,
    cp.aweth,
    cp.aweth_prev,
    coalesce(aave_c.aweth_deposit_eth, 0) as aweth_deposit_eth,
    coalesce(aave_c.aweth_withdraw_eth, 0) as aweth_withdraw_eth,
    cp.debt_usdc,
    cp.debt_usdc_prev,
    coalesce(aave_d.debt_usdc_borrow_eth, 0) as debt_usdc_borrow_eth,
    coalesce(aave_d.debt_usdc_repay_eth, 0) as debt_usdc_repay_eth,
    -- stablecoin denominated assets
    fx.dai_fx_change,
    fx.usdc_fx_change,
    fx.cover_re_fx_change,
    fx.debt_usdc_fx_change,
    fx.dai_fx_change + fx.usdc_fx_change + fx.cover_re_fx_change + fx.debt_usdc_fx_change as fx_change
  from capital_pool cp
    inner join stables_fx_impact fx on cp.block_month = fx.block_month
    left join aweth_collateral aave_c on cp.block_month = aave_c.block_month
    left join usdc_debt aave_d on cp.block_month = aave_d.block_month
    left join steth_sales_agg s on cp.block_month = s.block_month
    left join kiln_rewards kr on cp.block_month = kr.block_month
),

investment_returns as (
  select
    block_month,
    capital_pool,
    capital_pool_prev,
    capital_pool - capital_pool_prev as capital_pool_return,
    coalesce((capital_pool - capital_pool_prev) / nullif(capital_pool_prev, 0), 0) as capital_pool_pct,
    coalesce(power(1 + ((capital_pool - capital_pool_prev) / nullif(capital_pool_prev, 0)), 12) - 1, 0) as capital_pool_apy,
    -- eth investments
    --eth,
    --eth_prev,
    --eth - eth_prev as eth_return,
    --coalesce((eth - eth_prev) / nullif(eth_prev, 0), 0) as eth_pct,
    --coalesce(power(1 + ((eth - eth_prev) / nullif(eth_prev, 0)), 12) - 1, 0) as eth_apy,
    steth,
    steth_prev,
    steth_sale,
    steth - steth_sale - steth_prev as steth_return,
    coalesce((steth - steth_sale - steth_prev) / nullif(steth_prev, 0), 0) as steth_pct,
    coalesce(power(1 + ((steth - steth_sale - steth_prev) / nullif(steth_prev, 0)), 12) - 1, 0) as steth_apy,
    reth,
    reth_prev,
    reth - reth_prev as reth_return,
    coalesce((reth - reth_prev) / nullif(reth_prev, 0), 0) as reth_pct,
    coalesce(power(1 + ((reth - reth_prev) / nullif(reth_prev, 0)), 12) - 1, 0) as reth_apy,
    nxmty,
    nxmty_prev,
    (nxmty - nxmty_prev) * (1-0.0015) as nxmty_return, -- minus Enzyme fee
    coalesce((nxmty - nxmty_prev) / nullif(nxmty_prev, 0), 0) as nxmty_pct,
    coalesce(power(1 + ((nxmty - nxmty_prev) / nullif(nxmty_prev, 0)), 12) - 1, 0) as nxmty_apy,
    -- aave positions
    aweth,
    aweth_prev,
    aweth_deposit_eth,
    aweth_withdraw_eth,
    aweth - aweth_deposit_eth - aweth_withdraw_eth - aweth_prev as aweth_return,
    coalesce((aweth - aweth_deposit_eth - aweth_withdraw_eth - aweth_prev) / nullif(aweth_prev, 0), 0) as aweth_pct,
    coalesce(power(1 + ((aweth - aweth_deposit_eth - aweth_withdraw_eth - aweth_prev) / nullif(aweth_prev, 0)), 12) - 1, 0) as aweth_apy,
    debt_usdc,
    debt_usdc_prev,
    debt_usdc_borrow_eth,
    debt_usdc_repay_eth,
    debt_usdc - (debt_usdc_prev + debt_usdc_fx_change + debt_usdc_borrow_eth + debt_usdc_repay_eth) as debt_usdc_return,
    coalesce((debt_usdc - (debt_usdc_prev + debt_usdc_fx_change + debt_usdc_borrow_eth + debt_usdc_repay_eth)) / nullif(debt_usdc_prev, 0), 0) as debt_usdc_pct,
    coalesce(power(1 + ((debt_usdc - (debt_usdc_prev + debt_usdc_fx_change + debt_usdc_borrow_eth + debt_usdc_repay_eth)) / nullif(debt_usdc_prev, 0)), 12) - 1, 0) as debt_usdc_apy,
    -- stablecoins fx impact
    dai_fx_change,
    usdc_fx_change,
    cover_re_fx_change,
    debt_usdc_fx_change,
    fx_change
  from capital_pool_enriched
)

select
  date_format(block_month, '%Y-%m') as block_month,
  -- capital pool total
  capital_pool,
  capital_pool_return,
  capital_pool_pct,
  capital_pool_apy,
  -- individual eth investments
  --eth,
  --eth_return,
  --eth_apy,
  steth,
  steth_sale,
  steth_return,
  steth_apy,
  reth,
  reth_return,
  reth_apy,
  nxmty,
  nxmty_return,
  nxmty_apy,
  -- aave positions
  aweth,
  aweth_deposit_eth,
  aweth_withdraw_eth,
  aweth_return,
  aweth_apy,
  debt_usdc,
  debt_usdc_borrow_eth,
  debt_usdc_repay_eth,
  debt_usdc_return,
  debt_usdc_apy,
  aweth_return + debt_usdc_return as aave_net_return,
  coalesce(power(1 + ((aweth_return + debt_usdc_return) / nullif(aweth_prev, 0)), 12) - 1, 0) as aweth_net_apy,
  -- total eth investment returns
  steth_return + reth_return + nxmty_return + (aweth_return + debt_usdc_return) as eth_inv_returns,
  coalesce(power(1 + ((steth_return + reth_return + nxmty_return + (aweth_return + debt_usdc_return))
   / nullif(((capital_pool_prev + capital_pool) / 2), 0)), 12) - 1, 0) as eth_inv_apy,
  -- stablecoins fx impact
  dai_fx_change,
  usdc_fx_change,
  cover_re_fx_change,
  debt_usdc_fx_change,
  fx_change
from investment_returns
order by 1 desc
