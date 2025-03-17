with

prices as (
  select
    block_date,
    avg_eth_usd_price
  from query_4627588 -- Capital Pool - base root
),

capital_pool as (
  select
    date_trunc('month', block_date) as block_month,
    -- === ETH ===
    eth_capital_pool,
    lag(eth_capital_pool, 1) over (order by block_date) as eth_capital_pool_prev,
    eth_steth,
    lag(eth_steth, 1) over (order by block_date) as eth_steth_prev,
    eth_reth,
    lag(eth_reth, 1) over (order by block_date) as eth_reth_prev,
    eth_nxmty,
    lag(eth_nxmty, 1) over (order by block_date) as eth_nxmty_prev,
    eth_aweth,
    lag(eth_aweth, 1) over (order by block_date) as eth_aweth_prev,
    eth_debt_usdc,
    lag(eth_debt_usdc, 1) over (order by block_date) as eth_debt_usdc_prev,
    eth_dai,
    lag(eth_dai, 1) over (order by block_date) as eth_dai_prev,
    eth_usdc,
    lag(eth_usdc, 1) over (order by block_date) as eth_usdc_prev,
    eth_cover_re,
    lag(eth_cover_re, 1) over (order by block_date) as eth_cover_re_prev,
    -- === USD ===
    usd_capital_pool,
    lag(usd_capital_pool, 1) over (order by block_date) as usd_capital_pool_prev,
    usd_steth,
    lag(usd_steth, 1) over (order by block_date) as usd_steth_prev,
    usd_reth,
    lag(usd_reth, 1) over (order by block_date) as usd_reth_prev,
    usd_nxmty,
    lag(usd_nxmty, 1) over (order by block_date) as usd_nxmty_prev,
    usd_aweth,
    lag(usd_aweth, 1) over (order by block_date) as usd_aweth_prev,
    usd_debt_usdc,
    lag(usd_debt_usdc, 1) over (order by block_date) as usd_debt_usdc_prev,
    usd_dai,
    lag(usd_dai, 1) over (order by block_date) as usd_dai_prev,
    usd_usdc,
    lag(usd_usdc, 1) over (order by block_date) as usd_usdc_prev,
    usd_cover_re,
    lag(usd_cover_re, 1) over (order by block_date) as usd_cover_re_prev
  from (
    select
      block_date,
      avg_capital_pool_eth_total as eth_capital_pool,
      steth_total as eth_steth,
      avg_reth_eth_total as eth_reth,
      nxmty_eth_total as eth_nxmty,
      aave_collateral_weth_total as eth_aweth,
      avg_aave_debt_usdc_eth_total as eth_debt_usdc,
      avg_dai_eth_total as eth_dai,
      avg_usdc_eth_total as eth_usdc,
      avg_cover_re_usdc_eth_total as eth_cover_re,
      avg_capital_pool_usd_total as usd_capital_pool,
      avg_steth_usd_total as usd_steth,
      avg_reth_usd_total as usd_reth,
      avg_nxmty_usd_total as usd_nxmty,
      avg_aave_collateral_weth_usd_total as usd_aweth,
      avg_aave_debt_usdc_usd_total as usd_debt_usdc,
      avg_dai_usd_total as usd_dai,
      avg_usdc_usd_total as usd_usdc,
      avg_cover_re_usdc_usd_total as usd_cover_re,
      row_number() over (partition by date_trunc('month', block_date) order by block_date desc) as rn
    from query_4627588 -- Capital Pool - base root
  ) t
  where t.rn = 1
  order by 1 desc
  limit 12 -- 12 months rolling
),

kiln_rewards as (
  select
    date_trunc('month', t.seq_date) as block_month,
    t.kiln_rewards as eth_kiln_rewards,
    lag(t.kiln_rewards, 1) over (order by t.seq_date) as eth_kiln_rewards_prev,
    t.kiln_rewards * p.avg_eth_usd_price as usd_kiln_rewards,
    lag(t.kiln_rewards * p.avg_eth_usd_price, 1) over (order by t.seq_date) as usd_kiln_rewards_prev
  from (
      select
        seq_date,
        kiln_rewards,
        row_number() over (partition by date_trunc('month', seq_date) order by seq_date desc) as rn
      from query_4830965 -- kiln rewards
    ) t
    inner join prices p on t.seq_date = p.block_date
  where t.rn = 1
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
    p.eth_usd_price_start,
    p.eth_usd_price_end,
    -- stables expressed in ETH
    s.eth_dai_prev * p.eth_usd_price_start / p.eth_usd_price_end - s.eth_dai_prev as eth_dai_fx_change,
    s.eth_usdc_prev * p.eth_usd_price_start / p.eth_usd_price_end - s.eth_usdc_prev as eth_usdc_fx_change,
    s.eth_cover_re_prev * p.eth_usd_price_start / p.eth_usd_price_end - s.eth_cover_re_prev as eth_cover_re_fx_change,
    s.eth_debt_usdc_prev * p.eth_usd_price_start / p.eth_usd_price_end - s.eth_debt_usdc_prev as eth_debt_usdc_fx_change,
    -- stables expressed in USD
    (s.eth_dai_prev * p.eth_usd_price_start / p.eth_usd_price_end - s.eth_dai_prev) * p.eth_usd_price_end as usd_dai_fx_change,
    (s.eth_usdc_prev * p.eth_usd_price_start / p.eth_usd_price_end - s.eth_usdc_prev) * p.eth_usd_price_end as usd_usdc_fx_change,
    (s.eth_cover_re_prev * p.eth_usd_price_start / p.eth_usd_price_end - s.eth_cover_re_prev) * p.eth_usd_price_end as usd_cover_re_fx_change,
    (s.eth_debt_usdc_prev * p.eth_usd_price_start / p.eth_usd_price_end - s.eth_debt_usdc_prev) * p.eth_usd_price_end as usd_debt_usdc_fx_change
  from capital_pool s
    inner join prices_start_end p on s.block_month = p.block_month
),

aweth_collateral as (
  select
    date_trunc('month', block_time) as block_month,
    sum(if(transaction_type = 'deposit', amount)) as eth_aweth_deposit,
    sum(if(transaction_type = 'deposit', amount_usd)) as usd_aweth_deposit,
    sum(if(transaction_type = 'withdraw', amount)) as eth_aweth_withdraw,
    sum(if(transaction_type = 'withdraw', amount_usd)) as usd_aweth_withdraw
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
    -1 * sum(if(b.transaction_type = 'borrow', b.amount) / p.avg_eth_usd_price) as eth_debt_usdc_borrow,
    -1 * sum(if(b.transaction_type = 'borrow', b.amount_usd)) as usd_debt_usdc_borrow,
    -1 * sum(if(b.transaction_type = 'repay', b.amount) / p.avg_eth_usd_price) as eth_debt_usdc_repay,
    -1 * sum(if(b.transaction_type = 'repay', b.amount_usd)) as usd_debt_usdc_repay
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
    -1 * sum(coalesce(sa.sell_amount, s.sell_amount)) as eth_sell_amount,
    -1 * sum(coalesce(sa.sell_amount, s.sell_amount) * p.avg_eth_usd_price) as usd_sell_amount,
    sum(s.fill_amount) as eth_fill_amount,
    sum(s.fill_amount * p.avg_eth_usd_price) as usd_fill_amount,
    -1 * sum(coalesce(sa.sell_amount, s.sell_amount) - s.fill_amount) as eth_disposal_loss,
    -1 * sum((coalesce(sa.sell_amount, s.sell_amount) - s.fill_amount) * p.avg_eth_usd_price) as usd_disposal_loss
  from steth_sales s
    inner join prices p on s.block_date = p.block_date
    left join steth_sales_adjusted sa on sa.block_time = s.block_time and sa.tx_hash = s.tx_hash
  where s.fill_type <> 'funds returned'
  group by 1
),

capital_pool_enriched as (
  select
    cp.block_month,
    -- === ETH ===
    cp.eth_capital_pool + coalesce(kr.eth_kiln_rewards, 0) as eth_capital_pool,
    cp.eth_capital_pool_prev + coalesce(kr.eth_kiln_rewards_prev, 0) as eth_capital_pool_prev,
    -- eth backed assets
    cp.eth_steth,
    cp.eth_steth_prev,
    coalesce(s.eth_sell_amount, 0) as eth_steth_sale,
    cp.eth_reth,
    cp.eth_reth_prev,
    cp.eth_nxmty + coalesce(kr.eth_kiln_rewards, 0) as eth_nxmty,
    cp.eth_nxmty_prev + coalesce(kr.eth_kiln_rewards_prev, 0) as eth_nxmty_prev,
    cp.eth_aweth,
    cp.eth_aweth_prev,
    coalesce(aave_c.eth_aweth_deposit, 0) as eth_aweth_deposit,
    coalesce(aave_c.eth_aweth_withdraw, 0) as eth_aweth_withdraw,
    cp.eth_debt_usdc,
    cp.eth_debt_usdc_prev,
    coalesce(aave_d.eth_debt_usdc_borrow, 0) as eth_debt_usdc_borrow,
    coalesce(aave_d.eth_debt_usdc_repay, 0) as eth_debt_usdc_repay,
    -- stablecoin denominated assets
    fx.eth_dai_fx_change,
    fx.eth_usdc_fx_change,
    fx.eth_cover_re_fx_change,
    fx.eth_debt_usdc_fx_change,
    fx.eth_dai_fx_change + fx.eth_usdc_fx_change + fx.eth_cover_re_fx_change + fx.eth_debt_usdc_fx_change as eth_fx_change,
    -- === USD ===
    cp.usd_capital_pool + coalesce(kr.usd_kiln_rewards, 0) as usd_capital_pool,
    cp.usd_capital_pool_prev + coalesce(kr.usd_kiln_rewards_prev, 0) as usd_capital_pool_prev,
    -- eth backed assets
    cp.usd_steth,
    cp.usd_steth_prev,
    coalesce(s.usd_sell_amount, 0) as usd_steth_sale,
    cp.usd_reth,
    cp.usd_reth_prev,
    cp.usd_nxmty + coalesce(kr.usd_kiln_rewards, 0) as usd_nxmty,
    cp.usd_nxmty_prev + coalesce(kr.usd_kiln_rewards_prev, 0) as usd_nxmty_prev,
    cp.usd_aweth,
    cp.usd_aweth_prev,
    coalesce(aave_c.usd_aweth_deposit, 0) as usd_aweth_deposit,
    coalesce(aave_c.usd_aweth_withdraw, 0) as usd_aweth_withdraw,
    cp.usd_debt_usdc,
    cp.usd_debt_usdc_prev,
    coalesce(aave_d.usd_debt_usdc_borrow, 0) as usd_debt_usdc_borrow,
    coalesce(aave_d.usd_debt_usdc_repay, 0) as usd_debt_usdc_repay,
    -- stablecoin denominated assets
    fx.usd_dai_fx_change,
    fx.usd_usdc_fx_change,
    fx.usd_cover_re_fx_change,
    fx.usd_debt_usdc_fx_change,
    fx.usd_dai_fx_change + fx.usd_usdc_fx_change + fx.usd_cover_re_fx_change + fx.usd_debt_usdc_fx_change as usd_fx_change
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
    -- === ETH ===
    eth_capital_pool,
    eth_capital_pool_prev,
    coalesce((eth_capital_pool - eth_capital_pool_prev) / nullif(eth_capital_pool_prev, 0), 0) as eth_capital_pool_pct,
    -- eth investments
    eth_steth,
    eth_steth_prev,
    eth_steth_sale,
    eth_steth - eth_steth_sale - eth_steth_prev as eth_steth_return,
    coalesce((eth_steth - eth_steth_sale - eth_steth_prev) / nullif(eth_steth_prev, 0), 0) as eth_steth_pct,
    coalesce(power(1 + ((eth_steth - eth_steth_sale - eth_steth_prev) / nullif(eth_steth_prev, 0)), 12) - 1, 0) as eth_steth_apy,
    eth_reth,
    eth_reth_prev,
    eth_reth - eth_reth_prev as eth_reth_return,
    coalesce((eth_reth - eth_reth_prev) / nullif(eth_reth_prev, 0), 0) as eth_reth_pct,
    coalesce(power(1 + ((eth_reth - eth_reth_prev) / nullif(eth_reth_prev, 0)), 12) - 1, 0) as eth_reth_apy,
    eth_nxmty,
    eth_nxmty_prev,
    (eth_nxmty - eth_nxmty_prev) * (1-0.0015) as eth_nxmty_return, -- minus Enzyme fee
    coalesce((eth_nxmty - eth_nxmty_prev) / nullif(eth_nxmty_prev, 0), 0) as eth_nxmty_pct,
    coalesce(power(1 + ((eth_nxmty - eth_nxmty_prev) / nullif(eth_nxmty_prev, 0)), 12) - 1, 0) as eth_nxmty_apy,
    -- aave positions
    eth_aweth,
    eth_aweth_prev,
    eth_aweth_deposit,
    eth_aweth_withdraw,
    eth_aweth - eth_aweth_deposit - eth_aweth_withdraw - eth_aweth_prev as eth_aweth_return,
    coalesce((eth_aweth - eth_aweth_deposit - eth_aweth_withdraw - eth_aweth_prev) / nullif(eth_aweth_prev, 0), 0) as eth_aweth_pct,
    coalesce(power(1 + ((eth_aweth - eth_aweth_deposit - eth_aweth_withdraw - eth_aweth_prev) / nullif(eth_aweth_prev, 0)), 12) - 1, 0) as eth_aweth_apy,
    eth_debt_usdc,
    eth_debt_usdc_prev,
    eth_debt_usdc_borrow,
    eth_debt_usdc_repay,
    eth_debt_usdc - (eth_debt_usdc_prev + eth_debt_usdc_fx_change + eth_debt_usdc_borrow + eth_debt_usdc_repay) as eth_debt_usdc_return,
    coalesce((eth_debt_usdc - (eth_debt_usdc_prev + eth_debt_usdc_fx_change + eth_debt_usdc_borrow + eth_debt_usdc_repay))
     / nullif(eth_debt_usdc_prev, 0), 0) as eth_debt_usdc_pct,
    coalesce(power(1 + ((eth_debt_usdc - (eth_debt_usdc_prev + eth_debt_usdc_fx_change + eth_debt_usdc_borrow + eth_debt_usdc_repay))
     / nullif(eth_debt_usdc_prev, 0)), 12) - 1, 0) as eth_debt_usdc_apy,
    -- fx impact
    eth_dai_fx_change,
    eth_usdc_fx_change,
    eth_cover_re_fx_change,
    eth_debt_usdc_fx_change,
    eth_fx_change,
    -- === USD ===
    usd_capital_pool,
    usd_capital_pool_prev,
    coalesce((usd_capital_pool - usd_capital_pool_prev) / nullif(usd_capital_pool_prev, 0), 0) as usd_capital_pool_pct,
    -- eth investments
    usd_steth,
    usd_steth_prev,
    usd_steth_sale,
    usd_steth - usd_steth_sale - usd_steth_prev as usd_steth_return,
    coalesce((usd_steth - usd_steth_sale - usd_steth_prev) / nullif(usd_steth_prev, 0), 0) as usd_steth_pct,
    coalesce(power(1 + ((usd_steth - usd_steth_sale - usd_steth_prev) / nullif(usd_steth_prev, 0)), 12) - 1, 0) as usd_steth_apy,
    usd_reth,
    usd_reth_prev,
    usd_reth - usd_reth_prev as usd_reth_return,
    coalesce((usd_reth - usd_reth_prev) / nullif(usd_reth_prev, 0), 0) as usd_reth_pct,
    coalesce(power(1 + ((usd_reth - usd_reth_prev) / nullif(usd_reth_prev, 0)), 12) - 1, 0) as usd_reth_apy,
    usd_nxmty,
    usd_nxmty_prev,
    (usd_nxmty - usd_nxmty_prev) * (1-0.0015) as usd_nxmty_return, -- minus Enzyme fee
    coalesce((usd_nxmty - usd_nxmty_prev) / nullif(usd_nxmty_prev, 0), 0) as usd_nxmty_pct,
    coalesce(power(1 + ((usd_nxmty - usd_nxmty_prev) / nullif(usd_nxmty_prev, 0)), 12) - 1, 0) as usd_nxmty_apy,
    -- aave positions
    usd_aweth,
    usd_aweth_prev,
    usd_aweth_deposit,
    usd_aweth_withdraw,
    usd_aweth - usd_aweth_deposit - usd_aweth_withdraw - usd_aweth_prev as usd_aweth_return,
    coalesce((usd_aweth - usd_aweth_deposit - usd_aweth_withdraw - usd_aweth_prev) / nullif(usd_aweth_prev, 0), 0) as usd_aweth_pct,
    coalesce(power(1 + ((usd_aweth - usd_aweth_deposit - usd_aweth_withdraw - usd_aweth_prev) / nullif(usd_aweth_prev, 0)), 12) - 1, 0) as usd_aweth_apy,
    usd_debt_usdc,
    usd_debt_usdc_prev,
    usd_debt_usdc_borrow,
    usd_debt_usdc_repay,
    usd_debt_usdc - (usd_debt_usdc_prev + usd_debt_usdc_borrow + usd_debt_usdc_repay) as usd_debt_usdc_return,
    coalesce((usd_debt_usdc - (usd_debt_usdc_prev + usd_debt_usdc_borrow + usd_debt_usdc_repay))
     / nullif(usd_debt_usdc_prev, 0), 0) as usd_debt_usdc_pct,
    coalesce(power(1 + ((usd_debt_usdc - (usd_debt_usdc_prev + usd_debt_usdc_borrow + usd_debt_usdc_repay))
     / nullif(usd_debt_usdc_prev, 0)), 12) - 1, 0) as usd_debt_usdc_apy,
    -- fx impact
    usd_dai_fx_change,
    usd_usdc_fx_change,
    usd_cover_re_fx_change,
    usd_debt_usdc_fx_change,
    usd_fx_change
  from capital_pool_enriched
),

investment_returns_ext as (
  select
    block_month,
    -- === ETH ===
    -- capital pool total
    eth_capital_pool_prev as eth_capital_pool_start,
    eth_capital_pool as eth_capital_pool_end,
    eth_capital_pool_pct,
    -- individual eth investments
    eth_steth,
    eth_steth_sale,
    eth_steth_return,
    eth_steth_apy,
    eth_reth,
    eth_reth_return,
    eth_reth_apy,
    eth_nxmty,
    eth_nxmty_return,
    eth_nxmty_apy,
    -- aave positions
    eth_aweth,
    eth_aweth_deposit,
    eth_aweth_withdraw,
    eth_aweth_return,
    eth_aweth_apy,
    eth_debt_usdc,
    eth_debt_usdc_borrow,
    eth_debt_usdc_repay,
    eth_debt_usdc_return,
    eth_debt_usdc_apy,
    eth_aweth_return + eth_debt_usdc_return as eth_aave_net_return,
    coalesce(power(1 + ((eth_aweth_return + eth_debt_usdc_return) / nullif(eth_aweth_prev, 0)), 12) - 1, 0) as eth_aave_net_apy,
    -- total eth investment returns
    eth_steth_return + eth_reth_return + eth_nxmty_return + (eth_aweth_return + eth_debt_usdc_return) as eth_inv_returns,
    coalesce(power(1 + ((eth_steth_return + eth_reth_return + eth_nxmty_return + (eth_aweth_return + eth_debt_usdc_return))
    / nullif(((eth_capital_pool_prev + eth_capital_pool) / 2), 0)), 12) - 1, 0) as eth_inv_apy,
    -- fx impact
    eth_dai_fx_change,
    eth_usdc_fx_change,
    eth_cover_re_fx_change,
    eth_debt_usdc_fx_change,
    eth_fx_change,
    -- === USD ===
    -- capital pool total
    usd_capital_pool_prev as usd_capital_pool_start,
    usd_capital_pool as usd_capital_pool_end,
    usd_capital_pool_pct,
    -- individual eth investments
    usd_steth,
    usd_steth_sale,
    usd_steth_return,
    usd_steth_apy,
    usd_reth,
    usd_reth_return,
    usd_reth_apy,
    usd_nxmty,
    usd_nxmty_return,
    usd_nxmty_apy,
    -- aave positions
    usd_aweth,
    usd_aweth_deposit,
    usd_aweth_withdraw,
    usd_aweth_return,
    usd_aweth_apy,
    usd_debt_usdc,
    usd_debt_usdc_borrow,
    usd_debt_usdc_repay,
    usd_debt_usdc_return,
    usd_debt_usdc_apy,
    usd_aweth_return + usd_debt_usdc_return as usd_aave_net_return,
    coalesce(power(1 + ((usd_aweth_return + usd_debt_usdc_return) / nullif(usd_aweth_prev, 0)), 12) - 1, 0) as usd_aave_net_apy,
    -- total eth investment returns
    usd_steth_return + usd_reth_return + usd_nxmty_return + (usd_aweth_return + usd_debt_usdc_return) as usd_inv_returns,
    coalesce(power(1 + ((usd_steth_return + usd_reth_return + usd_nxmty_return + (usd_aweth_return + usd_debt_usdc_return))
    / nullif(((usd_capital_pool_prev + usd_capital_pool) / 2), 0)), 12) - 1, 0) as usd_inv_apy,
    -- fx impact
    usd_dai_fx_change,
    usd_usdc_fx_change,
    usd_cover_re_fx_change,
    usd_debt_usdc_fx_change,
    usd_fx_change
  from investment_returns
)

select
  --date_format(block_month, '%Y-%m') as block_month,
  ir.block_month,
  -- === ETH ===
  -- capital pool total
  ir.eth_capital_pool_start,
  ir.eth_capital_pool_end,
  ir.eth_capital_pool_pct,
  -- individual eth investments
  ir.eth_steth,
  ir.eth_steth_sale,
  ir.eth_steth_return,
  ir.eth_steth_apy,
  ir.eth_reth,
  ir.eth_reth_return,
  ir.eth_reth_apy,
  ir.eth_nxmty,
  ir.eth_nxmty_return,
  ir.eth_nxmty_apy,
  -- aave positions
  ir.eth_aweth,
  ir.eth_aweth_deposit,
  ir.eth_aweth_withdraw,
  ir.eth_aweth_return,
  ir.eth_aweth_apy,
  ir.eth_debt_usdc,
  ir.eth_debt_usdc_borrow,
  ir.eth_debt_usdc_repay,
  ir.eth_debt_usdc_return,
  ir.eth_debt_usdc_apy,
  ir.eth_aave_net_return,
  ir.eth_aave_net_apy,
  -- total eth investment returns
  ir.eth_inv_returns,
  ir.eth_inv_apy,
  -- fx impact
  ir.eth_dai_fx_change,
  ir.eth_usdc_fx_change,
  ir.eth_cover_re_fx_change,
  ir.eth_debt_usdc_fx_change,
  ir.eth_fx_change,
  -- === USD ===
  -- capital pool total
  ir.eth_capital_pool_start * p.eth_usd_price_end as usd_capital_pool_start,
  ir.eth_capital_pool_end * p.eth_usd_price_end as usd_capital_pool_end,
  ir.eth_capital_pool_pct as usd_capital_pool_pct,
  -- individual eth investments
  ir.eth_steth * p.eth_usd_price_end as usd_steth,
  ir.eth_steth_sale * p.eth_usd_price_end as usd_steth_sale,
  ir.eth_steth_return * p.eth_usd_price_end as usd_steth_return,
  ir.eth_steth_apy as usd_steth_apy,
  ir.eth_reth * p.eth_usd_price_end as usd_reth,
  ir.eth_reth_return * p.eth_usd_price_end as usd_reth_return,
  ir.eth_reth_apy as usd_reth_apy,
  ir.eth_nxmty * p.eth_usd_price_end as usd_nxmty,
  ir.eth_nxmty_return * p.eth_usd_price_end as usd_nxmty_return,
  ir.eth_nxmty_apy as usd_nxmty_apy,
  -- aave positions
  ir.eth_aweth * p.eth_usd_price_end as usd_aweth,
  ir.eth_aweth_deposit * p.eth_usd_price_end as usd_aweth_deposit,
  ir.eth_aweth_withdraw * p.eth_usd_price_end as usd_aweth_withdraw,
  ir.eth_aweth_return * p.eth_usd_price_end as usd_aweth_return,
  ir.eth_aweth_apy as usd_aweth_apy,
  ir.eth_debt_usdc * p.eth_usd_price_end as usd_debt_usdc,
  ir.eth_debt_usdc_borrow * p.eth_usd_price_end as usd_debt_usdc_borrow,
  ir.eth_debt_usdc_repay * p.eth_usd_price_end as usd_debt_usdc_repay,
  ir.eth_debt_usdc_return * p.eth_usd_price_end as usd_debt_usdc_return,
  ir.eth_debt_usdc_apy as usd_debt_usdc_apy,
  ir.eth_aave_net_return * p.eth_usd_price_end as usd_aave_net_return,
  ir.eth_aave_net_apy as usd_aave_net_apy,
  -- total eth investment returns
  ir.eth_inv_returns * p.eth_usd_price_end as usd_inv_returns,
  ir.eth_inv_apy as usd_inv_apy,
  -- fx impact
  usd_dai_fx_change,
  usd_usdc_fx_change,
  usd_cover_re_fx_change,
  usd_debt_usdc_fx_change,
  usd_fx_change
from investment_returns_ext ir
  inner join prices_start_end p on ir.block_month = p.block_month
order by 1 desc
