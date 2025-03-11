/*
with

waterfall_base as (
  select
    ir.capital_pool_start as cp_start,
    pc.eth_premiums_minus_claims as premiums,
    ir.eth_inv_returns as inv_returns,
    ir.fx_change,
    -2806 as ramm_net, -- TODO
    ir.capital_pool_end as cp_end
  --from query_4770697 ir -- investment returns
  --  inner join query_4836553 pc -- premiums - claims
  -- bc of: -- Error: This query has too many stages and is too complex to execute at once. ...
  from dune.nexus_mutual.result_investment_returns ir
    inner join dune.nexus_mutual.result_premiums_claims pc
      on ir.block_month = pc.cover_month
  where ir.block_month = timestamp '2025-02-01'
),

waterfall_base_ext as (
  select
    -- refs
    if(premiums > 0, premiums, 0) + if(inv_returns > 0, inv_returns, 0) + if(fx_change > 0, fx_change, 0) + if(ramm_net > 0, ramm_net, 0) as up_tick,
    if(premiums < 0, premiums, 0) + if(inv_returns < 0, inv_returns, 0) + if(fx_change < 0, fx_change, 0) + if(ramm_net < 0, ramm_net, 0) as down_tick,
    premiums + inv_returns + fx_change + ramm_net + (cp_start - 2000) as net_diff, -- 2000: arbitrary starting bar size
    -- totals adjusted
    cp_start,
    premiums,
    inv_returns,
    fx_change,
    ramm_net,
    cp_end
  from waterfall_base
),

waterfall_items (id, item, amount, prop_amount) as (
  select 1, 'opening', cp_start - net_diff, 0 from waterfall_base_ext union all
  select 2, 'cover fee', premiums, cp_start - net_diff from waterfall_base_ext union all
  select 3, 'inv returns', inv_returns, cp_start - net_diff from waterfall_base_ext union all
  select 4, 'stablecoin impact', fx_change, cp_start - net_diff from waterfall_base_ext union all
  select 5, 'ramm net redemptions', ramm_net, cp_start - net_diff from waterfall_base_ext union all
  select 6, 'closing', cp_end - net_diff, 0 from waterfall_base_ext
)

select id, item, amount, prop_amount from waterfall_items order by 1
*/

-- possible but looks crap..
with

waterfall_items (id, item, amount, prop_amount) as (
  select 1, 'opening', 7242.6, 0 union all
  select 2, 'premiums', 22.9, 7242.6 union all
  select 3, 'investments', 17.5, 7242.6 + (22.9) union all
  select 4, 'fx impact', 140.1, 7242.6 + (22.9 + 17.5) union all
  select 5, 'ramm net', 280.6, 7423.1 - 280.6 union all
  select 6, 'closing', 7144.5, 0
)

select id, item, amount, prop_amount from waterfall_items order by 1
