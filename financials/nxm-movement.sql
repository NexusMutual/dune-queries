with

params as (
  select cast(case '{{month}}'
      when 'current MTD ⏳' then cast(date_trunc('month', current_date) as varchar)
      when 'last month' then cast(date_add('month', -1, date_trunc('month', current_date)) as varchar)
      else '{{month}}'
    end as timestamp) as report_month
),

items as (
  select fi_id, label, label_tab
  from query_4832890 -- fin items
  where scope = 'bs'
),

nxm_movement as (
  select
    eth_nxm_supply_start,
    eth_nxm_supply_end,
    eth_nxm_cover_buy_burn,
    eth_nxm_rewards_mint,
    eth_claim_paid,
    eth_nxm_assessor_rewards,
    eth_nxm_gov_rewards,
    eth_nxm_in,
    eth_nxm_out,
    -- USD
    usd_nxm_supply_start,
    usd_nxm_supply_end,
    usd_nxm_cover_buy_burn,
    usd_nxm_rewards_mint,
    usd_claim_paid,
    usd_nxm_assessor_rewards,
    usd_nxm_gov_rewards,
    usd_nxm_in,
    usd_nxm_out
  from query_4911759 -- nxm movement
  where block_month in (select report_month from params)
)

select
  i.label_tab,
  case i.label
    when 'NXM Movement' then null
    when 'Opening NXM' then coalesce(nullif(eth_nxm_supply_start, 0), 1e-6)
    when 'NXM Burned from Cover Purchases' then coalesce(nullif(eth_nxm_cover_buy_burn, 0), 1e-6)
    when 'NXM Burned from Claims' then coalesce(nullif(eth_claim_paid, 0), 1e-6)
    when 'NXM Minted as Rewards (from staking, claims and governance)' then eth_nxm_rewards_mint + eth_nxm_assessor_rewards + eth_nxm_gov_rewards
    when 'NXM Minted from Contributions' then coalesce(nullif(eth_nxm_in, 0), 1e-6)
    when 'NXM Burned from Withdrawals' then coalesce(nullif(eth_nxm_out, 0), 1e-6)
    when 'Closing NXM' then coalesce(nullif(eth_nxm_supply_end, 0), 1e-6)
  end as eth_val,
  case i.label
    when 'NXM Movement' then null
    when 'Opening NXM' then coalesce(nullif(usd_nxm_supply_start, 0), 1e-6)
    when 'NXM Burned from Cover Purchases' then coalesce(nullif(usd_nxm_cover_buy_burn, 0), 1e-6)
    when 'NXM Burned from Claims' then coalesce(nullif(usd_claim_paid, 0), 1e-6)
    when 'NXM Minted as Rewards (from staking, claims and governance)' then usd_nxm_rewards_mint + usd_nxm_assessor_rewards + usd_nxm_gov_rewards
    when 'NXM Minted from Contributions' then coalesce(nullif(usd_nxm_in, 0), 1e-6)
    when 'NXM Burned from Withdrawals' then coalesce(nullif(usd_nxm_out, 0), 1e-6)
    when 'Closing NXM' then coalesce(nullif(usd_nxm_supply_end, 0), 1e-6)
  end as usd_val
from nxm_movement nm
  cross join items i
order by i.fi_id
