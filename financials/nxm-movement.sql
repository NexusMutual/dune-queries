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
  where scope = 'nm'
),

nxm_movement as (
  select
    nxm_supply_start,
    nxm_supply_end,
    nxm_cover_buy_burn,
    nxm_reward_mint,
    nxm_claim_burn,
    nxm_assessor_rewards,
    nxm_gov_rewards,
    nxm_contribution,
    nxm_withdrawal
  from query_4911759 -- nxm movement
  where block_month in (select report_month from params)
)

select
  i.label_tab,
  case i.label
    when 'NXM Movement' then null
    when 'Opening NXM' then coalesce(nullif(nxm_supply_start, 0), 1e-6)
    when 'NXM Burned from Cover Purchases' then coalesce(nullif(nxm_cover_buy_burn, 0), 1e-6)
    when 'NXM Burned from Claims' then coalesce(nullif(nxm_claim_burn, 0), 1e-6)
    when 'NXM Minted as Rewards' then nxm_reward_mint + nxm_assessor_rewards + nxm_gov_rewards
    when 'NXM Minted from Contributions' then coalesce(nullif(nxm_contribution, 0), 1e-6)
    when 'NXM Burned from Withdrawals' then coalesce(nullif(nxm_withdrawal, 0), 1e-6)
    when 'Closing NXM' then coalesce(nullif(nxm_supply_end, 0), 1e-6)
  end as nxm_val
from nxm_movement nm
  cross join items i
order by i.fi_id
