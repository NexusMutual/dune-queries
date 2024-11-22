with

cover_details as (
  select
    cde.evt_block_time as block_time,
    cde.evt_block_number as block_number,
    cde.cid as cover_id,
    cde.evt_block_time as cover_start_time,
    from_unixtime(cde.expiry) as cover_end_time,
    cde.scAdd as product_contract,
    cast(cde.sumAssured as double) as sum_assured,
    case
      when ct.call_success or cm.payWithNXM then 'NXM'
      when cde.curr = 0x45544800 then 'ETH'
      when cde.curr = 0x44414900 then 'DAI'
    end as premium_asset,
    cde.premium / 1e18 as premium,
    case
      when cde.curr = 0x45544800 then 'ETH'
      when cde.curr = 0x44414900 then 'DAI'
    end as cover_asset,
    cde.premiumNXM / 1e18 as premium_nxm,
    ac._userAddress as cover_owner,
    cde.evt_index,
    cde.evt_tx_hash as tx_hash
  from nexusmutual_ethereum.QuotationData_evt_CoverDetailsEvent cde
    left join nexusmutual_ethereum.Quotation_call_buyCoverWithMetadata cm
      on cde.evt_tx_hash = cm.call_tx_hash and cde.evt_block_number = cm.call_block_number and cm.call_success
    left join nexusmutual_ethereum.Quotation_call_makeCoverUsingNXMTokens ct
      on cde.evt_tx_hash = ct.call_tx_hash and cde.evt_block_number = ct.call_block_number and ct.call_success
    left join nexusmutual_ethereum.QuotationData_call_addCover ac
      on cde.evt_tx_hash = ac.call_tx_hash and cde.evt_block_number = ac.call_block_number and ac.call_success
)

select
  cd.block_time,
  date_trunc('day', cd.block_time) as block_date,
  cd.block_number,
  cd.cover_id,
  cd.cover_start_time,
  cd.cover_end_time,
  date_trunc('day', cd.cover_start_time) as cover_start_date,
  date_trunc('day', cd.cover_end_time) as cover_end_date,
  cd.product_contract,
  'v1' as syndicate,
  cast(null as int) as product_id,
  coalesce(pi.product_name, 'unknown') as product_name,
  coalesce(pi.product_type, 'unknown') as product_type,
  cd.sum_assured,
  cd.premium_asset,
  cd.premium,
  cd.cover_asset,
  cd.premium_nxm,
  cd.cover_owner,
  cd.evt_index,
  cd.tx_hash
from cover_details cd
  left join nexusmutual_ethereum.products_v1 pi on cd.product_contract = pi.product_contract_address
--order by cd.cover_id desc
