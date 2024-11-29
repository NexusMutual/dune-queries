with

cover_base as (
  select distinct
    cover_id, cover_start_date, cover_end_date, cover_asset, sum_assured, cover_owner, cover_ipfs_data
  from query_3788370 -- covers v2 - base
  where cover_ipfs_data <> ''
    and cover_id in (
      1572, -- Entry sample(s)
      1559, 1566, -- Essential sample(s)
      1575 -- Elite sample(s)
    )
  --order by 1 desc
  --limit 20 -- sample data for now
),

cover_ipfs_data as (
  select
    cover_id, cover_start_date, cover_end_date, cover_asset, sum_assured, cover_owner,
    http_get(concat('https://api.nexusmutual.io/ipfs/', cover_ipfs_data)) as cover_data
  from cover_base
),

cover_ipfs_data_ext as (
  select
    cover_id, cover_start_date, cover_end_date, cover_asset, sum_assured, cover_owner,
    case
      when try(json_array_length(json_parse(cover_data))) is not null then
        sequence(1, json_array_length(json_parse(cover_data)))
      else
        sequence(1, 1) -- for single JSON object, wrap it into a sequence of one element
    end as idx,
    json_parse(cover_data) as cover_data
  from cover_ipfs_data
),

cover as (
  select
    cover_id,
    cover_start_date,
    cover_end_date,
    cover_asset,
    sum_assured,
    cover_owner,
    try(from_hex(coalesce(
      nullif(json_extract_scalar(cover_data, '$.walletAddress'), ''), 
      nullif(json_extract_scalar(json_array_element, '$.Wallet'), '')
    ))) as cover_data_address
  from cover_ipfs_data_ext c
    cross join unnest(idx) as u(id)
    cross join lateral (
      select json_array_get(cover_data, id - 1) as json_array_element
    ) l
)

select
  cover_id,
  cover_start_date,
  cover_end_date,
  cover_asset,
  sum_assured,
  case
    when sum_assured < 200000 then 'Entry'
    when sum_assured < 1000000 then 'Essential'
    else 'Elite'
  end as plan,
  cover_owner,
  coalesce(cover_data_address, cover_owner) as monitored_wallet
from cover
