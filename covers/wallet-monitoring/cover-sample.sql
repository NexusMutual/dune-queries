with

cover as (
  select distinct
    cover_id, cover_start_date, cover_end_date, cover_asset, sum_assured, cover_owner, cover_ipfs_data
  from query_3788370
  where cover_ipfs_data <> ''
  order by 1 desc
  limit 10 -- sample data for now
),

cover_ipfs_data as (
  select
    cover_id, cover_start_date, cover_end_date, cover_asset, sum_assured, cover_owner,
    http_get(concat('https://api.nexusmutual.io/ipfs/', cover_ipfs_data)) as cover_data
  from cover
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
)

select
  cover_id,
  cover_start_date,
  cover_end_date,
  cover_asset,
  sum_assured,
  cover_owner,
  coalesce(json_extract_scalar(cover_data, '$.walletAddress'), json_extract_scalar(json_array_element, '$.Wallet')) as cover_data_address
from cover_ipfs_data_ext c
  cross join unnest(idx) as u(id)
  cross join lateral (
    select json_array_get(cover_data, id - 1) as json_array_element
  ) l
