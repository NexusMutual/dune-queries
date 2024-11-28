with

sample_cover as (
  select distinct
    cover_id, cover_start_date, cover_end_date, cover_asset, sum_assured, cover_owner, cover_ipfs_data
  from query_3788370
  where cover_ipfs_data <> ''
  order by 1 desc
  limit 10
),

sample_cover_ipfs_data as (
  select
    cover_id, cover_start_date, cover_end_date, cover_asset, sum_assured, cover_owner,
    http_get(concat('https://api.nexusmutual.io/ipfs/', cover_ipfs_data)) as cover_data
  from sample_cover
)

select
  cover_id,
  value,
  case
    when json_extract_scalar(cover_data, '$.walletAddress') is not null then json_extract_scalar(cover_data, '$.walletAddress')
    else json_extract_scalar(value, '$.Wallet')
  end as wallet,
  cover_data
from (
  select
    cover_id,
    cover_data,
    case
      when try(json_array_length(json_parse(cover_data))) is not null then
        sequence(1, json_array_length(json_parse(cover_data)))
      else
        sequence(1, 1) -- for single JSON object, wrap it into a sequence of one element
    end as idx,
    json_parse(cover_data) as parsed_data
  from sample_cover_ipfs_data
) t
cross join unnest(idx) as u (index)
cross join lateral (
  select json_array_get(parsed_data, index - 1) as value
) l
