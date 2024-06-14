with

daily_avg_prices as (
  select
    block_date,
    avg_eth_usd_price,
    avg_dai_usd_price,
    avg_nxm_eth_price,
    avg_nxm_usd_price
  from query_3789851 -- prices base (fallback) query
),

covers as (
  select
    c.cover_id,
    c.syndicate as staking_pool,
    date_trunc('day', c.cover_start_time) as cover_start_date,
    date_trunc('day', c.cover_end_time) as cover_end_date,
    c.cover_asset,
    c.sum_assured,
    coalesce(if(cover_asset = 'ETH', c.sum_assured, 0), 0) as eth_cover_amount,
    coalesce(if(cover_asset = 'DAI', c.sum_assured, 0), 0) as dai_cover_amount
    --c.premium_asset,
    --c.premium * if(c.cover_asset = 'DAI', p.avg_dai_usd_price, p.avg_eth_usd_price) as premium_usd,
    --c.premium * if(c.cover_asset = 'DAI', p.avg_dai_usd_price, p.avg_eth_usd_price) / p.avg_eth_usd_price as premium_eth
  from query_3788367 c -- covers v1 base (fallback) query
    inner join daily_avg_prices p on c.block_date = p.block_date
  union all
  select
    c.cover_id,
    c.staking_pool,
    date_trunc('day', c.cover_start_time) as cover_start_date,
    date_trunc('day', c.cover_end_time) as cover_end_date,
    c.cover_asset,
    c.sum_assured,
    coalesce(if(cover_asset = 'ETH', c.sum_assured, 0), 0) as eth_cover_amount,
    coalesce(if(cover_asset = 'DAI', c.sum_assured, 0), 0) as dai_cover_amount
    --c.premium_asset,
    --c.premium_incl_commission * p.avg_nxm_usd_price as premium_usd,
    --c.premium_incl_commission * p.avg_nxm_usd_price / p.avg_eth_usd_price as premium_eth
  from query_3788370 c -- covers v2 base (fallback) query
    inner join daily_avg_prices p on c.block_date = p.block_date
  where c.is_migrated = false
),


  -- Join v2 cover data with RAMM price if there is one, or historical book value if not
  v2_cover_in_native_cover_asset AS (
    SELECT
      NULL AS v1_cover_id,
      cover_id AS v2_cover_id,
      cover_asset,
      sum_assured * partial_cover_amount_in_nxm / total_nxm_in_cover AS sum_assured_partial,
      cover_start_time,
      cover_end_time,
      pool_id as staking_pool,
      product_name,
      product_type
    FROM v2_cover_brought AS t
  ),
  cover_data as (
    SELECT
      v1_cover_id,
      v2_cover_id,
      cover_asset,
      cover_start_time,
      cover_end_time,
      sum_assured_partial AS sum_assured,
      staking_pool,
      product_name,
      product_type
    FROM v2_cover_in_native_cover_asset
    UNION ALL
    SELECT
      v1_cover_id,
      v2_cover_id,
      cover_asset,
      cover_start_time,
      cover_end_time,
      sum_assured,
      syndicate as staking_pool,
      product_name,
      product_type
    FROM v1_migrated_cover
  ),
  cover as (
    select
      1 AS anchor,
      cover_start_time,
      cover_end_time,
      v1_cover_id,
      v2_cover_id,
      staking_pool,
      product_name,
      product_type,
      cover_asset,
      sum_assured
    from cover_data
    WHERE cover_end_time >= NOW()
  ),
  running_total_separated as (
    SELECT
      v1_cover_id,
      v2_cover_id,
      cover_start_time,
      cover_end_time,
      staking_pool,
      cover_asset,
      product_name,
      product_type,
      case
        when '{{display_currency}}' = 'USD' AND cover_asset = 'ETH' then eth_price_dollar * CAST(sum_assured AS DOUBLE)
        when '{{display_currency}}' = 'ETH' AND cover_asset = 'ETH' then CAST(sum_assured AS DOUBLE)
        when '{{display_currency}}' = 'USD' AND cover_asset = 'DAI' then dai_price_dollar * CAST(sum_assured AS DOUBLE)
        when '{{display_currency}}' = 'ETH' AND cover_asset = 'DAI' then CAST(sum_assured AS DOUBLE) * dai_price_dollar / eth_price_dollar
        ELSE 0
      END as running_net_display_curr
    FROM cover as t
      LEFT JOIN (
        SELECT
          1 AS anchor,
          price_dollar AS dai_price_dollar
        FROM day_prices
        WHERE symbol = 'DAI'
        ORDER BY day DESC
        LIMIT 1
      ) AS v ON v.anchor = t.anchor
      LEFT JOIN (
        SELECT
          1 AS anchor,
          price_dollar AS eth_price_dollar
        FROM day_prices
        WHERE symbol = 'ETH'
        ORDER BY day DESC
        LIMIT 1
      ) AS u ON u.anchor = t.anchor
  )
SELECT
  *,
  SUM(running_net_display_curr) OVER () as net_total_display_currency,
  SUM(running_net_display_curr) OVER (partition by product_name) as net_total_product_name_currency
FROM running_total_separated
