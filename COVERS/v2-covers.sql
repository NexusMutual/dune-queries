WITH
  product_info AS (
    select DISTINCT
      product_contract_address AS product_address,
      syndicate AS syndicate,
      product_name AS product_name,
      product_type AS product_type,
      output_0 AS v2_product_id
    from
      nexusmutual_ethereum.product_information AS t
      FULL JOIN nexusmutual_ethereum.ProductsV1_call_getNewProductId AS s ON t.product_contract_address = s.legacyProductId
    WHERE
      output_0 IS NOT NULL
      and call_success
  ),
  v1_cover_details AS (
    select
      evt_block_time AS cover_start_time,
      cid AS v1_cover_id,
      premium * 1E-18 AS premium,
      premiumNXM * 1E-18 AS premium_nxm,
      scAdd AS productContract,
      CAST(sumAssured AS DOUBLE) AS sum_assured,
      syndicate,
      product_name,
      product_type,
      CASE
        WHEN b.call_success THEN 'NXM'
        WHEN curr = 0x45544800 THEN 'ETH'
        WHEN curr = 0x44414900 THEN 'DAI'
      END AS premium_asset,
      case
        when curr = 0x45544800 then 'ETH'
        when curr = 0x44414900 then 'DAI'
      end AS cover_asset,
      from_unixtime(CAST(expiry AS DOUBLE)) AS cover_end_time,
      evt_tx_hash AS call_tx_hash
    from
      nexusmutual_ethereum.QuotationData_evt_CoverDetailsEvent AS t
      LEFT JOIN product_info ON product_info.product_address = t.scAdd
      FULL JOIN nexusmutual_ethereum.Quotation_call_makeCoverUsingNXMTokens AS b ON t.evt_tx_hash = b.call_tx_hash
    where
      (
        b.call_success
        and b.call_tx_hash is not null
      )
      or b.call_tx_hash is null
  ),
  v1_migrated_cover AS (
    SELECT
      cover_start_time,
      cover_end_time,
      premium,
      premium_nxm,
      coverIdV2 AS cover_id,
      sum_assured,
      syndicate,
      product_name,
      product_type,
      cover_asset,
      premium_asset,
      newOwner AS owner,
      call_tx_hash
    FROM
      nexusmutual_ethereum.CoverMigrator_evt_CoverMigrated AS t
      INNER JOIN v1_cover_details ON v1_cover_details.v1_cover_id = t.coverIdV1
  ),
  product_data AS (
    SELECT DISTINCT
      call_block_time,
      productParams
    FROM
      nexusmutual_ethereum.Cover_call_setProducts
    WHERE
      call_success
      AND contract_address = 0xcafeac0ff5da0a2777d915531bfa6b29d282ee62
    ORDER BY
      call_block_time
  ),
  product_set_raw AS (
    SELECT
      call_block_time,
      JSON_QUERY(
        productParamsOpened,
        'lax $.productName' OMIT QUOTES
      ) AS product_name,
      CAST(
        JSON_QUERY(
          JSON_QUERY(productParamsOpened, 'lax $.product' OMIT QUOTES),
          'lax $.productType'
        ) AS UINT256
      ) AS product_type_id,
      array_position(t.productParams, productParamsOpened) AS array_order
    FROM
      product_data AS t
      CROSS JOIN UNNEST (t.productParams) AS t (productParamsOpened)
    WHERE
      CAST(
        JSON_QUERY(productParamsOpened, 'lax $.productId') AS UINT256
      ) > CAST(1000000000000 AS UINT256)
  ),
  product_set AS (
    SELECT
      *,
      RANK() OVER (
        ORDER BY
          call_block_time ASC,
          array_order ASC
      ) - 1 AS product_id
    FROM
      product_set_raw
  ),
  product_type_data AS (
    SELECT DISTINCT
      call_block_time,
      productTypeParams
    FROM
      nexusmutual_ethereum.Cover_call_setProductTypes
    WHERE
      call_success
    ORDER BY
      call_block_time
  ),
  raw_product_types AS (
    SELECT
      *,
      CAST(
        JSON_QUERY(
          productTypeOpened,
          'lax $.productTypeName' OMIT QUOTES
        ) AS VARCHAR
      ) AS product_type_name,
      CAST(
        JSON_QUERY(
          productTypeOpened,
          'lax $.productTypeId' OMIT QUOTES
        ) AS VARCHAR
      ) AS product_type_id_input,
      array_position(t.productTypeParams, productTypeOpened) AS array_order
    FROM
      product_type_data AS t
      CROSS JOIN UNNEST (t.productTypeParams) AS t (productTypeOpened)
  ),
  product_types AS (
    SELECT
      call_block_time,
      CAST(
        RANK() OVER (
          ORDER BY
            call_block_time ASC,
            array_order ASC
        ) - 1 AS UINT256
      ) AS product_type_id,
      product_type_name
    FROM
      raw_product_types
    WHERE
      length(product_type_name) > 0
      AND CAST(product_type_id_input AS UINT256) > CAST(1000000 AS UINT256)
  ),
  v2_products AS (
    SELECT
      product_id,
      product_name,
      a.product_type_id,
      product_type_name
    FROM
      product_set AS a
      LEFT JOIN product_types AS b ON a.product_type_id = b.product_type_id
    ORDER BY
      product_id
  ),
  v2_cover_brought AS (
    SELECT
      CAST(t.call_block_time AS TIMESTAMP) AS cover_start_time,
      CAST(output_coverId AS UINT256) AS cover_id,
      CAST(coverAmount AS DOUBLE) / 100.0 AS partial_cover_amount_in_nxm,
      CAST(output_premium AS DOUBLE) * 1E-18 AS premium_pool,
      CAST(output_premium AS DOUBLE) * 1E-18 AS premium_nxm_pool,
      (
        1.0 + (
          CAST(
            JSON_QUERY(params, 'lax $.commissionRatio') AS DOUBLE
          ) / 10000.0
        )
      ) * CAST(output_premium AS DOUBLE) * 1E-18 AS premium,
      (
        1.0 + (
          CAST(
            JSON_QUERY(params, 'lax $.commissionRatio') AS DOUBLE
          ) / 10000.0
        )
      ) * CAST(output_premium AS DOUBLE) * 1E-18 AS premium_nxm,
      CAST(JSON_QUERY(params, 'lax $.productId') AS UINT256) AS product_id,
      CASE
        WHEN CAST(JSON_QUERY(params, 'lax $.coverAsset') AS INT) = 0 THEN 'ETH'
        WHEN CAST(JSON_QUERY(params, 'lax $.coverAsset') AS INT) = 1 THEN 'DAI'
        ELSE 'NA'
      END AS cover_asset,
      CASE
        WHEN CAST(JSON_QUERY(params, 'lax $.paymentAsset') AS INT) = 0 THEN 'ETH'
        WHEN CAST(JSON_QUERY(params, 'lax $.paymentAsset') AS INT) = 1 THEN 'DAI'
        WHEN CAST(JSON_QUERY(params, 'lax $.paymentAsset') AS INT) = 255 THEN 'NXM'
        ELSE 'NA'
      END AS premium_asset,
      CAST(JSON_QUERY(params, 'lax $.amount') AS DOUBLE) * 1E-18 AS sum_assured,
      CAST(JSON_QUERY(params, 'lax $.period') AS BIGINT) AS expiry,
      date_add(
        'second',
        CAST(JSON_QUERY(params, 'lax $.period') AS BIGINT),
        CAST(t.call_block_time AS TIMESTAMP)
      ) AS cover_end_time,
      CAST(
        JSON_QUERY(params, 'lax $.commissionDestination') AS VARBINARY
      ) AS commission_destination,
      CAST(
        JSON_QUERY(params, 'lax $.commissionRatio') AS DOUBLE
      ) / 10000.0 AS commission_ratio,
      1.0 + (
        CAST(
          JSON_QUERY(params, 'lax $.commissionRatio') AS DOUBLE
        ) / 10000.0
      ) AS cover_fee_multiplier,
      CAST(u.poolId AS VARCHAR) AS pool_id,
      product_name,
      product_type_name AS product_type,
      params,
      from_hex(
        trim(
          '"'
          FROM
            CAST(JSON_QUERY(params, 'lax $.owner') AS VARCHAR)
        )
      ) AS owner,
      t.call_tx_hash
    FROM
      nexusmutual_ethereum.Cover_call_buyCover AS t
      LEFT JOIN v2_products AS s ON CAST(s.product_id AS UINT256) = CAST(
        JSON_QUERY(t.params, 'lax $.productId') AS UINT256
      )
      INNER JOIN nexusmutual_ethereum.StakingProducts_call_getPremium AS u ON t.call_tx_hash = u.call_tx_hash
    WHERE
      t.call_success
      AND u.call_success
      AND u.contract_address = 0xcafea573fbd815b5f59e8049e71e554bde3477e4
      AND (
        t.call_trace_address IS NULL
        OR SLICE(
          u.call_trace_address,
          1,
          cardinality(t.call_trace_address)
        ) = t.call_trace_address
      )
  ),
  covers AS (
    SELECT
      cover_id,
      cover_asset,
      premium_asset,
      premium,
      premium_nxm,
      cover_start_time,
      cover_end_time,
      sum_assured,
      partial_cover_amount_in_nxm,
      pool_id AS syndicate,
      product_name,
      product_type,
      owner,
      call_tx_hash
    FROM
      v2_cover_brought
    UNION
    SELECT
      cover_id,
      cover_asset,
      premium_asset,
      premium,
      premium_nxm,
      cover_start_time,
      cover_end_time,
      sum_assured,
      sum_assured AS partial_cover_amount_in_nxm, -- No partial covers in v1 migrated covers
      syndicate AS staking_pool,
      product_name,
      product_type,
      owner,
      call_tx_hash
    FROM
      v1_migrated_cover
    ORDER BY
      cover_id
  ),
  eth_daily_transactions_fix AS (
    select distinct
      date_trunc('day', block_time) as day,
      SUM(
        CASE
          WHEN "to" IN (
            0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8,
            0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb,
            0xcafea35ce5a2fc4ced4464da4349f81a122fd12b,
            0xcafea8321b5109d22c53ac019d7a449c947701fb,
            0xfd61352232157815cf7b71045557192bf0ce1884,
            0x7cbe5682be6b648cc1100c76d4f6c96997f753d6,
            0xcafea112Db32436c2390F5EC988f3aDB96870627,
            0xcafeaBED7e0653aFe9674A3ad862b78DB3F36e60
          ) THEN CAST(value AS DOUBLE) * 1E-18
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          date_trunc('day', block_time)
      ) as eth_ingress,
      SUM(
        CASE
          WHEN "from" IN (
            0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8,
            0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb,
            0xcafea35ce5a2fc4ced4464da4349f81a122fd12b,
            0xcafea8321b5109d22c53ac019d7a449c947701fb,
            0xfd61352232157815cf7b71045557192bf0ce1884,
            0x7cbe5682be6b648cc1100c76d4f6c96997f753d6,
            0xcafea112Db32436c2390F5EC988f3aDB96870627,
            0xcafeaBED7e0653aFe9674A3ad862b78DB3F36e60
          ) THEN CAST(value AS DOUBLE) * 1E-18
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          date_trunc('day', block_time)
      ) as eth_egress
    from
      ethereum.traces
    where
      success = true
      AND block_time > CAST('2019-01-01 00:00:00' AS TIMESTAMP)
      AND (
        "to" IN (
          0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8,
          0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb,
          0xcafea35ce5a2fc4ced4464da4349f81a122fd12b,
          0xcafea8321b5109d22c53ac019d7a449c947701fb,
          0xfd61352232157815cf7b71045557192bf0ce1884,
          0x7cbe5682be6b648cc1100c76d4f6c96997f753d6,
          0xcafea112Db32436c2390F5EC988f3aDB96870627,
          0xcafeaBED7e0653aFe9674A3ad862b78DB3F36e60
        )
        OR "from" IN (
          0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8,
          0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb,
          0xcafea35ce5a2fc4ced4464da4349f81a122fd12b,
          0xcafea8321b5109d22c53ac019d7a449c947701fb,
          0xfd61352232157815cf7b71045557192bf0ce1884,
          0x7cbe5682be6b648cc1100c76d4f6c96997f753d6,
          0xcafea112Db32436c2390F5EC988f3aDB96870627,
          0xcafeaBED7e0653aFe9674A3ad862b78DB3F36e60
        )
      )
      AND NOT (
        (
          "to" = 0xcafea35ce5a2fc4ced4464da4349f81a122fd12b
          AND "from" = 0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8
        )
        OR (
          "to" = 0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8
          AND "from" = 0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb
        )
        OR (
          "to" = 0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8
          AND "from" = 0xfd61352232157815cf7b71045557192bf0ce1884
        )
      )
  ),
  eth_daily_transactions AS (
    SELECT
      day,
      eth_ingress,
      eth_egress,
      eth_ingress - eth_egress AS net_eth
    FROM
      --nexusmutual_ethereum.eth_daily_transactions
      eth_daily_transactions_fix
  ),
  labels AS (
    SELECT
      name,
      CAST(address AS varbinary) AS contract_address
    FROM
      labels.all
    WHERE
      name IN (
        'Maker: dai',
        'Lido: steth',
        'Rocketpool: RocketTokenRETH'
      )
  ),
  erc_transactions AS (
    SELECT
      name,
      CAST(a.contract_address AS varbinary) AS contract_address,
      DATE_TRUNC('day', evt_block_time) AS day,
      CASE
        WHEN "to" IN (
          0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8,
          0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb,
          0xcafea35ce5a2fc4ced4464da4349f81a122fd12b,
          0xcafea8321b5109d22c53ac019d7a449c947701fb,
          0xfd61352232157815cf7b71045557192bf0ce1884,
          0x7cbe5682be6b648cc1100c76d4f6c96997f753d6,
          0xcafea112Db32436c2390F5EC988f3aDB96870627,
          0xcafeaBED7e0653aFe9674A3ad862b78DB3F36e60
        ) THEN CAST(value AS DOUBLE) * 1E-18
        ELSE 0
      END AS ingress,
      CASE
        WHEN "from" IN (
          0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8,
          0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb,
          0xcafea35ce5a2fc4ced4464da4349f81a122fd12b,
          0xcafea8321b5109d22c53ac019d7a449c947701fb,
          0xfd61352232157815cf7b71045557192bf0ce1884,
          0x7cbe5682be6b648cc1100c76d4f6c96997f753d6,
          0xcafea112Db32436c2390F5EC988f3aDB96870627,
          0xcafeaBED7e0653aFe9674A3ad862b78DB3F36e60
        ) THEN CAST(value AS DOUBLE) * 1E-18
        ELSE 0
      END AS egress
    FROM
      erc20_ethereum.evt_Transfer AS a
      LEFT JOIN labels ON a.contract_address = labels.contract_address
    WHERE
      (
        "to" IN (
          0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8,
          0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb,
          0xcafea35ce5a2fc4ced4464da4349f81a122fd12b,
          0xcafea8321b5109d22c53ac019d7a449c947701fb,
          0xfd61352232157815cf7b71045557192bf0ce1884,
          0x7cbe5682be6b648cc1100c76d4f6c96997f753d6,
          0xcafea112Db32436c2390F5EC988f3aDB96870627,
          0xcafeaBED7e0653aFe9674A3ad862b78DB3F36e60
        )
        OR "from" IN (
          0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8,
          0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb,
          0xcafea35ce5a2fc4ced4464da4349f81a122fd12b,
          0xcafea8321b5109d22c53ac019d7a449c947701fb,
          0xfd61352232157815cf7b71045557192bf0ce1884,
          0x7cbe5682be6b648cc1100c76d4f6c96997f753d6,
          0xcafea112Db32436c2390F5EC988f3aDB96870627,
          0xcafeaBED7e0653aFe9674A3ad862b78DB3F36e60
        )
      )
      AND evt_block_time > CAST('2019-01-01 00:00:00' AS TIMESTAMP)
      AND (
        name IN (
          'Maker: dai',
          'Lido: steth',
          'Rocketpool: RocketTokenRETH'
        )
        OR CAST(a.contract_address AS varbinary) = 0x27f23c710dd3d878fe9393d93465fed1302f2ebd /* nxmty */
      )
      AND NOT (
        (
          "to" = 0xcafea35ce5a2fc4ced4464da4349f81a122fd12b
          AND "from" = 0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8
        )
        OR (
          "to" = 0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8
          AND "from" = 0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb
        )
        OR (
          "to" = 0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8
          AND "from" = 0xfd61352232157815cf7b71045557192bf0ce1884
        )
      )
  ),
  dai_transactions AS (
    SELECT DISTINCT
      day,
      SUM(ingress) OVER (
        PARTITION BY
          day
      ) AS dai_ingress,
      SUM(egress) OVER (
        PARTITION BY
          day
      ) AS dai_egress,
      SUM(ingress - egress) OVER (
        PARTITION BY
          day
      ) AS dai_net_total
    FROM
      erc_transactions
    WHERE
      name = 'Maker: dai'
  ),
  rocket_pool_transactions AS (
    SELECT DISTINCT
      day,
      SUM(ingress - egress) OVER (
        PARTITION BY
          day
      ) AS rpl_net_total
    FROM
      erc_transactions
    WHERE
      name = 'Rocketpool: RocketTokenRETH'
  ),
  lido AS (
    SELECT
      1 AS anchor,
      DATE_TRUNC('day', evt_block_time) AS day,
      CAST(postTotalPooledEther AS DOUBLE) / CAST(totalShares AS DOUBLE) AS rebase
    FROM
      lido_ethereum.LegacyOracle_evt_PostTotalShares
    WHERE
      evt_block_time > CAST('2021-05-26' AS TIMESTAMP)
  ),
  lido_staking_net_steth AS (
    SELECT DISTINCT
      1 AS anchor,
      lido.day AS day,
      ingress,
      egress,
      ingress - egress AS steth_amount,
      rebase AS rebase2
    FROM
      lido
      INNER JOIN erc_transactions ON erc_transactions.day = lido.day
      AND erc_transactions.name = 'Lido: steth'
  ),
  expanded_rebase_steth AS (
    SELECT
      lido.day AS day,
      steth_amount,
      lido.rebase AS rebase,
      lido_staking_net_steth.rebase2 AS rebase2
    FROM
      lido_staking_net_steth
      FULL JOIN lido ON lido.anchor = lido_staking_net_steth.anchor
      AND lido_staking_net_steth.day <= lido.day
    ORDER BY
      lido.day DESC
  ),
  steth AS (
    SELECT DISTINCT
      day,
      SUM(
        steth_amount * CAST(rebase AS DOUBLE) / CAST(rebase2 AS DOUBLE)
      ) OVER (
        PARTITION BY
          day
      ) AS lido_ingress
    FROM
      expanded_rebase_steth
  ),
  weth_nxmty_transactions AS (
    SELECT DISTINCT
      day,
      SUM(ingress) OVER (
        PARTITION BY
          day
      ),
      SUM(egress) OVER (
        PARTITION BY
          day
      ),
      SUM(ingress - egress) OVER (
        PARTITION BY
          day
      ) AS value
    from
      erc_transactions
    where
      erc_transactions.contract_address = 0x27f23c710dd3d878fe9393d93465fed1302f2ebd
  ),
  chainlink_oracle_nxmty_price AS (
    SELECT
      date_trunc('day', evt_block_time) AS day,
      CAST(answer AS double) / 1e18 AS nxmty_price
    FROM
      chainlink_ethereum.AccessControlledOffchainAggregator_evt_NewTransmission
    WHERE
      contract_address = 0xca71bbe491079e138927f3f0ab448ae8782d1dca
      AND evt_block_time > CAST('2022-08-15 00:00:00' AS TIMESTAMP)
  ),
  nxmty AS (
    SELECT
      chainlink_oracle_nxmty_price.day,
      nxmty_price,
      COALESCE(value, 0) AS net_enzyme
    FROM
      chainlink_oracle_nxmty_price
      FULL JOIN weth_nxmty_transactions ON weth_nxmty_transactions.day = chainlink_oracle_nxmty_price.day
  ),
  day_prices AS (
    SELECT DISTINCT
      date_trunc('day', minute) AS day,
      symbol,
      AVG(price) OVER (
        PARTITION BY
          date_trunc('day', minute),
          symbol
      ) AS price_dollar
    FROM prices.usd
    WHERE symbol IN ('DAI', 'ETH', 'rETH')
      AND coalesce(blockchain, 'ethereum') = 'ethereum'
      AND minute > CAST('2019-05-23' AS TIMESTAMP)
  ),
  eth_day_prices AS (
    SELECT
      day,
      price_dollar AS eth_price_dollar
    FROM
      day_prices
    WHERE
      symbol = 'ETH'
  ),
  dai_day_prices AS (
    SELECT
      day,
      price_dollar AS dai_price_dollar
    FROM
      day_prices
    WHERE
      symbol = 'DAI'
  ),
  rpl_day_prices AS (
    SELECT
      day,
      price_dollar AS rpl_price_dollar
    FROM
      day_prices
    WHERE
      symbol = 'rETH'
  ),
  price AS (
    SELECT
      a.day,
      eth_price_dollar,
      COALESCE(b.dai_price_dollar, 0) AS dai_price_dollar,
      COALESCE(c.rpl_price_dollar, 0) AS rpl_price_dollar
    FROM
      eth_day_prices AS a
      LEFT JOIN dai_day_prices AS b ON a.day = b.day
      LEFT JOIN rpl_day_prices AS c ON a.day = c.day
  ),
  MCR_event AS (
    select
      date_trunc('day', evt_block_time) AS day,
      RANK() OVER (
        PARTITION BY
          date_trunc('day', evt_block_time)
        ORDER BY
          evt_block_time DESC
      ) AS day_rank,
      mcrEtherx100 * 1E-18 AS mcr_eth
    from
      nexusmutual_ethereum.MCR_evt_MCREvent
  ),
  MCR_updated AS (
    select
      date_trunc('day', evt_block_time) AS day,
      RANK() OVER (
        PARTITION BY
          date_trunc('day', evt_block_time)
        ORDER BY
          evt_block_time DESC
      ) AS day_rank,
      mcr * 1E-18 AS mcr_eth
    from
      nexusmutual_ethereum.MCR_evt_MCRUpdated
  ),
  MCR_updated_event AS (
    select
      day,
      mcr_eth
    from
      MCR_event
    WHERE
      day_rank = 1
    UNION ALL
    select
      day,
      mcr_eth
    from
      MCR_updated
    WHERE
      day_rank = 1
  ),
  all_running_totals AS (
    select DISTINCT
      a.day AS day,
      eth_price_dollar,
      dai_price_dollar,
      rpl_price_dollar,
      eth_ingress,
      eth_egress,
      net_eth,
      net_enzyme,
      nxmty_price,
      dai_net_total,
      rpl_net_total,
      lido_ingress,
      mcr_eth
    from
      price AS a
      LEFT JOIN nxmty ON a.day = nxmty.day
      LEFT JOIN dai_transactions ON a.day = dai_transactions.day
      LEFT JOIN rocket_pool_transactions ON a.day = rocket_pool_transactions.day
      LEFT JOIN steth ON a.day = steth.day
      LEFT JOIN eth_daily_transactions ON a.day = eth_daily_transactions.day
      LEFT JOIN MCR_updated_event ON a.day = MCR_updated_event.day
  ),
  min_count_lagged AS (
    select
      day,
      eth_price_dollar,
      dai_price_dollar,
      rpl_price_dollar,
      net_eth,
      dai_net_total,
      rpl_net_total,
      net_enzyme,
      nxmty_price,
      lido_ingress,
      mcr_eth,
      COUNT(net_eth) OVER (
        ORDER BY
          day
      ) AS running_net_eth_min_count,
      COUNT(dai_net_total) OVER (
        ORDER BY
          day
      ) AS dai_net_total_min_count,
      COUNT(net_enzyme) OVER (
        ORDER BY
          day
      ) AS net_enzyme_min_count,
      COUNT(nxmty_price) OVER (
        ORDER BY
          day
      ) AS nxmty_price_min_count,
      COUNT(lido_ingress) OVER (
        ORDER BY
          day
      ) AS lido_ingress_min_count,
      COUNT(mcr_eth) OVER (
        ORDER BY
          day
      ) AS mcr_eth_min_count
    from
      all_running_totals
  ),
  lagged AS (
    select
      day,
      eth_price_dollar,
      dai_price_dollar,
      rpl_price_dollar,
      COALESCE(
        FIRST_VALUE(net_eth) OVER (
          PARTITION BY
            running_net_eth_min_count
          ORDER BY
            day
        ),
        0
      ) AS net_eth,
      COALESCE(
        FIRST_VALUE(dai_net_total) OVER (
          PARTITION BY
            dai_net_total_min_count
          ORDER BY
            day
        ),
        0
      ) AS running_net_dai,
      COALESCE(rpl_net_total, 0) AS running_net_rpl,
      COALESCE(
        FIRST_VALUE(net_enzyme) OVER (
          PARTITION BY
            net_enzyme_min_count
          ORDER BY
            day
        ),
        0
      ) AS running_net_enzyme,
      COALESCE(
        FIRST_VALUE(nxmty_price) OVER (
          PARTITION BY
            nxmty_price_min_count
          ORDER BY
            day
        ),
        0
      ) AS nxmty_price,
      COALESCE(
        FIRST_VALUE(lido_ingress) OVER (
          PARTITION BY
            lido_ingress_min_count
          ORDER BY
            day
        ),
        0
      ) AS running_net_lido,
      FIRST_VALUE(mcr_eth) OVER (
        PARTITION BY
          mcr_eth_min_count
        ORDER BY
          day
      ) AS mcr_eth
    from
      min_count_lagged
  ),
  summed AS (
    SELECT
      day,
      eth_price_dollar,
      dai_price_dollar,
      rpl_price_dollar,
      SUM(net_eth) OVER (
        ORDER BY
          day
      ) AS running_net_eth,
      SUM(running_net_dai) OVER (
        ORDER BY
          day
      ) AS running_net_dai,
      SUM(running_net_rpl) OVER (
        ORDER BY
          day
      ) AS running_net_rpl,
      SUM(running_net_enzyme * nxmty_price) OVER (
        ORDER BY
          day
      ) AS running_net_enzyme,
      running_net_lido,
      COALESCE(mcr_eth, 0) AS mcr_eth
    FROM
      lagged
  ),
  nxm_token_price AS (
    SELECT
      day,
      mcr_eth,
      eth_price_dollar,
      dai_price_dollar,
      rpl_price_dollar,
      running_net_eth,
      running_net_enzyme,
      running_net_lido,
      running_net_rpl,
      running_net_dai / eth_price_dollar AS running_net_dai,
      (
        running_net_eth + running_net_enzyme + running_net_lido + (running_net_dai / eth_price_dollar) + (
          running_net_rpl * rpl_price_dollar / eth_price_dollar
        )
      ) AS total,
      CASE
        WHEN mcr_eth = 0 THEN 0
        ELSE CAST(
          CAST(0.01028 AS DOUBLE) + (mcr_eth / CAST(5800000 AS DOUBLE)) * power(
            (
              CAST(
                running_net_eth + running_net_enzyme + running_net_lido + (
                  CAST(running_net_dai AS DOUBLE) / CAST(eth_price_dollar AS DOUBLE)
                ) + (
                  CAST(running_net_rpl AS DOUBLE) * CAST(rpl_price_dollar AS DOUBLE) / CAST(eth_price_dollar AS DOUBLE)
                ) AS DOUBLE
              ) / CAST(mcr_eth AS DOUBLE)
            ),
            4
          ) AS DOUBLE
        )
      END AS nxm_token_price_in_eth,
      CASE
        WHEN mcr_eth = 0 THEN 0
        ELSE CAST(
          CAST(0.01028 AS DOUBLE) + (mcr_eth / CAST(5800000 AS DOUBLE)) * power(
            (
              CAST(
                running_net_eth + running_net_enzyme + running_net_lido + (
                  CAST(running_net_dai AS DOUBLE) / CAST(eth_price_dollar AS DOUBLE)
                ) + (
                  CAST(running_net_rpl AS DOUBLE) * CAST(rpl_price_dollar AS DOUBLE) / CAST(eth_price_dollar AS DOUBLE)
                ) AS DOUBLE
              ) / CAST(mcr_eth AS DOUBLE)
            ),
            4
          ) AS DOUBLE
        ) * CAST(eth_price_dollar AS DOUBLE)
      END AS nxm_token_price_in_dollar
    FROM
      summed
    ORDER BY
      day
  ),
  ramm_nxm_queries AS (
    SELECT DISTINCT
      t.call_tx_hash,
      AVG(CAST(output_internalprice * 1E-18 AS DOUBLE)) OVER (
        PARTITION BY
          t.call_tx_hash
      ) AS ramm_nxm_price_in_eth
    FROM
      nexusmutual_ethereum.Cover_call_buyCover as t
      INNER JOIN nexusmutual_ethereum.Ramm_call_getInternalPriceAndUpdateTwap AS v ON v.call_tx_hash = t.call_tx_hash
  )
SELECT DISTINCT
  CAST(cover_id AS INT) AS cover_id,
  CASE
    when cover_end_time >= NOW() then 'Active'
    when cover_end_time < NOW() then 'Expired'
  END AS active,
  cover_asset,
  --  eth_price_dollar,
  --  dai_price_dollar,
  --  moving_average_eth,
  --  moving_average_dai,
  --  nxm_token_price_in_dollar,
  CAST(sum_assured AS DOUBLE) AS native_cover_amount,
  CASE
    WHEN cover_asset = 'ETH' THEN sum_assured * eth_price_dollar
    WHEN cover_asset = 'DAI' THEN sum_assured * dai_price_dollar
  END AS dollar_value,
  CASE
    WHEN cover_asset = 'ETH' THEN sum_assured
    WHEN cover_asset = 'DAI' THEN (sum_assured * dai_price_dollar) / eth_price_dollar
  END AS eth_value,
  partial_cover_amount_in_nxm,
  partial_cover_amount_in_nxm * (
    CASE
      WHEN cover_asset = 'ETH' THEN COALESCE(ramm_nxm_price_in_eth, nxm_token_price_in_eth)
      WHEN cover_asset = 'DAI' THEN COALESCE(
        ramm_nxm_price_in_eth * eth_price_dollar,
        nxm_token_price_in_dollar
      )
    END
  ) * 100.0 / (
    CASE
      WHEN cover_asset = 'ETH' THEN sum_assured * eth_price_dollar
      WHEN cover_asset = 'DAI' THEN sum_assured * dai_price_dollar
    END
  ) AS cover_percentage,
  partial_cover_amount_in_nxm * COALESCE(ramm_nxm_price_in_eth, nxm_token_price_in_eth) AS partial_cover_amount_in_eth,
  partial_cover_amount_in_nxm * COALESCE(
    ramm_nxm_price_in_eth * eth_price_dollar,
    nxm_token_price_in_dollar
  ) AS partial_cover_amount_in_dollar,
  premium_asset,
  premium_nxm,
  /* CASE
  WHEN premium_asset = 'DAI' THEN premium_nxm / nxm_token_price_in_dollar
  WHEN premium_asset = 'ETH' THEN premium / nxm_token_price_in_eth
  WHEN premium_asset = 'NXM' THEN premium
  END AS premium_nxm,*/
  CASE
    WHEN cover_asset = 'ETH' THEN premium_nxm * COALESCE(ramm_nxm_price_in_eth, nxm_token_price_in_eth)
    WHEN cover_asset = 'DAI' THEN premium_nxm * COALESCE(
      ramm_nxm_price_in_eth * eth_price_dollar,
      nxm_token_price_in_dollar
    ) / dai_price_dollar
  END AS premium_native,
  premium_nxm * COALESCE(
    ramm_nxm_price_in_eth * eth_price_dollar,
    nxm_token_price_in_dollar
  ) AS premium_dollar,
  syndicate AS staking_pool,
  COALESCE(product_type, 'unknown') AS product_type,
  COALESCE(product_name, 'unknown') AS product_name,
  cover_start_time,
  cover_end_time,
  owner,
  date_diff('day', cover_start_time, cover_end_time) AS cover_period,
  t.call_tx_hash
FROM
  covers AS t
  LEFT JOIN nxm_token_price AS f ON f.day = DATE_TRUNC('day', cover_start_time)
  LEFT JOIN ramm_nxm_queries AS v ON v.call_tx_hash = t.call_tx_hash
ORDER BY
  cover_id DESC