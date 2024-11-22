WITH
  eth_daily_transactions_fix AS (
  select distinct
      date_trunc('day', block_time) AS day,
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
      ) AS eth_ingress,
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
      ) AS eth_egress
    FROM
      ethereum.traces
    WHERE
      success
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
  eth AS (
    SELECT DISTINCT
      'ETH' AS asset_type,
      day,
      SUM(eth_ingress - eth_egress) OVER () AS net_eth
    FROM
      eth_daily_transactions
    ORDER BY
      day DESC
    LIMIT
      1
  ),
  labels AS (
    SELECT
      name,
      address
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
      LEFT JOIN labels ON CAST(a.contract_address AS VARBINARY) = labels.address
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
        OR CAST(a.contract_address AS VARBINARY) = 0x27f23c710dd3d878fe9393d93465fed1302f2ebd /* nxmty */
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
      'DAI' AS asset_type,
      day,
      SUM(ingress - egress) OVER () AS dai_net_total
    FROM
      erc_transactions
    WHERE
      name = 'Maker: dai'
    ORDER BY
      day DESC
    LIMIT
      1
  ),
  rocket_pool_transactions AS (
    SELECT DISTINCT
      'rETH' AS asset_type,
      day,
      SUM(ingress - egress) OVER () AS rpl_net_total
    FROM
      erc_transactions
    WHERE
      name = 'Rocketpool: RocketTokenRETH'
    ORDER BY
      day DESC
    LIMIT
      1
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
      'stEth' AS asset_type,
      lido.rebase AS rebase,
      lido_staking_net_steth.rebase2 AS rebase2,
      steth_amount
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
      'stEth' AS asset_type,
      SUM(
        steth_amount * CAST(rebase AS DOUBLE) / CAST(rebase2 AS DOUBLE)
      ) OVER (
        PARTITION BY
          day
      ) AS eth_total
    FROM
      expanded_rebase_steth
    ORDER BY
      day desc
    LIMIT
      1
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
    FROM
      erc_transactions
    WHERE
      erc_transactions.contract_address = 0x27f23c710dd3d878fe9393d93465fed1302f2ebd
  ),
  chainlink_oracle_nxmty_price AS (
    SELECT
      DATE_TRUNC('day', evt_block_time) AS day,
      CAST(answer AS DOUBLE) / 1e18 AS nxmty_price
    FROM
      chainlink_ethereum.AccessControlledOffchainAggregator_evt_NewTransmission
    WHERE
      CAST(contract_address as VARBINARY) = 0xca71bbe491079e138927f3f0ab448ae8782d1dca
      AND evt_block_time > CAST('2022-08-15 00:00:00' AS TIMESTAMP)
  ),
  nxmty AS (
    SELECT
      'nxmty' AS asset_type,
      chainlink_oracle_nxmty_price.day,
      nxmty_price,
      SUM(COALESCE(CAST(value AS DOUBLE), 0)) OVER (
        ORDER BY
          chainlink_oracle_nxmty_price.day
      ) * nxmty_price AS running_net_enzyme
    FROM
      chainlink_oracle_nxmty_price
      FULL JOIN weth_nxmty_transactions ON weth_nxmty_transactions.day = chainlink_oracle_nxmty_price.day
    ORDER BY
      day DESC
    LIMIT
      1
  ),
  asset_split AS (
    SELECT
      1 AS anchor,
      asset_type,
      day,
      running_net_enzyme AS value
    FROM
      nxmty
    UNION
    SELECT
      1 AS anchor,
      asset_type,
      day,
      eth_total AS value
    FROM
      steth
    UNION
    SELECT
      1 AS anchor,
      asset_type,
      day,
      dai_net_total AS value
    FROM
      dai_transactions
    UNION
    SELECT
      1 AS anchor,
      asset_type,
      day,
      net_eth AS value
    FROM
      eth
    UNION
    SELECT
      1 AS anchor,
      asset_type,
      day,
      rpl_net_total AS value
    FROM
      rocket_pool_transactions
  ),
  day_prices AS (
    SELECT DISTINCT
      DATE_TRUNC('day', minute) AS day,
      symbol,
      AVG(price) OVER (
        PARTITION BY
          DATE_TRUNC('day', minute),
          symbol
      ) AS price_dollar
    FROM prices.usd
    WHERE minute > CAST('2019-05-01' AS TIMESTAMP)
      and ((symbol = 'ETH' and blockchain is null)
        or (symbol in ('DAI', 'rETH') and blockchain = 'ethereum'))
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
  ethereum_price_ma7 AS (
    SELECT
      day,
      1 AS anchor,
      eth_price_dollar,
      AVG(eth_price_dollar) OVER (
        ORDER BY
          day ROWS BETWEEN 6 PRECEDING
          AND CURRENT ROW
      ) AS moving_average_eth
    FROM
      eth_day_prices
    ORDER BY
      day DESC
    LIMIT 1
  ),
  dai_price_ma7 AS (
    SELECT
      1 AS anchor,
      day,
      dai_price_dollar,
      AVG(dai_price_dollar) OVER (
        ORDER BY
          day ROWS BETWEEN 6 PRECEDING
          AND CURRENT ROW
      ) AS moving_average_dai
    FROM
      dai_day_prices
    ORDER BY
      day DESC
    LIMIT 1
  ),
  rpl_price_ma7 AS (
    SELECT
      1 AS anchor,
      day,
      rpl_price_dollar,
      AVG(rpl_price_dollar) OVER (
        ORDER BY
          day ROWS BETWEEN 6 PRECEDING
          AND CURRENT ROW
      ) AS moving_average_rpl
    FROM
      rpl_day_prices
    ORDER BY
      day DESC
    LIMIT 1
  ),
  price_ma AS (
    SELECT
      ethereum_price_ma7.day,
      ethereum_price_ma7.moving_average_eth,
      COALESCE(dai_price_ma7.moving_average_dai, 0) AS moving_average_dai,
      COALESCE(rpl_price_ma7.moving_average_rpl, 0) AS moving_average_rpl,
      asset_type,
      CAST(value AS DOUBLE) as asset_value
    FROM
      ethereum_price_ma7
      LEFT JOIN dai_price_ma7 ON ethereum_price_ma7.anchor = dai_price_ma7.anchor
      LEFT JOIN rpl_price_ma7 ON ethereum_price_ma7.anchor = rpl_price_ma7.anchor
      LEFT JOIN asset_split ON ethereum_price_ma7.anchor = asset_split.anchor
  )
SELECT
  asset_type,
  CASE
    WHEN asset_type IN ('ETH', 'stEth', 'nxmty')
    AND '{{display_currency}}' = 'ETH' THEN CAST(asset_value AS DOUBLE)
    WHEN asset_type IN ('ETH', 'stEth', 'nxmty')
    AND '{{display_currency}}' = 'USD' THEN CAST(asset_value AS DOUBLE) * moving_average_eth
    WHEN asset_type IN ('DAI')
    AND '{{display_currency}}' = 'ETH' THEN CAST(asset_value AS DOUBLE) * moving_average_dai / moving_average_eth
    WHEN asset_type IN ('DAI')
    AND '{{display_currency}}' = 'USD' THEN CAST(asset_value AS DOUBLE) * moving_average_dai
    WHEN asset_type IN ('rETH')
    AND '{{display_currency}}' = 'ETH' THEN CAST(asset_value AS DOUBLE) * moving_average_rpl / moving_average_eth
    WHEN asset_type IN ('rETH')
    AND '{{display_currency}}' = 'USD' THEN CAST(asset_value AS DOUBLE) * moving_average_rpl
  END AS asset_value
FROM
  price_ma
WHERE
  NOT asset_type IS NULL