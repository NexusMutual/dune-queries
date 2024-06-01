WITH
  eth_daily_transactions AS (
    select distinct
      date_trunc('day', block_time) as day,
      SUM(
        CASE
          WHEN `to` in (
            '0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8',
            '0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb',
            '0xcafea35ce5a2fc4ced4464da4349f81a122fd12b',
            '0xcafea8321b5109d22c53ac019d7a449c947701fb',
            '0xfd61352232157815cf7b71045557192bf0ce1884',
            '0x7cbe5682be6b648cc1100c76d4f6c96997f753d6',
            lower('0xcafea112Db32436c2390F5EC988f3aDB96870627')
          ) THEN value * 1E-18
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          date_trunc('day', block_time)
      ) as eth_ingress,
      SUM(
        CASE
          WHEN `from` in (
            '0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8',
            '0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb',
            '0xcafea35ce5a2fc4ced4464da4349f81a122fd12b',
            '0xcafea8321b5109d22c53ac019d7a449c947701fb',
            '0xfd61352232157815cf7b71045557192bf0ce1884',
            '0x7cbe5682be6b648cc1100c76d4f6c96997f753d6',
            lower('0xcafea112Db32436c2390F5EC988f3aDB96870627')
          ) THEN value * 1E-18
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
      AND block_time > '2019-01-01 00:00:00'
      AND (
        `to` in (
          '0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8',
          '0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb',
          '0xcafea35ce5a2fc4ced4464da4349f81a122fd12b',
          '0xcafea8321b5109d22c53ac019d7a449c947701fb',
          '0xfd61352232157815cf7b71045557192bf0ce1884',
          '0x7cbe5682be6b648cc1100c76d4f6c96997f753d6',
          lower('0xcafea112Db32436c2390F5EC988f3aDB96870627')
        )
        OR `from` in (
          -- found from etherscan of mutant deploy contract
          '0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8',
          '0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb',
          '0xcafea35ce5a2fc4ced4464da4349f81a122fd12b',
          '0xcafea8321b5109d22c53ac019d7a449c947701fb',
          '0xfd61352232157815cf7b71045557192bf0ce1884',
          '0x7cbe5682be6b648cc1100c76d4f6c96997f753d6',
          lower('0xcafea112Db32436c2390F5EC988f3aDB96870627')
        )
      )
      AND NOT (
        (
          `to` = '0xcafea35ce5a2fc4ced4464da4349f81a122fd12b'
          AND `from` = '0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8'
        )
        OR (
          `to` = '0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8'
          AND `from` = '0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb'
        )
        OR (
          `to` = '0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8'
          AND `from` = '0xfd61352232157815cf7b71045557192bf0ce1884'
        )
      )
  ),
  eth as (
    select
      distinct
      'ETH' as asset_type,
       day,
      SUM(eth_ingress - eth_egress) OVER (
      ) as net_eth
    from
      eth_daily_transactions
    ORDER BY day DESC
    LIMIT 1
  ),
  labels AS (
    select
      *
    from
      labels.all
    where
      name in ('Maker: dai', 'Lido: steth')
  ),
 erc_transactions AS (
    select
      name,
      contract_address,
      date_trunc('day', evt_block_time) as day,
      CASE
        WHEN `to` in (
          '0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8',
          '0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb',
          '0xcafea35ce5a2fc4ced4464da4349f81a122fd12b',
          '0xcafea8321b5109d22c53ac019d7a449c947701fb',
          '0xfD61352232157815cF7B71045557192Bf0CE1884',
          '0x7cbe5682be6b648cc1100c76d4f6c96997f753d6',
          lower('0xcafea112Db32436c2390F5EC988f3aDB96870627')
        ) THEN value * 1E-18
        ELSE 0
      END as ingress,
      CASE
        WHEN `from` in (
          '0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8',
          '0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb',
          '0xcafea35ce5a2fc4ced4464da4349f81a122fd12b',
          '0xcafea8321b5109d22c53ac019d7a449c947701fb',
          '0xfD61352232157815cF7B71045557192Bf0CE1884',
          '0x7cbe5682be6b648cc1100c76d4f6c96997f753d6',
          lower('0xcafea112Db32436c2390F5EC988f3aDB96870627')
        ) THEN value * 1E-18
        ELSE 0
      END as egress
    from
      erc20_ethereum.evt_Transfer
      LEFT JOIN labels ON erc20_ethereum.evt_Transfer.contract_address = labels.address
    where
      (
        `to` in (
          '0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8',
          '0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb',
          '0xcafea35ce5a2fc4ced4464da4349f81a122fd12b',
          '0xcafea8321b5109d22c53ac019d7a449c947701fb',
          '0xfD61352232157815cF7B71045557192Bf0CE1884',
          '0x7cbe5682be6b648cc1100c76d4f6c96997f753d6',
          lower('0xcafea112Db32436c2390F5EC988f3aDB96870627')
        )
        OR `from` in (
          -- found from etherscan of mutant deploy contract
          '0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8',
          '0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb',
          '0xcafea35ce5a2fc4ced4464da4349f81a122fd12b',
          '0xcafea8321b5109d22c53ac019d7a449c947701fb',
          '0xfD61352232157815cF7B71045557192Bf0CE1884',
          '0x7cbe5682be6b648cc1100c76d4f6c96997f753d6',
          lower('0xcafea112Db32436c2390F5EC988f3aDB96870627')
        )
      )
      AND evt_block_time > '2019-01-01 00:00:00'
      AND (
        name in ('Maker: dai', 'Lido: steth')
        OR contract_address = '0x27f23c710dd3d878fe9393d93465fed1302f2ebd' --nxmty
      )
      AND NOT (
        (
          `to` = '0xcafea35ce5a2fc4ced4464da4349f81a122fd12b'
          AND `from` = '0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8'
        )
        OR (
          `to` = '0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8'
          AND `from` = '0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb'
        )
        OR (
          `to` = '0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8'
          AND `from` = '0xfd61352232157815cf7b71045557192bf0ce1884'
        )
      )
  ),
  dai_transactions as (
    SELECT
      DISTINCT
      'DAI' as asset_type,
      day,
      SUM(ingress - egress) OVER (
      ) as dai_net_total
    FROM
      erc_transactions
    WHERE
      name = 'Maker: dai'
    ORDER BY day DESC LIMIT 1
  ),
  lido AS (
    SELECT
      date_trunc('day', evt_block_time) AS day,
      postTotalPooledEther / totalShares AS rebase
    FROM
      lido_ethereum.LidoOracle_evt_PostTotalShares
    WHERE
      evt_block_time > '2021-05-26'
  ),
  lido_staking_net_steth AS (
    SELECT
      distinct
      lido.day,
      SUM(ingress - egress / 1e18) OVER (
        PARTITION BY
          erc_transactions.day
      ) AS steth_amount,
      rebase
    FROM
      lido
      INNER JOIN erc_transactions ON erc_transactions.day = lido.day
      AND erc_transactions.name = 'Lido: steth'
  ),
  expanded_rebase_steth as (
    select
      distinct
      'stEth' as asset_type,
      lido.day,
      SUM(steth_amount * lido.rebase / lido_staking_net_steth.rebase) OVER (PARTITION BY lido.day) as eth_total
    FROM
      lido_staking_net_steth
      FULL OUTER JOIN lido
      ORDER BY day DESC LIMIT 1
  ),
  weth_nxmty_transactions as (
    select
      distinct day,
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
      ) as value
    from
      erc_transactions
    where
      erc_transactions.contract_address = '0x27f23c710dd3d878fe9393d93465fed1302f2ebd'
  ),
  chainlink_oracle_nxmty_price as (
    SELECT
      date_trunc('day', evt_block_time) AS day,
      answer / 1e18 as nxmty_price
    FROM
      chainlink_ethereum.AccessControlledOffchainAggregator_evt_NewTransmission
    WHERE
      contract_address = '0xca71bbe491079e138927f3f0ab448ae8782d1dca'
      AND evt_block_time > '2022-08-15 00:00:00'
  ),
  nxmty as (
    SELECT
      'nxmty' as asset_type,
      chainlink_oracle_nxmty_price.day,
      nxmty_price,
      SUM(COALESCE(value, 0)) OVER (
        ORDER BY
          chainlink_oracle_nxmty_price.day
      ) * nxmty_price as running_net_maple
    FROM
      chainlink_oracle_nxmty_price
      FULL JOIN weth_nxmty_transactions ON weth_nxmty_transactions.day = chainlink_oracle_nxmty_price.day
      ORDER BY day DESC LIMIT 1
  ), asset_split as (
    SELECT 
        asset_type,
        day,
        running_net_maple as value
    FROM nxmty
    UNION
    SELECT
        asset_type,
        day,
        eth_total as value
    FROM
    expanded_rebase_steth
    UNION
    SELECT
        asset_type,
        day,
        dai_net_total as value
    FROM
    dai_transactions
    UNION
    SELECT
        asset_type,
        day,
        net_eth as value
    FROM
    eth
  ), day_prices as (
    SELECT
      DISTINCT date_trunc('day', minute) AS day,
      symbol,
      AVG(price) OVER (
        PARTITION BY
          date_trunc('day', minute),
          symbol
      ) AS price_dollar
    FROM
      prices.usd
    WHERE
      (
        symbol = 'DAI'
        OR symbol = 'ETH'
      )
      AND minute > '2019-01-01 00:00:00':: TIMESTAMP
  ),
  eth_day_prices AS (
    SELECT
      day,
      price_dollar as eth_price_dollar
    FROM
      day_prices
    WHERE
      symbol = 'ETH'
  ),
  dai_day_prices AS (
    SELECT
      day,
      price_dollar as dai_price_dollar
    FROM
      day_prices
    WHERE
      symbol = 'DAI'
  ),
  ethereum_price_ma7 as (
    select
      day,
      eth_price_dollar,
      avg(eth_price_dollar) OVER (
        ORDER BY
          day ROWS BETWEEN 6 PRECEDING
          AND CURRENT ROW
      ) as moving_average_eth
    from
      eth_day_prices
    ORDER BY
      day DESC
  ),
  dai_price_ma7 as (
    select
      day,
      dai_price_dollar,
      avg(dai_price_dollar) OVER (
        ORDER BY
          day ROWS BETWEEN 6 PRECEDING
          AND CURRENT ROW
      ) as moving_average_dai
    from
      dai_day_prices
    ORDER BY
      day DESC
  ),
  price_ma as (
    select
      ethereum_price_ma7.day,
      ethereum_price_ma7.moving_average_eth,
      dai_price_ma7.moving_average_dai,
      asset_type,
      value
    from
      ethereum_price_ma7
      INNER JOIN dai_price_ma7 ON ethereum_price_ma7.day = dai_price_ma7.day
      LEFT JOIN asset_split ON ethereum_price_ma7.day = asset_split.day
  )
select
  asset_type,
  CASE
      WHEN asset_type in ('ETH', 'stEth', 'nxmty') AND '{{display_currency}}' = 'ETH' THEN value
      WHEN asset_type in ('ETH', 'stEth', 'nxmty') AND '{{display_currency}}' = 'USD' THEN value * moving_average_eth
      WHEN asset_type in ('DAI') AND '{{display_currency}}' = 'ETH' THEN value * moving_average_dai / moving_average_eth
      WHEN asset_type in ('DAI') AND '{{display_currency}}' = 'USD' THEN value * moving_average_dai
  END as value
from
  price_ma
WHERE asset_type IS NOT NULL
  