WITH
  eth_transactions as (
    select
      `to` as to_,
      `from` as from_,
      date_trunc('day', block_time) as day,
      gas_used * 1E-18 as gas_used,
      CASE
        WHEN `to` = '0xfD61352232157815cF7B71045557192Bf0CE1884' THEN value * 1E-18
        WHEN `to` = '0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8' THEN value * 1E-18
        WHEN `to` = '0xcafea35ce5a2fc4ced4464da4349f81a122fd12b' THEN value * 1E-18
        WHEN `to` = '0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb' THEN value * 1E-18
        WHEN `to` = '0xcafea8321b5109d22c53ac019d7a449c947701fb' THEN value * 1E-18
        ELSE 0
      END as eth_ingress,
      CASE
        WHEN `from` = '0xfD61352232157815cF7B71045557192Bf0CE1884' THEN value * 1E-18
        WHEN `from` = '0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8' THEN value * 1E-18
        WHEN `from` = '0xcafea35ce5a2fc4ced4464da4349f81a122fd12b' THEN value * 1E-18
        WHEN `from` = '0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb' THEN value * 1E-18
        WHEN `from` = '0xcafea8321b5109d22c53ac019d7a449c947701fb' THEN value * 1E-18
        ELSE 0
      END as eth_egress
    from
      ethereum.traces
    where
      success = true
      and value > 0
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
      and (
        `to` = '0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8'
        OR `to` = '0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb'
        OR `to` = '0xcafea35ce5a2fc4ced4464da4349f81a122fd12b'
        OR `to` = '0xcafea8321b5109d22c53ac019d7a449c947701fb'
        OR `to` = '0xfD61352232157815cF7B71045557192Bf0CE1884' -- found from etherscan of mutant deploy contract
        OR `from` = '0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8'
        OR `from` = '0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb'
        OR `from` = '0xcafea35ce5a2fc4ced4464da4349f81a122fd12b'
        OR `from` = '0xcafea8321b5109d22c53ac019d7a449c947701fb'
        OR `from` = '0xfD61352232157815cF7B71045557192Bf0CE1884' -- found from etherscan of mutant deploy contract
      )
    ORDER BY
      day
  ),
  eth as (
    select
      distinct day,
      SUM(eth_ingress) OVER (
        PARTITION BY
          day
      ) as eth_ingress,
      SUM(eth_egress) OVER (
        PARTITION BY
          day
      ) as eth_egress,
      SUM(eth_ingress - eth_egress) OVER (
        PARTITION BY
          day
      ) as net_eth
    from
      eth_transactions
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
          '0x7cbe5682be6b648cc1100c76d4f6c96997f753d6'
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
          '0x7cbe5682be6b648cc1100c76d4f6c96997f753d6'
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
          '0x7cbe5682be6b648cc1100c76d4f6c96997f753d6'
        )
        OR `from` in (
          -- found from etherscan of mutant deploy contract
          '0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8',
          '0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb',
          '0xcafea35ce5a2fc4ced4464da4349f81a122fd12b',
          '0xcafea8321b5109d22c53ac019d7a449c947701fb',
          '0xfD61352232157815cF7B71045557192Bf0CE1884',
          '0x7cbe5682be6b648cc1100c76d4f6c96997f753d6'
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
      DISTINCT day,
      SUM(ingress) OVER (
        PARTITION BY
          day
      ) as dai_ingress,
      SUM(egress) OVER (
        PARTITION BY
          day
      ) as dai_egress,
      SUM(ingress - egress) OVER (
        PARTITION BY
          day
      ) as dai_net_total
    FROM
      erc_transactions
    WHERE
      name = 'Maker: dai'
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
  deposits AS (
    SELECT
      erc_transactions.day,
      SUM(ingress - egress / 1e18) OVER (
        PARTITION BY
          erc_transactions.day
      ) AS dai,
      rebase
    FROM
      erc_transactions
      INNER JOIN lido ON erc_transactions.day = lido.day
      AND erc_transactions.name = 'Lido: steth'
  ),
  lido_staking_net_steth as (
    SELECT
      lido.day AS day,
      sum(
        CASE
          WHEN lido.rebase >= deposits.rebase THEN dai * lido.rebase / deposits.rebase
          ELSE 0
        END
      ) AS lido_ingress,
      0 as lido_egress -- This will require changing when we can withdraw ethereum
    FROM
      lido
      FULL OUTER JOIN deposits
    GROUP BY
      1
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
      chainlink_oracle_nxmty_price.day,
      nxmty_price,
      SUM(COALESCE(value, 0)) OVER (
        ORDER BY
          chainlink_oracle_nxmty_price.day
      ) * nxmty_price as running_net_maple
    FROM
      chainlink_oracle_nxmty_price
      FULL JOIN weth_nxmty_transactions ON weth_nxmty_transactions.day = chainlink_oracle_nxmty_price.day
  ),
  day_prices as (
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
      dai_price_ma7.moving_average_dai
    from
      ethereum_price_ma7
      INNER JOIN dai_price_ma7 ON ethereum_price_ma7.day = dai_price_ma7.day
  )
select
  *
from
  price_ma
  LEFT JOIN nxmty ON price_ma.day = nxmty.day
  LEFT JOIN dai_transactions ON price_ma.day = dai_transactions.day
  LEFT JOIN lido_staking_net_steth ON price_ma.day = lido_staking_net_steth.day