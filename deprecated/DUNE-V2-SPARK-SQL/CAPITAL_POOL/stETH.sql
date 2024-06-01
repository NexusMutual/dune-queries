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
            '0x7cbe5682be6b648cc1100c76d4f6c96997f753d6'
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
            '0x7cbe5682be6b648cc1100c76d4f6c96997f753d6'
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
          '0x7cbe5682be6b648cc1100c76d4f6c96997f753d6'
        )
        OR `from` in (
          -- found from etherscan of mutant deploy contract
          '0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8',
          '0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb',
          '0xcafea35ce5a2fc4ced4464da4349f81a122fd12b',
          '0xcafea8321b5109d22c53ac019d7a449c947701fb',
          '0xfd61352232157815cf7b71045557192bf0ce1884',
          '0x7cbe5682be6b648cc1100c76d4f6c96997f753d6'
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
    select distinct
      'ETH' as asset_type,
      day,
      SUM(eth_ingress - eth_egress) OVER () as net_eth
    from
      eth_daily_transactions
    ORDER BY
      day DESC
    LIMIT
      1
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
    SELECT DISTINCT
      'DAI' as asset_type,
      day,
      SUM(ingress - egress) OVER () as dai_net_total
    FROM
      erc_transactions
    WHERE
      name = 'Maker: dai'
    ORDER BY
      day DESC
    LIMIT
      1
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
      lido.day,
      steth_amount,
      lido.rebase as new_rebase,
      lido_staking_net_steth.rebase as starting_rebase,
      lido.rebase / lido_staking_net_steth.rebase as rebase,
      steth_amount * lido.rebase / lido_staking_net_steth.rebase as new_eth_total
    FROM
      lido_staking_net_steth
      FULL OUTER JOIN lido
  )
SELECT
distinct
  day,
  SUM(new_eth_total) OVER (
    PARTITION BY
      day
  )
FROM
  expanded_rebase_steth