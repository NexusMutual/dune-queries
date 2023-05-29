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
select
  day,
  eth_ingress,
  eth_egress,
  eth_ingress - eth_egress as total,
  SUM(eth_ingress) OVER (
    ORDER BY
      day
  ) as running_eth_ingress,
  SUM(eth_egress) OVER (
    ORDER BY
      day
  ) as running_eth_egress,
  SUM(eth_ingress - eth_egress) OVER (
    ORDER BY
      day
  ) as running_eth_total
FROM
  eth_daily_transactions