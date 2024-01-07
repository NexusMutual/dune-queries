with
  erc_transactions as (
    select
      "contract_address",
      date_trunc('day', evt_block_time) as day,
      CASE
        WHEN "to" = '\xfD61352232157815cF7B71045557192Bf0CE1884' THEN value * 1E-18
        WHEN "to" = '\xcafea7934490ef8b9d2572eaefeb9d48162ea5d8' THEN value * 1E-18
        WHEN "to" = '\xcafea35ce5a2fc4ced4464da4349f81a122fd12b' THEN value * 1E-18
        WHEN "to" = '\xcafeada4d15bbc7592113d5d5af631b5dcd53dcb' THEN value * 1E-18
        WHEN "to" = '\xcafea8321b5109d22c53ac019d7a449c947701fb' THEN value * 1E-18
        ELSE 0
      END as eth_ingress,
      CASE
        WHEN "from" = '\xfD61352232157815cF7B71045557192Bf0CE1884' THEN value * 1E-18
        WHEN "from" = '\xcafea7934490ef8b9d2572eaefeb9d48162ea5d8' THEN value * 1E-18
        WHEN "from" = '\xcafea35ce5a2fc4ced4464da4349f81a122fd12b' THEN value * 1E-18
        WHEN "from" = '\xcafeada4d15bbc7592113d5d5af631b5dcd53dcb' THEN value * 1E-18
        WHEN "from" = '\xcafea8321b5109d22c53ac019d7a449c947701fb' THEN value * 1E-18
        ELSE 0
      END as eth_egress
    from
      erc20."ERC20_evt_Transfer"
    where
      value > 0
      AND NOT (
        (
          "to" = '\xcafea35ce5a2fc4ced4464da4349f81a122fd12b'
          AND "from" = '\xcafea7934490ef8b9d2572eaefeb9d48162ea5d8'
        )
        OR (
          "to" = '\xcafea7934490ef8b9d2572eaefeb9d48162ea5d8'
          AND "from" = '\xcafeada4d15bbc7592113d5d5af631b5dcd53dcb'
        )
        OR (
          "to" = '\xcafea7934490ef8b9d2572eaefeb9d48162ea5d8'
          AND "from" = '\xfd61352232157815cf7b71045557192bf0ce1884'
        )
      )
      AND (
        "to" = '\xcafea7934490ef8b9d2572eaefeb9d48162ea5d8'
        OR "to" = '\xcafeada4d15bbc7592113d5d5af631b5dcd53dcb'
        OR "to" = '\xcafea35ce5a2fc4ced4464da4349f81a122fd12b'
        OR "to" = '\xcafea8321b5109d22c53ac019d7a449c947701fb'
        OR "to" = '\xfD61352232157815cF7B71045557192Bf0CE1884' -- found from etherscan of mutant deploy contract
        OR "from" = '\xcafea7934490ef8b9d2572eaefeb9d48162ea5d8'
        OR "from" = '\xcafeada4d15bbc7592113d5d5af631b5dcd53dcb'
        OR "from" = '\xcafea35ce5a2fc4ced4464da4349f81a122fd12b'
        OR "from" = '\xcafea8321b5109d22c53ac019d7a449c947701fb'
        OR "from" = '\xfD61352232157815cF7B71045557192Bf0CE1884'
      )
    ORDER BY
      evt_block_time
  )
select
  *
from
  erc_transactions