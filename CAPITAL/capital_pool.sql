WITH
  eth_traces as (
    select
      date_trunc('day', block_time) as day,
      CASE
        WHEN "to" = '\xcafea35ce5a2fc4ced4464da4349f81a122fd12b' THEN value * 1E-18
        WHEN "to" = '\xcafeada4d15bbc7592113d5d5af631b5dcd53dcb' THEN value * 1E-18
        ELSE 0
      END as eth_ingress,
      CASE
        WHEN "from" = '\xcafea35ce5a2fc4ced4464da4349f81a122fd12b' THEN value * 1E-18
        WHEN "from" = '\xcafeada4d15bbc7592113d5d5af631b5dcd53dcb' THEN value * 1E-18
        ELSE 0
      END as eth_egress
    from
      ethereum."traces"
    where
      "to" = '\xcafea35ce5a2fc4ced4464da4349f81a122fd12b'
      OR "to" = '\xcafeada4d15bbc7592113d5d5af631b5dcd53dcb'
      OR "from" = '\xcafeada4d15bbc7592113d5d5af631b5dcd53dcb'
      OR "from" = '\xcafea35ce5a2fc4ced4464da4349f81a122fd12b'
      and success = true
    ORDER BY
      day
  ),
  eth_transactions as (
    select
      date_trunc('day', block_time) as day,
      CASE
        WHEN "to" = '\xcafea35ce5a2fc4ced4464da4349f81a122fd12b' THEN value * 1E-18
        WHEN "to" = '\xcafeada4d15bbc7592113d5d5af631b5dcd53dcb' THEN value * 1E-18
        ELSE 0
      END as eth_ingress,
      CASE
        WHEN "from" = '\xcafea35ce5a2fc4ced4464da4349f81a122fd12b' THEN value * 1E-18
        WHEN "from" = '\xcafeada4d15bbc7592113d5d5af631b5dcd53dcb' THEN value * 1E-18
        ELSE 0
      END as eth_egress
    from
      ethereum."transactions"
    where
      "to" = '\xcafea35ce5a2fc4ced4464da4349f81a122fd12b'
      OR "to" = '\xcafeada4d15bbc7592113d5d5af631b5dcd53dcb'
      OR "from" = '\xcafeada4d15bbc7592113d5d5af631b5dcd53dcb'
      OR "from" = '\xcafea35ce5a2fc4ced4464da4349f81a122fd12b'
      and success = true
    ORDER BY
      day
  ),
  eth_all_transaction as (
    SELECT
      *
    FROM
      eth_traces
    UNION
    SELECT
      *
    from
      eth_transactions
  ),
  eth as (
    SELECT
      DISTINCT day,
      SUM(eth_ingress) OVER (
        PARTITION BY day
        ORDER BY
          day
      ) as total_eth_ingress,
      SUM(eth_egress) OVER (
        PARTITION BY day
        ORDER BY
          day
      ) as total_eth_egress
    FROM
      eth_all_transaction
  ),
  erc_transactions as (
    select
      "contract_address",
      date_trunc('day', evt_block_time) as day,
      CASE
        WHEN "to" = '\xcafea35ce5a2fc4ced4464da4349f81a122fd12b' THEN value * 1E-18
        WHEN "to" = '\xcafeada4d15bbc7592113D5d5af631b5dcd53dcb' THEN value * 1E-18
        ELSE 0
      END as dai_ingress,
      CASE
        WHEN "from" = '\xcafea35ce5a2fc4ced4464da4349f81a122fd12b' THEN value * 1E-18
        WHEN "from" = '\xcafeada4d15bbc7592113D5d5af631b5dcd53dcb' THEN value * 1E-18
        ELSE 0
      END as dai_egress
    from
      erc20."ERC20_evt_Transfer"
    where
      "to" = '\xcafea35ce5a2fc4ced4464da4349f81a122fd12b'
      OR "to" = '\xcafeada4d15bbc7592113d5d5af631b5dcd53dcb'
      OR "from" = '\xcafeada4d15bbc7592113d5d5af631b5dcd53dcb'
      OR "from" = '\xcafea35ce5a2fc4ced4464da4349f81a122fd12b'
      AND NOT (
        "from" = '\xcafea35ce5a2fc4ced4464da4349f81a122fd12b'
        AND "to" = '\xcafeada4d15bbc7592113d5d5af631b5dcd53dcb'
      )
    ORDER BY
      evt_block_time
  ),
  labels_dune as (
    SELECT
      DISTINCT "name",
      "address"
    FROM
      labels.labels
    WHERE
      "name" in ('dai', 'lido')
      and 'name' is not null
  ),
  dai_transactions as (
    select
      day,
      name,
      SUM(dai_ingress) as dai_ingress,
      -1 * SUM(dai_egress) as dai_egress,
      SUM(dai_ingress) - SUM(dai_egress) as net
    from
      erc_transactions
      LEFT JOIN labels_dune ON labels_dune."address" = erc_transactions."contract_address"
    WHERE
      "name" = 'dai'
    GROUP BY
      day,
      name
  ),
  lido_transactions as (
    select
      day,
      name,
      SUM(dai_ingress) as lido_ingress,
      -1 * SUM(dai_egress) as lido_egress,
      SUM(dai_ingress) - SUM(dai_egress) as net
    from
      erc_transactions
      LEFT JOIN labels_dune ON labels_dune."address" = erc_transactions."contract_address"
    WHERE
      "name" = 'lido'
    GROUP BY
      day,
      name
  )
select
  COALESCE(
    eth.day,
    dai_transactions.day,
    lido_transactions.day
  ) as day,
  COALESCE(dai_ingress, 0) as dai_ingress,
  COALESCE(dai_egress, 0) as dai_egress,
  COALESCE(lido_ingress, 0) as lido_ingress,
  COALESCE(lido_ingress, 0) as lido_egress,
  COALESCE(total_eth_ingress, 0) as total_eth_ingress,
  COALESCE(total_eth_ingress, 0) as total_eth_egress
from
  dai_transactions
  FULL JOIN eth ON dai_transactions.day = eth.day
  FULL JOIN lido_transactions ON dai_transactions.day = lido_transactions.day