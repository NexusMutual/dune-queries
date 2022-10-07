






WITH
  eth_traces as (
    select
      date_trunc('day', block_time) as day,
      gas_used * 1E-18 as gas_used,
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
      gas_used * 1E-18 as gas_used,
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
  SUM(gas_used) OVER (
    PARTITION BY day
    ORDER BY
      day
  ) as total_gas_used,
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
  eth
  )
  select * from eth
























































