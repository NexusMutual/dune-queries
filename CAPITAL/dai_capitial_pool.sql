with
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
      "name" = 'dai'
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
  )
select
  *,
  SUM(net) OVER (
    ORDER BY
      day
  )
from
  dai_transactions