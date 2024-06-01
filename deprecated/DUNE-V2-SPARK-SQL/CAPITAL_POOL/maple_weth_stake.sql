WITH
  weth_nxmty_transactions as (
    select
      date_trunc('day', evt_block_time) as day,
      CASE
        WHEN `from` = '0xcafea35ce5a2fc4ced4464da4349f81a122fd12b' THEN value * -1E-18
        WHEN `to` = '0xcafea35ce5a2fc4ced4464da4349f81a122fd12b' THEN value * 1E-18
      END as value
    from
      erc20_ethereum.evt_Transfer
    where
      (
        `from` = '0xcafea35ce5a2fc4ced4464da4349f81a122fd12b'
        OR `to` = '0xcafea35ce5a2fc4ced4464da4349f81a122fd12b'
      )
      AND contract_address = '0x27f23c710dd3d878fe9393d93465fed1302f2ebd'
      AND evt_block_time > '2022-08-15 00:00:00'
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
      ) * nxmty_price as running_total
    FROM
      chainlink_oracle_nxmty_price
      FULL JOIN weth_nxmty_transactions ON weth_nxmty_transactions.day = chainlink_oracle_nxmty_price.day
  )
select
  *,
  day,
  running_total
FROM
  nxmty