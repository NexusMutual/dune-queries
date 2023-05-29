WITH
  lido AS (
    SELECT
      date_trunc('day', evt_block_time) AS DAY,
      postTotalPooledEther / totalShares AS rebase
    FROM
      lido_ethereum.LidoOracle_evt_PostTotalShares
    WHERE
      evt_block_time > '2021-05-26'
  ),
  deposits AS (
    SELECT
      date_trunc('day', evt_block_time),
      value / 1e18 AS dai,
      rebase
    FROM
      erc20_ethereum.evt_Transfer
      INNER JOIN lido ON date_trunc('day', evt_block_time) = lido.day
      AND contract_address = '0xae7ab96520de3a18e5e111b5eaab095312d7fe84'
      AND `to` = '0xcafea35ce5a2fc4ced4464da4349f81a122fd12b'
      AND evt_block_time > '2021-05-26'
  )
SELECT
  lido.day AS DAY,
  sum(
    CASE
      WHEN lido.rebase >= deposits.rebase THEN dai * lido.rebase / deposits.rebase
      ELSE 0
    END
  ) AS steth
FROM
  lido
  FULL OUTER JOIN deposits
GROUP BY
  1