with
  nxm AS (
    select
      `from`,
      `to`,
      value
    from
      erc20_ethereum.evt_Transfer as t
    where
      contract_address = '0xd7c49cee7e9188cca6ad8ff264c1da2e69d4cf3b'
  ),
  nxm_sent as (
    select distinct
      `from` as address,
      SUM(value * 1E-18) OVER (
        PARTITION BY
          `from`
      ) as total_sent
    from
      nxm
  ),
  nxm_recv as (
    select distinct
      `to` as address,
      SUM(value * 1E-18) OVER (
        PARTITION BY
          `to`
      ) as total_recv
    from
      nxm
  )
SELECT
  COALESCE(nxm_recv.address, nxm_sent.address) as user_address,
  COALESCE(nxm_sent.total_sent, 0) as sent,
  COALESCE(nxm_recv.total_recv, 0) as recieved,
  COALESCE(nxm_recv.total_recv, 0) - COALESCE(nxm_sent.total_sent, 0) as total
FROM
  nxm_sent
  FULL JOIN nxm_recv ON nxm_recv.address = nxm_sent.address
where
  COALESCE(nxm_recv.total_recv, 0) - COALESCE(nxm_sent.total_sent, 0) > 0
ORDER BY
  total DESC