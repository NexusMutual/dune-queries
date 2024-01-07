with
  nxm_sent as (
    select
      t.from as address,
      SUM(-1 * value * 1E-18) as total_sent
    from
      erc20."ERC20_evt_Transfer" as t
    where
      value > 0
      AND "contract_address" = '\xd7c49cee7e9188cca6ad8ff264c1da2e69d4cf3b'
    GROUP BY
      address
  ),
  nxm_recv as (
    select
      t.to as address,
      SUM(value * 1E-18) as total_recv
    from
      erc20."ERC20_evt_Transfer" as t
    where
      value > 0
      AND "contract_address" = '\xd7c49cee7e9188cca6ad8ff264c1da2e69d4cf3b'
    GROUP BY
      address
  )
SELECT
  COALESCE(nxm_recv.address, nxm_sent.address) as user_address,
  COALESCE(nxm_sent.total_sent, 0),
  COALESCE(nxm_recv.total_recv, 0),
  COALESCE(nxm_sent.total_sent, 0) + COALESCE(nxm_recv.total_recv, 0) as total
FROM
  nxm_sent
  FULL JOIN nxm_recv ON nxm_recv.address = nxm_sent.address
where
  COALESCE(nxm_sent.total_sent, 0) + COALESCE(nxm_recv.total_recv, 0) > 0
ORDER BY
  total DESC