with
  nxm_distribution as (
    select
      date_trunc('day', evt_block_time) as day,
      "to" as address,
      value * 1E-18 as value
    from
      erc20."ERC20_evt_Transfer" as s
    where
      value > 0
      AND "contract_address" = '\xd7c49cee7e9188cca6ad8ff264c1da2e69d4cf3b'
    UNION ALL
    select
      date_trunc('day', evt_block_time) as day,
      t.from as address,
      value * -1E-18 as value
    from
      erc20."ERC20_evt_Transfer" as t
    where
      value > 0
      AND "contract_address" = '\xd7c49cee7e9188cca6ad8ff264c1da2e69d4cf3b'
  ),
  nxm_running_total as (
    SELECT
      distinct day,
      address,
      SUM(value) OVER (
        PARTITION BY address
        ORDER BY
          day
      ) as running_total
    FROM
      nxm_distribution
    WHERE
      address != '\x0000000000000000000000000000000000000000'
  ),
  exit_entrance as (
    select
      *,
      CASE
        when running_total = 0 then -1
        ELSE 0
      END as exited_mututal,
      CASE
        when running_total > 0 then 1
        ELSE 0
      END as in_mututal
    from
      nxm_running_total
  ),
  unqiue_addresses as (
    select
      day,
      address,
      running_total,
      exited_mututal,
      in_mututal,
      case
        when exited_mututal - COALESCE(
          lag(exited_mututal) OVER (
            PARTITION BY address
            ORDER BY
              day
          ),
          -1
        ) = -1 then -1
        when in_mututal - COALESCE(
          lag(in_mututal) OVER (
            PARTITION BY address
            ORDER BY
              day
          ),
          0
        ) = 1 then 1
      END as count_,
      case
        when exited_mututal - COALESCE(
          lag(exited_mututal) OVER (
            PARTITION BY address
            ORDER BY
              day
          ),
          -1
        ) = -1 then -1
      END as exited,
      case
        when in_mututal - COALESCE(
          lag(in_mututal) OVER (
            PARTITION BY address
            ORDER BY
              day
          ),
          0
        ) = 1 then 1
      END as entered
    FROM
      exit_entrance
  ),
  entered_and_exited as (
    select
      distinct day,
      COALESCE(SUM(exited) OVER (PARTITION BY day), 0) as exited_per_day,
      COALESCE(SUM(entered) OVER (PARTITION BY day), 0) as entered_per_day
    from
      unqiue_addresses
  )
select
  day,
  exited_per_day,
  entered_per_day,
  entered_per_day + exited_per_day as net_change,
  SUM(entered_per_day + exited_per_day) OVER (
    ORDER BY
      day
  ) as running_unique_users
from
  entered_and_exited
WHERE
  day >= '{{Start Date}}'
  AND day <= '{{End Date}}'
ORDER BY
  day DESC