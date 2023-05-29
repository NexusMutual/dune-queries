WITH
  changes_config as (
    select
      "call_block_time" as date,
      case
        when code = '\x43414d494e565400' then val
      end as mintime_hrs,
      case
        when code = '\x43414d4158565400' then val
      end as maxTime_hrs
    from
      nexusmutual."ClaimsData_call_updateUintParameters"
    WHERE
      code in ('\x43414d4158565400', '\x43414d494e565400')
    UNION
    SELECT
      '2019-01-01 00:00' as date,
      12 as minTime_hrs,
      48 as maxTime_hrs
  )
SELECT
  date,
  Coalesce(
    minTime_hrs,
    lag(minTime_hrs) OVER(
      ORDER BY
        date DESC
    )
  ) as minTime_hrs,
  Coalesce(
    maxTime_hrs,
    lag(maxTime_hrs) OVER(
      ORDER BY
        date
    )
  ) as maxTime_hrs
FROM
  changes_config