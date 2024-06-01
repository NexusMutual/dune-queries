-- 1 ether * _stakingPool.getRewardPerSecond() * 365 days / _stakingPool.getActiveStake()
select
  1 as pool_id,
  --cast('16427530800893280784322' as uint256) / 1e18, cast('110568034158299' as uint256) / 1e18,
  100.0 * cast('110568034158299' as uint256) * (365 * 24 * 60 * 60) / cast('16427530800893280784322' as uint256) as apy
union all
select
  2, 
  100.0 * cast('218760344885903' as uint256) * (365 * 24 * 60 * 60) / cast('97878924992317989243797' as uint256) as apy
order by 1



select
  from_unixtime(0) as test,
  from_unixtime(91.0 * 86400.0 * cast(217 as double)) as start_date,
  from_unixtime(91.0 * 86400.0 * cast(217 + 1 as double)) as exiry_date



WITH constants AS (
  SELECT
    91 AS TRANCHE_DURATION,
    28 AS BUCKET_DURATION
)
SELECT 
    FLOOR(DATE_DIFF('day', from_unixtime(0), CURRENT_TIMESTAMP) / TRANCHE_DURATION) AS trancheId,
    FLOOR(DATE_DIFF('day', from_unixtime(0), CURRENT_TIMESTAMP) / BUCKET_DURATION) AS bucketId
FROM constants;

