WITH
  implimentations as (
    select
      "_contractNames" as name,
      "contract_address" :: bytea as addr,
      "call_tx_hash" as hash,
      "call_block_time" as time
    from
      nexusmutual."NXMaster_call_upgradeMultipleImplementations"
    WHERE
      "call_success" = 'true'
  ),
  contract_upgrades as (
    SELECT
      "_contractsName" :: text [] as name,
      "_contractsAddress" :: text [] as addr,
      "call_tx_hash" as hash,
      "call_block_time" as time
    from
      nexusmutual."NXMaster_call_upgradeMultipleContracts"
    WHERE
      "call_success" = 'true'
  )
select
  time,
  ROW_NUMBER() OVER (
    ORDER BY
      hash ASC
  ),
  UNNEST(addr) as contract_address,
  UNNEST(name) as hex_name
FROM
  contract_upgrades