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
  ),
  unnested_contract_history as (
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
  )
SELECT
  *,
  convert_from(hex_name :: bytea, 'UTF8')
FROM
   unnested_contract_history

--- some contract history is currently missing from 2019 to 2020



