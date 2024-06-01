    select
      date_trunc('day', evt_block_time) as day,
      `from`,
      `to`,
      value * -1E-18 as value
    from
      erc20_ethereum.evt_Transfer
    where contract_address = '0xd7c49cee7e9188cca6ad8ff264c1da2e69d4cf3b'