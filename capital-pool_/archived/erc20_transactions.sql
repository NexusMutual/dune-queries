WITH
  labels AS (
    SELECT
      name,
      cast(address as varbinary) as contract_address
    FROM
      labels.all
    WHERE
      name IN ('Maker: dai', 'Lido: steth')
  )
SELECT
  name,
  CAST(a.contract_address AS varbinary) AS contract_address,
  DATE_TRUNC('day', evt_block_time) AS day,
  CASE
    WHEN "to" IN (
      0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8,
      0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb,
      0xcafea35ce5a2fc4ced4464da4349f81a122fd12b,
      0xcafea8321b5109d22c53ac019d7a449c947701fb,
      0xfd61352232157815cf7b71045557192bf0ce1884,
      0x7cbe5682be6b648cc1100c76d4f6c96997f753d6,
      0xcafea112Db32436c2390F5EC988f3aDB96870627
    ) THEN CAST(value AS DOUBLE) * 1E-18
    ELSE 0
  END AS ingress,
  CASE
    WHEN "from" IN (
      0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8,
      0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb,
      0xcafea35ce5a2fc4ced4464da4349f81a122fd12b,
      0xcafea8321b5109d22c53ac019d7a449c947701fb,
      0xfd61352232157815cf7b71045557192bf0ce1884,
      0x7cbe5682be6b648cc1100c76d4f6c96997f753d6,
      0xcafea112Db32436c2390F5EC988f3aDB96870627
    ) THEN CAST(value AS DOUBLE) * 1E-18
    ELSE 0
  END AS egress
FROM
  erc20_ethereum.evt_Transfer AS a
  LEFT JOIN labels ON a.contract_address = labels.contract_address
WHERE
  (
    "to" IN (
      0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8,
      0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb,
      0xcafea35ce5a2fc4ced4464da4349f81a122fd12b,
      0xcafea8321b5109d22c53ac019d7a449c947701fb,
      0xfd61352232157815cf7b71045557192bf0ce1884,
      0x7cbe5682be6b648cc1100c76d4f6c96997f753d6,
      0xcafea112Db32436c2390F5EC988f3aDB96870627
    )
    OR "from" IN (
      0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8,
      0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb,
      0xcafea35ce5a2fc4ced4464da4349f81a122fd12b,
      0xcafea8321b5109d22c53ac019d7a449c947701fb,
      0xfd61352232157815cf7b71045557192bf0ce1884,
      0x7cbe5682be6b648cc1100c76d4f6c96997f753d6,
      0xcafea112Db32436c2390F5EC988f3aDB96870627
    )
  )
  AND evt_block_time > CAST('2019-01-01 00:00:00' AS TIMESTAMP)
  AND (
    name IN ('Maker: dai', 'Lido: steth')
    OR cast(a.contract_address AS varbinary) = 0x27f23c710dd3d878fe9393d93465fed1302f2ebd /* nxmty */
  )
  AND NOT (
    (
      "to" = 0xcafea35ce5a2fc4ced4464da4349f81a122fd12b
      AND "from" = 0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8
    )
    OR (
      "to" = 0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8
      AND "from" = 0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb
    )
    OR (
      "to" = 0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8
      AND "from" = 0xfd61352232157815cf7b71045557192bf0ce1884
    )
  )