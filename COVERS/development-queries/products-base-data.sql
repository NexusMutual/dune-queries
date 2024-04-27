with

product_v1_to_v2_mapping as (
  select
    legacyProductId as product_contract_address,
    output_0 as product_id_v2
  from nexusmutual_ethereum.ProductsV1_call_getNewProductId
  where call_success
  group by 1, 2
),

product_info as (
  select
    i.product_contract_address as product_address,
    i.syndicate,
    i.product_name,
    i.product_type,
    m.product_id_v2
  from nexusmutual_ethereum.product_information i
    inner join product_v1_to_v2_mapping m on i.product_contract_address = m.product_contract_address
),

products_v2 as (
  select
    pt.product_type_id,
    pt.product_type_name,
    p.product_id,
    p.product_name
  from query_3676071 pt -- product types
    inner join query_3676060 p -- products
      on pt.product_type_id = p.product_type_id
)

select *
from products_v2
order by product_id
