with

reimbursements (block_number, product_name, amount, symbol, tx_hash) as (
  values
  (19420020, 'FTX', 1406382.90, 'USDC', 0x7bdc2b71d6078f6129e3d38012245be1c9deaf20dd34b4db07d9dafddb3bf796),
  (19009119, 'FTX', 25218, 'USDC', 0x2b908780406a06ef8f936c3b801ff722a0f0ba36d00699f53ad64f24260d6b45),
  (18628643, 'FTX', 276063.38, 'USDC', 0x0ee9bb0d9fb5aeacaa7273be5d15a2f29849d350e7450e2447e6355bff268547),
  (18627647, 'FTX', 10, 'USDC', 0xc12603a55d14ae01bca4c506f7e82ed6e05959c60b364640d59c9f0bac561dcf),
  (18643871, 'FTX', 4800, 'USDC', 0xb699014515bfce28843dd997f48d015f7373bf8a6887d3fcd411ce90005795bf),
  (17029553, 'Euler', 23308, 'DAI', 0x3c1d3fd228b439e7cb1237d379e1b390e25502f7a6a83b59b0e012292ec30f08),
  (17029589, 'Euler', 1000, 'DAI', 0x12318d3283e89d34981b313d62a7a9bbfd373c4ac042fc41663692e1f44850f2),
  (17029603, 'Euler', 129000, 'DAI', 0x4ab03ff81f7190e55f379692a2f34db0d08a4880a60952c29435d4bff4307544),
  (17030225, 'Euler', 106.73, 'ETH', 0xbe171af672c1a3630e880e0715640c2e028b9ec3ad943b5b27b282148d7f59b1),
  (17031762, 'Euler', 7495.54, 'DAI', 0x3cc61b1fdfc50237acd8b34507d4842f9040926ca8827fe9f6ea2ef716528f64),
  (17031804, 'Euler', 8.671842028, 'ETH', 0x4a76b1f39631d6891003b1bb66c39d547f3960e7b0fbbcfc32264d8ee2be2666),
  (17037739, 'Euler', 1559400, 'DAI', 0xa8e13835cb13a7a970afa93749ea7c45bd2315418ff09b6495d0aacaed198407),
  (17037549, 'Euler', 3.28, 'DAI', 0x7eb73ff88f66b621562b35994eb5bac4ff6f9bf7a590ba78d5fa38fb8e6b9e35),
  (17037888, 'Euler', 4.32, 'ETH', 0x0ba35a4f7c8fb1f898dba727782033ce6a99ee99d476817add9b44570019252b),
  (17059525, 'Euler', 0.84, 'ETH', 0xa4d581396a085ed4b2f0f4aec615dad0bb25777b60a46a530cde196a817ac491)
),

reimbursement_transfers as (
  select
    t.block_time,
    t.block_number,
    t.block_date,
    r.product_name,
    t.symbol,
    t.amount,
    t.amount_usd,
    t.amount_usd / p_eth.price as amount_eth,
    t.tx_hash
  from tokens_ethereum.transfers t
    inner join reimbursements r on t.block_number = r.block_number and t.tx_hash = r.tx_hash
    inner join prices.usd p_eth on date_trunc('minute', t.block_time) = p_eth.minute
  where p_eth.symbol = 'ETH'
    and p_eth.blockchain is null
    and p_eth.contract_address is null
),

reimbursement_ext as (
  select
    block_date,
    product_name,
    if(symbol in ('ETH', 'WETH'), amount_eth, 0) as eth_eth_reimbursement_amount,
    if(symbol in ('ETH', 'WETH'), amount_usd, 0) as eth_usd_reimbursement_amount,
    if(symbol = 'DAI', amount_eth, 0) as dai_eth_reimbursement_amount,
    if(symbol = 'DAI', amount_usd, 0) as dai_usd_reimbursement_amount,
    if(symbol = 'USDC', amount_eth, 0) as usdc_eth_reimbursement_amount,
    if(symbol = 'USDC', amount_usd, 0) as usdc_usd_reimbursement_amount
  from reimbursement_transfers
)

select
  date_trunc('month', block_date) as block_month,
  product_name,
  sum(eth_usd_reimbursement_amount + dai_usd_reimbursement_amount + usdc_usd_reimbursement_amount) as usd_reimbursement,
  sum(eth_eth_reimbursement_amount + dai_eth_reimbursement_amount + usdc_eth_reimbursement_amount) as eth_reimbursement
from reimbursement_ext
group by 1, 2
order by 1 desc
