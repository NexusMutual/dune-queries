/*
Revenue statement
- Cash Surplus
    - Premiums
    - Membership Fees
    - Claims - Reimbursements
- Investments Total
    - breakdown of each investment line
    - FX Impact
- Capital Movement Total
    - Contributions (minting NXM for contributing ETH)
    - Withdrawals (withdrawing ETH for burning NXM)
- Total Cash Movement

Balance Sheet
- Crypto Denominated Assets
    - ETH, stETH, rETH, cbBTC etc
- Stablecoin Denominated Assets
    - DAI, USDC, Cover Re etc
*/

with items (fi_id, scope, label, label_tab) as (
  values
  (1000, 'rs', 'Revenue Statement', '<b>Revenue Statement</b>'),
  (1100, 'rs', 'Cash Surplus', '&nbsp; ⌄ Cash Surplus'),
  (1110, 'rs', 'Premiums', '&nbsp; &nbsp; &nbsp; Premiums'),
  (1120, 'rs', 'Membership Fees', '&nbsp; &nbsp; &nbsp; Membership Fees'),
  (1130, 'rs', 'Claims - Reimbursements', '&nbsp; &nbsp; &nbsp; Claims - Reimbursements'),
  (1200, 'rs', 'Investments Return Total', '&nbsp; ⌄ Investments Return Total'),
  (1200, 'rs', 'Total ETH Earned', '&nbsp; &nbsp; &nbsp; Total ETH Earned'),
  (1210, 'rs', 'stETH Return', '&nbsp; &nbsp; &nbsp; &nbsp; stETH Return'),
  (1220, 'rs', 'rETH Return', '&nbsp; &nbsp; &nbsp; &nbsp; rETH Return'),
  (1230, 'rs', 'Enzyme Vault Return', '&nbsp; &nbsp; &nbsp; &nbsp; Enzyme Vault Return'),
  (1240, 'rs', 'Aave Net Return', '&nbsp; &nbsp; &nbsp; &nbsp; Aave Net Return'),
  (1243, 'rs', 'aEthWETH Return', '&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; aEthWETH Return'),
  (1246, 'rs', 'debtUSDC Return', '&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; debtUSDC Return'),
  (1250, 'rs', 'FX Impact', '&nbsp; &nbsp; &nbsp; FX Impact'),
  (1300, 'rs', 'Capital Movement', '&nbsp; ⌄ Capital Movement'),
  (1310, 'rs', 'Contributions', '&nbsp; &nbsp; &nbsp; Contributions'),
  (1320, 'rs', 'Withdrawals', '&nbsp; &nbsp; &nbsp; Withdrawals'),
  (1400, 'rs', 'Total Cash Movement', '&nbsp; Total Cash Movement'),
  (1410, 'rs', 'Reconcilation Difference', '&nbsp; Reconcilation Difference'),
  (1420, 'rs', 'Total Cash Movement After Rec Diff', '&nbsp; Total Cash Movement After Rec Diff'),
  
  (2000, 'bs', 'Balance Sheet', '<b>Balance Sheet</b>'),
  (2050, 'bs', 'Opening Balance', '&nbsp; Opening Balance'),
  (2100, 'bs', 'Crypto Denominated Assets', '&nbsp; ⌄ Crypto Denominated Assets'),
  (2110, 'bs', 'ETH', '&nbsp; &nbsp; &nbsp; ETH'),
  (2120, 'bs', 'stETH', '&nbsp; &nbsp; &nbsp; stETH'),
  (2130, 'bs', 'rETH', '&nbsp; &nbsp; &nbsp; rETH'),
  (2140, 'bs', 'cbBTC', '&nbsp; &nbsp; &nbsp; cbBTC'),
  (2150, 'bs', 'Enzyme Vault', '&nbsp; &nbsp; &nbsp; Enzyme Vault'),
  (2160, 'bs', 'Aave aEthWETH', '&nbsp; &nbsp; &nbsp; Aave aEthWETH'),
  (2200, 'bs', 'Stablecoin Denominated Assets', '&nbsp; ⌄ Stablecoin Denominated Assets'),
  (2210, 'bs', 'DAI', '&nbsp; &nbsp; &nbsp; DAI'),
  (2220, 'bs', 'USDC', '&nbsp; &nbsp; &nbsp; USDC'),
  (2230, 'bs', 'Cover Re', '&nbsp; &nbsp; &nbsp; Cover Re'),
  (2240, 'bs', 'Aave debtUSDC', '&nbsp; &nbsp; &nbsp; Aave debtUSDC'),
  (2300, 'bs', 'Closing Balance', '&nbsp; Closing Balance'),
  
  (2400, 'bs', 'NXM Movement', '<b>NXM Movement</b>'),
  (2500, 'bs', 'Opening NXM', '&nbsp; Opening NXM'),
  (2510, 'bs', 'NXM Burned from Cover Purchases', '&nbsp; &nbsp; NXM Burned from Cover Purchases'),
  (2515, 'bs', 'NXM Burned from Claims', '&nbsp; &nbsp; NXM Burned from Claims'),
  (2520, 'bs', 'NXM Minted as Rewards (from staking, claims and governance)', '&nbsp; &nbsp; NXM Minted as Rewards (from staking, claims and governance)'),
  (2530, 'bs', 'NXM Minted from Contributions', '&nbsp; &nbsp; NXM Minted from Contributions'),
  (2540, 'bs', 'NXM Burned from Withdrawals', '&nbsp; &nbsp; NXM Burned from Withdrawals'),
  (2600, 'bs', 'Closing NXM', '&nbsp; Closing NXM')
)

select fi_id, scope, label, label_tab
from items
order by 1
