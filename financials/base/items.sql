/*
Revenue statement

- Cash surplus
    - Investment returns
        - investment returns
        - stablecoin impact
    - Premiums - Claims
    - Membership fees
    - Reserve Movement (?)
- Capital Movement
    - Contributions (minting NXM for contributing ETH)
    - Withdrawals (withdrawing ETH for burning NXM)

Balance Sheet

- ETH denominated Assets
    - ETH, stETH, rETH etc
- Stablecoin Denominated Assets
    - DAI, USDC, Cover Re etc
*/

with items (fi_id, scope, label, label_tab) as (
  values
  (1000, 'rs', 'Revenue Statement', '<b>Revenue Statement</b>'),
  (1100, 'rs', 'Cash Surplus', '&nbsp; Cash Surplus'),
  (1110, 'rs', 'Investments', '&nbsp; &nbsp; ⌄ Investments'),
  (1113, 'rs', 'Investment Returns', '&nbsp; &nbsp; &nbsp; &nbsp; Investment Returns'),
  (1116, 'rs', 'Stablecoin Impact', '&nbsp; &nbsp; &nbsp; &nbsp; Stablecoin Impact'),
  (1120, 'rs', 'Premiums - Claims', '&nbsp; &nbsp; Premiums - Claims'),
  (1130, 'rs', 'Membership Fees', '&nbsp; &nbsp; Membership Fees'),
  (1140, 'rs', 'Reserve Movement', '&nbsp; &nbsp; Reserve Movement'),
  (1200, 'rs', 'Capital Movement', '&nbsp; Capital Movement'),
  (1210, 'rs', 'Contributions', '&nbsp; &nbsp; Contributions'),
  (1220, 'rs', 'Withdrawals', '&nbsp; &nbsp; Withdrawals'),
  (2000, 'bs', 'Balance Sheet', '<b>Balance Sheet</b>'),
  (2100, 'bs', 'ETH Denominated Assets', '&nbsp; ⌄ ETH Denominated Assets'),
  (2110, 'bs', 'ETH', '&nbsp; &nbsp; ETH'),
  (2120, 'bs', 'stETH', '&nbsp; &nbsp; stETH'),
  (2130, 'bs', 'rETH', '&nbsp; &nbsp; rETH'),
  (2140, 'bs', 'cbBTC', '&nbsp; &nbsp; cbBTC'),
  (2150, 'bs', 'Enzyme Vault', '&nbsp; &nbsp; Enzyme Vault'),
  (2160, 'bs', 'Aave aEthWETH', '&nbsp; &nbsp; Aave aEthWETH'),
  (2200, 'bs', 'Stablecoin Denominated Assets', '&nbsp; ⌄ Stablecoin Denominated Assets'),
  (2210, 'bs', 'DAI', '&nbsp; &nbsp; DAI'),
  (2220, 'bs', 'USDC', '&nbsp; &nbsp; USDC'),
  (2230, 'bs', 'Cover Re', '&nbsp; &nbsp; Cover Re'),
  (2240, 'bs', 'Aave debtUSDC', '&nbsp; &nbsp; Aave debtUSDC')
)

select fi_id, scope, label, label_tab
from items
order by 1
