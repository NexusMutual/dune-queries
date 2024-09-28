with

staking_pool_names (pool_id, pool_name) as (
  values
  (1, 'Nexus Foundation'),
  (2, 'Hugh'),
  (3, 'Ease AAA Low Risk Pool'),
  (4, 'Ease AA Medium Risk Pool'),
  (5, 'Unity Cover'),
  (6, 'Safe Invest'),
  (7, 'ShieldX Staking Pool'),
  (8, 'DeFiSafety X OpenCover Blue Chip Protocol Pool'),
  (9, 'My Conservative Pool'),
  (10, 'SAFU Pool'),
  (11, 'Sherlock'),
  (12, 'Gm Exit Here (GLP) Pool'),
  (13, 'My Nexus Pool'),
  (14, 'My Private Pool'),
  (15, 'Reflection'),
  (16, 'Good KarMa Capital'),
  (17, 'High Trust Protocols'),
  (18, 'UnoRe WatchDog Pool'),
  (19, 'Broad And Diversified'),
  (20, 'Lowest Risk'),
  (21, 'Crypto Plaza'),
  (22, 'BraveNewDeFi''s Pool'),
  (23, 'Nexus Mutual Community Staking Pool'),
  (24, 'DimenRisk'),
  (25, 'Molecular Research'),
  (26, 'BALTACHI'),
  (27, '6666'),
  (28, 'Native')
)

select pool_id, pool_name
from staking_pool_names
