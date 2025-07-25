with

labels (address, address_label) as (
  values
    -- tokens
    (0x0d438F3b5175Bebc262bF23753C1E53d03432bDE, 'wNXM'),
    (0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48, 'USDC'),
    (0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf, 'cbBTC'),
    (0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2, 'wETH'),
    -- NM wallets
    (0x963Df0066ff8345922dF88eebeb1095BE4e4e12E, 'NM: Foundation'),
    (0xfa7e852ceb3f7d0f1ac02f3b8afca95e6dcbdb3c, 'NM: DAO Treasury [0xfa7e]'),
    (0x09abbe423fedee2332caea4117093d7d9b017cf5, 'NM: DAO Treasury [0x09ab]'),
    (0x586b9b2f8010b284a0197f392156f1a7eb5e86e9, 'NM: Community Fund [0x586b]'),
    (0x8e53d04644e9ab0412a8c6bd228c84da7664cfe3, 'NM: Community Fund [0x8e53]'),
    -- NM contracts
    (0xcafeaa5f9c401b7295890f309168Bbb8173690A3, 'NM: Assessment'),
    (0xcafeaA6660019915EC109052825ee7121480F0cD, 'NM: AssessmentViewer'),
    (0xcafeac0fF5dA0A2777d915531bfA6B29d282Ee62, 'NM: Cover'),
    (0xCB2B736652D2dBf7d72e4dB880Cf6B7d99507814, 'NM: CoverBroker'),
    (0xcafeaCa76be547F14D0220482667B42D8E7Bc3eb, 'NM: CoverNFT'),
    (0xcafead81a2c2508e7344155eB0DA67a3a487AA8d, 'NM: CoverProducts'),
    (0xcafea53a6c1774030F4B1C06B4A5743d5AFFF8b9, 'NM: CoverViewer'),
    (0x4A5C681dDC32acC6ccA51ac17e9d461e6be87900, 'NM: Governance'),
    (0xcafeac12feE6b65A710fA9299A98D65B4fdE7a62, 'NM: IndividualClaims'),
    (0xdc2D359F59F6a26162972c3Bd0cFBfd8C9Ef43af, 'NM: LegacyClaimsData'),
    (0x1776651F58a17a50098d31ba3C3cD259C1903f7A, 'NM: LegacyQuotationData'),
    (0xcafea53852E9f719c424Ec2fe1e7aDd27304210F, 'NM: LimitOrders'),
    (0xcafea92739e411a4D95bbc2275CA61dE6993C9a7, 'NM: MCR'),
    (0x055CC48f7968FD8640EF140610dd4038e1b03926, 'NM: MemberRoles'),
    (0xd7c49CEE7E9188cCa6AD8FF264C1DA2e69D4Cf3B, 'NM: NXMToken'),
    (0x01BFd82675DBCc7762C84019cA518e701C0cD07e, 'NM: NXMaster'),
    (0xcafeab03F219b7a8BCb92a5d61508A0AE16302b6, 'NM: NexusViewer'),
    (0xcafeaf6eA90CB931ae43a8Cf4B25a73a24cF6158, 'NM: Pool'),
    (0xcafea905B417AC7778843aaE1A0b3848CA97a592, 'NM: PriceFeedOracle'),
    (0x888eA6Ab349c854936b98586CE6a17E98BF254b2, 'NM: ProposalCategory'),
    (0xcafea54f03E1Cc036653444e581A10a43B2487CD, 'NM: Ramm'),
    (0xcafeaB8B01C74c2239eA9b2B0F6aB2dD409c6c13, 'NM: SafeTracker'),
    (0xcafea508a477D94c502c253A58239fb8F948e97f, 'NM: StakingNFT'),
    (0xcafeafb97BF8831D95C0FC659b8eB3946B101CB3, 'NM: StakingPoolFactory'),
    (0xcafea573fBd815B5f59e8049E71E554bde3477E4, 'NM: StakingProducts'),
    (0xcafea5E8a7a54dd14Bb225b66C7a016dfd7F236b, 'NM: StakingViewer'),
    (0xcafeaed98d7Fce8F355C03c9F3507B90a974f37e, 'NM: SwapOperator'),
    (0x5407381b6c251cFd498ccD4A1d877739CB7960B8, 'NM: TokenController'),
    -- NM staking pools
    (0x4ab04d7333293ab5e752fd4d2de0c0e88c2ca0f8, '1 : Nexus Foundation'),
    (0xf3745f76c137738b0371a820a098fc678672660a, '2 : Hugh'),
    (0x462340b61e2ae2c13f01f66b727d1bfdc907e53e, '3 : Ease AAA Low Risk Pool'),
    (0xed9915e07af860c3263801e223c9eab512eb7c09, '4 : Ease AA Medium Risk Pool'),
    (0xcf4a288ba45f53971d7acf481c28ffb4e049cf9f, '5 : Unity Cover'),
    (0xb57a99beedf92bbd4306233e3bc1e2616465f4fd, '6 : Safe Invest'),
    (0x5053f672f1ca522ecad0975e29b3192ef5ada845, '7 : ShieldX Staking Pool'),
    (0x34d250e9fa70748c8af41470323b4ea396f76c16, '8 : OpenCover'),
    (0x11547912aa7da2792e67698ad3d5386828d92cfd, '9 : My Conservative Pool'),
    (0x8b65f957f96cc81dc455f2b06ed137a4bca35555, '10 : SAFU Pool'),
    (0x0638fd10bb23a04efb41f1596aac28fb4d6ca910, '11 : Sherlock'),
    (0xb315340dabcf9b52cf0c592b37fde899cec7ad21, '12 : Gm Exit Here (GLP) Pool'),
    (0xa52d015c9aaaf81a78bb76087867e6564e9fb886, '13 : My Nexus Pool'),
    (0x4e48e13086668754b57f0b28acce9578f74a2339, '14 : My Private Pool'),
    (0x0efe933c8a36e63fc94d098e7d4b1352408f3979, '15 : Reflection'),
    (0xa12b4c1a1d5fe83ed843d150d90cc6444d7d1f85, '16 : Good KarMa Capital'),
    (0x10fc91cc80c480fa857b62070424d65fb9f97b64, '17 : High Trust Protocols'),
    (0x65fe7c7dd7ffd0ebcc5dbd06dec3d799ea89e751, '18 : UnoRe WatchDog Pool'),
    (0xddbbaa04a82bdc497fb9792cd41f50d1d389ae1c, '19 : Broad And Diversified'),
    (0xa49f22437969f95c937254566cce0d22edd3f028, '20 : Lowest Risk'),
    (0xbbd326fbb7017404d1ceb7b6a94d176a9d9bc56b, '21 : Crypto Plaza'),
    (0x5a44002a5ce1c2501759387895a3b4818c3f50b3, '22 : BraveNewDeFi`s Pool'),
    (0x778cc2e7d6f9c342b75ebfa1afef505e4477f378, '23 : Nexus Mutual Community Staking Pool'),
    (0x376e98c2ecadde98898cc9a3ddddf79eba7a8efa, '24 : DimenRisk'),
    (0x93bd6aa97d84e09a113207cd09cd5970f4e96ac1, '25 : Molecular Research'),
    (0xfd4f81533d8d642bbce67688142eea9aef93b33c, '26 : BALTACHI'),
    (0x37ac6b1d2c8873a1a772cd15a42208864d343ebd, '27 : 6666'),
    (0x79cfb42365efbf0240ae47db5bd74877f920cc0b, '28 : Native'),
    (0x6db6aa30ff73a752ad20b90d5591d49bfff5c7ff, '29 : Maximum Stake'),
    -- known addresses
    (0x8b86cf2684a3af9dd34defc62a18a96deadc40ff, 'TRM'), -- The Retail Mutual
    (0x666b8ebfbf4d5f0ce56962a25635cff563f13161, 'Sherlock'), -- Sherlock
    (0x5b453a19845e7492ee3a0df4ef085d4c75e5752b, 'Liquid Collective'),
    (0x2557fe0959934f3814c6ee72ab46e6687b81b8ca, 'Ensuro'),
    (0x95abc2a62ee543217cf7640b277ba13d056d904a, 'Unity'),
    (0xac0734c62b316041d190438d5d3e5d1359614407, 'Bright Union'),
    (0xe4994082a0e7f38b565e6c5f4afd608de5eddfbb, 'OpenCover [0xe499]'),
    (0x40329f3e27dd3fe228799b4a665f6f104c2ab6b4, 'OpenCover [0x4032]'),
    (0x5f2b6e70aa6a217e9ecd1ed7d0f8f38ce9a348a2, 'OpenCover [0x5f2b]'),
    (0x02bdacb2c3baa8a12d3957f3bd8637d6d2b35f10, 'OpenCover [0x02bd]'),
    -- nansen sleuthing
    (0x834b56ecB16D81f75732F633282a280c53BAa0d0, 'Hugh?'), -- linked acc: 0x7adddc4564b89377237d1e4554f7ceddb1c23a02, 116,362 NXM transfer from twiddle.eth to 0x7adddc (tx: 0x204963318fb7ebc75014d02798becd6db8e4ba94227605e2d7abd579cdc8c0b3)
    (0xa179f67882711957307edf3df0c9ee4f63026a12, 'KR1'), -- linked acc: 0x91715128a71c9c734cdc20e5edeeea02e72e428e, initial NXM dist: 110,776
    (0x25783b67b5e29c48449163db19842b8531fdde43, '1confirmation'), -- Nick Tomaino?
    (0xd7cba5b9a0240770cfd9671961dae064136fa240, 'Version One Ventures'),
    (0x741aa7cfb2c7bf2a1e7d4da2e3df6a56ca4131f3, '7 Siblings [0x741a]'),
    (0x28a55c4b4f9615fde3cdaddf6cc01fcf2e38a6b0, '7 Siblings [0x28a5]'),
    (0x2d089def3b1f95ec8b3052d0d9fa79882554906b, 'Blockchain Capital'),
    (0x28c6c06298d514db089934071355e5743bf21d60, 'Binance 14')
)

select * from labels
