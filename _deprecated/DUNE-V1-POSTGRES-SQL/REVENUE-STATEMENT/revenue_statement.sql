




membership_fees as (
    select date_trunc('month', et.block_time) as month, 
            sum(value/1e18) as member_fees_paid, 
            count(value) as new_member_singups,
            '1' as anchor
    from nexusmutual."MemberRoles_call_payJoiningFee" njf
    left join ethereum.transactions et on et.hash = njf.call_tx_hash
    where call_success = 'true'
    and block_number > 7772147
    group by 1
),