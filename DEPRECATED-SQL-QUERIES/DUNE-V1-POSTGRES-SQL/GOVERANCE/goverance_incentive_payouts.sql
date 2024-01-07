with a as (
select date_trunc('day', call_block_time) as day, 
                    sum("output_pendingDAppReward")/1e18 as NXM_gov_rewards_claimed, 
                    count (distinct "_memberAddress") as members_rewarded,
                    sum("output_pendingDAppReward")/1e18/count (distinct "_memberAddress") as avg_NXM_paid_per_member
            from nexusmutual."Governance_call_claimReward"
            where call_success = true
            group by 1
            order by 1
)
select a.day, 
       a.NXM_gov_rewards_claimed, 
       a.members_rewarded, 
       a.avg_NXM_paid_per_member,
       sum (a.NXM_gov_rewards_claimed) over (order by a.day) as cumul_NXM_gov_rewards_claimed
from a
