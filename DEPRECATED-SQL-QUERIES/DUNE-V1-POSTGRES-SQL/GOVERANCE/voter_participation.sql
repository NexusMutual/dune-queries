with  a as (
        select "_proposalId" as proposal_id,
        count(case when "_solutionChosen" = 1 then 1 end) as vote_yes, 
        count(case when "_solutionChosen" = 0 then 1 end) as vote_no,
        count("_solutionChosen") as total_votes
from nexusmutual."Governance_call_submitVote"
where call_success = true
group by 1
order by 1
),

     b as (
        select "_proposalId" as proposal_id, "_categoryId" as category_id, "_incentive"/1e18 as nxm_incentive
from nexusmutual."Governance_call_categorizeProposal"
where call_success = true
)

select a.proposal_id, 
       b.category_id, 
       b.nxm_incentive, 
       case when b.nxm_incentive > 0 then 'yes' else 'no' end as incentivized, 
       a.vote_yes, 
       a.vote_no, 
       a.total_votes
from a
left join b
on a.proposal_id = b.proposal_id
order by 1