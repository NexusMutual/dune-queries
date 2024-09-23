select space, count(*) from dune.shot.dataset_follows where space like '%nexusmutual.eth%' group by 1

select * from dune.shot.dataset_spaces_view where id = 'community.nexusmutual.eth'
select count(distinct follower) from dune.shot.dataset_follows where space = 'community.nexusmutual.eth'


select
  title,
  "type",
  start,
  "end",
  choices,
  votes,
  quorum,
  scores_state,
  scores_total,
  scores_by_strategy,
  snapshot,
  author,
  discussion,
  plugins
from dune.shot.dataset_proposals_view
where space = 'community.nexusmutual.eth'


select
  proposal,
  voter,
  choice,
  reason,
  cb,
  vp,
  vp_state,
  vp_by_strategy,
  created
from dune.shot.dataset_votes_view
where space = 'community.nexusmutual.eth'

