# Design

Quorum is a deterministic simulation of a Raft cluster. This document
covers the roles, the message types, and the three mechanisms that make
the protocol safe: leader election, log replication, and the term rule.

## Roles

Every node is in exactly one of three roles at a time.

- **Follower**, the default state. A follower does nothing on its own
  except wait, resetting its election timer whenever it hears from a
  leader it recognizes or grants a vote to a candidate.
- **Candidate**, a node that timed out waiting for a leader. It has
  bumped its term, voted for itself, and is waiting on votes from its
  peers.
- **Leader**, a node that won a majority of votes in the current term.
  It sends periodic `AppendEntries` (heartbeats, or heartbeats carrying
  new entries) to every follower and is the only node allowed to accept
  client requests.

## Term

A term is a monotonically increasing integer that both nodes and
messages carry. At most one leader can be elected per term, because
becoming leader requires a majority of votes and a node grants at most
one vote per term. Terms act as a logical clock: whenever a node sees a
term higher than its own, in any message, it adopts that term and steps
down to follower, no matter what role it held a moment before. This is
the mechanism that guarantees at most one leader is ever recognized for
a given term, and it is what lets a partitioned-then-reconnected node
rejoin the cluster safely instead of trying to keep leading a stale term.

## Message types

- `RequestVote { term, candidate_id, last_log_index, last_log_term }`,
  sent by a candidate to every peer at the start of an election.
- `RequestVoteReply { term, vote_granted, voter_id }`,
  a peer's answer. Granted only if the peer has not already voted in
  this term for someone else, and the candidate's log is at least as
  up to date as the peer's own.
- `AppendEntries { term, leader_id, prev_log_index, prev_log_term, entries, leader_commit }`,
  sent by the leader, either as a heartbeat with no entries or
  carrying new log entries to replicate. `prev_log_index` and
  `prev_log_term` let the follower check that its log agrees with the
  leader's up to that point before accepting anything new.
- `AppendEntriesReply { term, success, follower_id, match_index }`,
  the follower's answer. `success` is false when the consistency
  check on `prev_log_index` and `prev_log_term` fails, which tells the
  leader to back off `next_index` for that follower and retry.

## Election

A node's election timer is a randomized interval drawn from a seeded
generator, distinct per node, so that two nodes rarely time out at
exactly the same tick. When it fires, the node becomes a candidate,
increments its term, votes for itself, and sends `RequestVote` to every
peer carrying the index and term of its last log entry. A peer grants
the vote only if it has not already voted for someone else in that term
and the candidate's log is at least as up to date as its own: either the
candidate's last log term is strictly higher, or the terms match and the
candidate's log is at least as long. This up-to-date check is what
prevents a node with a stale or shorter log from ever collecting a
majority, even if every other node has timed out and is willing to vote.

If two candidates split the vote and neither reaches a majority, both
time out again on their own randomized schedules and retry in a later
term. Because the timers are randomized, the retry rarely splits a
second time, and the cluster converges quickly.

## Replication

Once elected, a leader initializes `next_index` for every follower to
one past the end of its own log, and starts sending `AppendEntries`.
Each `AppendEntries` carries the entries the leader believes the
follower is missing, plus the index and term of the entry immediately
before them. If the follower's log does not have a matching entry at
that position, it rejects the request, and the leader decrements
`next_index` for that follower and retries, walking backward until it
finds a point where the logs agree, then replicates forward from there.
On a successful append, the follower truncates any conflicting tail and
appends the new entries, and reports back the resulting `match_index`.

## Commit

The leader tracks `match_index` for every follower: the highest log
index it knows that follower has durably stored. An entry at index `i`
is committed once a strict majority of the cluster, leader included,
has `match_index >= i`, and the entry at that index was written in the
leader's own current term. That last condition matters: a leader never
commits an entry purely by counting replicas of an entry from an older
term, because a later leader could still overwrite it. It commits an
entry from a prior term only indirectly, by committing a new entry from
its own term that comes after it in the log.

## Safety, summarized

- At most one leader per term, because a vote is granted at most once
  per term and leadership requires a majority.
- A candidate whose log is behind cannot win, because the up-to-date
  check denies its vote request.
- A node in a stale term always steps down on contact with a higher
  term, whether from a `RequestVote` or an `AppendEntries`.
- A committed entry is never lost, because committing requires a
  majority to hold it, and any future leader must itself have been
  elected by a majority, which guarantees at least one of its voters
  already has that entry.

By Pavan Nallamothu.
