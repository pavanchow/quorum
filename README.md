<img src="docs/logo.svg" alt="Quorum logo" width="96">

# Quorum: a Raft consensus simulator in Rust

Quorum is a from-scratch, deterministic Raft consensus simulator written in
Rust. It models a Raft cluster with leader election, terms, and log replication
running over a seeded, steppable message bus instead of a real network, so given
the same seed and the same sequence of calls a run always produces the same
trace and a strange outcome is always reproducible. Use it to understand how
Raft works, or as a readable reference implementation of leader election and log
replication.

**[Live demo](https://pavanchow.github.io/quorum/)** · MIT licensed · written in Rust

## What it is

- A node model with the three Raft roles (follower, candidate, leader),
  a current term, a vote record, and a replicated log.
- A deterministic in-simulation message bus carrying `RequestVote`,
  `AppendEntries`, and their replies, with explicit delivery ticks
  instead of wall-clock timing.
- A tiny seeded pseudo-random generator (xorshift64\*) driving every
  election timeout, so nothing about a run depends on the system clock.
- Typed state throughout, no panics in the library code.

## Election and replication

A node becomes a candidate when its election timer expires. It bumps
its term, votes for itself, and asks every peer for a vote. A candidate
whose log is not at least as up to date as a peer's is refused that
peer's vote, which is what keeps a stale replica from ever winning. Once
a candidate collects votes from a strict majority of the cluster in the
same term, it becomes leader for that term.

The leader appends client entries to its own log and replicates them to
every follower with `AppendEntries`, backing off `next_index` on a
mismatch until each follower's log lines up. An entry commits once a
majority of the cluster, leader included, holds it from the leader's
current term. Any message carrying a higher term than a node has seen
forces that node back to follower immediately, even a sitting leader,
which is what keeps two leaders from ever coexisting in the same term.

## Usage

```
cargo run -- run --nodes 5 --steps 2000
```

Prints the elected leader, the committed log, the state of every node,
and a short trace of the events that got the cluster there. `--nodes`,
`--steps`, and `--seed` are all configurable.

```
cargo test
```

Runs the deterministic test suite: a cluster elects exactly one leader,
a client entry commits once a majority replicates it, a candidate with
a stale log cannot win, a node stepping into a higher term steps down,
and a split vote resolves in a later term.

## Try it live

`docs/index.html` is a self-contained, in-browser port of the same
state machine. Step through a run, play it forward, submit a client
request, or kill a node mid-election and watch the cluster recover.

## License

MIT.

By Pavan Nallamothu.
