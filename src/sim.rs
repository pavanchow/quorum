//! The deterministic simulation: a message bus, a set of nodes, and a
//! tick counter. Nothing here touches the wall clock or the OS. Given
//! the same seed and the same sequence of calls, a run always produces
//! the same trace.

use crate::message::{Envelope, LogEntry, Message};
use crate::node::{Node, Role};
use crate::rng::Rng;

pub struct Simulation {
    pub nodes: Vec<Node>,
    pub tick: u64,
    pub events: Vec<String>,
    inbox: Vec<Envelope>,
    rng: Rng,
    election_timeout_min: u64,
    election_timeout_max: u64,
    heartbeat_interval: u64,
    network_delay: u64,
}

impl Simulation {
    pub fn new(node_count: usize, seed: u64) -> Self {
        let election_timeout_min = 150;
        let election_timeout_max = 300;
        let mut rng = Rng::new(seed);
        let mut nodes: Vec<Node> = (0..node_count).map(|id| Node::new(id, node_count)).collect();
        for node in nodes.iter_mut() {
            node.election_deadline = rng.range(election_timeout_min, election_timeout_max);
        }
        Simulation {
            nodes,
            tick: 0,
            events: Vec::new(),
            inbox: Vec::new(),
            rng,
            election_timeout_min,
            election_timeout_max,
            heartbeat_interval: 50,
            network_delay: 10,
        }
    }

    pub fn quorum_size(&self) -> usize {
        self.nodes.len() / 2 + 1
    }

    pub fn leader(&self) -> Option<&Node> {
        self.nodes.iter().find(|n| n.alive && n.role == Role::Leader)
    }

    pub fn leaders_in_term(&self, term: u64) -> Vec<usize> {
        self.nodes
            .iter()
            .filter(|n| n.alive && n.role == Role::Leader && n.current_term == term)
            .map(|n| n.id)
            .collect()
    }

    /// Advances the simulation by exactly one tick: deliver due
    /// messages, then let timers fire.
    pub fn step(&mut self) {
        self.tick += 1;

        let mut due = Vec::new();
        let mut remaining = Vec::new();
        for env in self.inbox.drain(..) {
            if env.deliver_at <= self.tick {
                due.push(env);
            } else {
                remaining.push(env);
            }
        }
        self.inbox = remaining;
        for env in due {
            self.deliver(env);
        }

        let n = self.nodes.len();
        for id in 0..n {
            if self.nodes[id].alive
                && self.nodes[id].role != Role::Leader
                && self.nodes[id].election_deadline <= self.tick
            {
                self.start_election(id);
            }
        }
        for id in 0..n {
            if self.nodes[id].alive
                && self.nodes[id].role == Role::Leader
                && self.nodes[id].heartbeat_deadline <= self.tick
            {
                self.send_heartbeats(id);
                self.nodes[id].heartbeat_deadline = self.tick + self.heartbeat_interval;
            }
        }
    }

    pub fn run(&mut self, steps: u64) {
        for _ in 0..steps {
            self.step();
        }
    }

    /// Appends a command to the current leader's log, if there is one.
    pub fn submit_client_request(&mut self, command: &str) -> Result<(), &'static str> {
        let leader_id = self
            .nodes
            .iter()
            .find(|n| n.alive && n.role == Role::Leader)
            .map(|n| n.id);
        match leader_id {
            Some(id) => {
                let term = self.nodes[id].current_term;
                self.nodes[id].log.push(LogEntry {
                    term,
                    command: command.to_string(),
                });
                let log_len = self.nodes[id].log.len();
                self.nodes[id].match_index[id] = log_len;
                self.log_event(format!(
                    "client request '{command}' appended at node {id} (log index {log_len})"
                ));
                Ok(())
            }
            None => Err("no leader available"),
        }
    }

    pub fn kill_node(&mut self, id: usize) {
        self.nodes[id].alive = false;
        self.log_event(format!("node {id} killed"));
    }

    pub fn revive_node(&mut self, id: usize) {
        self.nodes[id].alive = true;
        self.nodes[id].role = Role::Follower;
        self.reset_election_timer(id);
        self.log_event(format!("node {id} revived"));
    }

    fn reset_election_timer(&mut self, id: usize) {
        let timeout = self.rng.range(self.election_timeout_min, self.election_timeout_max);
        self.nodes[id].election_deadline = self.tick + timeout;
    }

    fn log_event(&mut self, msg: String) {
        self.events.push(format!("[tick {}] {}", self.tick, msg));
    }

    fn send(&mut self, from: usize, to: usize, message: Message) {
        if !self.nodes[to].alive {
            return;
        }
        self.inbox.push(Envelope {
            from,
            to,
            deliver_at: self.tick + self.network_delay,
            message,
        });
    }

    fn deliver(&mut self, env: Envelope) {
        let to = env.to;
        match env.message {
            Message::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => self.handle_request_vote(to, term, candidate_id, last_log_index, last_log_term),
            Message::RequestVoteReply {
                term,
                vote_granted,
                voter_id,
            } => self.handle_request_vote_reply(to, term, vote_granted, voter_id),
            Message::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => self.handle_append_entries(
                to,
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            ),
            Message::AppendEntriesReply {
                term,
                success,
                follower_id,
                match_index,
            } => self.handle_append_entries_reply(to, term, success, follower_id, match_index),
        }
    }

    fn start_election(&mut self, id: usize) {
        if !self.nodes[id].alive {
            return;
        }
        self.nodes[id].role = Role::Candidate;
        self.nodes[id].current_term += 1;
        self.nodes[id].voted_for = Some(id);
        self.nodes[id].votes_received.clear();
        self.nodes[id].votes_received.insert(id);
        let term = self.nodes[id].current_term;
        self.log_event(format!("node {id} starts election for term {term}"));
        self.reset_election_timer(id);

        let last_log_index = self.nodes[id].last_log_index();
        let last_log_term = self.nodes[id].last_log_term();
        let n = self.nodes.len();
        for peer in 0..n {
            if peer == id {
                continue;
            }
            self.send(
                id,
                peer,
                Message::RequestVote {
                    term,
                    candidate_id: id,
                    last_log_index,
                    last_log_term,
                },
            );
        }
        // A single-node cluster (or one that already has a majority of
        // votes, e.g. a cluster of one) wins immediately.
        if self.nodes[id].votes_received.len() >= self.quorum_size() {
            self.become_leader(id);
        }
    }

    fn become_leader(&mut self, id: usize) {
        let n = self.nodes.len();
        let log_len = self.nodes[id].log.len();
        self.nodes[id].role = Role::Leader;
        self.nodes[id].next_index = vec![log_len + 1; n];
        self.nodes[id].match_index = vec![0; n];
        self.nodes[id].match_index[id] = log_len;
        let term = self.nodes[id].current_term;
        self.log_event(format!("node {id} becomes leader for term {term}"));
        self.send_heartbeats(id);
        self.nodes[id].heartbeat_deadline = self.tick + self.heartbeat_interval;
    }

    fn send_heartbeats(&mut self, id: usize) {
        if !self.nodes[id].alive || self.nodes[id].role != Role::Leader {
            return;
        }
        let n = self.nodes.len();
        let term = self.nodes[id].current_term;
        let log = self.nodes[id].log.clone();
        let leader_commit = self.nodes[id].commit_index;
        let next_index = self.nodes[id].next_index.clone();
        for peer in 0..n {
            if peer == id {
                continue;
            }
            let prev_log_index = next_index[peer].saturating_sub(1);
            let prev_log_term = if prev_log_index > 0 {
                log[prev_log_index - 1].term
            } else {
                0
            };
            let entries: Vec<LogEntry> = if next_index[peer] <= log.len() {
                log[next_index[peer] - 1..].to_vec()
            } else {
                Vec::new()
            };
            self.send(
                id,
                peer,
                Message::AppendEntries {
                    term,
                    leader_id: id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                },
            );
        }
    }

    fn handle_request_vote(
        &mut self,
        id: usize,
        term: u64,
        candidate_id: usize,
        last_log_index: usize,
        last_log_term: u64,
    ) {
        if !self.nodes[id].alive {
            return;
        }
        if term > self.nodes[id].current_term {
            self.nodes[id].become_follower(term);
        }
        let grant = {
            let node = &self.nodes[id];
            if term < node.current_term {
                false
            } else {
                let log_ok = node.log_is_at_least_as_up_to_date(last_log_term, last_log_index);
                let can_vote = node.voted_for.is_none() || node.voted_for == Some(candidate_id);
                log_ok && can_vote
            }
        };
        if grant {
            self.nodes[id].voted_for = Some(candidate_id);
            self.reset_election_timer(id);
        }
        let reply_term = self.nodes[id].current_term;
        self.log_event(format!(
            "node {id} {} vote to node {candidate_id} for term {reply_term}",
            if grant { "grants" } else { "denies" }
        ));
        self.send(
            id,
            candidate_id,
            Message::RequestVoteReply {
                term: reply_term,
                vote_granted: grant,
                voter_id: id,
            },
        );
    }

    fn handle_request_vote_reply(&mut self, id: usize, term: u64, vote_granted: bool, voter_id: usize) {
        if !self.nodes[id].alive {
            return;
        }
        if term > self.nodes[id].current_term {
            self.nodes[id].become_follower(term);
            return;
        }
        if self.nodes[id].role != Role::Candidate || term != self.nodes[id].current_term {
            return;
        }
        if vote_granted {
            self.nodes[id].votes_received.insert(voter_id);
            if self.nodes[id].votes_received.len() >= self.quorum_size() {
                self.become_leader(id);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_append_entries(
        &mut self,
        id: usize,
        term: u64,
        leader_id: usize,
        prev_log_index: usize,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: usize,
    ) {
        if !self.nodes[id].alive {
            return;
        }
        if term >= self.nodes[id].current_term {
            if self.nodes[id].role != Role::Follower || term > self.nodes[id].current_term {
                self.nodes[id].role = Role::Follower;
            }
            self.nodes[id].current_term = term;
            self.reset_election_timer(id);
        }
        let current_term = self.nodes[id].current_term;
        if term < current_term {
            self.send(
                id,
                leader_id,
                Message::AppendEntriesReply {
                    term: current_term,
                    success: false,
                    follower_id: id,
                    match_index: 0,
                },
            );
            return;
        }

        let success = {
            let node = &self.nodes[id];
            if prev_log_index > 0 {
                prev_log_index <= node.log.len() && node.log[prev_log_index - 1].term == prev_log_term
            } else {
                true
            }
        };

        let match_index = if success {
            let node = &mut self.nodes[id];
            for (i, entry) in entries.iter().enumerate() {
                let idx = prev_log_index + i;
                if node.log.len() > idx {
                    if node.log[idx].term != entry.term {
                        node.log.truncate(idx);
                        node.log.push(entry.clone());
                    }
                } else {
                    node.log.push(entry.clone());
                }
            }
            if leader_commit > node.commit_index {
                node.commit_index = leader_commit.min(node.log.len());
            }
            prev_log_index + entries.len()
        } else {
            0
        };

        self.send(
            id,
            leader_id,
            Message::AppendEntriesReply {
                term: current_term,
                success,
                follower_id: id,
                match_index,
            },
        );
    }

    fn handle_append_entries_reply(
        &mut self,
        id: usize,
        term: u64,
        success: bool,
        follower_id: usize,
        match_index: usize,
    ) {
        if !self.nodes[id].alive {
            return;
        }
        if term > self.nodes[id].current_term {
            self.nodes[id].become_follower(term);
            return;
        }
        if self.nodes[id].role != Role::Leader || term != self.nodes[id].current_term {
            return;
        }
        if success {
            self.nodes[id].match_index[follower_id] = match_index;
            self.nodes[id].next_index[follower_id] = match_index + 1;
            self.advance_commit_index(id);
        } else if self.nodes[id].next_index[follower_id] > 1 {
            self.nodes[id].next_index[follower_id] -= 1;
        }
    }

    fn advance_commit_index(&mut self, id: usize) {
        let n = self.nodes.len();
        let quorum = self.quorum_size();
        let log_len = self.nodes[id].log.len();
        let current_term = self.nodes[id].current_term;
        let match_index = self.nodes[id].match_index.clone();
        let start = self.nodes[id].commit_index + 1;
        let mut new_commit = self.nodes[id].commit_index;
        for idx in (start..=log_len).rev() {
            let mut count = 1;
            for peer in 0..n {
                if peer != id && match_index[peer] >= idx {
                    count += 1;
                }
            }
            if count >= quorum && self.nodes[id].log[idx - 1].term == current_term {
                new_commit = idx;
                break;
            }
        }
        if new_commit > self.nodes[id].commit_index {
            self.nodes[id].commit_index = new_commit;
            self.log_event(format!("node {id} advances commit index to {new_commit}"));
        }
    }
}
