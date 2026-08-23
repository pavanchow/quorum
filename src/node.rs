//! A single Raft node: its role, term, vote, and replicated log.

use crate::message::LogEntry;
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Role::Follower => "follower",
            Role::Candidate => "candidate",
            Role::Leader => "leader",
        };
        write!(f, "{s}")
    }
}

#[derive(Clone, Debug)]
pub struct Node {
    pub id: usize,
    pub role: Role,
    pub current_term: u64,
    pub voted_for: Option<usize>,
    pub log: Vec<LogEntry>,
    /// Number of log entries known to be committed (0 = none).
    pub commit_index: usize,
    pub alive: bool,

    // Volatile leader state, sized to the cluster and only meaningful
    // while `role == Leader`.
    pub next_index: Vec<usize>,
    pub match_index: Vec<usize>,

    // Timers, expressed as absolute simulation ticks.
    pub election_deadline: u64,
    pub heartbeat_deadline: u64,

    pub votes_received: BTreeSet<usize>,
}

impl Node {
    pub fn new(id: usize, cluster_size: usize) -> Self {
        Node {
            id,
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            alive: true,
            next_index: vec![1; cluster_size],
            match_index: vec![0; cluster_size],
            election_deadline: 0,
            heartbeat_deadline: 0,
            votes_received: BTreeSet::new(),
        }
    }

    pub fn last_log_index(&self) -> usize {
        self.log.len()
    }

    pub fn last_log_term(&self) -> u64 {
        self.log.last().map(|e| e.term).unwrap_or(0)
    }

    /// Raft's up-to-date check (section 5.4.1): a candidate's log wins a
    /// tie on term by having at least as many entries.
    pub fn log_is_at_least_as_up_to_date(&self, other_last_term: u64, other_last_index: usize) -> bool {
        if other_last_term != self.last_log_term() {
            other_last_term > self.last_log_term()
        } else {
            other_last_index >= self.last_log_index()
        }
    }

    pub fn become_follower(&mut self, term: u64) {
        self.role = Role::Follower;
        self.current_term = term;
        self.voted_for = None;
        self.votes_received.clear();
    }
}
