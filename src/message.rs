//! Message types exchanged between nodes: RequestVote, AppendEntries,
//! and their replies. These are the only ways two nodes ever interact.

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogEntry {
    pub term: u64,
    pub command: String,
}

#[derive(Clone, Debug)]
pub enum Message {
    RequestVote {
        term: u64,
        candidate_id: usize,
        last_log_index: usize,
        last_log_term: u64,
    },
    RequestVoteReply {
        term: u64,
        vote_granted: bool,
        voter_id: usize,
    },
    AppendEntries {
        term: u64,
        leader_id: usize,
        prev_log_index: usize,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: usize,
    },
    AppendEntriesReply {
        term: u64,
        success: bool,
        follower_id: usize,
        // The follower's log length after applying this AppendEntries,
        // used by the leader to advance next_index / match_index.
        match_index: usize,
    },
}

/// An in-flight message with a scheduled delivery tick. The simulation
/// bus holds a list of these instead of delivering anything instantly,
/// which is what makes the network model explicit and steppable.
#[derive(Clone, Debug)]
pub struct Envelope {
    pub from: usize,
    pub to: usize,
    pub deliver_at: u64,
    pub message: Message,
}
