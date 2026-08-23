//! Quorum: a deterministic Raft consensus simulator.
//!
//! The simulation never touches the wall clock. Timeouts come from a
//! seeded random generator, message delivery is scheduled on an
//! explicit tick counter, and every run is a pure function of its seed
//! and its sequence of calls.

pub mod message;
pub mod node;
pub mod rng;
pub mod sim;

pub use message::{LogEntry, Message};
pub use node::{Node, Role};
pub use sim::Simulation;
