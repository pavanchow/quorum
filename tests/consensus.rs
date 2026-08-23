//! Deterministic end-to-end tests against the public simulation API.
//! Every test uses a fixed seed, so a failure reproduces exactly.

use quorum::{LogEntry, Role, Simulation};

#[test]
fn elects_exactly_one_leader_five_nodes() {
    let mut sim = Simulation::new(5, 1);
    sim.run(2000);

    let leaders: Vec<_> = sim.nodes.iter().filter(|n| n.role == Role::Leader).collect();
    assert_eq!(leaders.len(), 1, "expected exactly one leader, got {leaders:?}");

    let leader = leaders[0];
    let followers_agreeing_on_term = sim
        .nodes
        .iter()
        .filter(|n| n.current_term == leader.current_term)
        .count();
    assert!(
        followers_agreeing_on_term >= sim.quorum_size(),
        "a majority of the cluster should have converged on the leader's term"
    );
}

#[test]
fn elects_exactly_one_leader_three_nodes() {
    let mut sim = Simulation::new(3, 7);
    sim.run(1500);

    let leaders: Vec<_> = sim.nodes.iter().filter(|n| n.role == Role::Leader).collect();
    assert_eq!(leaders.len(), 1, "expected exactly one leader, got {leaders:?}");
}

#[test]
fn client_entry_commits_once_majority_replicates() {
    let mut sim = Simulation::new(5, 2);

    // Let a leader emerge, then submit a client command.
    sim.run(500);
    assert!(sim.leader().is_some(), "no leader emerged before submitting a request");
    sim.submit_client_request("set balance=100").expect("leader should accept request");

    // Give the leader time to replicate and advance its commit index.
    sim.run(500);

    let leader = sim.leader().expect("leader should still be in charge");
    assert_eq!(leader.commit_index, 1, "the entry should be committed once a majority holds it");
    assert_eq!(leader.log[0].command, "set balance=100");

    let replicas_with_entry = sim
        .nodes
        .iter()
        .filter(|n| n.log.first().map(|e| e.command.as_str()) == Some("set balance=100"))
        .count();
    assert!(
        replicas_with_entry >= sim.quorum_size(),
        "a majority of nodes should hold the replicated entry"
    );
}

#[test]
fn candidate_with_stale_log_does_not_win() {
    // Five nodes so quorum is 3. Node 1 (the candidate-to-be) and node 2
    // share a short log; nodes 0, 3, and 4 have a longer log and will
    // correctly refuse to vote for a less up-to-date candidate.
    let mut sim = Simulation::new(5, 99);

    let ahead_log = vec![
        LogEntry { term: 1, command: "a".to_string() },
        LogEntry { term: 1, command: "b".to_string() },
    ];
    let behind_log = vec![LogEntry { term: 1, command: "a".to_string() }];

    for id in [0usize, 3, 4] {
        sim.nodes[id].log = ahead_log.clone();
        sim.nodes[id].current_term = 1;
        sim.nodes[id].election_deadline = 1_000_000; // never times out during this test
    }
    for id in [1usize, 2] {
        sim.nodes[id].log = behind_log.clone();
        sim.nodes[id].current_term = 1;
    }
    // Force node 1 to call an election on the very next step.
    sim.nodes[1].election_deadline = sim.tick;
    sim.nodes[2].election_deadline = 1_000_000;

    sim.run(200);

    assert_ne!(sim.nodes[1].role, Role::Leader, "a candidate behind on the log must not win");
    assert!(
        sim.nodes.iter().all(|n| n.role != Role::Leader),
        "no one should have won this election: the up-to-date majority withholds its vote"
    );
}

#[test]
fn higher_term_forces_step_down_to_follower() {
    let mut sim = Simulation::new(5, 3);

    // Node 0 believes itself leader of term 5.
    sim.nodes[0].role = Role::Leader;
    sim.nodes[0].current_term = 5;
    sim.nodes[0].election_deadline = 1_000_000;
    for id in 1..5 {
        sim.nodes[id].election_deadline = 1_000_000;
    }

    // Node 1 starts an election that will produce term 6, strictly
    // higher than node 0's current term.
    sim.nodes[1].current_term = 5;
    sim.nodes[1].election_deadline = sim.tick;

    sim.run(100);

    assert_eq!(sim.nodes[0].role, Role::Follower, "a higher term must force a step down");
    assert!(sim.nodes[0].current_term >= 6, "the stepped-down node should adopt the higher term");
}

#[test]
fn split_vote_resolves_in_a_later_term() {
    // Four nodes, quorum of 3. Nodes 0 and 1 both campaign in term 1 at
    // the same tick; nodes 2 and 3 have already pre-committed their
    // term-1 votes to opposite candidates, so neither 0 nor 1 can reach
    // a majority (each gets exactly 2 of 4 votes). The election must
    // retry and resolve in a later term.
    let mut sim = Simulation::new(4, 11);

    sim.nodes[2].current_term = 1;
    sim.nodes[2].voted_for = Some(0);
    sim.nodes[3].current_term = 1;
    sim.nodes[3].voted_for = Some(1);

    sim.nodes[0].election_deadline = 5;
    sim.nodes[1].election_deadline = 5;
    sim.nodes[2].election_deadline = 1_000_000;
    sim.nodes[3].election_deadline = 1_000_000;

    sim.run(3000);

    let leaders: Vec<_> = sim.nodes.iter().filter(|n| n.role == Role::Leader).collect();
    assert_eq!(leaders.len(), 1, "the cluster must still converge on exactly one leader");
    assert!(
        leaders[0].current_term >= 2,
        "a split vote should push the winning election into a later term, got term {}",
        leaders[0].current_term
    );
}
