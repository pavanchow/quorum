use clap::{Parser, Subcommand};
use quorum::Simulation;

#[derive(Parser)]
#[command(name = "quorum", about = "A deterministic Raft consensus simulator")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a cluster simulation and report the outcome.
    Run {
        /// Number of nodes in the cluster.
        #[arg(long, default_value_t = 5)]
        nodes: usize,

        /// Number of simulation ticks to run.
        #[arg(long, default_value_t = 2000)]
        steps: u64,

        /// Seed for the deterministic timeout generator.
        #[arg(long, default_value_t = 42)]
        seed: u64,
    },
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Commands::Run { nodes, steps, seed } => run_scenario(nodes, steps, seed),
    }
}

fn run_scenario(nodes: usize, steps: u64, seed: u64) {
    if nodes == 0 {
        eprintln!("error: --nodes must be at least 1");
        std::process::exit(1);
    }

    let mut sim = Simulation::new(nodes, seed);
    let client_commands = ["set x=1", "set y=2", "set z=3"];
    let mut submitted = 0usize;

    for _ in 0..steps {
        sim.step();
        if submitted < client_commands.len() && sim.leader().is_some() {
            if sim.submit_client_request(client_commands[submitted]).is_ok() {
                submitted += 1;
            }
        }
    }

    println!("quorum run --nodes {nodes} --steps {steps} --seed {seed}");
    println!();

    match sim.leader() {
        Some(leader) => {
            println!(
                "elected leader: node {} (term {}, log length {})",
                leader.id,
                leader.current_term,
                leader.log.len()
            );
        }
        None => println!("no leader elected within {steps} steps"),
    }

    let reference = sim
        .nodes
        .iter()
        .max_by_key(|n| n.commit_index)
        .expect("simulation always has at least one node");

    println!("committed entries ({}):", reference.commit_index);
    for (i, entry) in reference.log.iter().take(reference.commit_index).enumerate() {
        println!("  {}: term {} -> {}", i + 1, entry.term, entry.command);
    }

    println!();
    println!("cluster state:");
    for node in &sim.nodes {
        let status = if node.alive { "up" } else { "down" };
        println!(
            "  node {}: {:<9} term {:<4} log {:<3} commit {:<3} [{}]",
            node.id,
            node.role.to_string(),
            node.current_term,
            node.log.len(),
            node.commit_index,
            status
        );
    }

    println!();
    println!("event trace (last {} of {}):", 20.min(sim.events.len()), sim.events.len());
    for event in sim.events.iter().rev().take(20).collect::<Vec<_>>().into_iter().rev() {
        println!("  {event}");
    }
}
