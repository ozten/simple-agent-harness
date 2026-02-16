mod adapters;
mod boundary_violation;
mod brief;
mod circular_dep;
mod commit;
mod compress;
mod config;
mod coordinator;
mod cycle_detect;
mod data_dir;
mod db;
mod estimation;
mod expansion_event;
mod fan_in;
mod file_resolution;
mod gc;
mod god_file;
mod hooks;
mod import_graph;
mod improve;
mod ingest;
mod integrator;
mod intent;
mod metadata_regen;
mod metrics;
mod metrics_cmd;
mod migrate;
mod module_detect;
mod pool;
mod prompt;
mod proposal_validation;
mod public_api;
mod ratelimit;
mod retention;
mod retry;
mod runner;
mod scheduler;
mod session;
mod signal_correlator;
mod signals;
mod singleton;
mod status;
mod structural_metrics;
mod task_manifest;
mod watchdog;
mod worktree;

use clap::{Parser, Subcommand};
use config::{CliOverrides, HarnessConfig};
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

/// A Rust CLI tool that runs an AI coding agent in a supervised loop:
/// dispatch a prompt, monitor the session, enforce health invariants,
/// collect metrics, and repeat.
#[derive(Parser, Debug)]
#[command(name = "blacksmith", version, about)]
pub struct Cli {
    /// Override max iterations (default: from config)
    #[arg(value_name = "MAX_ITERATIONS")]
    max_iterations: Option<u32>,

    /// Config file path
    #[arg(short, long, default_value = "blacksmith.toml")]
    config: PathBuf,

    /// Prompt file path (overrides config)
    #[arg(short, long)]
    prompt: Option<PathBuf>,

    /// Output directory (overrides config)
    #[arg(short, long, global = true)]
    output_dir: Option<PathBuf>,

    /// Stale timeout in minutes (overrides config)
    #[arg(long)]
    timeout: Option<u64>,

    /// Max empty retries (overrides config)
    #[arg(long)]
    retries: Option<u32>,

    /// Validate config and print resolved settings, don't run
    #[arg(long)]
    dry_run: bool,

    /// Extra logging (watchdog checks, retry decisions)
    #[arg(short, long, global = true)]
    verbose: bool,

    /// Suppress per-iteration banners, only errors and summary
    #[arg(short, long, global = true)]
    quiet: bool,

    /// Print current loop state and exit
    #[arg(long)]
    status: bool,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Initialize the .blacksmith/ data directory
    Init,
    /// Generate performance brief for prompt injection
    Brief,
    /// Manage improvement records (institutional memory)
    Improve {
        #[command(subcommand)]
        action: ImproveAction,
    },
    /// View and manage session metrics
    Metrics {
        #[command(subcommand)]
        action: MetricsAction,
    },
    /// Run session file cleanup (retention + compression)
    Gc {
        /// Show what would be cleaned without making changes
        #[arg(long)]
        dry_run: bool,
        /// Compress all eligible files and delete beyond retention
        #[arg(long)]
        aggressive: bool,
    },
    /// Migrate legacy files into .blacksmith/ structure
    Migrate {
        /// Consolidate V2 files (output_prefix-*.jsonl, counter, db) into .blacksmith/
        #[arg(long)]
        consolidate: bool,
    },
    /// Manage concurrent agent workers
    Workers {
        #[command(subcommand)]
        action: WorkersAction,
    },
    /// View integration history and retry failed integrations
    Integration {
        #[command(subcommand)]
        action: IntegrationAction,
    },
    /// Show time estimates for completing remaining beads
    Estimate {
        /// Number of parallel workers (overrides config workers.max)
        #[arg(long)]
        workers: Option<u32>,
    },
    /// Inspect and test agent adapters
    Adapter {
        #[command(subcommand)]
        action: AdapterAction,
    },
}

#[derive(Subcommand, Debug)]
enum MetricsAction {
    /// Ingest a JSONL session file into the metrics database
    Log {
        /// Path to the JSONL session output file
        file: PathBuf,
    },
    /// Display a dashboard of recent session metrics
    Status {
        /// Number of recent sessions to show (default: 10)
        #[arg(long, default_value = "10")]
        last: i64,
    },
    /// Show configured targets vs recent performance
    Targets {
        /// Number of recent sessions to evaluate (default: 10)
        #[arg(long, default_value = "10")]
        last: i64,
    },
    /// Import data from a V1 self-improvement database
    Migrate {
        /// Path to the V1 self-improvement.db file
        #[arg(long)]
        from: PathBuf,
    },
    /// Rebuild observations from events (drops and recreates)
    Rebuild,
    /// Export all observations
    Export {
        /// Output format: json or csv (default: json)
        #[arg(long, default_value = "json")]
        format: String,
    },
    /// Query events for a specific metric kind
    Query {
        /// Event kind to query (e.g. turns.total, cost.estimate_usd)
        kind: String,
        /// Limit to last N sessions
        #[arg(long)]
        last: Option<i64>,
        /// Aggregation mode: avg or trend
        #[arg(long)]
        aggregate: Option<String>,
    },
    /// Dump raw events
    Events {
        /// Filter to a specific session number
        #[arg(long)]
        session: Option<i64>,
    },
    /// Re-ingest historical sessions (after extraction rules change)
    Reingest {
        /// Re-ingest the N most recent sessions
        #[arg(long)]
        last: Option<u64>,
        /// Re-ingest all sessions
        #[arg(long)]
        all: bool,
    },
    /// Show per-bead timing report
    Beads,
}

#[derive(Subcommand, Debug)]
enum WorkersAction {
    /// Show current worker pool state
    Status,
    /// Kill a specific worker by its worker ID
    Kill {
        /// Worker ID to kill (from the worker_id column)
        worker_id: i64,
    },
}

#[derive(Subcommand, Debug)]
enum IntegrationAction {
    /// Show integration history
    Log {
        /// Number of recent integrations to show (default: all)
        #[arg(long)]
        last: Option<i64>,
    },
    /// Retry a failed integration by bead ID
    Retry {
        /// Bead ID to retry (e.g. beads-abc)
        bead_id: String,
    },
    /// Rollback an integrated task (git revert + manifest reversal)
    Rollback {
        /// Bead ID to rollback (e.g. beads-abc)
        bead_id: String,
        /// Force rollback even if other tasks depend on this one (entangled)
        #[arg(long)]
        force: bool,
    },
}

#[derive(Subcommand, Debug)]
enum AdapterAction {
    /// Show the detected/configured adapter for the current project
    Info,
    /// List all available adapters with their supported metrics
    List,
    /// Parse a file with the configured adapter and show extracted metrics
    Test {
        /// Path to the session output file to parse
        file: PathBuf,
    },
}

#[derive(Subcommand, Debug)]
enum ImproveAction {
    /// Add a new improvement observation
    Add {
        /// Title of the improvement
        title: String,

        /// Category (e.g. workflow, cost, reliability, performance, code-quality)
        #[arg(long)]
        category: String,

        /// Detailed description (markdown)
        #[arg(long)]
        body: Option<String>,

        /// Evidence/context (e.g. "sessions 340-348 showed high narration turns")
        #[arg(long)]
        context: Option<String>,

        /// Comma-separated tags
        #[arg(long)]
        tags: Option<String>,
    },
    /// List improvements with optional filters
    List {
        /// Filter by status (open, promoted, dismissed, revisit, validated)
        #[arg(long)]
        status: Option<String>,

        /// Filter by category
        #[arg(long)]
        category: Option<String>,
    },
    /// Show a single improvement by ref (e.g. R1)
    Show {
        /// Improvement ref (e.g. R1, R42)
        #[arg(name = "REF")]
        ref_id: String,
    },
    /// Update an improvement's fields
    Update {
        /// Improvement ref (e.g. R1, R42)
        #[arg(name = "REF")]
        ref_id: String,

        /// New status
        #[arg(long)]
        status: Option<String>,

        /// New body text
        #[arg(long)]
        body: Option<String>,

        /// New context text
        #[arg(long)]
        context: Option<String>,
    },
    /// Promote an improvement (shorthand for --status=promoted)
    Promote {
        /// Improvement ref (e.g. R1, R42)
        #[arg(name = "REF")]
        ref_id: String,
    },
    /// Dismiss an improvement (shorthand for --status=dismissed)
    Dismiss {
        /// Improvement ref (e.g. R1, R42)
        #[arg(name = "REF")]
        ref_id: String,

        /// Reason for dismissal (stored in meta JSON)
        #[arg(long)]
        reason: Option<String>,
    },
    /// Search improvements by keyword across title, body, and context
    Search {
        /// Search query
        query: String,
    },
}

impl Cli {
    /// Extract the override-able fields into a CliOverrides struct.
    fn to_overrides(&self) -> CliOverrides {
        CliOverrides {
            max_iterations: self.max_iterations,
            prompt: self.prompt.clone(),
            output_dir: self.output_dir.clone(),
            timeout: self.timeout,
            retries: self.retries,
        }
    }
}

/// Handle `blacksmith integration log [--last N]` — show integration history.
fn handle_integration_log(
    db_path: &std::path::Path,
    last: Option<i64>,
) -> Result<(), Box<dyn std::error::Error>> {
    let conn = db::open_or_create(db_path)?;
    let entries = db::recent_integration_log(&conn, last)?;

    if entries.is_empty() {
        println!("No integration history.");
        return Ok(());
    }

    println!(
        "{:<8} {:<24} {:<12} {:<22} {:<24} STATUS",
        "ASSIGN", "BEAD", "COMMIT", "MERGED AT", "MANIFEST"
    );
    println!("{}", "-".repeat(100));

    for entry in &entries {
        let commit_short = if entry.merge_commit.len() >= 7 {
            &entry.merge_commit[..7]
        } else {
            &entry.merge_commit
        };

        let manifest = entry.manifest_entries_applied.as_deref().unwrap_or("-");

        let reconciliation = if entry.reconciliation_run {
            " [reconciled]"
        } else {
            ""
        };

        println!(
            "{:<8} {:<24} {:<12} {:<22} {:<24} {}{}",
            entry.assignment_id,
            entry.bead_id,
            commit_short,
            entry.merged_at,
            manifest,
            entry.status,
            reconciliation,
        );
    }

    println!("\n{} integration(s).", entries.len());
    Ok(())
}

/// Handle `blacksmith integration retry <bead-id>` — retry a failed integration.
fn handle_integration_retry(
    db_path: &std::path::Path,
    config: &HarnessConfig,
    bead_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let conn = db::open_or_create(db_path)?;

    let assignment = db::find_failed_assignment_by_bead(&conn, bead_id)?;
    let assignment = match assignment {
        Some(a) => a,
        None => {
            eprintln!("No failed integration found for bead '{bead_id}'.");
            eprintln!("Use `blacksmith integration log` to see integration history.");
            std::process::exit(1);
        }
    };

    let worktree_path = std::path::PathBuf::from(&assignment.worktree_path);
    if !worktree_path.exists() {
        eprintln!("Worktree no longer exists: {}", worktree_path.display());
        eprintln!("The worktree may have been cleaned up. A fresh integration is needed.");
        std::process::exit(1);
    }

    // Reset the assignment status to 'completed' so it's eligible for integration
    db::update_worker_assignment_status(&conn, assignment.id, "completed", None)?;

    let repo_dir = std::env::current_dir()?;
    let base_branch = config.workers.base_branch.clone();

    let integration_agent = config.agent.resolved_integration();
    let agent_config = if integration_agent.command.is_empty() {
        None
    } else {
        Some(integration_agent)
    };

    let queue = integrator::IntegrationQueue::new(repo_dir, base_branch);
    let mut cb = integrator::CircuitBreaker::new();

    let result = queue.integrate(
        assignment.worker_id as u32,
        assignment.id,
        bead_id,
        &worktree_path,
        &conn,
        agent_config.as_ref(),
        &mut cb,
    );

    if result.success {
        println!(
            "Integration succeeded for '{bead_id}' (commit: {}).",
            result.merge_commit.as_deref().unwrap_or("unknown")
        );
    } else {
        eprintln!(
            "Integration retry failed for '{bead_id}': {}",
            result.failure_reason.as_deref().unwrap_or("unknown error")
        );
        std::process::exit(1);
    }

    Ok(())
}

/// Handle `blacksmith integration rollback <bead-id>` — rollback an integrated task.
fn handle_integration_rollback(
    db_path: &std::path::Path,
    config: &HarnessConfig,
    bead_id: &str,
    force: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let conn = db::open_or_create(db_path)?;

    // Find the integration log entry for this bead
    let entry = db::find_integration_by_bead(&conn, bead_id)?;
    let entry = match entry {
        Some(e) => e,
        None => {
            eprintln!("No integration found for bead '{bead_id}'.");
            eprintln!("Use `blacksmith integration log` to see integration history.");
            std::process::exit(1);
        }
    };

    if entry.status == "rolled_back" {
        eprintln!("Bead '{bead_id}' has already been rolled back.");
        std::process::exit(1);
    }

    let repo_dir = std::env::current_dir()?;
    let base_branch = config.workers.base_branch.clone();

    let queue = integrator::IntegrationQueue::new(repo_dir, base_branch);
    let result = queue.rollback(
        bead_id,
        &entry.merge_commit,
        entry.assignment_id,
        entry.manifest_entries_applied.as_deref(),
        &conn,
        force,
    )?;

    if result.success {
        println!(
            "Rollback succeeded for '{bead_id}' (revert commit: {}).",
            result.reverted_commit.as_deref().unwrap_or("unknown")
        );
        if result.manifest_entries_reversed > 0 {
            println!(
                "  {} manifest entries reversed.",
                result.manifest_entries_reversed
            );
        }
    } else {
        eprintln!("Rollback blocked for '{bead_id}': entangled with other tasks.");
        for blocked_id in &result.blocked_by {
            eprintln!("  - {blocked_id} imports from this bead's module");
        }
        eprintln!("\nUse --force to rollback anyway (may break dependent tasks).");
        std::process::exit(1);
    }

    Ok(())
}

/// Handle `blacksmith workers status` — show current worker pool state.
fn handle_workers_status(db_path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    let conn = db::open_or_create(db_path)?;
    let assignments = db::active_worker_assignments(&conn)?;

    if assignments.is_empty() {
        println!("No active workers.");
        return Ok(());
    }

    // Print header
    println!(
        "{:<10} {:<8} {:<24} {:<14} {:<20} WORKTREE",
        "WORKER", "ASSIGN", "BEAD", "STATUS", "STARTED"
    );
    println!("{}", "-".repeat(96));

    for wa in &assignments {
        // Calculate elapsed time from started_at
        let elapsed = chrono::DateTime::parse_from_rfc3339(&wa.started_at)
            .ok()
            .map(|start| {
                let duration = chrono::Utc::now() - start.with_timezone(&chrono::Utc);
                let secs = duration.num_seconds().max(0);
                let hours = secs / 3600;
                let mins = (secs % 3600) / 60;
                if hours > 0 {
                    format!("{}h {}m", hours, mins)
                } else {
                    format!("{}m", mins)
                }
            })
            .unwrap_or_else(|| wa.started_at.clone());

        println!(
            "{:<10} {:<8} {:<24} {:<14} {:<20} {}",
            wa.worker_id, wa.id, wa.bead_id, wa.status, elapsed, wa.worktree_path
        );
    }

    println!("\n{} active worker(s).", assignments.len());
    Ok(())
}

/// Handle `blacksmith workers kill <worker-id>` — kill a specific worker process.
///
/// Finds active worker assignments for the given worker_id, locates agent processes
/// running in the worker's worktree directory, and sends SIGTERM to their process groups.
fn handle_workers_kill(
    db_path: &std::path::Path,
    worker_id: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let conn = db::open_or_create(db_path)?;
    let assignments = db::active_worker_assignments(&conn)?;

    let worker_assignments: Vec<_> = assignments
        .iter()
        .filter(|wa| wa.worker_id == worker_id)
        .collect();

    if worker_assignments.is_empty() {
        eprintln!("No active assignment found for worker {worker_id}.");
        std::process::exit(1);
    }

    for wa in &worker_assignments {
        let worktree = &wa.worktree_path;
        println!(
            "Killing worker {} (bead: {}, worktree: {})...",
            worker_id, wa.bead_id, worktree
        );

        // Find processes whose CWD is the worktree using lsof
        let output = std::process::Command::new("lsof")
            .args(["+D", worktree, "-t"])
            .output();

        let mut killed = false;

        if let Ok(output) = output {
            let pids_str = String::from_utf8_lossy(&output.stdout);
            for pid_str in pids_str.lines() {
                if let Ok(pid) = pid_str.trim().parse::<i32>() {
                    // Don't kill ourselves
                    if pid == std::process::id() as i32 {
                        continue;
                    }
                    // Send SIGTERM to the process group
                    match nix::sys::signal::killpg(
                        nix::unistd::Pid::from_raw(pid),
                        nix::sys::signal::Signal::SIGTERM,
                    ) {
                        Ok(()) => {
                            println!("  Sent SIGTERM to process group (PID {pid})");
                            killed = true;
                        }
                        Err(nix::errno::Errno::ESRCH) => {
                            // Process doesn't exist, try next
                        }
                        Err(nix::errno::Errno::EPERM) => {
                            // Not a process group leader, try killing the process directly
                            match nix::sys::signal::kill(
                                nix::unistd::Pid::from_raw(pid),
                                nix::sys::signal::Signal::SIGTERM,
                            ) {
                                Ok(()) => {
                                    println!("  Sent SIGTERM to PID {pid}");
                                    killed = true;
                                }
                                Err(e) => {
                                    eprintln!("  Failed to kill PID {pid}: {e}");
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("  Failed to kill process group {pid}: {e}");
                        }
                    }
                }
            }
        }

        if !killed {
            // Fallback: try to find processes via /proc or pgrep with the worktree as argument
            let pgrep = std::process::Command::new("pgrep")
                .args(["-f", worktree])
                .output();

            if let Ok(output) = pgrep {
                let pids_str = String::from_utf8_lossy(&output.stdout);
                for pid_str in pids_str.lines() {
                    if let Ok(pid) = pid_str.trim().parse::<i32>() {
                        if pid == std::process::id() as i32 {
                            continue;
                        }
                        if nix::sys::signal::kill(
                            nix::unistd::Pid::from_raw(pid),
                            nix::sys::signal::Signal::SIGTERM,
                        )
                        .is_ok()
                        {
                            println!("  Sent SIGTERM to PID {pid}");
                            killed = true;
                        }
                    }
                }
            }
        }

        if !killed {
            println!("  No running process found for worker {worker_id}.");
        }

        // Update the DB assignment to 'failed'
        db::update_worker_assignment_status(&conn, wa.id, "failed", Some("killed by user"))?;
        println!("  Marked assignment {} as failed.", wa.id);
    }

    Ok(())
}

/// Handle `blacksmith adapter info` — show detected/configured adapter.
fn handle_adapter_info(config: &HarnessConfig) {
    let adapter_name =
        adapters::resolve_adapter_name(config.agent.adapter.as_deref(), &config.agent.command);
    let detected = adapters::detect_adapter_name(&config.agent.command);

    println!("Agent command:     {}", config.agent.command);
    println!("Detected adapter:  {detected}");

    if let Some(ref explicit) = config.agent.adapter {
        println!("Configured adapter: {explicit}");
    } else {
        println!("Configured adapter: (auto-detect)");
    }

    println!("Resolved adapter:  {adapter_name}");

    let adapter = adapters::create_adapter(adapter_name);
    let supported = adapter.supported_metrics();
    if supported.is_empty() {
        println!("Supported metrics: (none)");
    } else {
        println!("Supported metrics:");
        for metric in supported {
            println!("  - {metric}");
        }
    }
}

/// Handle `blacksmith adapter list` — list all available adapters.
fn handle_adapter_list() {
    let all_names = ["claude", "codex", "opencode", "aider", "raw"];

    for name in &all_names {
        let adapter = adapters::create_adapter(name);
        let supported = adapter.supported_metrics();

        print!("{name}");
        if supported.is_empty() {
            println!("  (no built-in metrics)");
        } else {
            println!();
            for metric in supported {
                println!("  - {metric}");
            }
        }
    }
}

/// Handle `blacksmith adapter test <file>` — parse a file and show extracted metrics.
fn handle_adapter_test(
    config: &HarnessConfig,
    file: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    if !file.exists() {
        eprintln!("File not found: {}", file.display());
        std::process::exit(1);
    }

    let adapter_name =
        adapters::resolve_adapter_name(config.agent.adapter.as_deref(), &config.agent.command);
    let adapter = adapters::create_adapter(adapter_name);

    println!("Adapter: {} ({})", adapter.name(), adapter_name);
    println!("File:    {}", file.display());
    println!();

    match adapter.extract_builtin_metrics(file) {
        Ok(metrics) => {
            if metrics.is_empty() {
                println!("No metrics extracted.");
            } else {
                println!("Extracted metrics:");
                for (kind, value) in &metrics {
                    println!("  {kind} = {value}");
                }
            }
        }
        Err(e) => {
            eprintln!("Error extracting metrics: {e}");
            std::process::exit(1);
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Log level: --quiet = error only, --verbose = debug+, default = info+
    let level = if cli.quiet {
        "error"
    } else if cli.verbose {
        "debug"
    } else {
        "info"
    };
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));

    tracing_subscriber::fmt()
        .with_target(false)
        .with_thread_ids(false)
        .with_env_filter(filter)
        .init();

    tracing::debug!(?cli, "parsed CLI arguments");

    // Handle subcommands that don't need the full config
    if let Some(Commands::Init) = &cli.command {
        let config = HarnessConfig::load(&cli.config).unwrap_or_default();
        let dd = data_dir::DataDir::new(&config.storage.data_dir);
        match dd.ensure_initialized() {
            Ok(()) => {
                println!(
                    "Initialized data directory: {}",
                    config.storage.data_dir.display()
                );
            }
            Err(e) => {
                eprintln!("Error initializing data directory: {e}");
                std::process::exit(1);
            }
        }
        return;
    }

    if let Some(Commands::Brief) = &cli.command {
        let config_for_brief = HarnessConfig::load(&cli.config).unwrap_or_default();
        let dd = data_dir::DataDir::new(&config_for_brief.storage.data_dir);
        let db_path = dd.db();
        let targets = &config_for_brief.metrics.targets;
        let targets_opt = if targets.rules.is_empty() {
            None
        } else {
            Some(targets)
        };
        let adapter_name = adapters::resolve_adapter_name(
            config_for_brief.agent.adapter.as_deref(),
            &config_for_brief.agent.command,
        );
        let adapter = adapters::create_adapter(adapter_name);
        let supported = adapter.supported_metrics();

        if let Err(e) = brief::handle_brief(&db_path, targets_opt, Some(supported)) {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
        return;
    }

    if let Some(Commands::Gc {
        dry_run,
        aggressive,
    }) = &cli.command
    {
        let config_for_gc = HarnessConfig::load(&cli.config).unwrap_or_default();
        let dd = data_dir::DataDir::new(&config_for_gc.storage.data_dir);
        if let Err(e) = dd.ensure_initialized() {
            eprintln!("Error initializing data directory: {e}");
            std::process::exit(1);
        }
        gc::handle_gc(
            &dd,
            &config_for_gc.storage.retention,
            config_for_gc.storage.compress_after,
            *dry_run,
            *aggressive,
        );
        return;
    }

    if let Some(Commands::Migrate { consolidate }) = &cli.command {
        if !consolidate {
            eprintln!("Usage: blacksmith migrate --consolidate");
            eprintln!("Run with --consolidate to move legacy files into .blacksmith/");
            std::process::exit(1);
        }
        let config_for_migrate = HarnessConfig::load(&cli.config).unwrap_or_default();
        let dd = data_dir::DataDir::new(&config_for_migrate.storage.data_dir);
        if let Err(e) = dd.ensure_initialized() {
            eprintln!("Error initializing data directory: {e}");
            std::process::exit(1);
        }
        if let Err(e) = migrate::consolidate(&config_for_migrate, &dd) {
            eprintln!("Migration failed: {e}");
            std::process::exit(1);
        }
        return;
    }

    if let Some(Commands::Workers { action }) = &cli.command {
        let config_for_workers = HarnessConfig::load(&cli.config).unwrap_or_default();
        let dd = data_dir::DataDir::new(&config_for_workers.storage.data_dir);
        let db_path = dd.db();

        match action {
            WorkersAction::Status => {
                if let Err(e) = handle_workers_status(&db_path) {
                    eprintln!("Error: {e}");
                    std::process::exit(1);
                }
            }
            WorkersAction::Kill { worker_id } => {
                if let Err(e) = handle_workers_kill(&db_path, *worker_id) {
                    eprintln!("Error: {e}");
                    std::process::exit(1);
                }
            }
        }
        return;
    }

    if let Some(Commands::Adapter { action }) = &cli.command {
        let config_for_adapter = HarnessConfig::load(&cli.config).unwrap_or_default();
        match action {
            AdapterAction::Info => handle_adapter_info(&config_for_adapter),
            AdapterAction::List => handle_adapter_list(),
            AdapterAction::Test { file } => {
                if let Err(e) = handle_adapter_test(&config_for_adapter, file) {
                    eprintln!("Error: {e}");
                    std::process::exit(1);
                }
            }
        }
        return;
    }

    if let Some(Commands::Estimate { workers }) = &cli.command {
        let config_for_estimate = HarnessConfig::load(&cli.config).unwrap_or_default();
        let dd = data_dir::DataDir::new(&config_for_estimate.storage.data_dir);
        let db_path = dd.db();
        let conn = match db::open_or_create(&db_path) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Error opening database: {e}");
                std::process::exit(1);
            }
        };
        let num_workers = workers.unwrap_or(config_for_estimate.workers.max);
        let open_beads = estimation::query_open_beads();
        let est = estimation::estimate(&conn, &open_beads, num_workers);
        println!("{}", estimation::format_estimate(&est));
        return;
    }

    if let Some(Commands::Integration { action }) = &cli.command {
        let config_for_integration = HarnessConfig::load(&cli.config).unwrap_or_default();
        let dd = data_dir::DataDir::new(&config_for_integration.storage.data_dir);
        let db_path = dd.db();

        let result = match action {
            IntegrationAction::Log { last } => handle_integration_log(&db_path, *last),
            IntegrationAction::Retry { bead_id } => {
                handle_integration_retry(&db_path, &config_for_integration, bead_id)
            }
            IntegrationAction::Rollback { bead_id, force } => {
                handle_integration_rollback(&db_path, &config_for_integration, bead_id, *force)
            }
        };

        if let Err(e) = result {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
        return;
    }

    if let Some(Commands::Improve { action }) = &cli.command {
        let config_for_improve = HarnessConfig::load(&cli.config).unwrap_or_default();
        let dd = data_dir::DataDir::new(&config_for_improve.storage.data_dir);
        let db_path = dd.db();

        let result = match action {
            ImproveAction::Add {
                title,
                category,
                body,
                context,
                tags,
            } => improve::handle_add(
                &db_path,
                title,
                category,
                body.as_deref(),
                context.as_deref(),
                tags.as_deref(),
            ),
            ImproveAction::List { status, category } => {
                improve::handle_list(&db_path, status.as_deref(), category.as_deref())
            }
            ImproveAction::Show { ref_id } => improve::handle_show(&db_path, ref_id),
            ImproveAction::Update {
                ref_id,
                status,
                body,
                context,
            } => improve::handle_update(
                &db_path,
                ref_id,
                status.as_deref(),
                body.as_deref(),
                context.as_deref(),
            ),
            ImproveAction::Promote { ref_id } => improve::handle_promote(&db_path, ref_id),
            ImproveAction::Dismiss { ref_id, reason } => {
                improve::handle_dismiss(&db_path, ref_id, reason.as_deref())
            }
            ImproveAction::Search { query } => improve::handle_search(&db_path, query),
        };

        if let Err(e) = result {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
        return;
    }

    if let Some(Commands::Metrics { action }) = &cli.command {
        let config_for_metrics = HarnessConfig::load(&cli.config).unwrap_or_default();
        let dd = data_dir::DataDir::new(&config_for_metrics.storage.data_dir);
        let db_path = dd.db();
        let result = match action {
            MetricsAction::Log { file } => metrics_cmd::handle_log(&db_path, file),
            MetricsAction::Status { last } => metrics_cmd::handle_status(&db_path, *last),
            MetricsAction::Targets { last } => {
                let adapter_name = adapters::resolve_adapter_name(
                    config_for_metrics.agent.adapter.as_deref(),
                    &config_for_metrics.agent.command,
                );
                let adapter = adapters::create_adapter(adapter_name);
                metrics_cmd::handle_targets(
                    &db_path,
                    *last,
                    &config_for_metrics.metrics.targets,
                    adapter_name,
                    adapter.supported_metrics(),
                )
            }
            MetricsAction::Migrate { from } => metrics_cmd::handle_migrate(&db_path, from),
            MetricsAction::Rebuild => metrics_cmd::handle_rebuild(&db_path),
            MetricsAction::Export { format } => metrics_cmd::handle_export(&db_path, format),
            MetricsAction::Query {
                kind,
                last,
                aggregate,
            } => metrics_cmd::handle_query(&db_path, kind, *last, aggregate.as_deref()),
            MetricsAction::Events { session } => metrics_cmd::handle_events(&db_path, *session),
            MetricsAction::Reingest { last, all } => {
                let adapter_name = adapters::resolve_adapter_name(
                    config_for_metrics.agent.adapter.as_deref(),
                    &config_for_metrics.agent.command,
                );
                let adapter = adapters::create_adapter(adapter_name);
                let rules: Vec<_> = config_for_metrics
                    .metrics
                    .extract
                    .rules
                    .iter()
                    .filter_map(|r| r.compile().ok())
                    .collect();
                metrics_cmd::handle_reingest(
                    &db_path,
                    &dd.sessions_dir(),
                    *last,
                    *all,
                    &rules,
                    adapter.as_ref(),
                )
            }
            MetricsAction::Beads => metrics_cmd::handle_beads(&db_path),
        };

        if let Err(e) = result {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
        return;
    }

    // Load config: file > defaults, then CLI > file
    let mut config = match HarnessConfig::load(&cli.config) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("Configuration error: {e}");
            std::process::exit(1);
        }
    };
    config.apply_cli_overrides(&cli.to_overrides());

    tracing::debug!(?config, "resolved configuration");

    // Warn about deprecated config keys superseded by storage.data_dir
    config.warn_deprecated_paths();

    // Validate config on load so errors surface immediately
    let validation_errors = config.validate();
    if !validation_errors.is_empty() {
        eprintln!("Configuration validation failed:");
        for err in &validation_errors {
            eprintln!("  {err}");
        }
        std::process::exit(1);
    }

    // Auto-initialize data directory on first run
    let data_dir = data_dir::DataDir::new(&config.storage.data_dir);
    if let Err(e) = data_dir.ensure_initialized() {
        tracing::error!(error = %e, "failed to initialize data directory");
        std::process::exit(1);
    }

    if cli.dry_run {
        println!("blacksmith v{}", env!("CARGO_PKG_VERSION"));
        println!("Config file: {}", cli.config.display());
        println!();
        println!("Resolved configuration:");
        println!(
            "  session.max_iterations = {}",
            config.session.max_iterations
        );
        println!(
            "  session.prompt_file = {}",
            config.session.prompt_file.display()
        );
        println!("  storage.data_dir = {}", config.storage.data_dir.display());
        println!("  storage.retention = {}", config.storage.retention);
        println!(
            "  storage.compress_after = {}",
            config.storage.compress_after
        );
        println!("  data_dir.db = {}", data_dir.db().display());
        println!(
            "  data_dir.sessions = {}",
            data_dir.sessions_dir().display()
        );
        println!("  data_dir.counter = {}", data_dir.counter().display());
        println!("  data_dir.status = {}", data_dir.status().display());
        let coding = config.agent.resolved_coding();
        let integration = config.agent.resolved_integration();
        println!("  agent.coding.command = {}", coding.command);
        println!("  agent.coding.args = {:?}", coding.args);
        println!("  agent.coding.prompt_via = {}", coding.prompt_via);
        println!("  agent.integration.command = {}", integration.command);
        println!("  agent.integration.args = {:?}", integration.args);
        println!(
            "  agent.integration.prompt_via = {}",
            integration.prompt_via
        );
        println!(
            "  watchdog.check_interval_secs = {}",
            config.watchdog.check_interval_secs
        );
        println!(
            "  watchdog.stale_timeout_mins = {}",
            config.watchdog.stale_timeout_mins
        );
        println!(
            "  watchdog.min_output_bytes = {}",
            config.watchdog.min_output_bytes
        );
        println!(
            "  retry.max_empty_retries = {}",
            config.retry.max_empty_retries
        );
        println!(
            "  retry.retry_delay_secs = {}",
            config.retry.retry_delay_secs
        );
        println!(
            "  backoff.initial_delay_secs = {}",
            config.backoff.initial_delay_secs
        );
        println!(
            "  backoff.max_delay_secs = {}",
            config.backoff.max_delay_secs
        );
        println!(
            "  backoff.max_consecutive_rate_limits = {}",
            config.backoff.max_consecutive_rate_limits
        );
        println!(
            "  shutdown.stop_file = {}",
            config.shutdown.stop_file.display()
        );
        println!("  hooks.pre_session = {:?}", config.hooks.pre_session);
        println!("  hooks.post_session = {:?}", config.hooks.post_session);
        println!(
            "  prompt.file = {:?}",
            config.prompt.file.as_ref().map(|p| p.display().to_string())
        );
        println!(
            "  prompt.prepend_commands = {:?}",
            config.prompt.prepend_commands
        );
        println!(
            "  output.event_log = {:?}",
            config
                .output
                .event_log
                .as_ref()
                .map(|p| p.display().to_string())
        );
        println!(
            "  commit_detection.patterns = {:?}",
            config.commit_detection.patterns
        );
        if config.metrics.extract.rules.is_empty() {
            println!("  metrics.extract.rules = (none)");
        } else {
            println!(
                "  metrics.extract.rules = ({} rules)",
                config.metrics.extract.rules.len()
            );
            for (i, rule) in config.metrics.extract.rules.iter().enumerate() {
                println!(
                    "    [{}] kind={:?} pattern={:?} source={:?} count={} first_match={}",
                    i, rule.kind, rule.pattern, rule.source, rule.count, rule.first_match
                );
            }
        }
        if config.metrics.targets.rules.is_empty() {
            println!("  metrics.targets.rules = (none)");
        } else {
            println!(
                "  metrics.targets.rules = ({} rules)",
                config.metrics.targets.rules.len()
            );
            for (i, rule) in config.metrics.targets.rules.iter().enumerate() {
                println!(
                    "    [{}] kind={:?} compare={:?} threshold={} direction={:?} label={:?}",
                    i, rule.kind, rule.compare, rule.threshold, rule.direction, rule.label
                );
            }
        }
        println!(
            "  metrics.targets.streak_threshold = {}",
            config.metrics.targets.streak_threshold
        );
        println!();
        println!("Dry run mode — config validated, not running.");
        return;
    }

    if cli.status {
        let status_path = data_dir.status();
        let db_path = data_dir.db();
        match status::display_status(&status_path, Some(&db_path), config.workers.max) {
            Ok(true) => {}
            Ok(false) => {
                println!("No running blacksmith detected.");
            }
            Err(e) => {
                tracing::error!(error = %e, "failed to read status");
                std::process::exit(1);
            }
        }
        return;
    }

    // Acquire singleton lock to prevent concurrent instances
    let _lock = match singleton::try_acquire(&data_dir.lock()) {
        Ok(guard) => guard,
        Err(e) => {
            tracing::error!("{e}");
            std::process::exit(1);
        }
    };

    // Install signal handlers
    let signals = signals::SignalHandler::install();

    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        max_iterations = config.session.max_iterations,
        "blacksmith starting"
    );

    // Run: multi-agent coordinator when workers.max > 1, serial loop otherwise
    if config.workers.max > 1 {
        let summary = coordinator::run(&config, &data_dir, &signals, cli.quiet).await;
        tracing::info!(
            completed = summary.completed_beads,
            failed = summary.failed_beads,
            reason = ?summary.exit_reason,
            "coordinator finished"
        );
    } else {
        let summary = runner::run(&config, &data_dir, &signals, cli.quiet).await;
        tracing::info!(
            productive = summary.productive_iterations,
            global = summary.global_iteration,
            reason = ?summary.exit_reason,
            "loop finished"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workers_status_empty() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = db::open_or_create(&db_path).unwrap();
        drop(conn);

        // Should succeed with "No active workers." message
        let result = handle_workers_status(&db_path);
        assert!(result.is_ok());
    }

    #[test]
    fn test_workers_status_with_active() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = db::open_or_create(&db_path).unwrap();

        db::insert_worker_assignment(&conn, 0, "beads-abc", "/tmp/wt-0", "coding", None).unwrap();
        db::insert_worker_assignment(&conn, 1, "beads-def", "/tmp/wt-1", "integrating", None)
            .unwrap();
        drop(conn);

        let result = handle_workers_status(&db_path);
        assert!(result.is_ok());
    }

    #[test]
    fn test_workers_kill_no_active_assignment() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = db::open_or_create(&db_path).unwrap();

        // Insert a completed worker — not active
        db::insert_worker_assignment(&conn, 0, "beads-done", "/tmp/wt-0", "completed", None)
            .unwrap();
        drop(conn);

        // Should fail because no active assignment for worker 0
        // (handle_workers_kill calls process::exit, so we test the DB function directly)
        let conn2 = db::open_or_create(&db_path).unwrap();
        let active = db::active_worker_assignments(&conn2).unwrap();
        let worker_0: Vec<_> = active.iter().filter(|wa| wa.worker_id == 0).collect();
        assert!(worker_0.is_empty());
    }

    #[test]
    fn test_workers_kill_marks_as_failed() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = db::open_or_create(&db_path).unwrap();

        // Insert a coding worker pointing to a nonexistent worktree (no process to find)
        let assignment_id = db::insert_worker_assignment(
            &conn,
            0,
            "beads-kill-test",
            "/tmp/nonexistent-worktree-kill-test",
            "coding",
            None,
        )
        .unwrap();
        drop(conn);

        // Call handle_workers_kill — the process search will find nothing but
        // it should still mark the assignment as failed
        let result = handle_workers_kill(&db_path, 0);
        assert!(result.is_ok());

        let conn2 = db::open_or_create(&db_path).unwrap();
        let wa = db::get_worker_assignment(&conn2, assignment_id)
            .unwrap()
            .unwrap();
        assert_eq!(wa.status, "failed");
        assert_eq!(wa.failure_notes, Some("killed by user".to_string()));
    }

    #[test]
    fn test_integration_log_empty() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = db::open_or_create(&db_path).unwrap();
        drop(conn);

        let result = handle_integration_log(&db_path, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_integration_log_with_entries() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = db::open_or_create(&db_path).unwrap();

        let a1 =
            db::insert_worker_assignment(&conn, 0, "beads-abc", "/tmp/wt-0", "integrated", None)
                .unwrap();
        db::insert_integration_log(
            &conn,
            a1,
            "2026-02-15T12:00:00Z",
            "abc123def456",
            Some("3 entries applied"),
            None,
            false,
        )
        .unwrap();
        drop(conn);

        let result = handle_integration_log(&db_path, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_integration_log_with_last() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = db::open_or_create(&db_path).unwrap();

        for i in 0..5 {
            let aid = db::insert_worker_assignment(
                &conn,
                i,
                &format!("beads-{i}"),
                &format!("/tmp/wt-{i}"),
                "integrated",
                None,
            )
            .unwrap();
            db::insert_integration_log(
                &conn,
                aid,
                &format!("2026-02-15T1{i}:00:00Z"),
                &format!("commit{i}abc"),
                None,
                None,
                false,
            )
            .unwrap();
        }
        drop(conn);

        let result = handle_integration_log(&db_path, Some(2));
        assert!(result.is_ok());
    }

    #[test]
    fn test_integration_retry_no_failed_assignment() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = db::open_or_create(&db_path).unwrap();
        // Insert a completed (not failed) assignment
        db::insert_worker_assignment(&conn, 0, "beads-ok", "/tmp/wt-0", "completed", None).unwrap();
        drop(conn);

        // handle_integration_retry calls process::exit(1) when no failed assignment found,
        // so we test the DB function directly
        let conn2 = db::open_or_create(&db_path).unwrap();
        let result = db::find_failed_assignment_by_bead(&conn2, "beads-ok").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_integration_rollback_no_integration_found() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = db::open_or_create(&db_path).unwrap();
        // Insert assignment but no integration log entry
        db::insert_worker_assignment(&conn, 0, "beads-noint", "/tmp/wt-0", "completed", None)
            .unwrap();
        drop(conn);

        // Test the DB function directly since the handler calls process::exit
        let conn2 = db::open_or_create(&db_path).unwrap();
        let result = db::find_integration_by_bead(&conn2, "beads-noint").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_integration_rollback_already_rolled_back() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = db::open_or_create(&db_path).unwrap();

        let aid =
            db::insert_worker_assignment(&conn, 0, "beads-rb", "/tmp/wt-0", "rolled_back", None)
                .unwrap();
        db::insert_integration_log(
            &conn,
            aid,
            "2026-02-15T12:00:00Z",
            "abc123",
            None,
            None,
            false,
        )
        .unwrap();

        // The handler would check status == "rolled_back" and exit.
        // Test the DB query to verify it returns the rolled_back entry.
        let result = db::find_integration_by_bead(&conn, "beads-rb").unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().status, "rolled_back");
    }

    #[test]
    fn test_adapter_info_default_config() {
        // Should not panic with default config
        let config = HarnessConfig::default();
        handle_adapter_info(&config);
    }

    #[test]
    fn test_adapter_info_explicit_adapter() {
        let mut config = HarnessConfig::default();
        config.agent.adapter = Some("raw".to_string());
        handle_adapter_info(&config);
    }

    #[test]
    fn test_adapter_list() {
        // Should not panic
        handle_adapter_list();
    }

    #[test]
    fn test_adapter_test_with_claude_jsonl() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("session.jsonl");
        // Write a minimal Claude JSONL session
        std::fs::write(
            &file,
            r#"{"type":"result","subtype":"success","cost_usd":0.05,"duration_ms":1234,"duration_api_ms":1000,"num_turns":3,"session_id":"test"}
"#,
        )
        .unwrap();

        let config = HarnessConfig::default(); // default command is "claude"
        let result = handle_adapter_test(&config, &file);
        assert!(result.is_ok());
    }

    #[test]
    fn test_adapter_test_with_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("empty.jsonl");
        std::fs::write(&file, "").unwrap();

        let config = HarnessConfig::default();
        // Should handle empty file without panic
        let result = handle_adapter_test(&config, &file);
        assert!(result.is_ok());
    }

    #[test]
    fn test_adapter_test_with_raw_adapter() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("output.txt");
        std::fs::write(&file, "some output\n").unwrap();

        let mut config = HarnessConfig::default();
        config.agent.adapter = Some("raw".to_string());
        let result = handle_adapter_test(&config, &file);
        assert!(result.is_ok());
    }
}
