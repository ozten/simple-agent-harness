mod adapters;
mod brief;
mod commit;
mod config;
mod data_dir;
mod db;
mod hooks;
mod improve;
mod ingest;
mod metrics;
mod metrics_cmd;
mod pool;
mod prompt;
mod ratelimit;
mod retry;
mod runner;
mod scheduler;
mod session;
mod signals;
mod status;
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

        if let Err(e) = brief::handle_brief(&db_path, targets_opt) {
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
                metrics_cmd::handle_targets(&db_path, *last, &config_for_metrics.metrics.targets)
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
        println!("  data_dir.db = {}", data_dir.db().display());
        println!(
            "  data_dir.sessions = {}",
            data_dir.sessions_dir().display()
        );
        println!("  data_dir.counter = {}", data_dir.counter().display());
        println!("  data_dir.status = {}", data_dir.status().display());
        println!("  agent.command = {}", config.agent.command);
        println!("  agent.args = {:?}", config.agent.args);
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
        match status::display_status(&status_path) {
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

    // Install signal handlers
    let signals = signals::SignalHandler::install();

    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        max_iterations = config.session.max_iterations,
        "blacksmith starting"
    );

    // Run the main loop
    let summary = runner::run(&config, &data_dir, &signals, cli.quiet).await;

    tracing::info!(
        productive = summary.productive_iterations,
        global = summary.global_iteration,
        reason = ?summary.exit_reason,
        "loop finished"
    );
}
