mod commit;
mod config;
mod hooks;
mod metrics;
mod prompt;
mod ratelimit;
mod retry;
mod runner;
mod session;
mod signals;
mod status;
mod watchdog;

use clap::Parser;
use config::{CliOverrides, HarnessConfig};
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

/// A Rust CLI tool that runs an AI coding agent in a supervised loop:
/// dispatch a prompt, monitor the session, enforce health invariants,
/// collect metrics, and repeat.
#[derive(Parser, Debug)]
#[command(name = "simple-agent-harness", version, about)]
pub struct Cli {
    /// Override max iterations (default: from config)
    #[arg(value_name = "MAX_ITERATIONS")]
    max_iterations: Option<u32>,

    /// Config file path
    #[arg(short, long, default_value = "harness.toml")]
    config: PathBuf,

    /// Prompt file path (overrides config)
    #[arg(short, long)]
    prompt: Option<PathBuf>,

    /// Output directory (overrides config)
    #[arg(short, long)]
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
    #[arg(short, long)]
    verbose: bool,

    /// Suppress per-iteration banners, only errors and summary
    #[arg(short, long)]
    quiet: bool,

    /// Print current loop state and exit
    #[arg(long)]
    status: bool,
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

    // Log level: --quiet = warn+, --verbose = debug+, default = info+
    let level = if cli.quiet {
        "warn"
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

    if cli.dry_run {
        println!("simple-agent-harness v{}", env!("CARGO_PKG_VERSION"));
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
        println!(
            "  session.output_dir = {}",
            config.session.output_dir.display()
        );
        println!("  session.output_prefix = {}", config.session.output_prefix);
        println!(
            "  session.counter_file = {}",
            config.session.counter_file.display()
        );
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
        println!();
        println!("Dry run mode — config validated, not running.");
        return;
    }

    if cli.status {
        let status_path = config.session.output_dir.join("harness.status");
        match status::display_status(&status_path) {
            Ok(true) => {}
            Ok(false) => {
                println!("No running harness detected.");
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
        "simple-agent-harness starting"
    );

    // Run the main loop
    let summary = runner::run(&config, &signals).await;

    tracing::info!(
        productive = summary.productive_iterations,
        global = summary.global_iteration,
        reason = ?summary.exit_reason,
        "loop finished"
    );
}
