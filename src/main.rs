//! `kraken-conformance`: runs the conformance checks of a feature level against a library's
//! conformance service and prints the report (kraken `protocol/conformance.md`).

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand, ValueEnum};
use kraken_conformance::checks::{self, Needs, Options};
use kraken_conformance::report::Report;

#[derive(Parser)]
#[command(
    name = "kraken-conformance",
    version,
    about = "Proves a kraken library's feature level"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, Copy, ValueEnum)]
enum Format {
    Text,
    Json,
}

#[derive(Subcommand)]
enum Command {
    /// Runs the checks of a level and prints the report; exit code 0 only when every check
    /// that ran passed.
    Run {
        /// The command that starts one instance of the library's conformance service, once or
        /// more per check, with the CONFORMANCE_* environment. Run without a shell: split into
        /// words at blanks, '...' and "..." quote, \ escapes; no variables or redirections.
        #[arg(long)]
        service_cmd: String,
        /// More environment for every instance, NAME=VALUE (repeatable).
        #[arg(long = "service-env", value_parser = parse_env)]
        service_env: Vec<(String, String)>,
        /// The service name the instances register (`conformance-<library>`).
        #[arg(long, default_value = "conformance-service")]
        service: String,
        /// A real hub, `host:port` (plain text, access control off); twice for the two-hub check.
        #[arg(long = "hub")]
        hubs: Vec<String>,
        /// The level to check (1 is the only one the runner checks itself).
        #[arg(long, default_value_t = 1)]
        level: u32,
        /// Only these check ids, comma-separated.
        #[arg(long, value_delimiter = ',')]
        only: Vec<String>,
        #[arg(long, value_enum, default_value_t = Format::Text)]
        report: Format,
        /// Where the instances' logs are written (default: a fresh temporary directory).
        #[arg(long)]
        work_dir: Option<PathBuf>,
        /// The longest one check may take, in seconds.
        #[arg(long, default_value_t = 180)]
        check_timeout: u64,
        /// How long an instance may take to start and register, in seconds.
        #[arg(long, default_value_t = 30)]
        startup_timeout: u64,
    },
    /// Lists the checks of level 1 and what each needs.
    List,
}

#[tokio::main]
async fn main() -> ExitCode {
    match Cli::parse().command {
        Command::List => {
            for c in checks::level_1() {
                let needs = match c.needs {
                    Needs::Nothing => "fake hubs".to_string(),
                    Needs::Hub => "a real hub".to_string(),
                    Needs::TwoHubs => "two real hubs".to_string(),
                    Needs::NotImplemented(why) => format!("not implemented: {why}"),
                };
                println!("{:<20}{needs}", c.id);
            }
            ExitCode::SUCCESS
        }
        Command::Run {
            service_cmd,
            service_env,
            service,
            hubs,
            level,
            only,
            report,
            work_dir,
            check_timeout,
            startup_timeout,
        } => {
            if level != 1 {
                eprintln!(
                    "level {level}: levels 2 to 4 are proved by the libraries' event-store and message-store kits; this runner checks level 1"
                );
                return ExitCode::from(2);
            }
            let mut all = checks::level_1();
            let partial = !only.is_empty();
            if !only.is_empty() {
                let unknown: Vec<&String> = only
                    .iter()
                    .filter(|id| !all.iter().any(|c| c.id == id.as_str()))
                    .collect();
                if !unknown.is_empty() {
                    eprintln!("unknown check ids: {unknown:?}");
                    return ExitCode::from(2);
                }
                all.retain(|c| only.iter().any(|id| id == c.id));
            }
            let work_dir = work_dir.unwrap_or_else(|| {
                std::env::temp_dir().join(format!("kraken-conformance-{}", std::process::id()))
            });
            if let Err(e) = std::fs::create_dir_all(&work_dir) {
                eprintln!("{}: {e}", work_dir.display());
                return ExitCode::from(2);
            }
            eprintln!("[kraken-conformance] logs in {}", work_dir.display());
            let options = Options {
                service_cmd,
                service: service.clone(),
                hubs,
                work_dir,
                check_timeout: Duration::from_secs(check_timeout),
                startup: Duration::from_secs(startup_timeout),
                extra_env: service_env,
            };
            if let Err(e) = kraken_conformance::sut::split_command(&options.service_cmd) {
                eprintln!("{e}");
                return ExitCode::from(2);
            }
            match checks::preflight(&options).await {
                Ok(note) => eprintln!("[kraken-conformance] {note}"),
                Err(e) => {
                    eprintln!("[kraken-conformance] {e}");
                    return ExitCode::from(2);
                }
            }
            let verdicts = checks::run_all(&options, all).await;
            let report_ = Report::new(&service, level, partial, verdicts);
            match report {
                Format::Text => print!("{}", report_.text()),
                Format::Json => println!("{}", serde_json::to_string_pretty(&report_).unwrap()),
            }
            if report_.passed {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            }
        }
    }
}

fn parse_env(s: &str) -> Result<(String, String), String> {
    match s.split_once('=') {
        Some((k, v)) if !k.is_empty() => Ok((k.to_string(), v.to_string())),
        _ => Err(format!("expected NAME=VALUE, got {s:?}")),
    }
}
