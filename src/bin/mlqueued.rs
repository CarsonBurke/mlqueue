use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "mlqueued", version, about = "mlqueue daemon (machine-wide ML job queue)")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Internal durable attempt supervisor; spawned by the daemon, never run
    /// by hand.
    #[command(name = "__runner", hide = true)]
    Runner {
        #[arg(long)]
        attempt_dir: PathBuf,
    },
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Some(Command::Runner { attempt_dir }) => {
            std::process::exit(mlqueue::process::runner::runner_main(&attempt_dir));
        }
        None => {
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
                )
                .init();
            let paths = match mlqueue::paths::Paths::resolve() {
                Ok(paths) => paths,
                Err(err) => {
                    eprintln!("error: {err:#}");
                    std::process::exit(1);
                }
            };
            if let Err(err) = mlqueue::daemon::run(paths) {
                eprintln!("error: {err:#}");
                std::process::exit(1);
            }
        }
    }
}
