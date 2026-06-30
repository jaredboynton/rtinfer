//! rtinferd: always-on loopback rtinfer/1 inference daemon.

mod endpoint_file;
mod install;
mod self_update;
mod server;

use clap::{Parser, Subcommand};

/// Default loopback port. rtinfer owns its own port (cse-toold cockpit keeps
/// 8787); clients discover the live port through `~/.cse-rtinfer/endpoint.json`.
const DEFAULT_PORT: u16 = 8765;

#[derive(Parser)]
#[command(
    name = "rtinferd",
    version,
    about = "rtinfer/1 loopback inference daemon"
)]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the server in the foreground (used by the LaunchAgent).
    Serve {
        #[arg(long, env = "RTINFER_PORT", default_value_t = DEFAULT_PORT)]
        port: u16,
    },
    /// Install + load the always-on LaunchAgent (macOS).
    Install {
        #[arg(long, env = "RTINFER_PORT", default_value_t = DEFAULT_PORT)]
        port: u16,
    },
    /// Unload + remove the LaunchAgent and the endpoint file.
    Uninstall,
    /// Print the resolved well-known endpoint path + current contents.
    Status,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Cmd::Serve { port } => server::serve(port).await,
        Cmd::Install { port } => install::run_install(port),
        Cmd::Uninstall => install::run_uninstall(),
        Cmd::Status => {
            match endpoint_file::path() {
                Some(p) => {
                    println!("endpoint file: {}", p.display());
                    match std::fs::read_to_string(&p) {
                        Ok(s) => println!("{s}"),
                        Err(e) => println!("(not present: {e})"),
                    }
                }
                None => println!("(no home directory)"),
            }
            Ok(())
        }
    }
}
