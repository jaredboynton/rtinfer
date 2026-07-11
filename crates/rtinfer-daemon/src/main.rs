//! rtinferd: always-on loopback rtinfer/1 inference daemon.

mod cse_toold_auth;
mod endpoint_file;
mod install;
mod lock;
mod self_update;
mod server;

use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// Default loopback port. rtinfer owns its own port (cse-toold cockpit keeps
/// 8787); clients discover the live port through `~/.cse-rtinfer/endpoint.json`.
/// Debug builds shift to 8766 so `cargo run -- serve` never collides with the
/// release LaunchAgent on 8765.
const DEFAULT_PORT: u16 = if cfg!(debug_assertions) { 8766 } else { 8765 };

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
        /// Absolute cse-toold binary used as the Codex credential process.
        #[arg(long, env = "RTINFER_CSE_TOOLD_BIN", value_name = "ABSOLUTE_PATH")]
        cse_toold_bin: Option<PathBuf>,
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
        Cmd::Serve {
            port,
            cse_toold_bin,
        } => server::serve(port, cse_toold_bin).await,
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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn serve_accepts_explicit_cse_toold_bin() {
        let cli = Cli::try_parse_from([
            "rtinferd",
            "serve",
            "--port",
            "9876",
            "--cse-toold-bin",
            "/opt/cse/bin/cse-toold",
        ])
        .unwrap();
        match cli.command {
            Cmd::Serve {
                port,
                cse_toold_bin,
            } => {
                assert_eq!(port, 9876);
                assert_eq!(
                    cse_toold_bin.as_deref(),
                    Some(std::path::Path::new("/opt/cse/bin/cse-toold"))
                );
            }
            _ => panic!("expected serve command"),
        }
    }

    #[test]
    fn serve_cse_toold_bin_declares_environment_source() {
        let command = Cli::command();
        let serve = command
            .get_subcommands()
            .find(|subcommand| subcommand.get_name() == "serve")
            .unwrap();
        let argument = serve
            .get_arguments()
            .find(|argument| argument.get_id() == "cse_toold_bin")
            .unwrap();
        assert_eq!(
            argument.get_env(),
            Some(std::ffi::OsStr::new("RTINFER_CSE_TOOLD_BIN"))
        );
    }
}
