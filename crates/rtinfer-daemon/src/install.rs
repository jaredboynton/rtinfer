//! LaunchAgent install / uninstall for the rtinfer daemon (macOS).
//!
//! Pins a per-user `KeepAlive` LaunchAgent to the current binary running
//! `rtinferd serve --port <port>`. launchd respawns it on crash and starts it
//! at login, so the daemon is always-on without an external supervisor.
//!
//! Unlike cse-toold there is no keychain / TCC dependency here: the daemon
//! only opens loopback sockets and reads `~/.codex/auth.json`, so a versioned
//! binary path is fine and no codesign pinning is required.

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};

/// launchd label for the rtinfer daemon.
pub const LABEL: &str = "com.jaredboynton.rtinferd";

fn home() -> Result<PathBuf> {
    dirs_home().context("cannot resolve home directory")
}

fn dirs_home() -> Option<PathBuf> {
    #[cfg(unix)]
    {
        std::env::var_os("HOME").map(PathBuf::from)
    }
    #[cfg(not(unix))]
    {
        std::env::var_os("USERPROFILE").map(PathBuf::from)
    }
}

#[cfg(target_os = "macos")]
fn plist_path() -> Result<PathBuf> {
    Ok(home()?.join(format!("Library/LaunchAgents/{LABEL}.plist")))
}

/// Render the LaunchAgent plist XML for `rtinferd serve --port <port>`.
pub fn render_plist(program: &Path, home: &Path, port: u16) -> String {
    let program_str = program.display();
    let log = home.join("Library/Logs/rtinferd.log");
    let err = home.join("Library/Logs/rtinferd.err");
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{LABEL}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{program_str}</string>
        <string>serve</string>
        <string>--port</string>
        <string>{port}</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>ProcessType</key>
    <string>Background</string>
    <key>StandardOutPath</key>
    <string>{log}</string>
    <key>StandardErrorPath</key>
    <string>{err}</string>
</dict>
</plist>
"#,
        log = log.display(),
        err = err.display(),
    )
}

/// Install + load the LaunchAgent, pinned to the current binary.
#[cfg(target_os = "macos")]
pub fn run_install(port: u16) -> Result<()> {
    let program = std::env::current_exe().context("cannot resolve current executable path")?;
    let program = program.canonicalize().unwrap_or(program);
    let home = home()?;
    let plist = plist_path()?;
    if let Some(parent) = plist.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("cannot create {}", parent.display()))?;
    }
    std::fs::write(&plist, render_plist(&program, &home, port))
        .with_context(|| format!("cannot write plist {}", plist.display()))?;

    let uid = unsafe { libc_getuid() };
    let target = format!("gui/{uid}");
    // Bootout any prior instance (ignore failure: may not be loaded), then
    // bootstrap + kickstart so the change takes effect immediately.
    let _ = Command::new("launchctl")
        .args(["bootout", &format!("{target}/{LABEL}")])
        .status();
    Command::new("launchctl")
        .args(["bootstrap", &target, &plist.display().to_string()])
        .status()
        .context("launchctl bootstrap")?;
    let _ = Command::new("launchctl")
        .args(["kickstart", "-k", &format!("{target}/{LABEL}")])
        .status();
    eprintln!("rtinferd installed: {} (port {port})", plist.display());
    Ok(())
}

/// Unload + remove the LaunchAgent and the well-known endpoint file.
#[cfg(target_os = "macos")]
pub fn run_uninstall() -> Result<()> {
    let plist = plist_path()?;
    let uid = unsafe { libc_getuid() };
    let _ = Command::new("launchctl")
        .args(["bootout", &format!("gui/{uid}/{LABEL}")])
        .status();
    if plist.exists() {
        std::fs::remove_file(&plist)
            .with_context(|| format!("cannot remove {}", plist.display()))?;
    }
    let _ = crate::endpoint_file::remove();
    eprintln!("rtinferd uninstalled");
    Ok(())
}

#[cfg(not(target_os = "macos"))]
pub fn run_install(_port: u16) -> Result<()> {
    anyhow::bail!("rtinferd install is only supported on macOS; run `rtinferd serve` directly")
}

#[cfg(not(target_os = "macos"))]
pub fn run_uninstall() -> Result<()> {
    let _ = crate::endpoint_file::remove();
    Ok(())
}

#[cfg(target_os = "macos")]
unsafe fn libc_getuid() -> u32 {
    unsafe extern "C" {
        fn getuid() -> u32;
    }
    unsafe { getuid() }
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::*;

    #[test]
    fn plist_pins_program_and_port() {
        let p = render_plist(
            Path::new("/usr/local/bin/rtinferd"),
            Path::new("/Users/x"),
            8765,
        );
        assert!(p.contains("<string>/usr/local/bin/rtinferd</string>"));
        assert!(p.contains("<string>serve</string>"));
        assert!(p.contains("<string>8765</string>"));
        assert!(p.contains("com.jaredboynton.rtinferd"));
        assert!(p.contains("<key>KeepAlive</key>"));
    }
}
