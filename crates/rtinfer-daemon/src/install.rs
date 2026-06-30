//! LaunchAgent install / uninstall for the rtinfer daemon (macOS).
//!
//! Pins a per-user `KeepAlive` LaunchAgent to the STABLE npm global bin shim
//! (`$(npm prefix -g)/bin/rtinferd`) running `serve --port <port>`. npm rewrites
//! that shim in place on every `npm i -g`, so the daemon's self-update can drain
//! + exit and launchd respawns the updated binary WITHOUT rewriting the plist.
//! launchd respawns on crash and starts it at login, so the daemon is always-on
//! without an external supervisor.
//!
//! When the stable shim cannot be resolved (manual `rtinferd serve`, dev builds,
//! `npm` absent) the install falls back to the current executable path so a
//! hand-run install still works; that path is simply not self-update-stable.
//!
//! Unlike cse-toold there is no keychain / TCC dependency here: the daemon
//! only opens loopback sockets and reads `~/.codex/auth.json`, so no codesign
//! pinning is required.

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};

/// launchd label for the rtinfer daemon.
pub const LABEL: &str = "com.jaredboynton.rtinferd";

/// Override the launched program path (set by the npm postinstall to the exact
/// global bin shim). Takes precedence over npm-prefix discovery.
const LAUNCH_BIN_ENV: &str = "RTINFER_LAUNCH_BIN";

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

/// The stable npm global bin shim for `rtinferd`, if it can be located and
/// exists. This is the path the LaunchAgent should pin so self-update is a
/// no-op for the plist.
fn npm_global_shim() -> Option<PathBuf> {
    if let Some(explicit) = std::env::var_os(LAUNCH_BIN_ENV) {
        // Trust the postinstall-provided path even if npm has not created the
        // global bin shim yet; launchd should pin the stable future path.
        return Some(PathBuf::from(explicit));
    }
    let out = Command::new("npm").args(["prefix", "-g"]).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let prefix = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if prefix.is_empty() {
        return None;
    }
    let shim = PathBuf::from(prefix).join("bin").join("rtinferd");
    shim.exists().then_some(shim)
}

/// Resolve the program path the LaunchAgent runs: prefer the stable npm shim,
/// fall back to the current executable (dev / manual installs).
fn resolve_launch_program() -> Result<PathBuf> {
    if let Some(shim) = npm_global_shim() {
        return Ok(shim);
    }
    let program = std::env::current_exe().context("cannot resolve current executable path")?;
    Ok(program.canonicalize().unwrap_or(program))
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

/// Install + load the LaunchAgent, pinned to the stable npm global shim (or the
/// current binary as a dev/manual fallback).
#[cfg(target_os = "macos")]
pub fn run_install(port: u16) -> Result<()> {
    let program = resolve_launch_program()?;
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

    #[test]
    fn launch_program_prefers_explicit_shim_env() {
        let shim = PathBuf::from("/tmp/rtinferd-stable-shim-for-test");
        let prev = std::env::var_os(LAUNCH_BIN_ENV);
        // SAFETY: single-threaded test; restored below.
        unsafe { std::env::set_var(LAUNCH_BIN_ENV, &shim) };
        let got = resolve_launch_program().unwrap();
        assert_eq!(
            got, shim,
            "explicit shim env must win over npm discovery, even before npm creates it"
        );
        match prev {
            Some(v) => unsafe { std::env::set_var(LAUNCH_BIN_ENV, v) },
            None => unsafe { std::env::remove_var(LAUNCH_BIN_ENV) },
        }
    }
}
