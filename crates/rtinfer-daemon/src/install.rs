//! LaunchAgent install / uninstall for the rtinfer daemon (macOS).
//!
//! Pins a per-user `KeepAlive` LaunchAgent to the STABLE npm global bin shim
//! (`$(npm prefix -g)/bin/rtinferd`) running `serve --port <port>`. npm rewrites
//! that shim in place on every `npm i -g`, so the daemon's self-update can drain
//! and exit, and launchd respawns the updated binary WITHOUT rewriting the
//! plist. The daemon is always-on: launchd respawns on crash and starts it at
//! login, so no external supervisor is needed.
//!
//! When the stable shim cannot be resolved (manual `rtinferd serve`, dev builds,
//! `npm` absent) the install falls back to the current executable path so a
//! hand-run install still works; that path is simply not self-update-stable.
//!
//! rtinfer itself has no keychain / TCC dependency. File auth reads
//! `~/.codex/auth.json`; credential-process mode delegates auth to an explicit
//! cse-toold binary. rtinfer needs no codesign pinning.

#[cfg(target_os = "macos")]
use std::path::{Path, PathBuf};
#[cfg(target_os = "macos")]
use std::process::Command;

#[cfg(target_os = "macos")]
use anyhow::Context;
use anyhow::Result;

/// launchd label for the rtinfer daemon.
#[cfg(target_os = "macos")]
pub const LABEL: &str = "com.jaredboynton.rtinferd";

/// Override the launched program path (set by the npm postinstall to the exact
/// global bin shim). Takes precedence over npm-prefix discovery.
#[cfg(target_os = "macos")]
const LAUNCH_BIN_ENV: &str = "RTINFER_LAUNCH_BIN";

/// Explicit node interpreter path (set by the npm postinstall to
/// `process.execPath`). Most reliable node for the launchd plist because
/// launchd does not inherit the user's shell PATH (fnm/nvm shims are absent).
#[cfg(target_os = "macos")]
const LAUNCH_NODE_ENV: &str = "RTINFER_LAUNCH_NODE";

#[cfg(target_os = "macos")]
fn home() -> Result<PathBuf> {
    dirs_home().context("cannot resolve home directory")
}

#[cfg(target_os = "macos")]
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
#[cfg(target_os = "macos")]
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

/// Resolve the `node` binary that launchd should use to run the npm shim.
/// launchd does NOT inherit the user's shell PATH (fnm/nvm shims are absent),
/// so a `#!/usr/bin/env node` shim fails with "env: node: No such file or
/// directory". We resolve the real node binary and bake both its directory
/// (into PATH) and the absolute node path (into ProgramArguments as an
/// explicit interpreter) so the shim works regardless of the launchd env.
/// Returns None if node cannot be resolved (non-shim installs don't need it).
#[cfg(target_os = "macos")]
fn resolve_node_bin() -> Option<PathBuf> {
    // Most reliable: the exact node the npm postinstall ran under
    // (`process.execPath`), handed through the environment.
    if let Some(node) = std::env::var_os(LAUNCH_NODE_ENV) {
        let p = PathBuf::from(&node);
        if p.exists() {
            return Some(p.canonicalize().unwrap_or(p));
        }
    }
    // Prefer the node running THIS process (works for npm lifecycle scripts
    // where npm sets the real node path in the environment).
    if let Some(node) = std::env::var_os("NODE") {
        if PathBuf::from(&node).exists() {
            return Some(PathBuf::from(node));
        }
    }
    if let Ok(exe) = std::env::current_exe() {
        // If we ARE the node binary (unlikely for the shim path), use it.
        if exe.file_name() == Some(std::ffi::OsStr::new("node")) {
            return Some(exe);
        }
    }
    // Fall back to `which node` via the user's shell.
    if let Some(out) = Command::new("sh")
        .args(["-lc", "command -v node"])
        .output()
        .ok()
        .filter(|o| o.status.success())
    {
        let node = String::from_utf8_lossy(&out.stdout).trim().to_string();
        let p = PathBuf::from(&node);
        if !node.is_empty() && p.exists() {
            return Some(p.canonicalize().unwrap_or(p));
        }
    }
    // Last resort: scan stable, version-manager-independent locations that
    // survive an fnm version switch/GC.
    for cand in [
        "/opt/homebrew/bin/node",
        "/usr/local/bin/node",
        "/usr/bin/node",
    ] {
        let p = PathBuf::from(cand);
        if p.exists() {
            return Some(p);
        }
    }
    None
}

/// Resolve the program path the LaunchAgent runs: prefer the stable npm shim,
/// fall back to the current executable (dev / manual installs).
#[cfg(target_os = "macos")]
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
///
/// When `node_bin` is Some, the program is an npm shim (`#!/usr/bin/env node`)
/// that needs the node interpreter on launchd's PATH. We bake the node binary
/// into ProgramArguments (as an explicit interpreter) and add EnvironmentVariables
/// with PATH so the shim's `require()` resolution and any child processes can
/// find node. Without this, launchd's environment lacks fnm/nvm shims and the
/// shim fails with "env: node: No such file or directory".
#[cfg(target_os = "macos")]
pub fn render_plist(program: &Path, home: &Path, port: u16, node_bin: Option<&Path>) -> String {
    let log = home.join("Library/Logs/rtinferd.log");
    let err = home.join("Library/Logs/rtinferd.err");

    // Build ProgramArguments: optionally prefix with the node interpreter.
    let program_args = if let Some(node) = node_bin {
        format!(
            "        <string>{node_str}</string>\n        <string>{program_str}</string>\n        <string>serve</string>\n        <string>--port</string>\n        <string>{port}</string>",
            node_str = node.display(),
            program_str = program.display(),
            port = port,
        )
    } else {
        format!(
            "        <string>{program_str}</string>\n        <string>serve</string>\n        <string>--port</string>\n        <string>{port}</string>",
            program_str = program.display(),
            port = port,
        )
    };

    // Build EnvironmentVariables with PATH including the node bin directory.
    let env_vars = if let Some(node) = node_bin {
        let node_dir = node
            .parent()
            .map(|p| p.display().to_string())
            .unwrap_or_default();
        let current_path = std::env::var("PATH").unwrap_or_default();
        // Prepend the node dir, then append stable fallbacks that survive an
        // fnm version switch/GC, so the shim's child processes find node even
        // if the version-specific node dir later disappears.
        let mut parts: Vec<String> = vec![node_dir];
        if !current_path.is_empty() {
            parts.push(current_path);
        }
        parts.push("/opt/homebrew/bin".to_string());
        parts.push("/usr/local/bin".to_string());
        parts.push("/usr/bin".to_string());
        let path_val = parts.join(":");
        format!(
            "    <key>EnvironmentVariables</key>\n    <dict>\n        <key>PATH</key>\n        <string>{path_val}</string>\n    </dict>\n"
        )
    } else {
        String::new()
    };

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{LABEL}</string>
    <key>ProgramArguments</key>
    <array>
{program_args}
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>ProcessType</key>
    <string>Background</string>
{env_vars}    <key>StandardOutPath</key>
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

/// Detect whether `program` is the npm JS shim (needs a node interpreter) vs a
/// native binary. Checks for a real `#!/usr/bin/env node` shebang or a `.js`
/// extension rather than a loose substring match.
#[cfg(target_os = "macos")]
fn is_node_shim(program: &Path) -> bool {
    if program.extension() == Some(std::ffi::OsStr::new("js")) {
        return true;
    }
    if let Ok(head) = std::fs::read_to_string(program) {
        let first = head.lines().next().unwrap_or_default();
        return first.starts_with("#!") && first.contains("node");
    }
    false
}

/// True if a process with `pid` is currently alive.
#[cfg(target_os = "macos")]
fn pid_alive(pid: u32) -> bool {
    unsafe extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }
    // Signal 0 performs error checking without sending a signal.
    unsafe { kill(pid as i32, 0) == 0 }
}

/// Send a signal to `pid`, ignoring the result.
#[cfg(target_os = "macos")]
fn signal(pid: u32, sig: i32) {
    unsafe extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }
    unsafe {
        let _ = kill(pid as i32, sig);
    }
}

/// Best-effort: find PIDs listening on the loopback `port` via `lsof`.
#[cfg(target_os = "macos")]
fn pids_on_port(port: u16) -> Vec<u32> {
    let out = Command::new("lsof")
        .args(["-ti", &format!("tcp:{port}"), "-sTCP:LISTEN"])
        .output();
    match out {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout)
            .split_whitespace()
            .filter_map(|s| s.parse::<u32>().ok())
            .collect(),
        _ => Vec::new(),
    }
}

/// Drain a running daemon `pid`: SIGTERM, poll up to ~5s for exit, then SIGKILL.
#[cfg(target_os = "macos")]
fn drain_pid(pid: u32) {
    const SIGTERM: i32 = 15;
    const SIGKILL: i32 = 9;
    if !pid_alive(pid) {
        return;
    }
    signal(pid, SIGTERM);
    for _ in 0..50 {
        if !pid_alive(pid) {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    signal(pid, SIGKILL);
    for _ in 0..30 {
        if !pid_alive(pid) {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

/// Poll until `127.0.0.1:port` can be bound (the previous holder fully released
/// it). Returns true if free within the timeout.
#[cfg(target_os = "macos")]
fn wait_port_free(port: u16) -> bool {
    use std::net::{Ipv4Addr, SocketAddr, TcpListener};
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    for _ in 0..60 {
        match TcpListener::bind(addr) {
            Ok(l) => {
                drop(l);
                return true;
            }
            Err(_) => std::thread::sleep(std::time::Duration::from_millis(50)),
        }
    }
    false
}

/// Install + load the LaunchAgent, pinned to the stable npm global shim (or the
/// current binary as a dev/manual fallback).
///
/// Intelligent replace: serialized by a single-flight lock, it detects every
/// holder of `port` (the launchd job, the pid in the endpoint file, and any
/// `lsof` listener), drains them, waits for the port to free, then bootstraps
/// exactly one LaunchAgent entry. No duplicates accumulate across re-installs,
/// and a stray debug/manual `rtinferd serve` is cleanly superseded.
#[cfg(target_os = "macos")]
pub fn run_install(port: u16) -> Result<()> {
    // A debug build must never become the system LaunchAgent (it self-isolates
    // on port 8766 and skips self-update); installing one would pin a
    // throwaway target into the plist.
    if cfg!(debug_assertions) {
        anyhow::bail!(
            "refusing to install a debug build as a LaunchAgent; build with --release or install via npm"
        );
    }

    // Single-flight: serialize against a concurrent self-update `npm i -g`
    // (whose postinstall also runs `rtinferd install`). Fail-open if the lock
    // cannot be set up; back off if another install holds it.
    let _lock = match crate::lock::try_acquire() {
        Ok(Some(g)) => Some(g),
        Ok(None) => {
            eprintln!("rtinferd install: another install is in progress; skipping");
            return Ok(());
        }
        Err(()) => None,
    };

    let program = resolve_launch_program()?;
    let home = home()?;
    let plist = plist_path()?;

    // Resolve the node interpreter when the program is an npm shim. launchd
    // does not inherit the shell PATH, so `#!/usr/bin/env node` fails without
    // an explicit interpreter in ProgramArguments.
    let is_shim = program.is_file() && is_node_shim(&program);
    let node_bin = if is_shim { resolve_node_bin() } else { None };
    if is_shim && node_bin.is_none() {
        anyhow::bail!(
            "cannot resolve a node interpreter for the npm shim ({}); \
             ensure node is on PATH and re-run `rtinferd install`, or run `rtinferd serve` directly",
            program.display()
        );
    }

    let uid = unsafe { libc_getuid() };
    let target = format!("gui/{uid}");
    let domain = format!("{target}/{LABEL}");

    // 1) Bootout any launchd-managed instance first so KeepAlive cannot respawn
    //    it while we drain the port.
    let _ = Command::new("launchctl")
        .args(["bootout", &domain])
        .status();

    // 2) Drain any remaining holder of the port: the pid recorded in the
    //    endpoint file (covers a bare `rtinferd serve`) and any lsof listener.
    if let Some(pid) = crate::endpoint_file::read_pid() {
        drain_pid(pid);
    }
    for pid in pids_on_port(port) {
        drain_pid(pid);
    }

    // 3) Confirm the port is actually free before bootstrapping (replaces the
    //    racy fixed sleep that caused "bootstrap failed: 5: Input/output error").
    if !wait_port_free(port) {
        anyhow::bail!("port {port} is still in use after draining; refusing to bootstrap");
    }

    if let Some(parent) = plist.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("cannot create {}", parent.display()))?;
    }
    std::fs::write(
        &plist,
        render_plist(&program, &home, port, node_bin.as_deref()),
    )
    .with_context(|| format!("cannot write plist {}", plist.display()))?;

    // 4) Bootstrap exactly one entry. On the residual "already loaded" race
    //    (EIO / exit 5), bootout once more and retry a single time.
    let bootstrap = Command::new("launchctl")
        .args(["bootstrap", &target, &plist.display().to_string()])
        .status();
    if !matches!(bootstrap, Ok(s) if s.success()) {
        let _ = Command::new("launchctl")
            .args(["bootout", &domain])
            .status();
        let _ = wait_port_free(port);
        let retry = Command::new("launchctl")
            .args(["bootstrap", &target, &plist.display().to_string()])
            .status();
        if !matches!(retry, Ok(s) if s.success()) {
            anyhow::bail!(
                "launchctl bootstrap failed for {domain}; check ~/Library/Logs/rtinferd.err"
            );
        }
    }
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
            None,
        );
        assert!(p.contains("<string>/usr/local/bin/rtinferd</string>"));
        assert!(p.contains("<string>serve</string>"));
        assert!(p.contains("<string>8765</string>"));
        assert!(p.contains("com.jaredboynton.rtinferd"));
        assert!(p.contains("<key>KeepAlive</key>"));
        assert!(
            !p.contains("EnvironmentVariables"),
            "native binary needs no env"
        );
    }

    #[test]
    fn plist_adds_node_interpreter_and_path_for_shim() {
        let p = render_plist(
            Path::new("/Users/x/.fnm/bin/rtinferd"),
            Path::new("/Users/x"),
            8765,
            Some(Path::new("/Users/x/.fnm/node/v25/bin/node")),
        );
        assert!(
            p.contains("<string>/Users/x/.fnm/node/v25/bin/node</string>"),
            "node interpreter in argv"
        );
        assert!(
            p.contains("<string>/Users/x/.fnm/bin/rtinferd</string>"),
            "shim path in argv"
        );
        assert!(p.contains("EnvironmentVariables"), "env vars block present");
        assert!(p.contains("/Users/x/.fnm/node/v25/bin"), "node dir on PATH");
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

    #[test]
    fn detects_node_shim_by_shebang_and_extension() {
        let dir = std::env::temp_dir();
        let shim = dir.join("rtinferd-shim-test.shim");
        std::fs::write(&shim, "#!/usr/bin/env node\n'use strict';\n").unwrap();
        assert!(is_node_shim(&shim), "shebang shim detected");
        std::fs::remove_file(&shim).ok();

        let js = dir.join("rtinferd-shim-test.js");
        std::fs::write(&js, "console.log('x')\n").unwrap();
        assert!(is_node_shim(&js), ".js extension detected");
        std::fs::remove_file(&js).ok();

        let native = dir.join("rtinferd-native-test");
        std::fs::write(&native, b"\x7fELF native binary node\n").unwrap();
        assert!(
            !is_node_shim(&native),
            "native binary mentioning node is not a shim"
        );
        std::fs::remove_file(&native).ok();
    }
}
