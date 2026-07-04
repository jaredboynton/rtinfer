//! In-daemon self-update: keep the always-on daemon on the latest npm release
//! without an external supervisor.
//!
//! # Why
//!
//! `rtinferd` is published as `@jaredboynton/rtinfer` and installed globally
//! (`npm i -g`). The launchd plist pins the STABLE npm bin shim, which npm
//! rewrites in place on every `npm i -g`. So the only thing needed to adopt a
//! new release is: notice a newer version on the registry, run `npm i -g`, then
//! exit cleanly so launchd respawns the (now-updated) shim.
//!
//! This mirrors the SPIRIT of cse-toold's self-update without its
//! signature/codesign state machine: rtinferd is loopback-only, reads only
//! `~/.codex/auth.json`, and has no keychain/TCC/codesign pinning (see
//! `install.rs`), so a signed multi-phase installer would be unjustified weight.
//!
//! # Fail-open
//!
//! Every step is best-effort. A missing `npm`, an offline registry, a parse
//! failure, or a non-zero `npm i -g` is treated as "no update" and the daemon
//! keeps serving the running version. Only a CONFIRMED newer version that
//! installs cleanly triggers the drain+exit.

use std::process::Stdio;
use std::time::Duration;

use serde_json::json;
use tokio::process::Command;
use tokio::sync::watch;
use tokio::time::interval;
use tracing::{info, warn};

/// npm package that publishes the daemon binary.
const PACKAGE: &str = "@jaredboynton/rtinfer";

/// Running daemon version (the published meta-package version tracks this).
pub fn current_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Default poll cadence. Generous: a new release does not need to be adopted
/// within seconds, and frequent `npm view` calls are wasteful.
pub const DEFAULT_CHECK_INTERVAL: Duration = Duration::from_secs(30 * 60);

/// Cap on the consecutive-failure backoff multiplier (8 * 30min = 4h).
const MAX_BACKOFF_TICKS: u32 = 8;

/// Persisted self-update state: remembers the last version we tried to install
/// and the consecutive failure count so a broken release cannot drive an
/// infinite `npm i -g` loop. Best-effort: read/write failures degrade to the
/// current (stateless) behavior.
fn state_path() -> Option<std::path::PathBuf> {
    crate::endpoint_file::dir().map(|d| d.join("self-update.json"))
}

fn read_last_attempt() -> Option<String> {
    let p = state_path()?;
    let body = std::fs::read_to_string(p).ok()?;
    let v: serde_json::Value = serde_json::from_str(&body).ok()?;
    v.get("last_attempted")?.as_str().map(|s| s.to_string())
}

fn write_last_attempt(version: &str) {
    if let Some(p) = state_path() {
        if let Some(parent) = p.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let _ = std::fs::write(
            &p,
            serde_json::to_vec_pretty(&json!({ "last_attempted": version })).unwrap_or_default(),
        );
    }
}

/// Spawn the background self-update poller. On a confirmed newer published
/// version that installs cleanly, it flips `shutdown` so `serve` drains and
/// exits; launchd then respawns the updated shim. Returns immediately.
///
/// Disabled entirely for debug builds and when `RTINFER_SKIP_SELF_UPDATE=1`:
/// a `cargo run -- serve` must never run `npm i -g` against the global install.
pub fn spawn(shutdown: watch::Sender<bool>, check_interval: Duration) {
    if cfg!(debug_assertions) || std::env::var_os("RTINFER_SKIP_SELF_UPDATE").is_some() {
        info!("rtinfer: self-update disabled (debug build or RTINFER_SKIP_SELF_UPDATE)");
        // Leak the sender so the receiver's `changed()` in `serve`'s
        // graceful-shutdown future does not error immediately (all senders
        // dropped) and trigger an instant drain + exit. The server then stays
        // up until SIGINT/SIGTERM.
        std::mem::forget(shutdown);
        return;
    }
    tokio::spawn(async move {
        let mut tick = interval(check_interval);
        // The first tick fires immediately; skip it so we do not check before
        // the daemon has even finished booting.
        tick.tick().await;
        // Number of extra ticks to skip after a failure (exponential backoff).
        let mut skip_ticks: u32 = 0;
        let mut failures: u32 = 0;
        loop {
            tick.tick().await;
            if *shutdown.borrow() {
                return;
            }
            if skip_ticks > 0 {
                skip_ticks -= 1;
                continue;
            }
            match check_and_update().await {
                Ok(true) => {
                    info!("rtinfer: newer version installed; draining for launchd respawn");
                    let _ = shutdown.send(true);
                    return;
                }
                Ok(false) => {
                    failures = 0;
                }
                Err(e) => {
                    failures = failures.saturating_add(1);
                    skip_ticks = failures.min(MAX_BACKOFF_TICKS);
                    warn!(error = %e, failures, "rtinfer: self-update check skipped; backing off");
                }
            }
        }
    });
}

/// One check+update cycle. Returns `Ok(true)` only when a strictly-newer STABLE
/// version was found AND `npm i -g` succeeded (so the caller should drain+exit).
async fn check_and_update() -> Result<bool, String> {
    let latest = npm_latest_version().await?;
    let current = current_version();
    // Never auto-adopt a prerelease (e.g. `0.2.0-rc1`); `latest` should already
    // exclude them, but guard defensively.
    if is_prerelease(&latest) {
        return Ok(false);
    }
    if !is_newer(&latest, current) {
        return Ok(false);
    }
    // If we already tried this exact version and we are STILL on an older
    // build, the prior `npm i -g` did not advance the running version (broken
    // release / version decoupling). Skip rather than loop forever.
    if read_last_attempt().as_deref() == Some(latest.as_str()) {
        warn!(latest = %latest, current, "rtinfer: latest already attempted but version did not advance; skipping");
        return Ok(false);
    }
    info!(current, latest = %latest, "rtinfer: newer version published; installing");
    write_last_attempt(&latest);

    // Serialize against `rtinferd install` (postinstall) via the shared lock.
    let _guard = crate::lock::try_acquire();
    npm_install_global().await?;
    Ok(true)
}

/// `npm view <pkg> version` -> trimmed version string. Errors on any failure so
/// the caller treats it as "no update".
async fn npm_latest_version() -> Result<String, String> {
    let out = Command::new("npm")
        .args(["view", &format!("{PACKAGE}@latest"), "version"])
        .stdin(Stdio::null())
        .output()
        .await
        .map_err(|e| format!("npm view spawn: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "npm view exited {}",
            out.status.code().unwrap_or(-1)
        ));
    }
    let v = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if v.is_empty() {
        return Err("npm view returned empty version".into());
    }
    Ok(v)
}

/// `npm i -g <pkg>@latest`. Errors on non-zero exit.
///
/// Sets `RTINFER_SKIP_POSTINSTALL=1`: the new package's postinstall would
/// otherwise run `rtinferd install` and re-bootstrap the LaunchAgent while we
/// are draining for the respawn. launchd respawns the updated shim on exit, so
/// no reinstall is needed here.
async fn npm_install_global() -> Result<(), String> {
    let status = Command::new("npm")
        .args(["install", "-g", "--quiet", &format!("{PACKAGE}@latest")])
        .env("RTINFER_SKIP_POSTINSTALL", "1")
        .stdin(Stdio::null())
        .status()
        .await
        .map_err(|e| format!("npm install spawn: {e}"))?;
    if !status.success() {
        return Err(format!(
            "npm install -g exited {}",
            status.code().unwrap_or(-1)
        ));
    }
    Ok(())
}

/// Strict semver-ish "is `candidate` newer than `current`" by numeric
/// dot-segment comparison. Non-numeric segments compare as 0, and any parse
/// ambiguity returns false (never update on a version we cannot order).
pub fn is_newer(candidate: &str, current: &str) -> bool {
    let parse = |s: &str| -> Vec<u64> {
        s.split('.')
            .map(|seg| {
                seg.chars()
                    .take_while(|c| c.is_ascii_digit())
                    .collect::<String>()
                    .parse::<u64>()
                    .unwrap_or(0)
            })
            .collect()
    };
    let a = parse(candidate);
    let b = parse(current);
    let n = a.len().max(b.len());
    for i in 0..n {
        let x = a.get(i).copied().unwrap_or(0);
        let y = b.get(i).copied().unwrap_or(0);
        if x != y {
            return x > y;
        }
    }
    false
}

/// A version is a prerelease if it carries a `-` suffix (e.g. `0.2.0-rc1`).
pub fn is_prerelease(v: &str) -> bool {
    v.contains('-')
}

#[cfg(test)]
mod tests {
    use super::{is_newer, is_prerelease};

    #[test]
    fn detects_strictly_newer_versions() {
        assert!(is_newer("0.1.1", "0.1.0"));
        assert!(is_newer("0.2.0", "0.1.9"));
        assert!(is_newer("1.0.0", "0.99.99"));
        assert!(is_newer("0.1.10", "0.1.9"), "numeric, not lexical");
    }

    #[test]
    fn rejects_same_or_older() {
        assert!(!is_newer("0.1.0", "0.1.0"));
        assert!(!is_newer("0.1.0", "0.1.1"));
        assert!(!is_newer("0.1.9", "0.1.10"));
    }

    #[test]
    fn tolerates_prerelease_suffix_without_panicking() {
        // Suffix segments degrade to their numeric prefix; never panics.
        assert!(!is_newer("0.1.0-rc1", "0.1.0"));
        assert!(is_newer("0.2.0-rc1", "0.1.0"));
    }

    #[test]
    fn prerelease_is_detected() {
        assert!(is_prerelease("0.2.0-rc1"));
        assert!(is_prerelease("1.0.0-beta.2"));
        assert!(!is_prerelease("0.1.3"));
        assert!(!is_prerelease("10.20.30"));
    }
}
