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

/// Spawn the background self-update poller. On a confirmed newer published
/// version that installs cleanly, it flips `shutdown` so `serve` drains and
/// exits; launchd then respawns the updated shim. Returns immediately.
pub fn spawn(shutdown: watch::Sender<bool>, check_interval: Duration) {
    tokio::spawn(async move {
        let mut tick = interval(check_interval);
        // The first tick fires immediately; skip it so we do not check before
        // the daemon has even finished booting.
        tick.tick().await;
        loop {
            tick.tick().await;
            if *shutdown.borrow() {
                return;
            }
            match check_and_update().await {
                Ok(true) => {
                    info!("rtinfer: newer version installed; draining for launchd respawn");
                    let _ = shutdown.send(true);
                    return;
                }
                Ok(false) => {}
                Err(e) => warn!(error = %e, "rtinfer: self-update check skipped"),
            }
        }
    });
}

/// One check+update cycle. Returns `Ok(true)` only when a strictly-newer version
/// was found AND `npm i -g` succeeded (so the caller should drain+exit).
async fn check_and_update() -> Result<bool, String> {
    let latest = npm_latest_version().await?;
    let current = current_version();
    if !is_newer(&latest, current) {
        return Ok(false);
    }
    info!(current, latest = %latest, "rtinfer: newer version published; installing");
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
async fn npm_install_global() -> Result<(), String> {
    let status = Command::new("npm")
        .args(["install", "-g", "--quiet", &format!("{PACKAGE}@latest")])
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

#[cfg(test)]
mod tests {
    use super::is_newer;

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
}
