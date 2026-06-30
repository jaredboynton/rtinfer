//! Lint guard: forbid direct `RealtimeClient::new` construction outside
//! `crates/rtinfer-core/{src,tests}/`.
//!
//! The shared [`rtinfer_core::RealtimePool`] is the only sanctioned
//! entry point for opening a Realtime WebSocket so that auth fetch,
//! TLS provider install, and websocket handshake setup are amortised
//! across cockpit fan-out and orgchart Q&A. New callers that bypass
//! the pool reintroduce the per-ask file read + handshake cost the
//! pool exists to avoid, so this test fails CI before they land.
//!
//! ## How to authorise a new caller
//!
//! In almost every case the answer is "use `RealtimePool` instead".
//! If you genuinely need raw `RealtimeClient::new` (rare; the pool
//! covers cockpit + orgchart + tests), add the file's repo-relative
//! path to [`ALLOWED_PATHS`] below with a comment explaining why.
//!
//! ## Failure shape
//!
//! When this test fails, the assertion lists every offending file +
//! line + match snippet, plus a pointer back to this file's
//! `ALLOWED_PATHS` and the recommended replacement (`RealtimePool`).

use std::fs;
use std::path::{Path, PathBuf};

/// Repo-relative paths that are allowed to call
/// `RealtimeClient::new(...)` directly. Everything under
/// `crates/rtinfer-core/src/` and `crates/rtinfer-core/tests/` is
/// implicitly allowed by [`is_realtime_crate_path`]; this list is for
/// any other approved entry points (currently empty — cockpit and
/// orgchart go through `RealtimePool`).
const ALLOWED_PATHS: &[&str] = &[];

/// Substring matched against `Display`-formatted paths to identify
/// files inside the `rtinfer-core` crate proper. Anything matching
/// this is implicitly allowed: tests under
/// `crates/rtinfer-core/tests/` and source under
/// `crates/rtinfer-core/src/` legitimately need to construct the raw
/// client.
fn is_realtime_crate_path(path: &Path) -> bool {
    let s = path.to_string_lossy().replace('\\', "/");
    s.contains("/crates/rtinfer-core/src/") || s.contains("/crates/rtinfer-core/tests/")
}

/// Lint-guard self-exclusion. This test contains the matched substring
/// inside string literals (the failure-message hint); skipping it prevents
/// the guard from matching itself.
fn is_self_guard_path(path: &Path) -> bool {
    let s = path.to_string_lossy().replace('\\', "/");
    s.ends_with("/crates/rtinfer-core/tests/no_direct_construction.rs")
}

#[test]
fn no_direct_realtime_client_construction_outside_pool() {
    let workspace_root = workspace_root();
    let crates_dir = workspace_root.join("crates");
    assert!(
        crates_dir.is_dir(),
        "expected workspace `crates/` directory at {crates_dir:?}"
    );

    let allowed_abs: Vec<PathBuf> = ALLOWED_PATHS
        .iter()
        .map(|rel| workspace_root.join(rel))
        .collect();

    let mut offenders: Vec<String> = Vec::new();
    walk_rs(&crates_dir, &mut |path| {
        if is_realtime_crate_path(path) {
            return;
        }
        if is_self_guard_path(path) {
            return;
        }
        if allowed_abs
            .iter()
            .any(|allow| paths_equivalent(path, allow))
        {
            return;
        }
        let body = match fs::read_to_string(path) {
            Ok(b) => b,
            Err(_) => return,
        };
        for (idx, line) in body.lines().enumerate() {
            let trimmed = line.trim_start();
            if trimmed.starts_with("//") || trimmed.starts_with("///") || trimmed.starts_with("//!")
            {
                continue;
            }
            if line.contains("RealtimeClient::new(") {
                offenders.push(format!(
                    "{}:{}: {}",
                    path.strip_prefix(&workspace_root).unwrap_or(path).display(),
                    idx + 1,
                    line.trim()
                ));
            }
        }
    });

    if !offenders.is_empty() {
        let mut msg = String::from(
            "direct `RealtimeClient::new(...)` construction is forbidden outside \
             `crates/rtinfer-core/{src,tests}/`. Use `rtinfer_core::RealtimePool` \
             (see `crates/rtinfer-core/src/pool.rs`) so the auth fetch and \
             websocket handshake are amortised across cockpit fan-out and \
             orgchart-ask. To authorise a new path despite this guidance, edit \
             `ALLOWED_PATHS` in `crates/rtinfer-core/tests/no_direct_construction.rs`.\n\
             Offending sites:\n",
        );
        for o in offenders {
            msg.push_str("  - ");
            msg.push_str(&o);
            msg.push('\n');
        }
        panic!("{msg}");
    }
}

fn workspace_root() -> PathBuf {
    // We can't use the `CARGO_MANIFEST_DIR` compile-time macro here —
    // the original workspace build gate forbade it, see
    // its build.rs. Instead
    // we walk up from `current_dir` (cargo sets cwd to the crate root
    // when invoking `cargo test -p rtinfer-core`) until we find a
    // directory that contains `crates/`.
    let cwd = std::env::current_dir().expect("current_dir for workspace_root");
    let mut walker = cwd.as_path();
    loop {
        if walker.join("crates").is_dir() {
            return walker.to_path_buf();
        }
        match walker.parent() {
            Some(p) => walker = p,
            None => return cwd,
        }
    }
}

fn paths_equivalent(a: &Path, b: &Path) -> bool {
    match (a.canonicalize(), b.canonicalize()) {
        (Ok(ap), Ok(bp)) => ap == bp,
        _ => a == b,
    }
}

fn walk_rs(root: &Path, visit: &mut dyn FnMut(&Path)) {
    let entries = match fs::read_dir(root) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let file_name = entry.file_name();
        let name = file_name.to_string_lossy();
        if name == "target" || name.starts_with('.') {
            continue;
        }
        if path.is_dir() {
            walk_rs(&path, visit);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            // Walk only crate `src/` and `tests/` trees — skip examples,
            // benches, etc. that may legitimately ship example clients.
            let s = path.to_string_lossy().replace('\\', "/");
            if s.contains("/src/") || s.contains("/tests/") {
                visit(&path);
            }
        }
    }
}
