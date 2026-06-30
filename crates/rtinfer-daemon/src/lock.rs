//! Cross-process single-flight lock under `~/.cse-rtinfer/`.
//!
//! Both `rtinferd install` and the in-daemon self-update path can run
//! `launchctl` / `npm i -g` concurrently (e.g. a self-update's `npm i -g`
//! re-runs the postinstall, which runs `rtinferd install`). An advisory
//! `flock` serializes them so we never bootstrap two LaunchAgents or race a
//! bootout against a bootstrap.
//!
//! Fail-open: if the lock directory is unwritable or `flock` is unavailable,
//! callers proceed unlocked rather than refusing to install.

#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::path::PathBuf;

/// Lock file path: `~/.cse-rtinfer/install.lock`.
pub fn lock_path() -> Option<PathBuf> {
    crate::endpoint_file::dir().map(|d| d.join("install.lock"))
}

/// Held lock; releases (closes the fd) on drop.
pub struct LockGuard {
    #[allow(dead_code)]
    file: std::fs::File,
}

/// Try to acquire the exclusive install lock without blocking.
///
/// Returns `Ok(Some(guard))` when acquired, `Ok(None)` when another process
/// holds it (caller should back off), and `Err(())` when the lock could not be
/// set up at all (caller should fail-open and proceed unlocked).
#[cfg(unix)]
pub fn try_acquire() -> Result<Option<LockGuard>, ()> {
    let path = lock_path().ok_or(())?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|_| ())?;
    }
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .map_err(|_| ())?;

    unsafe extern "C" {
        fn flock(fd: i32, op: i32) -> i32;
    }
    // LOCK_EX | LOCK_NB
    const LOCK_EX_NB: i32 = 2 | 4;
    let rc = unsafe { flock(file.as_raw_fd(), LOCK_EX_NB) };
    if rc == 0 {
        Ok(Some(LockGuard { file }))
    } else {
        // EWOULDBLOCK -> held by another process.
        Ok(None)
    }
}

#[cfg(not(unix))]
pub fn try_acquire() -> Result<Option<LockGuard>, ()> {
    Err(())
}
