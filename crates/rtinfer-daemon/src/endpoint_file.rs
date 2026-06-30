//! Well-known endpoint discovery file.
//!
//! On boot the daemon writes `~/.cse-rtinfer/endpoint.json` so clients that do
//! not know the port can discover a running daemon. The file name and shape
//! match the contract every rtinfer client already probes:
//!
//! ```json
//! { "contract": "rtinfer/1", "base_url": "http://127.0.0.1:8765", "pid": 1234 }
//! ```
//!
//! Discovery order in every client is: `$CSE_RTINFER_URL` -> cockpit default
//! (`http://127.0.0.1:8787`) -> this file. The cockpit default predates the
//! standalone daemon; once cse-toold drops its `/v1/infer` server, this file is
//! the authoritative advertisement.

use std::io::Write;
use std::path::PathBuf;

use serde_json::json;

/// Directory holding the well-known file: `~/.cse-rtinfer`.
pub fn dir() -> Option<PathBuf> {
    home_dir().map(|h| h.join(".cse-rtinfer"))
}

/// Full path to the well-known file: `~/.cse-rtinfer/endpoint.json`.
///
/// Debug builds advertise at `endpoint-dev.json` instead so a local
/// `cargo run -- serve` (port 8766) never shadows the release daemon's
/// authoritative `endpoint.json`.
pub fn path() -> Option<PathBuf> {
    let name = if cfg!(debug_assertions) {
        "endpoint-dev.json"
    } else {
        "endpoint.json"
    };
    dir().map(|d| d.join(name))
}

/// Read the advertised daemon PID from the (release) endpoint file, if present.
/// Used by `install` to detect and drain a running instance.
pub fn read_pid() -> Option<u32> {
    let p = dir().map(|d| d.join("endpoint.json"))?;
    let body = std::fs::read_to_string(p).ok()?;
    let v: serde_json::Value = serde_json::from_str(&body).ok()?;
    v.get("pid")?.as_u64().map(|n| n as u32)
}

/// Write the endpoint advertisement atomically (tmp + rename, 0600).
pub fn write(base_url: &str) -> std::io::Result<()> {
    let path = path()
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "no home directory"))?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let body = serde_json::to_vec_pretty(&json!({
        "contract": crate::server::RTINFER_CONTRACT,
        "base_url": base_url,
        "pid": std::process::id(),
    }))?;
    let mut tmp = path.clone().into_os_string();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    {
        #[cfg(unix)]
        let mut f = {
            use std::os::unix::fs::OpenOptionsExt;
            std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(&tmp)?
        };
        #[cfg(not(unix))]
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&body)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, &path)
}

/// Best-effort removal of the well-known file (used by `uninstall`).
pub fn remove() -> std::io::Result<()> {
    match path() {
        Some(p) => match std::fs::remove_file(&p) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        },
        None => Ok(()),
    }
}

#[cfg(unix)]
fn home_dir() -> Option<PathBuf> {
    use std::ffi::CStr;
    use std::os::raw::{c_char, c_uint};

    #[cfg(any(target_os = "macos", target_os = "ios"))]
    #[repr(C)]
    struct Passwd {
        pw_name: *mut c_char,
        pw_passwd: *mut c_char,
        pw_uid: c_uint,
        pw_gid: c_uint,
        pw_change: isize,
        pw_class: *mut c_char,
        pw_gecos: *mut c_char,
        pw_dir: *mut c_char,
        pw_shell: *mut c_char,
        pw_expire: isize,
    }

    #[cfg(not(any(target_os = "macos", target_os = "ios")))]
    #[repr(C)]
    struct Passwd {
        pw_name: *mut c_char,
        pw_passwd: *mut c_char,
        pw_uid: c_uint,
        pw_gid: c_uint,
        pw_gecos: *mut c_char,
        pw_dir: *mut c_char,
        pw_shell: *mut c_char,
    }

    unsafe extern "C" {
        fn getuid() -> c_uint;
        fn getpwuid(uid: c_uint) -> *mut Passwd;
    }

    let uid = unsafe { getuid() };
    let passwd = unsafe { getpwuid(uid) };
    if passwd.is_null() {
        return None;
    }
    let dir = unsafe { (*passwd).pw_dir };
    if dir.is_null() {
        return None;
    }
    let path = unsafe { CStr::from_ptr(dir) };
    Some(PathBuf::from(path.to_string_lossy().into_owned()))
}

#[cfg(not(unix))]
fn home_dir() -> Option<PathBuf> {
    std::env::var_os("USERPROFILE").map(PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_ends_with_well_known() {
        if let Some(p) = path() {
            assert!(
                p.ends_with(".cse-rtinfer/endpoint.json")
                    || p.ends_with(".cse-rtinfer/endpoint-dev.json")
            );
        }
    }
}
