//! cse-toold credential-process [`CodexAuthSource`](rtinfer_core::CodexAuthSource).
//!
//! The provider owns refresh. rtinfer receives only a bounded v1 lease, never a
//! refresh token, and never reads `auth.json` while this source is configured.

use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rtinfer_core::{CodexAuth, CodexAuthSource, RealtimeError};
use serde::Deserialize;
use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;

/// Minimum remaining validity requested from cse-toold and required in its
/// response.
pub const MIN_VALID_FOR_SECS: u64 = 300;

// cse-toold can spend up to roughly six minutes in its bounded worker re-mint
// plus cold-mint recovery. HTTP callers may impose a shorter request deadline;
// kill_on_drop still terminates this child if their request is cancelled.
const CHILD_TIMEOUT: Duration = Duration::from_secs(7 * 60);
const MAX_STDOUT_BYTES: usize = 64 * 1024;

#[derive(Default)]
struct ProviderState {
    cached: Option<CachedLease>,
}

struct CachedLease {
    auth: CodexAuth,
    lease_id: String,
    expires_at: i64,
}

/// Codex auth source backed by an explicitly configured cse-toold binary.
pub struct CseTooldCodexAuthSource {
    bin: PathBuf,
    state: Mutex<ProviderState>,
    child_timeout: Duration,
}

impl std::fmt::Debug for CseTooldCodexAuthSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CseTooldCodexAuthSource")
            .field("bin", &self.bin)
            .field("child_timeout", &self.child_timeout)
            .finish_non_exhaustive()
    }
}

impl CseTooldCodexAuthSource {
    pub fn new(bin: impl Into<PathBuf>) -> Result<Self, RealtimeError> {
        let bin = bin.into();
        if !bin.is_absolute() {
            return Err(provider_error("cse-toold bin path must be absolute"));
        }
        Ok(Self {
            bin,
            state: Mutex::new(ProviderState::default()),
            child_timeout: CHILD_TIMEOUT,
        })
    }

    pub fn shared(bin: impl Into<PathBuf>) -> Result<Arc<Self>, RealtimeError> {
        Ok(Arc::new(Self::new(bin)?))
    }

    #[cfg(test)]
    fn with_timeout(mut self, timeout: Duration) -> Self {
        self.child_timeout = timeout;
        self
    }

    async fn mint_locked(
        &self,
        state: &mut ProviderState,
        rejected_lease: Option<&str>,
    ) -> Result<CodexAuth, RealtimeError> {
        let lease = self.run_lease(rejected_lease).await?;
        let auth = CodexAuth {
            access_token: lease.access_token,
            account_id: lease.account_id,
            id_token: lease.id_token,
            refresh_token: String::new(),
            source_path: None,
        };
        state.cached = Some(CachedLease {
            auth: auth.clone(),
            lease_id: lease.lease_id,
            expires_at: lease.expires_at,
        });
        Ok(auth)
    }

    async fn run_lease(
        &self,
        rejected_lease: Option<&str>,
    ) -> Result<LeaseResponse, RealtimeError> {
        let mut command = Command::new(&self.bin);
        command
            .arg("codex-lease")
            .arg("--min-valid-for-seconds")
            .arg(MIN_VALID_FOR_SECS.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        if let Some(lease_id) = rejected_lease {
            command.arg("--rejected-lease").arg(lease_id);
        }

        let mut child = command
            .spawn()
            .map_err(|e| provider_error(format!("failed to spawn cse-toold codex-lease: {e}")))?;
        let mut stdout = child
            .stdout
            .take()
            .ok_or_else(|| provider_error("cse-toold codex-lease missing stdout pipe"))?;

        let collect = async {
            let mut bytes = Vec::new();
            let mut chunk = [0_u8; 4096];
            loop {
                let read = stdout.read(&mut chunk).await.map_err(|e| {
                    provider_error(format!("cse-toold codex-lease stdout read failed: {e}"))
                })?;
                if read == 0 {
                    break;
                }
                if bytes.len().saturating_add(read) > MAX_STDOUT_BYTES {
                    return Err(provider_error(
                        "cse-toold codex-lease stdout exceeded bound",
                    ));
                }
                bytes.extend_from_slice(&chunk[..read]);
            }
            let status = child
                .wait()
                .await
                .map_err(|e| provider_error(format!("cse-toold codex-lease wait failed: {e}")))?;
            Ok::<_, RealtimeError>((bytes, status))
        };

        let (bytes, status) = match tokio::time::timeout(self.child_timeout, collect).await {
            Ok(Ok(output)) => output,
            Ok(Err(error)) => {
                kill_and_reap(&mut child).await;
                return Err(error);
            }
            Err(_) => {
                kill_and_reap(&mut child).await;
                return Err(provider_error("cse-toold codex-lease timed out"));
            }
        };

        if !status.success() {
            return Err(provider_error(format!(
                "cse-toold codex-lease exited nonzero ({})",
                status.code().unwrap_or(-1)
            )));
        }

        let lease: LeaseResponse = serde_json::from_slice(&bytes)
            .map_err(|_| provider_error("cse-toold codex-lease returned malformed v1 json"))?;
        validate_lease(&lease)?;
        Ok(lease)
    }
}

async fn kill_and_reap(child: &mut Child) {
    let _ = child.start_kill();
    let _ = child.wait().await;
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LeaseResponse {
    v: u32,
    lease_id: String,
    access_token: String,
    id_token: String,
    account_id: String,
    expires_at: i64,
}

fn validate_lease(lease: &LeaseResponse) -> Result<(), RealtimeError> {
    if lease.v != 1 {
        return Err(provider_error(format!(
            "cse-toold codex-lease unsupported version {}",
            lease.v
        )));
    }
    if lease.lease_id.is_empty()
        || lease.access_token.is_empty()
        || lease.id_token.is_empty()
        || lease.account_id.is_empty()
    {
        return Err(provider_error(
            "cse-toold codex-lease missing required nonempty field",
        ));
    }
    if lease.lease_id.len() != 64 || !lease.lease_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(provider_error(
            "cse-toold codex-lease returned invalid lease id",
        ));
    }
    if lease.expires_at.saturating_sub(now_unix()) < MIN_VALID_FOR_SECS as i64 {
        return Err(provider_error(
            "cse-toold codex-lease expires before requested minimum",
        ));
    }
    Ok(())
}

fn cached_still_valid(cached: &CachedLease) -> bool {
    cached.expires_at.saturating_sub(now_unix()) >= MIN_VALID_FOR_SECS as i64
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}

fn provider_error(message: impl Into<String>) -> RealtimeError {
    RealtimeError::Refresh(message.into())
}

#[async_trait]
impl CodexAuthSource for CseTooldCodexAuthSource {
    async fn load(&self) -> Result<CodexAuth, RealtimeError> {
        let mut state = self.state.lock().await;
        if let Some(cached) = state.cached.as_ref() {
            if cached_still_valid(cached) {
                return Ok(cached.auth.clone());
            }
        }
        self.mint_locked(&mut state, None).await
    }

    async fn force_refresh(&self, rejected_access_token: &str) -> Result<CodexAuth, RealtimeError> {
        let mut state = self.state.lock().await;
        if let Some(cached) = state.cached.as_ref() {
            if !rejected_access_token.is_empty()
                && cached.auth.access_token != rejected_access_token
                && cached_still_valid(cached)
            {
                return Ok(cached.auth.clone());
            }
        }
        let rejected_lease = state.cached.as_ref().map(|cached| cached.lease_id.clone());
        self.mint_locked(&mut state, rejected_lease.as_deref())
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use std::time::Instant;

    fn write_executable(dir: &Path, name: &str, body: &str) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, body).unwrap();
        let mut permissions = std::fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&path, permissions).unwrap();
        path
    }

    fn fake_lease_script(dir: &Path) -> (PathBuf, PathBuf, PathBuf) {
        let argv_log = dir.join("argv.log");
        let call_count = dir.join("call-count");
        let script = format!(
            r#"#!/bin/sh
set -eu
for arg in "$@"; do printf '%s\n' "$arg" >> "{argv}"; done
count=0
if [ -f "{count}" ]; then count=$(cat "{count}"); fi
count=$((count + 1))
printf '%s' "$count" > "{count}"
forced=0
for arg in "$@"; do
  if [ "$arg" = "--rejected-lease" ]; then forced=1; fi
done
expires=$(( $(date +%s) + 3600 ))
if [ "$forced" -eq 1 ]; then
  lease=$(printf '%064d' $((1000 + count)))
  printf '{{"v":1,"lease_id":"%s","access_token":"access-forced-%s","id_token":"id-token-%s","account_id":"acct-1","expires_at":%s}}\n' "$lease" "$count" "$count" "$expires"
else
  lease=$(printf '%064d' "$count")
  printf '{{"v":1,"lease_id":"%s","access_token":"access-%s","id_token":"id-token-%s","account_id":"acct-1","expires_at":%s}}\n' "$lease" "$count" "$count" "$expires"
fi
"#,
            argv = argv_log.display(),
            count = call_count.display(),
        );
        (
            write_executable(dir, "fake-cse-toold", &script),
            argv_log,
            call_count,
        )
    }

    fn valid_json_with(overrides: &str) -> String {
        let expires = now_unix() + 3600;
        format!(
            r#"{{"v":1,"lease_id":"lease-secret","access_token":"access-secret","id_token":"id-secret","account_id":"acct-1","expires_at":{expires}{overrides}}}"#
        )
    }

    #[test]
    fn child_timeout_covers_cse_toold_recovery_budget() {
        assert!(
            CHILD_TIMEOUT >= Duration::from_secs(5 * 60),
            "credential process must outlive cse-toold's bounded remint/cold-mint recovery"
        );
    }

    #[tokio::test]
    async fn load_uses_exact_argv_and_returns_non_refreshable_auth() {
        let dir = tempfile::tempdir().unwrap();
        let (bin, argv_log, call_count) = fake_lease_script(dir.path());
        let source = CseTooldCodexAuthSource::new(bin).unwrap();

        let auth = source.load().await.unwrap();
        let cached = source.load().await.unwrap();

        assert_eq!(auth.access_token, "access-1");
        assert_eq!(auth.id_token, "id-token-1");
        assert_eq!(auth.account_id, "acct-1");
        assert!(auth.refresh_token.is_empty());
        assert!(auth.source_path.is_none());
        assert!(!auth.can_refresh());
        assert_eq!(cached.access_token, "access-1");
        assert_eq!(std::fs::read_to_string(call_count).unwrap(), "1");
        assert_eq!(
            std::fs::read_to_string(argv_log)
                .unwrap()
                .lines()
                .collect::<Vec<_>>(),
            ["codex-lease", "--min-valid-for-seconds", "300"]
        );
    }

    #[tokio::test]
    async fn force_uses_cached_lease_id_not_rejected_access_token() {
        let dir = tempfile::tempdir().unwrap();
        let (bin, argv_log, _) = fake_lease_script(dir.path());
        let source = CseTooldCodexAuthSource::new(bin).unwrap();
        let first = source.load().await.unwrap();

        let forced = source.force_refresh(&first.access_token).await.unwrap();

        assert_eq!(forced.access_token, "access-forced-2");
        let argv = std::fs::read_to_string(argv_log).unwrap();
        let expected_lease = format!("{:064}", 1);
        assert_eq!(
            argv.lines().collect::<Vec<_>>(),
            [
                "codex-lease",
                "--min-valid-for-seconds",
                "300",
                "codex-lease",
                "--min-valid-for-seconds",
                "300",
                "--rejected-lease",
                expected_lease.as_str(),
            ]
        );
        assert!(!argv.contains(&first.access_token));
    }

    #[tokio::test]
    async fn concurrent_force_calls_collapse_after_generation_changes() {
        let dir = tempfile::tempdir().unwrap();
        let (bin, _, call_count) = fake_lease_script(dir.path());
        let source = Arc::new(CseTooldCodexAuthSource::new(bin).unwrap());
        let first = source.load().await.unwrap();
        let rejected = first.access_token;

        let (left, right) = tokio::join!(
            source.force_refresh(&rejected),
            source.force_refresh(&rejected)
        );

        assert_eq!(left.unwrap().access_token, "access-forced-2");
        assert_eq!(right.unwrap().access_token, "access-forced-2");
        assert_eq!(std::fs::read_to_string(call_count).unwrap(), "2");
    }

    #[tokio::test]
    async fn malformed_unknown_and_unsupported_versions_are_token_safe() {
        let dir = tempfile::tempdir().unwrap();
        let cases = vec![
            (
                "malformed",
                "#!/bin/sh\nprintf '%s' 'not-json-access-secret'\n".to_string(),
            ),
            (
                "unknown",
                format!(
                    "#!/bin/sh\nprintf '%s' '{}'\n",
                    valid_json_with(",\"refresh_token\":\"refresh-secret\"")
                ),
            ),
            (
                "version",
                "#!/bin/sh\nprintf '%s' '{\"v\":2,\"lease_id\":\"lease-secret\",\"access_token\":\"access-secret\",\"id_token\":\"id-secret\",\"account_id\":\"acct-1\",\"expires_at\":9999999999}'\n".to_string(),
            ),
        ];
        for (name, script) in cases {
            let bin = write_executable(dir.path(), name, &script);
            let source = CseTooldCodexAuthSource::new(bin).unwrap();
            let error = source.load().await.unwrap_err().to_string();
            for secret in [
                "access-secret",
                "id-secret",
                "refresh-secret",
                "lease-secret",
            ] {
                assert!(!error.contains(secret), "error leaked {secret}: {error}");
                assert!(
                    !format!("{source:?}").contains(secret),
                    "Debug leaked {secret}"
                );
            }
        }
    }

    #[tokio::test]
    async fn rejects_missing_fields_and_short_expiry() {
        let dir = tempfile::tempdir().unwrap();
        let missing = write_executable(
            dir.path(),
            "missing",
            "#!/bin/sh\nprintf '%s' '{\"v\":1,\"lease_id\":\"l\",\"access_token\":\"a\",\"id_token\":\"i\",\"expires_at\":9999999999}'\n",
        );
        assert!(CseTooldCodexAuthSource::new(missing)
            .unwrap()
            .load()
            .await
            .unwrap_err()
            .to_string()
            .contains("malformed"));

        let expiry_json = format!(
            r#"{{"v":1,"lease_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","access_token":"a","id_token":"i","account_id":"acct","expires_at":{}}}"#,
            now_unix() + 299
        );
        let expiry = write_executable(
            dir.path(),
            "expiry",
            &format!("#!/bin/sh\nprintf '%s' '{expiry_json}'\n"),
        );
        assert!(CseTooldCodexAuthSource::new(expiry)
            .unwrap()
            .load()
            .await
            .unwrap_err()
            .to_string()
            .contains("expires"));
    }

    #[tokio::test]
    async fn rejects_non_sha256_lease_id_before_it_reaches_argv() {
        let dir = tempfile::tempdir().unwrap();
        let expires = now_unix() + 3600;
        let invalid = write_executable(
            dir.path(),
            "invalid-lease-id",
            &format!(
                "#!/bin/sh\nprintf '%s' '{{\"v\":1,\"lease_id\":\"not-a-generation\",\"access_token\":\"a\",\"id_token\":\"i\",\"account_id\":\"acct\",\"expires_at\":{expires}}}'\n"
            ),
        );

        let error = CseTooldCodexAuthSource::new(invalid)
            .unwrap()
            .load()
            .await
            .unwrap_err();

        assert!(error.to_string().contains("lease id"));
    }

    #[tokio::test]
    async fn oversize_stdout_is_bounded_and_child_is_reaped() {
        let dir = tempfile::tempdir().unwrap();
        let completed = dir.path().join("oversize-completed");
        let bin = write_executable(
            dir.path(),
            "oversize",
            &format!(
                "#!/bin/sh\ni=0\nwhile [ \"$i\" -lt 20000 ]; do printf 'xxxx'; i=$((i + 1)); done\ntouch '{}'\n",
                completed.display()
            ),
        );
        let error = CseTooldCodexAuthSource::new(bin)
            .unwrap()
            .load()
            .await
            .unwrap_err();
        assert!(error.to_string().contains("exceeded bound"));
        assert!(!completed.exists(), "oversize child continued after error");
    }

    #[tokio::test]
    async fn timeout_kills_and_reaps_child() {
        let dir = tempfile::tempdir().unwrap();
        let pid_file = dir.path().join("timeout-pid");
        let bin = write_executable(
            dir.path(),
            "timeout",
            &format!(
                "#!/bin/sh\nprintf '%s' \"$$\" > '{}'\nexec sleep 10\n",
                pid_file.display()
            ),
        );
        let source = CseTooldCodexAuthSource::new(bin)
            .unwrap()
            .with_timeout(Duration::from_millis(100));
        let started = Instant::now();

        let error = source.load().await.unwrap_err();

        assert!(error.to_string().contains("timed out"));
        assert!(started.elapsed() < Duration::from_secs(2));
        let pid = std::fs::read_to_string(pid_file).unwrap();
        let child_exists = std::process::Command::new("/bin/kill")
            .args(["-0", pid.trim()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap()
            .success();
        assert!(!child_exists, "timed-out child still exists");
    }

    #[tokio::test]
    async fn nonzero_exit_discards_stderr() {
        let dir = tempfile::tempdir().unwrap();
        let bin = write_executable(
            dir.path(),
            "nonzero",
            "#!/bin/sh\nprintf '%s' 'stderr-access-secret' >&2\nexit 7\n",
        );
        let error = CseTooldCodexAuthSource::new(bin)
            .unwrap()
            .load()
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("nonzero (7)"));
        assert!(!error.contains("access-secret"));
    }

    #[test]
    fn absolute_path_is_required() {
        let error = CseTooldCodexAuthSource::new("relative/cse-toold").unwrap_err();
        assert!(error.to_string().contains("absolute"));
    }

    #[tokio::test]
    async fn provider_does_not_read_poisoned_auth_file() {
        let dir = tempfile::tempdir().unwrap();
        let auth_path = dir.path().join("poisoned-auth.json");
        std::fs::write(&auth_path, "access-secret-not-json").unwrap();
        assert!(rtinfer_core::CodexAuth::from_path(&auth_path).is_err());
        let (bin, _, _) = fake_lease_script(dir.path());

        let auth = CseTooldCodexAuthSource::new(bin)
            .unwrap()
            .load()
            .await
            .unwrap();

        assert_eq!(auth.access_token, "access-1");
        assert!(auth.source_path.is_none());
    }
}
