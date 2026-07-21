//! Aggregate completed-goodput benchmark for CodexResponsesClient.
//!
//! Rotates mode order across three repetitions as specified by PRD R9:
//! `wss,http,dual` / `http,dual,wss` / `dual,wss,http`.
//!
//! Records only non-sensitive counters, wall times, and a pass/blocked verdict.
//! Never prints prompts, model output, tokens, auth/account data, or raw frames.
//!
//! ```sh
//! cargo run --release -p rtinfer-core --example responses_goodput -- \
//!   --requests 24 --repetitions 3 --warmups 4 \
//!   --output .plans/evidence/adaptive-dual-transport-goodput-2026-07-20.json
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use rtinfer_core::{
    classify_responses_provider_code, CodexAuth, CodexResponsesClient, ResponsesResultClass,
    ResponsesRuntimeConfig, ResponsesTransportMode, CODEX_RESPONSES_MODEL,
};
use serde_json::{json, Value};

/// Tiny deterministic ask shared by every warmup and measured request.
const SYSTEM: &str = "Reply with the single digit 1.";
const USER: &str = "1";

/// Rotating run orders for three repetitions (PRD R9).
const MODE_ORDERS: [[ResponsesTransportMode; 3]; 3] = [
    [
        ResponsesTransportMode::Wss,
        ResponsesTransportMode::Http,
        ResponsesTransportMode::Dual,
    ],
    [
        ResponsesTransportMode::Http,
        ResponsesTransportMode::Dual,
        ResponsesTransportMode::Wss,
    ],
    [
        ResponsesTransportMode::Dual,
        ResponsesTransportMode::Wss,
        ResponsesTransportMode::Http,
    ],
];

/// Finite CLI maxima. Exact PRD inputs (24/3/4) remain valid; larger values
/// error so evidence memory and spawn work cannot grow without bound.
const MAX_REQUESTS: usize = 128;
const MAX_REPETITIONS: usize = 16;
const MAX_WARMUPS: usize = 64;

/// Before generic warmup asks, fill each enabled WSS pool to its configured
/// initial concurrency window. This keeps WSS-only and dual measured runs from
/// charging socket setup to the timed cohort.
const WSS_PREWARM_POLICY: &str = "enabled_wss_lane_to_configured_initial_before_generic_warmups";

#[derive(Clone, Copy)]
struct Cli {
    requests: usize,
    repetitions: usize,
    warmups: usize,
}

#[derive(Default)]
struct ClassCounts {
    success: u64,
    lane_overload: u64,
    shared_throttle: u64,
    failure: u64,
    indeterminate: u64,
}

#[derive(Default)]
struct RunCounters {
    completed: u64,
    failure: u64,
    classes: ClassCounts,
    wall_secs: f64,
    completed_per_sec: f64,
    http_dispatches_delta: u64,
    wss_dispatches_delta: u64,
    http_reuse_delta: usize,
    wss_handshake_delta: u64,
    http_success_delta: u64,
    wss_success_delta: u64,
    http_shared_throttle_delta: u64,
    wss_shared_throttle_delta: u64,
    http_lane_overload_delta: u64,
    wss_lane_overload_delta: u64,
    aggregate_limit: usize,
    http_limit: usize,
    wss_limit: usize,
    http_max: usize,
    wss_max: usize,
    wss_prewarm_target: usize,
    wss_prewarm_attained: usize,
}

/// Mode → WSS prewarm target: `wss_initial` for Wss/Dual, 0 for Http.
fn wss_prewarm_target_for_mode(mode: ResponsesTransportMode) -> usize {
    if matches!(
        mode,
        ResponsesTransportMode::Wss | ResponsesTransportMode::Dual
    ) {
        ResponsesRuntimeConfig::defaults_for_mode(mode).wss_initial
    } else {
        0
    }
}

fn print_help() {
    println!(
        "\
responses_goodput — credentialed aggregate goodput benchmark for CodexResponsesClient

USAGE:
  responses_goodput --requests <N> --repetitions <N> --warmups <N> --output <PATH>
  responses_goodput --help

OPTIONS:
  --requests <N>       Measured identical asks per mode run (default: 24, max: {max_requests})
  --repetitions <N>    Cohort repetitions; mode order rotates each rep (default: 3, max: {max_repetitions})
  --warmups <N>        Unmeasured warmups before each measured run (default: 4, max: {max_warmups})
  --output <PATH>      Write machine-readable JSON evidence to this path (required)
  --help               Show this help

NOTES:
  Uses production CodexResponsesClient with the default responses model.
  Clients are built with an explicit transport mode; process-default WSS is left alone.
  Exit 0 for verdict pass or qualifying blocked; exit 1 for inconclusive
  (threshold miss without an R9 qualifying cause). Auth/content are never emitted.",
        max_requests = MAX_REQUESTS,
        max_repetitions = MAX_REPETITIONS,
        max_warmups = MAX_WARMUPS,
    );
}

fn parse_positive(name: &str, raw: &str) -> Result<usize, String> {
    let v: usize = raw
        .parse()
        .map_err(|_| format!("invalid {name}: expected positive integer, got {raw:?}"))?;
    if v == 0 {
        return Err(format!("invalid {name}: must be >= 1"));
    }
    Ok(v)
}

fn parse_nonnegative(name: &str, raw: &str) -> Result<usize, String> {
    let v: usize = raw
        .parse()
        .map_err(|_| format!("invalid {name}: expected non-negative integer, got {raw:?}"))?;
    Ok(v)
}

fn enforce_max(name: &str, value: usize, max: usize) -> Result<usize, String> {
    if value > max {
        return Err(format!("invalid {name}: must be <= {max}, got {value}"));
    }
    Ok(value)
}

fn parse_args(args: &[String]) -> Result<(Cli, PathBuf), i32> {
    if args.iter().any(|a| a == "--help" || a == "-h") {
        print_help();
        return Err(0);
    }

    let mut requests = 24usize;
    let mut repetitions = 3usize;
    let mut warmups = 4usize;
    let mut output: Option<PathBuf> = None;

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--requests" => {
                i += 1;
                let raw = args.get(i).ok_or_else(|| {
                    eprintln!("error: --requests requires a value");
                    2
                })?;
                requests = parse_positive("requests", raw)
                    .and_then(|v| enforce_max("requests", v, MAX_REQUESTS))
                    .map_err(|e| {
                        eprintln!("error: {e}");
                        2
                    })?;
            }
            "--repetitions" => {
                i += 1;
                let raw = args.get(i).ok_or_else(|| {
                    eprintln!("error: --repetitions requires a value");
                    2
                })?;
                repetitions = parse_positive("repetitions", raw)
                    .and_then(|v| enforce_max("repetitions", v, MAX_REPETITIONS))
                    .map_err(|e| {
                        eprintln!("error: {e}");
                        2
                    })?;
            }
            "--warmups" => {
                i += 1;
                let raw = args.get(i).ok_or_else(|| {
                    eprintln!("error: --warmups requires a value");
                    2
                })?;
                warmups = parse_nonnegative("warmups", raw)
                    .and_then(|v| enforce_max("warmups", v, MAX_WARMUPS))
                    .map_err(|e| {
                        eprintln!("error: {e}");
                        2
                    })?;
            }
            "--output" => {
                i += 1;
                let raw = args.get(i).ok_or_else(|| {
                    eprintln!("error: --output requires a path");
                    2
                })?;
                output = Some(PathBuf::from(raw));
            }
            other => {
                eprintln!("error: unknown argument {other:?} (try --help)");
                return Err(2);
            }
        }
        i += 1;
    }

    let output = output.ok_or_else(|| {
        eprintln!("error: --output is required (try --help)");
        2
    })?;

    Ok((
        Cli {
            requests,
            repetitions,
            warmups,
        },
        output,
    ))
}

fn require_usable_auth() -> Result<CodexAuth, String> {
    CodexAuth::from_default_path().map_err(|e| {
        format!(
            "usable ChatGPT auth required (run `codex login`); auth load failed: {}",
            e.code_or_label()
        )
    })
}

fn build_client(
    mode: ResponsesTransportMode,
    auth: CodexAuth,
) -> Result<Arc<CodexResponsesClient>, String> {
    // Explicit mode only; core supplies mode-derived aggregate defaults.
    // Do not read or mutate RTINFER_RESPONSES_TRANSPORT.
    CodexResponsesClient::builder()
        .mode(mode)
        .initial_auth(auth)
        .model(CODEX_RESPONSES_MODEL)
        .build()
        .map_err(|e| format!("client build failed: {}", e.code_or_label()))
}

/// Classify a returned ask error from typed provider codes only.
///
/// Protocol and other non-Provider variants carry no typed class signal here;
/// never scan message text (429 / eof / malformed / etc.) for a qualifying cause.
fn classify_ask_err(err: &rtinfer_core::RealtimeError) -> ResponsesResultClass {
    match err {
        rtinfer_core::RealtimeError::Provider { code, .. } => {
            classify_responses_provider_code(code)
        }
        // No typed Indeterminate on RealtimeError without reading message text.
        _ => ResponsesResultClass::Failure,
    }
}

/// Authoritative shared-throttle count: adaptive snapshot deltas only.
/// Each logical SharedThrottle finishes on exactly one lane, so the sum of
/// per-lane deltas counts each event once (never add per-ask class bumps).
fn authoritative_shared_throttles(http_delta: u64, wss_delta: u64) -> u64 {
    http_delta.saturating_add(wss_delta)
}

fn bump_class(counts: &mut ClassCounts, class: ResponsesResultClass) {
    match class {
        ResponsesResultClass::Success => counts.success += 1,
        ResponsesResultClass::LaneOverload => counts.lane_overload += 1,
        ResponsesResultClass::SharedThrottle => counts.shared_throttle += 1,
        ResponsesResultClass::Failure => counts.failure += 1,
        ResponsesResultClass::Indeterminate => counts.indeterminate += 1,
    }
}

async fn run_asks(client: &Arc<CodexResponsesClient>, n: usize) -> (u64, u64, ClassCounts) {
    let mut joins = Vec::with_capacity(n);
    for _ in 0..n {
        let c = Arc::clone(client);
        joins.push(tokio::spawn(
            async move { c.ask_text(SYSTEM, USER, None).await },
        ));
    }

    let mut completed = 0u64;
    let mut failure = 0u64;
    let mut classes = ClassCounts::default();
    for join in joins {
        match join.await {
            Ok(Ok(_)) => {
                completed += 1;
                bump_class(&mut classes, ResponsesResultClass::Success);
            }
            Ok(Err(e)) => {
                failure += 1;
                bump_class(&mut classes, classify_ask_err(&e));
            }
            Err(_) => {
                failure += 1;
                bump_class(&mut classes, ResponsesResultClass::Failure);
            }
        }
    }
    (completed, failure, classes)
}

async fn run_mode(
    mode: ResponsesTransportMode,
    auth: CodexAuth,
    requests: usize,
    warmups: usize,
) -> Result<RunCounters, String> {
    let client = build_client(mode, auth)?;
    let wss_prewarm_target = wss_prewarm_target_for_mode(mode);
    let wss_prewarm_attained = if wss_prewarm_target > 0 {
        client.prewarm(wss_prewarm_target).await
    } else {
        0
    };
    if wss_prewarm_attained < wss_prewarm_target {
        return Err(format!(
            "wss prewarm attained {wss_prewarm_attained} idle sockets; need {wss_prewarm_target}"
        ));
    }
    if warmups > 0 {
        let _ = run_asks(&client, warmups).await;
    }

    let before = client.snapshot().await;
    let started = Instant::now();
    let (completed, failure, classes) = run_asks(&client, requests).await;
    let wall_secs = started.elapsed().as_secs_f64().max(1e-9);
    let after = client.snapshot().await;

    Ok(RunCounters {
        completed,
        failure,
        classes,
        wall_secs,
        completed_per_sec: completed as f64 / wall_secs,
        http_dispatches_delta: after.http_dispatches.saturating_sub(before.http_dispatches),
        wss_dispatches_delta: after.wss_dispatches.saturating_sub(before.wss_dispatches),
        http_reuse_delta: after
            .http_connection_reuse_count
            .saturating_sub(before.http_connection_reuse_count),
        wss_handshake_delta: after
            .wss_handshake_attempts
            .saturating_sub(before.wss_handshake_attempts),
        http_success_delta: after
            .adaptive
            .http
            .successes
            .saturating_sub(before.adaptive.http.successes),
        wss_success_delta: after
            .adaptive
            .websocket
            .successes
            .saturating_sub(before.adaptive.websocket.successes),
        http_shared_throttle_delta: after
            .adaptive
            .http
            .shared_throttles
            .saturating_sub(before.adaptive.http.shared_throttles),
        wss_shared_throttle_delta: after
            .adaptive
            .websocket
            .shared_throttles
            .saturating_sub(before.adaptive.websocket.shared_throttles),
        http_lane_overload_delta: after
            .adaptive
            .http
            .lane_overloads
            .saturating_sub(before.adaptive.http.lane_overloads),
        wss_lane_overload_delta: after
            .adaptive
            .websocket
            .lane_overloads
            .saturating_sub(before.adaptive.websocket.lane_overloads),
        aggregate_limit: after.adaptive.aggregate.limit,
        http_limit: after.adaptive.http.limit,
        wss_limit: after.adaptive.websocket.limit,
        http_max: after.adaptive.http.max,
        wss_max: after.adaptive.websocket.max,
        wss_prewarm_target,
        wss_prewarm_attained,
    })
}

fn median_f64(values: &mut [f64]) -> f64 {
    assert!(!values.is_empty());
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = values.len() / 2;
    if values.len() % 2 == 1 {
        values[mid]
    } else {
        (values[mid - 1] + values[mid]) / 2.0
    }
}

fn mode_str(mode: ResponsesTransportMode) -> &'static str {
    mode.as_str()
}

fn run_to_json(rep: usize, mode: ResponsesTransportMode, c: &RunCounters) -> Value {
    json!({
        "repetition": rep,
        "mode": mode_str(mode),
        "completed": c.completed,
        "failure": c.failure,
        "classes": {
            "success": c.classes.success,
            "lane_overload": c.classes.lane_overload,
            "shared_throttle": c.classes.shared_throttle,
            "failure": c.classes.failure,
            "indeterminate": c.classes.indeterminate,
        },
        "wall_seconds": c.wall_secs,
        "completed_per_sec": c.completed_per_sec,
        "wss_prewarm": {
            "target": c.wss_prewarm_target,
            "attained": c.wss_prewarm_attained,
        },
        "lane_dispatch_deltas": {
            "http": c.http_dispatches_delta,
            "wss": c.wss_dispatches_delta,
        },
        "reuse_handshake_deltas": {
            "http_connection_reuse": c.http_reuse_delta,
            "wss_handshake_attempts": c.wss_handshake_delta,
        },
        "lane_success_deltas": {
            "http": c.http_success_delta,
            "wss": c.wss_success_delta,
        },
        "capacity_snapshot": {
            "aggregate_limit": c.aggregate_limit,
            "http_limit": c.http_limit,
            "wss_limit": c.wss_limit,
            "http_max": c.http_max,
            "wss_max": c.wss_max,
            "http_shared_throttle_delta": c.http_shared_throttle_delta,
            "wss_shared_throttle_delta": c.wss_shared_throttle_delta,
            "http_lane_overload_delta": c.http_lane_overload_delta,
            "wss_lane_overload_delta": c.wss_lane_overload_delta,
        },
    })
}

/// R9 verdict selection.
///
/// - `pass`: threshold met and both dual lanes carried load with semantic successes.
/// - `blocked`: threshold missed AND a qualifying observed cause (shared throttle
///   only). Exit 0. Zero dispatch/success is not a protocol/reuse failure and
///   must never fabricate `lane_protocol_or_reuse_gate_failed`.
/// - `inconclusive`: threshold missed with no qualifying cause (or threshold met
///   without dual-lane semantic load). Never `pass`/`blocked`. Exit nonzero.
///
/// Aggregate ceiling saturation is not inferred from limits.
fn select_verdict(
    threshold_met: bool,
    total_shared_throttle: u64,
    dual_http_disp: u64,
    dual_wss_disp: u64,
    dual_http_ok: u64,
    dual_wss_ok: u64,
) -> (&'static str, Value, i32) {
    let dual_lanes_carried = dual_http_disp > 0 && dual_wss_disp > 0;
    let dual_lanes_succeeded = dual_http_ok > 0 && dual_wss_ok > 0;
    if threshold_met && dual_lanes_carried && dual_lanes_succeeded {
        return ("pass", Value::Null, 0);
    }

    if !threshold_met {
        if total_shared_throttle > 0 {
            return (
                "blocked",
                Value::String("shared_throttle_or_rate_limit_observed".to_owned()),
                0,
            );
        }
        return (
            "inconclusive",
            Value::String("dual_goodput_below_threshold_without_qualifying_cause".to_owned()),
            1,
        );
    }

    // Threshold met but dual lanes did not both carry with semantic successes.
    (
        "inconclusive",
        Value::String("dual_lanes_did_not_carry_load".to_owned()),
        1,
    )
}

fn ensure_parent_dir(path: &Path) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("failed to create output parent dir {parent:?}: {e}"))?;
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (cli, output) = match parse_args(&args) {
        Ok(v) => v,
        Err(code) => std::process::exit(code),
    };

    let _ = rustls::crypto::ring::default_provider().install_default();

    let auth = match require_usable_auth() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(2);
        }
    };

    // Mode-independent lane defaults from Default; aggregates vary by mode.
    let lane_defaults = ResponsesRuntimeConfig::default();
    let mut runs_json: Vec<Value> = Vec::new();
    let mut http_rates: Vec<f64> = Vec::new();
    let mut wss_rates: Vec<f64> = Vec::new();
    let mut dual_rates: Vec<f64> = Vec::new();

    let mut dual_http_disp = 0u64;
    let mut dual_wss_disp = 0u64;
    let mut dual_http_ok = 0u64;
    let mut dual_wss_ok = 0u64;
    let mut total_shared_throttle = 0u64;

    // Allow repetitions > 3 by cycling the PRD rotation table (bounded by MAX_REPETITIONS).
    for rep in 0..cli.repetitions {
        let order = MODE_ORDERS[rep % MODE_ORDERS.len()];
        for mode in order {
            let counters = match run_mode(mode, auth.clone(), cli.requests, cli.warmups).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("error: {e}");
                    std::process::exit(2);
                }
            };

            // One authoritative source: adaptive shared_throttles deltas only.
            total_shared_throttle += authoritative_shared_throttles(
                counters.http_shared_throttle_delta,
                counters.wss_shared_throttle_delta,
            );

            if mode == ResponsesTransportMode::Dual {
                dual_http_disp += counters.http_dispatches_delta;
                dual_wss_disp += counters.wss_dispatches_delta;
                dual_http_ok += counters.http_success_delta;
                dual_wss_ok += counters.wss_success_delta;
            }

            match mode {
                ResponsesTransportMode::Http => http_rates.push(counters.completed_per_sec),
                ResponsesTransportMode::Wss => wss_rates.push(counters.completed_per_sec),
                ResponsesTransportMode::Dual => dual_rates.push(counters.completed_per_sec),
            }

            runs_json.push(run_to_json(rep, mode, &counters));
        }
    }

    let median_http = median_f64(&mut http_rates);
    let median_wss = median_f64(&mut wss_rates);
    let median_dual = median_f64(&mut dual_rates);
    let best_single = median_http.max(median_wss);
    let threshold = 1.05 * best_single;
    let dual_lanes_carried = dual_http_disp > 0 && dual_wss_disp > 0;
    let threshold_met = median_dual >= threshold;

    let (verdict, reason, exit_code) = select_verdict(
        threshold_met,
        total_shared_throttle,
        dual_http_disp,
        dual_wss_disp,
        dual_http_ok,
        dual_wss_ok,
    );

    let evidence = json!({
        "version": env!("CARGO_PKG_VERSION"),
        "model": CODEX_RESPONSES_MODEL,
        "config": {
            "requests": cli.requests,
            "repetitions": cli.repetitions,
            "warmups": cli.warmups,
            "wss_prewarm": {
                "policy": WSS_PREWARM_POLICY,
                "count_per_enabled_mode": lane_defaults.wss_initial,
                "enabled_modes": ["wss", "dual"],
            },
            "runtime": {
                "http_initial": lane_defaults.http_initial,
                "http_max": lane_defaults.http_max,
                "wss_initial": lane_defaults.wss_initial,
                "wss_max": lane_defaults.wss_max,
                "aggregate_max_by_mode": {
                    "wss": ResponsesRuntimeConfig::defaults_for_mode(
                        ResponsesTransportMode::Wss
                    )
                    .aggregate_max,
                    "http": ResponsesRuntimeConfig::defaults_for_mode(
                        ResponsesTransportMode::Http
                    )
                    .aggregate_max,
                    "dual": ResponsesRuntimeConfig::defaults_for_mode(
                        ResponsesTransportMode::Dual
                    )
                    .aggregate_max,
                },
                "default_mode": lane_defaults.mode.as_str(),
            },
            "mode_order_rotation": [
                ["wss", "http", "dual"],
                ["http", "dual", "wss"],
                ["dual", "wss", "http"],
            ],
        },
        "runs": runs_json,
        "medians": {
            "http_completed_per_sec": median_http,
            "wss_completed_per_sec": median_wss,
            "dual_completed_per_sec": median_dual,
            "best_single_completed_per_sec": best_single,
            "dual_threshold_completed_per_sec": threshold,
        },
        "dual_lane_load": {
            "http_dispatches": dual_http_disp,
            "wss_dispatches": dual_wss_disp,
            "http_successes": dual_http_ok,
            "wss_successes": dual_wss_ok,
            "both_lanes_carried_load": dual_lanes_carried,
        },
        "verdict": verdict,
        "reason": reason,
    });

    if let Err(e) = ensure_parent_dir(&output) {
        eprintln!("error: {e}");
        std::process::exit(2);
    }
    let body = match serde_json::to_string_pretty(&evidence) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: failed to serialize evidence: {e}");
            std::process::exit(2);
        }
    };
    if let Err(e) = std::fs::write(&output, body) {
        eprintln!("error: failed to write {}: {e}", output.display());
        std::process::exit(2);
    }

    // Non-sensitive summary only.
    println!(
        "wrote {} verdict={verdict} median_dual={median_dual:.4} threshold={threshold:.4} both_lanes={dual_lanes_carried}",
        output.display()
    );
    if exit_code != 0 {
        std::process::exit(exit_code);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtinfer_core::RealtimeError;

    fn reason_str(reason: &Value) -> &str {
        reason.as_str().expect("reason string")
    }

    #[test]
    fn mode_defaults_start_at_32_with_a_48_ceiling() {
        let wss = ResponsesRuntimeConfig::defaults_for_mode(ResponsesTransportMode::Wss);
        let http = ResponsesRuntimeConfig::defaults_for_mode(ResponsesTransportMode::Http);
        let dual = ResponsesRuntimeConfig::defaults_for_mode(ResponsesTransportMode::Dual);
        assert_eq!(wss.aggregate_max, 48);
        assert_eq!(http.aggregate_max, 48);
        assert_eq!(dual.aggregate_max, 48);
        assert_eq!(wss.wss_initial, 32);
        assert_eq!(http.http_initial, 32);
        assert_eq!(dual.http_initial, 32);
        assert_eq!(dual.wss_initial, 32);
        assert_eq!(wss.wss_max, 48);
        assert_eq!(http.http_max, 48);
        assert_eq!(wss.mode, ResponsesTransportMode::Wss);
        assert_eq!(http.mode, ResponsesTransportMode::Http);
        assert_eq!(dual.mode, ResponsesTransportMode::Dual);
        assert_eq!(wss.http_max, http.http_max);
        assert_eq!(wss.wss_max, dual.wss_max);
    }

    #[test]
    fn wss_prewarm_target_is_wss_initial_for_enabled_modes_else_zero() {
        let wss = ResponsesRuntimeConfig::defaults_for_mode(ResponsesTransportMode::Wss);
        let dual = ResponsesRuntimeConfig::defaults_for_mode(ResponsesTransportMode::Dual);
        assert_eq!(
            wss_prewarm_target_for_mode(ResponsesTransportMode::Wss),
            wss.wss_initial
        );
        assert_eq!(
            wss_prewarm_target_for_mode(ResponsesTransportMode::Dual),
            dual.wss_initial
        );
        assert_eq!(wss_prewarm_target_for_mode(ResponsesTransportMode::Http), 0);
    }

    #[test]
    fn classify_ask_err_uses_typed_provider_codes_only() {
        assert_eq!(
            classify_ask_err(&RealtimeError::Provider {
                code: "rate_limit_exceeded".into(),
                message: "slow down".into(),
            }),
            ResponsesResultClass::SharedThrottle
        );
        assert_eq!(
            classify_ask_err(&RealtimeError::Provider {
                code: "server_is_overloaded".into(),
                message: "busy".into(),
            }),
            ResponsesResultClass::LaneOverload
        );
        assert_eq!(
            classify_ask_err(&RealtimeError::Provider {
                code: "unknown_code".into(),
                message: "429 rate_limit eof malformed".into(),
            }),
            ResponsesResultClass::Failure
        );
    }

    #[test]
    fn classify_ask_err_never_fabricates_class_from_protocol_text() {
        for msg in [
            "http status 429",
            "rate_limit from upstream",
            "incomplete stream",
            "indeterminate outcome",
            "unexpected eof",
            "malformed JSON",
        ] {
            assert_eq!(
                classify_ask_err(&RealtimeError::Protocol(msg.into())),
                ResponsesResultClass::Failure,
                "protocol text must not classify: {msg}"
            );
        }
    }

    #[test]
    fn authoritative_shared_throttles_counts_each_lane_delta_once() {
        assert_eq!(authoritative_shared_throttles(0, 0), 0);
        assert_eq!(authoritative_shared_throttles(2, 3), 5);
        assert_eq!(authoritative_shared_throttles(1, 0), 1);
    }

    #[test]
    fn select_verdict_pass_requires_threshold_and_dual_lane_successes() {
        let (verdict, reason, code) = select_verdict(true, 0, 4, 4, 4, 4);
        assert_eq!(verdict, "pass");
        assert!(reason.is_null());
        assert_eq!(code, 0);
    }

    #[test]
    fn select_verdict_blocked_on_shared_throttle_with_threshold_miss() {
        let (verdict, reason, code) = select_verdict(false, 2, 4, 4, 4, 4);
        assert_eq!(verdict, "blocked");
        assert_eq!(
            reason_str(&reason),
            "shared_throttle_or_rate_limit_observed"
        );
        assert_eq!(code, 0);
    }

    #[test]
    fn select_verdict_zero_dispatch_or_success_is_inconclusive_not_blocked() {
        // Zero dispatch/success is absence, not an observed protocol/reuse failure.
        let cases = [
            (false, 0u64, 4u64, 0u64, 4u64, 0u64), // zero wss dispatch + success
            (false, 0, 0, 4, 0, 4),                // zero http dispatch + success
            (false, 0, 4, 4, 0, 4),                // zero http success
            (false, 0, 4, 4, 4, 0),                // zero wss success
            (false, 0, 0, 0, 0, 0),                // all zeros
            (true, 0, 4, 0, 4, 0),                 // threshold met, missing lane
            (true, 0, 4, 4, 0, 4),                 // threshold met, zero success
        ];
        for (threshold_met, throttle, http_d, wss_d, http_ok, wss_ok) in cases {
            let (verdict, reason, code) =
                select_verdict(threshold_met, throttle, http_d, wss_d, http_ok, wss_ok);
            assert_eq!(
                verdict, "inconclusive",
                "expected inconclusive for disp=({http_d},{wss_d}) ok=({http_ok},{wss_ok}) threshold_met={threshold_met}"
            );
            assert_ne!(code, 0);
            assert_ne!(verdict, "blocked");
            assert_ne!(
                reason_str(&reason),
                "lane_protocol_or_reuse_gate_failed",
                "must never fabricate protocol/reuse from absent dispatch/success"
            );
        }
    }

    #[test]
    fn select_verdict_inconclusive_on_threshold_miss_without_qualifying_cause() {
        // Healthy dual lanes, no shared throttle, threshold missed — not R9 blocked.
        let (verdict, reason, code) = select_verdict(false, 0, 4, 4, 4, 4);
        assert_eq!(verdict, "inconclusive");
        assert_eq!(
            reason_str(&reason),
            "dual_goodput_below_threshold_without_qualifying_cause"
        );
        assert_ne!(code, 0);
        assert_ne!(verdict, "pass");
        assert_ne!(verdict, "blocked");
    }

    #[test]
    fn select_verdict_never_emits_blocked_for_bare_threshold_miss() {
        let (verdict, _, _) = select_verdict(false, 0, 8, 8, 8, 8);
        assert_ne!(verdict, "blocked");
        assert_eq!(verdict, "inconclusive");
    }

    #[test]
    fn cli_bounds_accept_prd_and_reject_above_maxima() {
        assert_eq!(enforce_max("requests", 24, MAX_REQUESTS), Ok(24));
        assert_eq!(enforce_max("repetitions", 3, MAX_REPETITIONS), Ok(3));
        assert_eq!(enforce_max("warmups", 4, MAX_WARMUPS), Ok(4));
        assert_eq!(
            enforce_max("requests", MAX_REQUESTS, MAX_REQUESTS),
            Ok(MAX_REQUESTS)
        );
        assert!(enforce_max("requests", MAX_REQUESTS + 1, MAX_REQUESTS).is_err());
        assert!(enforce_max("repetitions", MAX_REPETITIONS + 1, MAX_REPETITIONS).is_err());
        assert!(enforce_max("warmups", MAX_WARMUPS + 1, MAX_WARMUPS).is_err());
    }

    #[test]
    fn parse_args_rejects_unbounded_work() {
        let args = vec![
            "--requests".into(),
            (MAX_REQUESTS + 1).to_string(),
            "--output".into(),
            "/tmp/out.json".into(),
        ];
        assert!(matches!(parse_args(&args), Err(2)));

        let ok = vec![
            "--requests".into(),
            "24".into(),
            "--repetitions".into(),
            "3".into(),
            "--warmups".into(),
            "4".into(),
            "--output".into(),
            "/tmp/out.json".into(),
        ];
        let (cli, path) = parse_args(&ok).expect("PRD args valid");
        assert_eq!(cli.requests, 24);
        assert_eq!(cli.repetitions, 3);
        assert_eq!(cli.warmups, 4);
        assert_eq!(path, PathBuf::from("/tmp/out.json"));
    }
}
