//! Adaptive concurrency control shared by the HTTP and WebSocket Responses lanes.
//!
//! Both transports start with an independent window. A saturated lane grows
//! immediately until it observes upstream overload, then switches to additive
//! increase and multiplicative decrease. Keeping separate windows lets HTTP/2
//! discover its multiplexing ceiling without letting a WebSocket handshake or
//! provider limit suppress HTTP throughput (or vice versa).
//!
//! Admission is cancellation-safe: [`AdaptiveConcurrency::acquire`] returns an
//! [`AdaptiveLease`] that releases aggregate and lane capacity exactly once on
//! [`AdaptiveLease::finish`] or `Drop`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::Instant;

use crate::RealtimeError;

/// Upstream transport selected for one Responses request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponsesTransportKind {
    Http,
    WebSocket,
}

/// Which Responses lanes may receive admission for one acquire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnabledResponsesLanes {
    Http,
    WebSocket,
    Dual,
}

/// Per-transport concurrency bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConcurrencyLimits {
    pub initial: usize,
    pub min: usize,
    pub max: usize,
}

/// Independent starting and maximum windows plus a fixed aggregate ceiling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptiveConcurrencyConfig {
    pub http: ConcurrencyLimits,
    pub websocket: ConcurrencyLimits,
    /// Fixed process ceiling; does not grow adaptively.
    pub aggregate_max: usize,
}

impl Default for AdaptiveConcurrencyConfig {
    fn default() -> Self {
        Self {
            http: ConcurrencyLimits {
                initial: 32,
                min: 1,
                max: 48,
            },
            websocket: ConcurrencyLimits {
                initial: 32,
                min: 1,
                max: 48,
            },
            aggregate_max: 48,
        }
    }
}

/// Completion signal used to adapt a transport's concurrency window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveOutcome {
    Success,
    /// A definitive lane capacity signal such as `server_is_overloaded`.
    LaneOverload,
    /// Account-level throttle. Pauses both lanes; does not change windows.
    ///
    /// `retry_after_secs` is the integer `Retry-After` value when present and
    /// parseable. Absent or unusable values use a 1s pause; integers are
    /// clamped to `100ms..=30s`.
    SharedThrottle {
        retry_after_secs: Option<u64>,
    },
    /// A non-capacity failure. Releases the lease without changing the window.
    Failure,
    /// Post-send indeterminate outcome. Releases without changing the window.
    Indeterminate,
}

/// Observable state for one transport lane.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TransportConcurrencySnapshot {
    pub limit: usize,
    pub in_flight: usize,
    pub min: usize,
    pub max: usize,
    pub slow_start: bool,
    pub sample_count: u64,
    pub successes: u64,
    pub lane_overloads: u64,
    pub shared_throttles: u64,
    pub failures: u64,
    pub indeterminate: u64,
    pub cancellations: u64,
    /// EWMA of finished (non-cancel) elapsed times. Meaningful only when
    /// `sample_count > 0`.
    pub latency_ewma_ms: f64,
}

/// Observable aggregate admission state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AggregateConcurrencySnapshot {
    pub limit: usize,
    pub in_flight: usize,
    pub waiting: usize,
    pub throttled: bool,
}

/// Observable state for both transport lanes and the aggregate ceiling.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AdaptiveConcurrencySnapshot {
    pub http: TransportConcurrencySnapshot,
    pub websocket: TransportConcurrencySnapshot,
    pub aggregate: AggregateConcurrencySnapshot,
}

#[derive(Debug)]
struct Lane {
    limit: usize,
    in_flight: usize,
    min: usize,
    max: usize,
    slow_start: bool,
    successes_since_increase: usize,
    sample_count: u64,
    successes: u64,
    lane_overloads: u64,
    shared_throttles: u64,
    failures: u64,
    indeterminate: u64,
    cancellations: u64,
    latency_ewma_ms: f64,
    peak_in_flight: usize,
}

impl Lane {
    fn new(limits: ConcurrencyLimits) -> Self {
        Self {
            limit: limits.initial,
            in_flight: 0,
            min: limits.min,
            max: limits.max,
            slow_start: true,
            successes_since_increase: 0,
            sample_count: 0,
            successes: 0,
            lane_overloads: 0,
            shared_throttles: 0,
            failures: 0,
            indeterminate: 0,
            cancellations: 0,
            latency_ewma_ms: 0.0,
            peak_in_flight: 0,
        }
    }

    fn has_headroom(&self) -> bool {
        self.in_flight < self.limit
    }

    fn headroom(&self) -> usize {
        self.limit.saturating_sub(self.in_flight)
    }

    fn reserve(&mut self) {
        self.in_flight += 1;
        self.peak_in_flight = self.peak_in_flight.max(self.in_flight);
    }

    fn record_latency(&mut self, elapsed: Duration) {
        self.sample_count += 1;
        let elapsed_ms = elapsed.as_secs_f64() * 1_000.0;
        self.latency_ewma_ms = if self.sample_count == 1 {
            elapsed_ms
        } else {
            self.latency_ewma_ms * 0.8 + elapsed_ms * 0.2
        };
    }

    fn finish(&mut self, outcome: AdaptiveOutcome, elapsed: Duration) {
        let was_saturated = self.in_flight >= self.limit;
        self.in_flight = self.in_flight.saturating_sub(1);
        self.record_latency(elapsed);

        match outcome {
            AdaptiveOutcome::Success if was_saturated && self.limit < self.max => {
                self.successes += 1;
                if self.slow_start {
                    // One new permit per saturated completion doubles capacity
                    // per successful wave and finds the first ceiling quickly.
                    self.limit += 1;
                } else {
                    self.successes_since_increase += 1;
                    if self.successes_since_increase >= self.limit {
                        self.limit += 1;
                        self.successes_since_increase = 0;
                    }
                }
            }
            AdaptiveOutcome::Success => {
                self.successes += 1;
            }
            AdaptiveOutcome::LaneOverload => {
                self.lane_overloads += 1;
                self.limit = (self.limit / 2).max(self.min);
                self.slow_start = false;
                self.successes_since_increase = 0;
            }
            AdaptiveOutcome::SharedThrottle { .. } => {
                self.shared_throttles += 1;
            }
            AdaptiveOutcome::Failure => {
                self.failures += 1;
            }
            AdaptiveOutcome::Indeterminate => {
                self.indeterminate += 1;
            }
        }
    }

    fn cancel(&mut self) {
        self.in_flight = self.in_flight.saturating_sub(1);
        self.cancellations += 1;
    }

    fn snapshot(&self) -> TransportConcurrencySnapshot {
        TransportConcurrencySnapshot {
            limit: self.limit,
            in_flight: self.in_flight,
            min: self.min,
            max: self.max,
            slow_start: self.slow_start,
            sample_count: self.sample_count,
            successes: self.successes,
            lane_overloads: self.lane_overloads,
            shared_throttles: self.shared_throttles,
            failures: self.failures,
            indeterminate: self.indeterminate,
            cancellations: self.cancellations,
            latency_ewma_ms: self.latency_ewma_ms,
        }
    }
}

#[derive(Debug)]
struct State {
    http: Lane,
    websocket: Lane,
    aggregate_max: usize,
    aggregate_in_flight: usize,
    peak_aggregate_in_flight: usize,
    waiting: usize,
    throttle_until: Option<Instant>,
}

impl State {
    fn aggregate_has_headroom(&self) -> bool {
        self.aggregate_in_flight < self.aggregate_max
    }

    fn throttle_deadline(&self, now: Instant) -> Option<Instant> {
        self.throttle_until.filter(|until| *until > now)
    }

    fn clear_expired_throttle(&mut self, now: Instant) {
        if self.throttle_until.is_some_and(|until| until <= now) {
            self.throttle_until = None;
        }
    }

    fn apply_shared_throttle(&mut self, retry_after_secs: Option<u64>, now: Instant) {
        let pause = shared_throttle_duration(retry_after_secs);
        let until = now + pause;
        self.throttle_until = Some(match self.throttle_until {
            Some(current) if current > until => current,
            _ => until,
        });
    }

    fn reserve_aggregate(&mut self) {
        self.aggregate_in_flight += 1;
        self.peak_aggregate_in_flight = self.peak_aggregate_in_flight.max(self.aggregate_in_flight);
    }

    fn release_aggregate(&mut self) {
        self.aggregate_in_flight = self.aggregate_in_flight.saturating_sub(1);
    }

    fn lane_mut(&mut self, transport: ResponsesTransportKind) -> &mut Lane {
        match transport {
            ResponsesTransportKind::Http => &mut self.http,
            ResponsesTransportKind::WebSocket => &mut self.websocket,
        }
    }

    fn select(&self, enabled: EnabledResponsesLanes) -> Option<ResponsesTransportKind> {
        if !self.aggregate_has_headroom() {
            return None;
        }

        let http_open = matches!(
            enabled,
            EnabledResponsesLanes::Http | EnabledResponsesLanes::Dual
        ) && self.http.has_headroom();
        let websocket_open = matches!(
            enabled,
            EnabledResponsesLanes::WebSocket | EnabledResponsesLanes::Dual
        ) && self.websocket.has_headroom();

        match (http_open, websocket_open) {
            (false, false) => None,
            (true, false) => Some(ResponsesTransportKind::Http),
            (false, true) => Some(ResponsesTransportKind::WebSocket),
            (true, true) => {
                // Compare utilization ratios without floating-point rounding.
                let http_load = self.http.in_flight * self.websocket.limit;
                let websocket_load = self.websocket.in_flight * self.http.limit;
                if http_load < websocket_load {
                    Some(ResponsesTransportKind::Http)
                } else if websocket_load < http_load {
                    Some(ResponsesTransportKind::WebSocket)
                } else if self.http.headroom() > self.websocket.headroom() {
                    Some(ResponsesTransportKind::Http)
                } else if self.websocket.headroom() > self.http.headroom() {
                    Some(ResponsesTransportKind::WebSocket)
                } else {
                    // Deterministic tie-break: HTTP.
                    Some(ResponsesTransportKind::Http)
                }
            }
        }
    }

    fn try_reserve(&mut self, enabled: EnabledResponsesLanes) -> Option<ResponsesTransportKind> {
        let selected = self.select(enabled)?;
        match selected {
            ResponsesTransportKind::Http => self.http.reserve(),
            ResponsesTransportKind::WebSocket => self.websocket.reserve(),
        }
        self.reserve_aggregate();
        Some(selected)
    }

    fn snapshot(&self, now: Instant) -> AdaptiveConcurrencySnapshot {
        AdaptiveConcurrencySnapshot {
            http: self.http.snapshot(),
            websocket: self.websocket.snapshot(),
            aggregate: AggregateConcurrencySnapshot {
                limit: self.aggregate_max,
                in_flight: self.aggregate_in_flight,
                waiting: self.waiting,
                throttled: self.throttle_deadline(now).is_some(),
            },
        }
    }
}

fn shared_throttle_duration(retry_after_secs: Option<u64>) -> Duration {
    match retry_after_secs {
        Some(secs) => {
            let millis = secs.saturating_mul(1_000);
            Duration::from_millis(millis.clamp(100, 30_000))
        }
        None => Duration::from_secs(1),
    }
}

fn validate_lane_limits(
    limits: ConcurrencyLimits,
    name: &str,
    hard_max: usize,
) -> Result<(), RealtimeError> {
    if limits.min != 1 {
        return Err(RealtimeError::Protocol(format!(
            "responses config: {name}_min must be 1"
        )));
    }
    if limits.max < 1 || limits.max > hard_max {
        return Err(RealtimeError::Protocol(format!(
            "responses config: {name}_max must be in 1..={hard_max}"
        )));
    }
    if limits.initial < 1 || limits.initial > limits.max {
        return Err(RealtimeError::Protocol(format!(
            "responses config: {name}_initial must be in 1..={name}_max"
        )));
    }
    Ok(())
}

fn validate_config(config: &AdaptiveConcurrencyConfig) -> Result<(), RealtimeError> {
    validate_lane_limits(config.http, "http", 256)?;
    validate_lane_limits(config.websocket, "wss", 64)?;
    let enabled_sum = config.http.max + config.websocket.max;
    if config.aggregate_max < 1 || config.aggregate_max > enabled_sum {
        return Err(RealtimeError::Protocol(format!(
            "responses config: aggregate_max must be in 1..={enabled_sum}"
        )));
    }
    Ok(())
}

/// RAII admission lease for one Responses request.
///
/// [`finish`](AdaptiveLease::finish) consumes the lease and applies the
/// adaptive outcome. Dropping without finish releases capacity exactly once,
/// records a cancellation, leaves limits/latency unchanged, and wakes a waiter.
#[derive(Debug)]
pub struct AdaptiveLease {
    controller: Arc<AdaptiveConcurrency>,
    transport: ResponsesTransportKind,
    released: bool,
}

impl AdaptiveLease {
    fn new(controller: Arc<AdaptiveConcurrency>, transport: ResponsesTransportKind) -> Self {
        Self {
            controller,
            transport,
            released: false,
        }
    }

    pub fn transport(&self) -> ResponsesTransportKind {
        self.transport
    }

    /// Consume the lease, apply the outcome, and wake one waiter.
    pub fn finish(mut self, outcome: AdaptiveOutcome, elapsed: Duration) {
        self.release(ReleaseKind::Finished { outcome, elapsed });
        self.released = true;
    }

    fn release(&mut self, kind: ReleaseKind) {
        let mut state = self
            .controller
            .state
            .lock()
            .expect("adaptive controller poisoned");
        match kind {
            ReleaseKind::Finished { outcome, elapsed } => {
                let shared = match outcome {
                    AdaptiveOutcome::SharedThrottle { retry_after_secs } => Some(retry_after_secs),
                    _ => None,
                };
                state.lane_mut(self.transport).finish(outcome, elapsed);
                state.release_aggregate();
                if let Some(retry_after_secs) = shared {
                    state.apply_shared_throttle(retry_after_secs, Instant::now());
                    drop(state);
                    self.controller.notify.notify_waiters();
                    return;
                }
            }
            ReleaseKind::Cancelled => {
                state.lane_mut(self.transport).cancel();
                state.release_aggregate();
            }
        }
        drop(state);
        self.controller.notify.notify_one();
    }
}

impl Drop for AdaptiveLease {
    fn drop(&mut self) {
        if !self.released {
            self.release(ReleaseKind::Cancelled);
        }
    }
}

enum ReleaseKind {
    Finished {
        outcome: AdaptiveOutcome,
        elapsed: Duration,
    },
    Cancelled,
}

/// Decrements `waiting` exactly once when an acquire future is cancelled or
/// finishes its wait cycle.
struct WaitingGuard {
    controller: Arc<AdaptiveConcurrency>,
    active: bool,
}

impl WaitingGuard {
    fn new(controller: Arc<AdaptiveConcurrency>) -> Self {
        Self {
            controller,
            active: true,
        }
    }
}

impl Drop for WaitingGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut state = self
            .controller
            .state
            .lock()
            .expect("adaptive controller poisoned");
        state.waiting = state.waiting.saturating_sub(1);
    }
}

/// Fast feedback controller for combined HTTP/2 and WebSocket concurrency.
///
/// Production callers acquire through [`AdaptiveConcurrency::acquire`], which
/// returns an [`AdaptiveLease`]. Capacity is released only by finishing or
/// dropping that lease.
#[derive(Debug)]
pub struct AdaptiveConcurrency {
    state: Mutex<State>,
    notify: Notify,
}

impl AdaptiveConcurrency {
    /// Construct a controller after validating limits. Invalid bounds return
    /// [`RealtimeError::Protocol`] instead of being silently clamped.
    pub fn new(config: AdaptiveConcurrencyConfig) -> Result<Arc<Self>, RealtimeError> {
        validate_config(&config)?;
        Ok(Arc::new(Self {
            state: Mutex::new(State {
                http: Lane::new(config.http),
                websocket: Lane::new(config.websocket),
                aggregate_max: config.aggregate_max,
                aggregate_in_flight: 0,
                peak_aggregate_in_flight: 0,
                waiting: 0,
                throttle_until: None,
            }),
            notify: Notify::new(),
        }))
    }

    /// Wait until a lane and aggregate slot are available, then reserve both
    /// atomically and return an owned lease.
    pub async fn acquire(self: &Arc<Self>, enabled: EnabledResponsesLanes) -> AdaptiveLease {
        loop {
            // Register for wakeups before rechecking so a release cannot be lost
            // between the full-window observation and the wait.
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            enum Action {
                Granted(ResponsesTransportKind),
                WaitForNotify,
                WaitUntil(Instant),
            }

            let action = {
                let mut state = self.state.lock().expect("adaptive controller poisoned");
                let now = Instant::now();
                if let Some(until) = state.throttle_deadline(now) {
                    state.waiting += 1;
                    Action::WaitUntil(until)
                } else {
                    state.clear_expired_throttle(now);
                    if let Some(kind) = state.try_reserve(enabled) {
                        Action::Granted(kind)
                    } else {
                        state.waiting += 1;
                        Action::WaitForNotify
                    }
                }
            };

            match action {
                Action::Granted(kind) => {
                    return AdaptiveLease::new(Arc::clone(self), kind);
                }
                Action::WaitUntil(until) => {
                    let guard = WaitingGuard::new(Arc::clone(self));
                    tokio::select! {
                        _ = tokio::time::sleep_until(until) => {}
                        _ = &mut notified => {}
                    }
                    drop(guard);
                }
                Action::WaitForNotify => {
                    let guard = WaitingGuard::new(Arc::clone(self));
                    notified.await;
                    drop(guard);
                }
            }
        }
    }

    pub fn snapshot(&self) -> AdaptiveConcurrencySnapshot {
        let state = self.state.lock().expect("adaptive controller poisoned");
        state.snapshot(Instant::now())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small_config() -> AdaptiveConcurrencyConfig {
        AdaptiveConcurrencyConfig {
            http: ConcurrencyLimits {
                initial: 2,
                min: 1,
                max: 8,
            },
            websocket: ConcurrencyLimits {
                initial: 1,
                min: 1,
                max: 4,
            },
            aggregate_max: 12,
        }
    }

    #[test]
    fn defaults_start_at_32_with_a_48_ceiling() {
        let config = AdaptiveConcurrencyConfig::default();
        assert_eq!(config.http.initial, 32);
        assert_eq!(config.http.max, 48);
        assert_eq!(config.websocket.initial, 32);
        assert_eq!(config.websocket.max, 48);
        assert_eq!(config.aggregate_max, 48);
    }

    #[test]
    fn rejects_invalid_limits_instead_of_clamping() {
        let err = AdaptiveConcurrency::new(AdaptiveConcurrencyConfig {
            http: ConcurrencyLimits {
                initial: 0,
                min: 1,
                max: 8,
            },
            ..small_config()
        })
        .expect_err("zero initial");
        assert!(err.to_string().contains("responses config:"));

        let err = AdaptiveConcurrency::new(AdaptiveConcurrencyConfig {
            http: ConcurrencyLimits {
                initial: 4,
                min: 1,
                max: 2,
            },
            ..small_config()
        })
        .expect_err("initial > max");
        assert!(err.to_string().contains("responses config:"));

        let err = AdaptiveConcurrency::new(AdaptiveConcurrencyConfig {
            aggregate_max: 100,
            ..small_config()
        })
        .expect_err("aggregate above sum");
        assert!(err.to_string().contains("aggregate_max"));
    }

    #[test]
    fn shared_throttle_duration_clamps_and_defaults() {
        assert_eq!(shared_throttle_duration(None), Duration::from_secs(1));
        assert_eq!(
            shared_throttle_duration(Some(0)),
            Duration::from_millis(100)
        );
        assert_eq!(shared_throttle_duration(Some(60)), Duration::from_secs(30));
        assert_eq!(shared_throttle_duration(Some(5)), Duration::from_secs(5));
    }

    #[tokio::test(start_paused = true)]
    async fn slow_start_grows_saturated_lane_on_success() {
        let controller = AdaptiveConcurrency::new(small_config()).unwrap();
        let dual = EnabledResponsesLanes::Dual;

        let first = controller.acquire(dual).await;
        let second = controller.acquire(dual).await;
        let third = controller.acquire(dual).await;

        let kinds = [first.transport(), second.transport(), third.transport()];
        assert_eq!(
            kinds
                .iter()
                .filter(|k| **k == ResponsesTransportKind::Http)
                .count(),
            2
        );
        assert_eq!(
            kinds
                .iter()
                .filter(|k| **k == ResponsesTransportKind::WebSocket)
                .count(),
            1
        );

        let mut leases = vec![first, second, third];
        let http_idx = leases
            .iter()
            .position(|lease| lease.transport() == ResponsesTransportKind::Http)
            .unwrap();
        let http_lease = leases.swap_remove(http_idx);
        http_lease.finish(AdaptiveOutcome::Success, Duration::from_millis(10));

        let snapshot = controller.snapshot();
        assert_eq!(snapshot.http.limit, 3);
        assert_eq!(snapshot.http.in_flight, 1);
        assert_eq!(snapshot.aggregate.in_flight, 2);
        drop(leases);
    }

    #[tokio::test(start_paused = true)]
    async fn selection_uses_enabled_lanes_only() {
        let controller = AdaptiveConcurrency::new(small_config()).unwrap();
        let lease = controller.acquire(EnabledResponsesLanes::WebSocket).await;
        assert_eq!(lease.transport(), ResponsesTransportKind::WebSocket);
        lease.finish(AdaptiveOutcome::Success, Duration::from_millis(1));

        let lease = controller.acquire(EnabledResponsesLanes::Http).await;
        assert_eq!(lease.transport(), ResponsesTransportKind::Http);
        lease.finish(AdaptiveOutcome::Success, Duration::from_millis(1));
    }
}
