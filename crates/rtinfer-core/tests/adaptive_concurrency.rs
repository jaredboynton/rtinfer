use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rtinfer_core::{
    AdaptiveConcurrency, AdaptiveConcurrencyConfig, AdaptiveOutcome, ConcurrencyLimits,
    EnabledResponsesLanes, ResponsesTransportKind,
};
use tokio::time::timeout;

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
        aggregate_max: 3,
    }
}

fn controller() -> Arc<AdaptiveConcurrency> {
    AdaptiveConcurrency::new(small_config()).expect("valid adaptive config")
}

#[tokio::test(start_paused = true)]
async fn dropped_lease_releases_lane_and_aggregate() {
    let controller = controller();
    let dual = EnabledResponsesLanes::Dual;

    let mut held = Vec::new();
    for _ in 0..3 {
        held.push(controller.acquire(dual).await);
    }
    let before = controller.snapshot();
    assert_eq!(before.aggregate.in_flight, 3);

    let waiter = {
        let controller = Arc::clone(&controller);
        tokio::spawn(async move { controller.acquire(dual).await })
    };
    tokio::task::yield_now().await;
    assert_eq!(controller.snapshot().aggregate.waiting, 1);

    let dropped = held.pop().expect("lease");
    let transport = dropped.transport();
    let cancellations_before = match transport {
        ResponsesTransportKind::Http => before.http.cancellations,
        ResponsesTransportKind::WebSocket => before.websocket.cancellations,
    };
    drop(dropped);

    let woken = timeout(Duration::from_millis(100), waiter)
        .await
        .expect("waiter should acquire within 100ms")
        .expect("waiter task");

    let after = controller.snapshot();
    match transport {
        ResponsesTransportKind::Http => {
            assert_eq!(after.http.cancellations, cancellations_before + 1);
        }
        ResponsesTransportKind::WebSocket => {
            assert_eq!(after.websocket.cancellations, cancellations_before + 1);
        }
    }
    assert_eq!(after.http.limit, before.http.limit);
    assert_eq!(after.websocket.limit, before.websocket.limit);
    assert_eq!(after.http.sample_count, before.http.sample_count);
    assert_eq!(after.websocket.sample_count, before.websocket.sample_count);
    assert_eq!(after.aggregate.in_flight, 3);

    drop(woken);
    drop(held);
    let settled = controller.snapshot();
    assert_eq!(settled.aggregate.in_flight, 0);
    assert_eq!(settled.http.in_flight, 0);
    assert_eq!(settled.websocket.in_flight, 0);
}

#[tokio::test(start_paused = true)]
async fn aborted_holder_conserves_capacity() {
    let controller = controller();
    let dual = EnabledResponsesLanes::Dual;

    let (tx, rx) = tokio::sync::oneshot::channel();
    let holder = {
        let controller = Arc::clone(&controller);
        tokio::spawn(async move {
            let lease = controller.acquire(dual).await;
            let _ = tx.send(lease.transport());
            let _lease = lease;
            std::future::pending::<()>().await;
        })
    };

    let holder_transport = timeout(Duration::from_millis(100), rx)
        .await
        .expect("holder acquired within 100ms")
        .expect("holder transport");

    let mut fillers = Vec::new();
    while controller.snapshot().aggregate.in_flight < 3 {
        fillers.push(controller.acquire(dual).await);
    }

    let waiter = {
        let controller = Arc::clone(&controller);
        tokio::spawn(async move { controller.acquire(dual).await })
    };
    tokio::task::yield_now().await;
    assert_eq!(controller.snapshot().aggregate.waiting, 1);

    let before = controller.snapshot();
    let http_cancellations_before = before.http.cancellations;
    let websocket_cancellations_before = before.websocket.cancellations;
    let total_cancellations_before = http_cancellations_before + websocket_cancellations_before;

    holder.abort();
    let _ = holder.await;

    let woken = timeout(Duration::from_millis(100), waiter)
        .await
        .expect("waiter acquires within 100ms paused time")
        .expect("waiter task");

    let after = controller.snapshot();
    match holder_transport {
        ResponsesTransportKind::Http => {
            assert_eq!(after.http.cancellations, http_cancellations_before + 1);
            assert_eq!(
                after.websocket.cancellations,
                websocket_cancellations_before
            );
        }
        ResponsesTransportKind::WebSocket => {
            assert_eq!(
                after.websocket.cancellations,
                websocket_cancellations_before + 1
            );
            assert_eq!(after.http.cancellations, http_cancellations_before);
        }
    }
    assert_eq!(
        after.http.cancellations + after.websocket.cancellations,
        total_cancellations_before + 1
    );
    assert_eq!(after.aggregate.in_flight, before.aggregate.in_flight);
    assert_eq!(after.aggregate.in_flight, 3);
    assert_eq!(
        after.http.in_flight + after.websocket.in_flight,
        after.aggregate.in_flight
    );
    assert_eq!(after.http.limit, before.http.limit);
    assert_eq!(after.websocket.limit, before.websocket.limit);
    assert_eq!(after.http.sample_count, before.http.sample_count);
    assert_eq!(after.websocket.sample_count, before.websocket.sample_count);

    drop(woken);
    drop(fillers);
    let settled = controller.snapshot();
    assert_eq!(settled.aggregate.in_flight, 0);
    assert_eq!(settled.http.in_flight + settled.websocket.in_flight, 0);
}

#[tokio::test(start_paused = true)]
async fn cancelled_waiter_owns_no_capacity() {
    let controller = controller();
    let dual = EnabledResponsesLanes::Dual;

    let mut held = Vec::new();
    for _ in 0..3 {
        held.push(controller.acquire(dual).await);
    }
    let before = controller.snapshot();
    assert_eq!(before.aggregate.in_flight, 3);
    assert_eq!(before.aggregate.waiting, 0);

    let waiter = {
        let controller = Arc::clone(&controller);
        tokio::spawn(async move { controller.acquire(dual).await })
    };
    tokio::task::yield_now().await;
    assert_eq!(controller.snapshot().aggregate.waiting, 1);

    waiter.abort();
    let _ = waiter.await;

    let after = controller.snapshot();
    assert_eq!(after.aggregate.in_flight, before.aggregate.in_flight);
    assert_eq!(after.http.in_flight, before.http.in_flight);
    assert_eq!(after.websocket.in_flight, before.websocket.in_flight);
    assert_eq!(after.aggregate.waiting, 0);
    assert_eq!(after.http.cancellations, before.http.cancellations);
    assert_eq!(
        after.websocket.cancellations,
        before.websocket.cancellations
    );
    assert_eq!(after.http.limit, before.http.limit);
    assert_eq!(after.websocket.limit, before.websocket.limit);

    drop(held);
}

#[tokio::test(start_paused = true)]
async fn wake_registration_has_no_lost_notification() {
    let controller = controller();
    let dual = EnabledResponsesLanes::Dual;

    let mut held = Vec::new();
    for _ in 0..3 {
        held.push(controller.acquire(dual).await);
    }

    let waiter = {
        let controller = Arc::clone(&controller);
        tokio::spawn(async move { controller.acquire(dual).await })
    };
    tokio::task::yield_now().await;
    assert_eq!(controller.snapshot().aggregate.waiting, 1);

    let released = held.pop().expect("held lease");
    released.finish(AdaptiveOutcome::Success, Duration::from_millis(5));

    let lease = timeout(Duration::from_millis(100), waiter)
        .await
        .expect("no lost wakeup")
        .expect("waiter task");
    assert_eq!(controller.snapshot().aggregate.in_flight, 3);

    drop(lease);
    drop(held);
    assert_eq!(controller.snapshot().aggregate.in_flight, 0);
}

#[tokio::test(start_paused = true)]
async fn thousand_task_conservation_respects_all_maxima() {
    let controller = AdaptiveConcurrency::new(AdaptiveConcurrencyConfig {
        http: ConcurrencyLimits {
            initial: 4,
            min: 1,
            max: 8,
        },
        websocket: ConcurrencyLimits {
            initial: 2,
            min: 1,
            max: 4,
        },
        aggregate_max: 6,
    })
    .unwrap();
    let dual = EnabledResponsesLanes::Dual;
    let peak_aggregate = Arc::new(AtomicUsize::new(0));
    let peak_http = Arc::new(AtomicUsize::new(0));
    let peak_websocket = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::with_capacity(1000);
    for i in 0..1000 {
        let controller = Arc::clone(&controller);
        let peak_aggregate = Arc::clone(&peak_aggregate);
        let peak_http = Arc::clone(&peak_http);
        let peak_websocket = Arc::clone(&peak_websocket);
        handles.push(tokio::spawn(async move {
            let lease = controller.acquire(dual).await;
            let snap = controller.snapshot();
            peak_aggregate.fetch_max(snap.aggregate.in_flight, Ordering::Relaxed);
            peak_http.fetch_max(snap.http.in_flight, Ordering::Relaxed);
            peak_websocket.fetch_max(snap.websocket.in_flight, Ordering::Relaxed);
            assert!(snap.aggregate.in_flight <= snap.aggregate.limit);
            assert!(snap.http.in_flight <= snap.http.max);
            assert!(snap.websocket.in_flight <= snap.websocket.max);
            assert!(snap.http.in_flight <= snap.http.limit);
            assert!(snap.websocket.in_flight <= snap.websocket.limit);

            match i % 5 {
                0 => lease.finish(AdaptiveOutcome::Success, Duration::from_millis(1)),
                1 => lease.finish(AdaptiveOutcome::LaneOverload, Duration::from_millis(2)),
                2 => lease.finish(AdaptiveOutcome::Failure, Duration::from_millis(3)),
                3 => lease.finish(AdaptiveOutcome::Indeterminate, Duration::from_millis(4)),
                _ => drop(lease),
            }
        }));
    }

    for handle in handles {
        handle.await.expect("task");
    }

    let snap = controller.snapshot();
    assert_eq!(snap.aggregate.in_flight, 0);
    assert_eq!(snap.http.in_flight, 0);
    assert_eq!(snap.websocket.in_flight, 0);
    assert_eq!(snap.aggregate.waiting, 0);
    assert!(peak_aggregate.load(Ordering::Relaxed) <= 6);
    assert!(peak_http.load(Ordering::Relaxed) <= 8);
    assert!(peak_websocket.load(Ordering::Relaxed) <= 4);

    let lease = timeout(Duration::from_millis(100), controller.acquire(dual))
        .await
        .expect("post-storm acquire");
    lease.finish(AdaptiveOutcome::Success, Duration::from_millis(1));
}

#[tokio::test(start_paused = true)]
async fn isolated_lane_overload_halves_only_selected_lane() {
    let controller = controller();
    let dual = EnabledResponsesLanes::Dual;

    let mut leases = Vec::new();
    for _ in 0..3 {
        leases.push(controller.acquire(dual).await);
    }
    let before = controller.snapshot();
    assert_eq!(before.websocket.limit, 1);
    assert!(before.websocket.slow_start);

    let http_idx = leases
        .iter()
        .position(|lease| lease.transport() == ResponsesTransportKind::Http)
        .expect("http lease");
    let http = leases.swap_remove(http_idx);
    http.finish(AdaptiveOutcome::LaneOverload, Duration::from_millis(20));
    drop(leases);

    let snapshot = controller.snapshot();
    assert_eq!(snapshot.http.limit, 1);
    assert_eq!(snapshot.websocket.limit, 1);
    assert_eq!(snapshot.aggregate.limit, before.aggregate.limit);
    assert!(!snapshot.http.slow_start);
    assert!(snapshot.websocket.slow_start);
    assert_eq!(snapshot.http.lane_overloads, 1);
    assert_eq!(snapshot.websocket.lane_overloads, 0);
}

#[tokio::test(start_paused = true)]
async fn shared_throttle_pauses_both_lanes_without_halving() {
    let controller = controller();
    let dual = EnabledResponsesLanes::Dual;

    let lease = controller.acquire(dual).await;
    let before = controller.snapshot();
    let http_limit = before.http.limit;
    let websocket_limit = before.websocket.limit;

    lease.finish(
        AdaptiveOutcome::SharedThrottle {
            retry_after_secs: None,
        },
        Duration::from_millis(5),
    );

    let mid = controller.snapshot();
    assert!(mid.aggregate.throttled);
    assert_eq!(mid.http.limit, http_limit);
    assert_eq!(mid.websocket.limit, websocket_limit);

    let acquire = {
        let controller = Arc::clone(&controller);
        tokio::spawn(async move { controller.acquire(dual).await })
    };
    tokio::task::yield_now().await;

    // Before t=1s neither lane admits.
    tokio::time::advance(Duration::from_millis(999)).await;
    tokio::task::yield_now().await;
    assert!(
        !acquire.is_finished(),
        "must not admit before the 1s default pause"
    );
    assert_eq!(controller.snapshot().http.limit, http_limit);
    assert_eq!(controller.snapshot().websocket.limit, websocket_limit);

    // At t=1s one enabled lane admits; limits unchanged.
    tokio::time::advance(Duration::from_millis(1)).await;
    let lease = timeout(Duration::from_millis(100), acquire)
        .await
        .expect("admits at 1s")
        .expect("acquire task");
    let after = controller.snapshot();
    assert!(!after.aggregate.throttled);
    assert_eq!(after.http.limit, http_limit);
    assert_eq!(after.websocket.limit, websocket_limit);
    lease.finish(AdaptiveOutcome::Success, Duration::from_millis(1));
}

#[tokio::test(start_paused = true)]
async fn deterministic_selection_prefers_http_then_less_utilized() {
    let controller = AdaptiveConcurrency::new(AdaptiveConcurrencyConfig {
        http: ConcurrencyLimits {
            initial: 2,
            min: 1,
            max: 8,
        },
        websocket: ConcurrencyLimits {
            initial: 2,
            min: 1,
            max: 4,
        },
        aggregate_max: 4,
    })
    .unwrap();
    let dual = EnabledResponsesLanes::Dual;

    let first = controller.acquire(dual).await;
    assert_eq!(
        first.transport(),
        ResponsesTransportKind::Http,
        "equal utilization/headroom selects HTTP"
    );

    let second = controller.acquire(dual).await;
    assert_eq!(
        second.transport(),
        ResponsesTransportKind::WebSocket,
        "after HTTP utilization rises, select WSS"
    );

    first.finish(AdaptiveOutcome::Success, Duration::from_millis(1));
    second.finish(AdaptiveOutcome::Success, Duration::from_millis(1));
}

#[tokio::test(start_paused = true)]
async fn latency_is_observational_only() {
    async fn run(elapsed: Duration) -> (Vec<ResponsesTransportKind>, usize, usize, f64, u64) {
        let controller = AdaptiveConcurrency::new(AdaptiveConcurrencyConfig {
            http: ConcurrencyLimits {
                initial: 2,
                min: 1,
                max: 8,
            },
            websocket: ConcurrencyLimits {
                initial: 2,
                min: 1,
                max: 4,
            },
            aggregate_max: 4,
        })
        .unwrap();
        let dual = EnabledResponsesLanes::Dual;

        let mut kinds = Vec::new();
        let a = controller.acquire(dual).await;
        kinds.push(a.transport());
        let b = controller.acquire(dual).await;
        kinds.push(b.transport());
        let c = controller.acquire(dual).await;
        kinds.push(c.transport());

        a.finish(AdaptiveOutcome::Success, elapsed);
        b.finish(AdaptiveOutcome::Success, elapsed);
        c.finish(AdaptiveOutcome::LaneOverload, elapsed);

        let snap = controller.snapshot();
        (
            kinds,
            snap.http.limit,
            snap.websocket.limit,
            snap.http.latency_ewma_ms + snap.websocket.latency_ewma_ms,
            snap.http.sample_count + snap.websocket.sample_count,
        )
    }

    let (kinds_fast, http_fast, ws_fast, lat_fast, samples_fast) =
        run(Duration::from_millis(10)).await;
    let (kinds_slow, http_slow, ws_slow, lat_slow, samples_slow) =
        run(Duration::from_secs(10)).await;

    assert_eq!(kinds_fast, kinds_slow);
    assert_eq!(http_fast, http_slow);
    assert_eq!(ws_fast, ws_slow);
    assert_eq!(samples_fast, samples_slow);
    assert!(lat_fast < lat_slow);
}

#[tokio::test(start_paused = true)]
async fn fills_both_transports_and_grows_a_saturated_lane_immediately() {
    let controller = controller();
    let dual = EnabledResponsesLanes::Dual;

    let first = controller.acquire(dual).await;
    let second = controller.acquire(dual).await;
    let third = controller.acquire(dual).await;

    let kinds = [first.transport(), second.transport(), third.transport()];
    assert_eq!(
        kinds
            .iter()
            .filter(|kind| **kind == ResponsesTransportKind::Http)
            .count(),
        2
    );
    assert_eq!(
        kinds
            .iter()
            .filter(|kind| **kind == ResponsesTransportKind::WebSocket)
            .count(),
        1
    );

    let blocked = timeout(Duration::from_millis(5), controller.acquire(dual)).await;
    assert!(blocked.is_err(), "initial aggregate window is full");

    let mut leases = vec![first, second, third];
    let http_idx = leases
        .iter()
        .position(|lease| lease.transport() == ResponsesTransportKind::Http)
        .expect("http lease");
    let http = leases.swap_remove(http_idx);
    http.finish(AdaptiveOutcome::Success, Duration::from_millis(10));

    let snapshot = controller.snapshot();
    assert_eq!(
        snapshot.http.limit, 3,
        "slow start probes on saturated success"
    );
    assert_eq!(snapshot.http.in_flight, 1);
    drop(leases);
}
