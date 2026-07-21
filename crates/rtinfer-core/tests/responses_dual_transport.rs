//! Local dual-transport harness for CodexResponsesClient.
//!
//! Cleartext WSS fixture is implemented with a minimal RFC6455 server (no extra
//! deps). HTTP/2 uses `h2` + Warpsock `http2_prior_knowledge` when available.
//!
//! Known limitation: Warpsock cleartext prior-knowledge currently fails with
//! `Expected h2 ALPN, got Unknown` before sending an H2 preface, so the local
//! H2 fixture does not observe POSTs. True HTTP/2 protocol/reuse remains the
//! credentialed live seam. Local HTTP tests assert client dispatch counts and
//! that exact ALPN protocol failure rather than claiming cleartext multiplex.

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use h2::server;
use rtinfer_core::{
    AdaptiveConcurrencySnapshot, CodexAuth, CodexAuthSource, CodexResponsesClient, RealtimeError,
    ResponsesRuntimeConfig, ResponsesTransportMode, StaticCodexAuthSource,
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, Mutex, Notify};
use tokio::time::timeout;

fn test_auth() -> CodexAuth {
    CodexAuth {
        access_token: "dual-access".into(),
        account_id: "acct".into(),
        id_token: "id".into(),
        refresh_token: String::new(),
        source_path: None,
    }
}

fn install_id() -> String {
    "22222222-2222-4222-8222-222222222222".into()
}

const CLEARTEXT_ALPN_UNKNOWN: &str = "Expected h2 ALPN, got Unknown";
const MAX_CAPTURED_HANDSHAKE_HEADERS: usize = 8;
const MAX_CAPTURED_HTTP_REQUESTS: usize = 16;

// ---------------------------------------------------------------------------
// Minimal cleartext WebSocket server (RFC6455) for local fixtures
// ---------------------------------------------------------------------------

#[derive(Default)]
struct SocketTrack {
    /// Peak concurrent active asks observed on this socket.
    max_active: usize,
    /// Current active asks on this socket.
    active: usize,
    creates: usize,
}

#[derive(Default)]
struct WssFixtureState {
    handshakes: AtomicUsize,
    creates: AtomicUsize,
    /// Peak concurrent in-progress handshakes (after request headers, before 101).
    max_active_handshakes: AtomicUsize,
    active_handshakes: AtomicUsize,
    /// Peak concurrent active asks across all sockets.
    max_active: AtomicUsize,
    active: AtomicUsize,
    next_socket_id: AtomicUsize,
    sockets: Mutex<HashMap<usize, SocketTrack>>,
    /// Captured handshake request header blocks (bounded).
    handshake_headers: Mutex<Vec<HashMap<String, String>>>,
    /// Per-logical create bodies observed (for no-replay checks).
    create_bodies: Mutex<Vec<Value>>,
    mode: Mutex<WssReplyMode>,
    /// Release signal for [`WssReplyMode::HoldUntilRelease`].
    hold_release: Notify,
    /// Handshake hold so concurrent cold asks overlap inside HANDSHAKE_GATE.
    handshake_hold: Mutex<Duration>,
}

#[derive(Clone, Copy, Default)]
enum WssReplyMode {
    #[default]
    CompleteOk,
    IncompleteThenClose,
    MalformedJson,
    HoldUntilRelease,
}

async fn spawn_wss_fixture() -> (String, Arc<WssFixtureState>, oneshot::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = Arc::new(WssFixtureState {
        handshake_hold: Mutex::new(Duration::from_millis(40)),
        ..Default::default()
    });
    let (stop_tx, mut stop_rx) = oneshot::channel::<()>();
    let state_accept = Arc::clone(&state);
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut stop_rx => break,
                accept = listener.accept() => {
                    let Ok((stream, _)) = accept else { break; };
                    let state = Arc::clone(&state_accept);
                    tokio::spawn(async move {
                        if let Err(e) = handle_wss_client(stream, state).await {
                            eprintln!("wss fixture client error: {e}");
                        }
                    });
                }
            }
        }
    });
    (format!("ws://{addr}"), state, stop_tx)
}

async fn handle_wss_client(
    mut stream: TcpStream,
    state: Arc<WssFixtureState>,
) -> Result<(), String> {
    let mut buf = BytesMut::with_capacity(4096);
    loop {
        let mut tmp = [0u8; 1024];
        let n = stream.read(&mut tmp).await.map_err(|e| e.to_string())?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        if buf.len() > 64 * 1024 {
            return Err("handshake too large".into());
        }
    }
    let header_end = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|i| i + 4)
        .unwrap();
    let req_bytes = buf.split_to(header_end);
    let req = String::from_utf8_lossy(&req_bytes);
    let headers = parse_http_headers(&req);
    {
        let mut captured = state.handshake_headers.lock().await;
        if captured.len() < MAX_CAPTURED_HANDSHAKE_HEADERS {
            captured.push(headers.clone());
        }
    }
    let key = headers
        .get("sec-websocket-key")
        .cloned()
        .ok_or_else(|| "missing sec-websocket-key".to_string())?;

    let cur_hs = state.active_handshakes.fetch_add(1, Ordering::SeqCst) + 1;
    state
        .max_active_handshakes
        .fetch_max(cur_hs, Ordering::SeqCst);
    let hold = *state.handshake_hold.lock().await;
    if !hold.is_zero() {
        tokio::time::sleep(hold).await;
    }
    let accept = ws_accept_key(&key);
    let resp = format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
    );
    stream
        .write_all(resp.as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    state.active_handshakes.fetch_sub(1, Ordering::SeqCst);
    state.handshakes.fetch_add(1, Ordering::SeqCst);

    let socket_id = state.next_socket_id.fetch_add(1, Ordering::SeqCst);
    {
        let mut map = state.sockets.lock().await;
        map.insert(socket_id, SocketTrack::default());
    }
    let mut pending = buf;

    loop {
        while let Some(frame) = try_read_ws_frame(&mut pending)? {
            match frame {
                WsFrame::Text(text) => {
                    let v: Value = serde_json::from_str(&text).map_err(|e| e.to_string())?;
                    if v.get("type").and_then(Value::as_str) == Some("response.create") {
                        state.creates.fetch_add(1, Ordering::SeqCst);
                        state.create_bodies.lock().await.push(v.clone());
                        let cur = state.active.fetch_add(1, Ordering::SeqCst) + 1;
                        state.max_active.fetch_max(cur, Ordering::SeqCst);
                        {
                            let mut map = state.sockets.lock().await;
                            let track = map.entry(socket_id).or_default();
                            track.creates += 1;
                            track.active += 1;
                            track.max_active = track.max_active.max(track.active);
                        }
                        let mode = *state.mode.lock().await;
                        match mode {
                            WssReplyMode::CompleteOk => {
                                send_complete_ok(&mut stream).await?;
                            }
                            WssReplyMode::IncompleteThenClose => {
                                send_ws_text(
                                    &mut stream,
                                    &json!({"type":"response.output_text.delta","delta":"partial"})
                                        .to_string(),
                                )
                                .await?;
                                send_ws_close(&mut stream).await?;
                                finish_active_ask(&state, socket_id).await;
                                return Ok(());
                            }
                            WssReplyMode::MalformedJson => {
                                send_ws_text(&mut stream, "{not-json").await?;
                                finish_active_ask(&state, socket_id).await;
                                return Ok(());
                            }
                            WssReplyMode::HoldUntilRelease => {
                                state.hold_release.notified().await;
                                send_complete_ok(&mut stream).await?;
                            }
                        }
                        finish_active_ask(&state, socket_id).await;
                    }
                }
                WsFrame::Close => return Ok(()),
                WsFrame::Ping(p) => send_ws_pong(&mut stream, &p).await?,
                WsFrame::Pong | WsFrame::Binary(_) => {}
            }
        }
        let mut tmp = [0u8; 4096];
        let n = stream.read(&mut tmp).await.map_err(|e| e.to_string())?;
        if n == 0 {
            return Ok(());
        }
        pending.extend_from_slice(&tmp[..n]);
    }
}

async fn finish_active_ask(state: &WssFixtureState, socket_id: usize) {
    state.active.fetch_sub(1, Ordering::SeqCst);
    let mut map = state.sockets.lock().await;
    if let Some(track) = map.get_mut(&socket_id) {
        track.active = track.active.saturating_sub(1);
    }
}

async fn send_complete_ok(stream: &mut TcpStream) -> Result<(), String> {
    send_ws_text(
        stream,
        &json!({"type":"response.output_text.delta","delta":"ok"}).to_string(),
    )
    .await?;
    send_ws_text(
        stream,
        &json!({"type":"response.output_text.done","text":"ok"}).to_string(),
    )
    .await?;
    send_ws_text(
        stream,
        &json!({"type":"response.completed","response":{"status":"completed"}}).to_string(),
    )
    .await?;
    Ok(())
}

fn parse_http_headers(req: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for line in req.lines().skip(1) {
        if line.is_empty() {
            break;
        }
        if let Some((k, v)) = line.split_once(':') {
            map.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }
    map
}

enum WsFrame {
    Text(String),
    Binary(#[allow(dead_code)] Vec<u8>),
    Ping(Vec<u8>),
    Pong,
    Close,
}

fn try_read_ws_frame(buf: &mut BytesMut) -> Result<Option<WsFrame>, String> {
    if buf.len() < 2 {
        return Ok(None);
    }
    let b0 = buf[0];
    let b1 = buf[1];
    let opcode = b0 & 0x0f;
    let masked = b1 & 0x80 != 0;
    let mut len = (b1 & 0x7f) as usize;
    let mut off = 2usize;
    if len == 126 {
        if buf.len() < 4 {
            return Ok(None);
        }
        len = u16::from_be_bytes([buf[2], buf[3]]) as usize;
        off = 4;
    } else if len == 127 {
        if buf.len() < 10 {
            return Ok(None);
        }
        len = u64::from_be_bytes(buf[2..10].try_into().unwrap()) as usize;
        off = 10;
    }
    let mask_len = if masked { 4 } else { 0 };
    if buf.len() < off + mask_len + len {
        return Ok(None);
    }
    let _ = buf.split_to(off);
    let mut mask = [0u8; 4];
    if masked {
        mask.copy_from_slice(&buf.split_to(4));
    }
    let mut payload = buf.split_to(len).to_vec();
    if masked {
        for (i, b) in payload.iter_mut().enumerate() {
            *b ^= mask[i % 4];
        }
    }
    Ok(Some(match opcode {
        0x1 => WsFrame::Text(String::from_utf8(payload).map_err(|e| e.to_string())?),
        0x2 => WsFrame::Binary(payload),
        0x8 => WsFrame::Close,
        0x9 => WsFrame::Ping(payload),
        0xA => WsFrame::Pong,
        _ => return Err(format!("unsupported opcode {opcode}")),
    }))
}

async fn send_ws_text(stream: &mut TcpStream, text: &str) -> Result<(), String> {
    let payload = text.as_bytes();
    let mut frame = Vec::new();
    frame.push(0x81);
    if payload.len() < 126 {
        frame.push(payload.len() as u8);
    } else if payload.len() <= 65535 {
        frame.push(126);
        frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    } else {
        frame.push(127);
        frame.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    }
    frame.extend_from_slice(payload);
    stream.write_all(&frame).await.map_err(|e| e.to_string())
}

async fn send_ws_pong(stream: &mut TcpStream, payload: &[u8]) -> Result<(), String> {
    let mut frame = vec![0x8A, payload.len() as u8];
    frame.extend_from_slice(payload);
    stream.write_all(&frame).await.map_err(|e| e.to_string())
}

async fn send_ws_close(stream: &mut TcpStream) -> Result<(), String> {
    stream
        .write_all(&[0x88, 0x00])
        .await
        .map_err(|e| e.to_string())
}

fn ws_accept_key(key: &str) -> String {
    const GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    let mut data = Vec::with_capacity(key.len() + GUID.len());
    data.extend_from_slice(key.as_bytes());
    data.extend_from_slice(GUID.as_bytes());
    let digest = sha1_bytes(&data);
    base64_encode(&digest)
}

fn sha1_bytes(data: &[u8]) -> [u8; 20] {
    fn rol(x: u32, n: u32) -> u32 {
        x.rotate_left(n)
    }
    let mut h0: u32 = 0x67452301;
    let mut h1: u32 = 0xEFCDAB89;
    let mut h2: u32 = 0x98BADCFE;
    let mut h3: u32 = 0x10325476;
    let mut h4: u32 = 0xC3D2E1F0;
    let mut msg = data.to_vec();
    let bit_len = (msg.len() as u64) * 8;
    msg.push(0x80);
    while (msg.len() % 64) != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());
    for chunk in msg.chunks_exact(64) {
        let mut w = [0u32; 80];
        for i in 0..16 {
            w[i] = u32::from_be_bytes(chunk[i * 4..i * 4 + 4].try_into().unwrap());
        }
        for i in 16..80 {
            w[i] = rol(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16], 1);
        }
        let (mut a, mut b, mut c, mut d, mut e) = (h0, h1, h2, h3, h4);
        for (i, &wi) in w.iter().enumerate() {
            let (f, k) = if i < 20 {
                ((b & c) | ((!b) & d), 0x5A827999)
            } else if i < 40 {
                (b ^ c ^ d, 0x6ED9EBA1)
            } else if i < 60 {
                ((b & c) | (b & d) | (c & d), 0x8F1BBCDC)
            } else {
                (b ^ c ^ d, 0xCA62C1D6)
            };
            let temp = rol(a, 5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(wi);
            e = d;
            d = c;
            c = rol(b, 30);
            b = a;
            a = temp;
        }
        h0 = h0.wrapping_add(a);
        h1 = h1.wrapping_add(b);
        h2 = h2.wrapping_add(c);
        h3 = h3.wrapping_add(d);
        h4 = h4.wrapping_add(e);
    }
    let mut out = [0u8; 20];
    out[0..4].copy_from_slice(&h0.to_be_bytes());
    out[4..8].copy_from_slice(&h1.to_be_bytes());
    out[8..12].copy_from_slice(&h2.to_be_bytes());
    out[12..16].copy_from_slice(&h3.to_be_bytes());
    out[16..20].copy_from_slice(&h4.to_be_bytes());
    out
}

fn base64_encode(input: &[u8]) -> String {
    const T: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in input.chunks(3) {
        let mut buf = [0u8; 3];
        for (i, b) in chunk.iter().enumerate() {
            buf[i] = *b;
        }
        let n = chunk.len();
        let b0 = buf[0] as u32;
        let b1 = buf[1] as u32;
        let b2 = buf[2] as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(T[((triple >> 18) & 63) as usize] as char);
        out.push(T[((triple >> 12) & 63) as usize] as char);
        if n > 1 {
            out.push(T[((triple >> 6) & 63) as usize] as char);
        } else {
            out.push('=');
        }
        if n > 2 {
            out.push(T[(triple & 63) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

fn logical_id_from_create(body: &Value) -> String {
    body.get("prompt_cache_key")
        .and_then(Value::as_str)
        .or_else(|| {
            body.get("client_metadata")
                .and_then(|m| m.get("thread_id"))
                .and_then(Value::as_str)
        })
        .unwrap_or("")
        .to_string()
}

// ---------------------------------------------------------------------------
// Cleartext HTTP/2 prior-knowledge fixture
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
#[allow(dead_code)]
struct CapturedHttpRequest {
    method: String,
    version: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

#[derive(Default)]
struct HttpFixtureState {
    connections: AtomicUsize,
    posts: AtomicUsize,
    max_concurrent_streams: AtomicUsize,
    concurrent: AtomicUsize,
    reply: Mutex<HttpReplyMode>,
    requests: Mutex<Vec<CapturedHttpRequest>>,
}

#[derive(Clone, Default)]
enum HttpReplyMode {
    #[default]
    SseOk,
    NonSse,
    #[allow(dead_code)]
    MissingContentType,
    #[allow(dead_code)]
    PartialEof,
    #[allow(dead_code)]
    MalformedJson,
    #[allow(dead_code)]
    MalformedUtf8,
    #[allow(dead_code)]
    Status {
        code: u16,
        body: String,
    },
}

async fn spawn_h2_fixture() -> (String, Arc<HttpFixtureState>, oneshot::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = Arc::new(HttpFixtureState {
        reply: Mutex::new(HttpReplyMode::SseOk),
        ..Default::default()
    });
    let (stop_tx, mut stop_rx) = oneshot::channel::<()>();
    let state_accept = Arc::clone(&state);
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut stop_rx => break,
                accept = listener.accept() => {
                    let Ok((socket, _)) = accept else { break; };
                    let state = Arc::clone(&state_accept);
                    tokio::spawn(async move {
                        if let Err(e) = handle_h2_conn(socket, state).await {
                            eprintln!("h2 fixture error: {e}");
                        }
                    });
                }
            }
        }
    });
    (format!("http://{addr}"), state, stop_tx)
}

async fn handle_h2_conn(socket: TcpStream, state: Arc<HttpFixtureState>) -> Result<(), String> {
    state.connections.fetch_add(1, Ordering::SeqCst);
    let mut connection = server::handshake(socket).await.map_err(|e| e.to_string())?;
    while let Some(request) = connection.accept().await {
        let (request, mut respond) = request.map_err(|e| e.to_string())?;
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            let cur = state.concurrent.fetch_add(1, Ordering::SeqCst) + 1;
            state
                .max_concurrent_streams
                .fetch_max(cur, Ordering::SeqCst);
            state.posts.fetch_add(1, Ordering::SeqCst);

            let method = request.method().as_str().to_string();
            let version = format!("{:?}", request.version());
            let mut headers = HashMap::new();
            for (k, v) in request.headers().iter() {
                headers.insert(
                    k.as_str().to_ascii_lowercase(),
                    v.to_str().unwrap_or("").to_string(),
                );
            }
            let mut body = request.into_body();
            let mut body_bytes = Vec::new();
            while let Some(chunk) = body.data().await {
                match chunk {
                    Ok(bytes) => {
                        let _ = body.flow_control().release_capacity(bytes.len());
                        body_bytes.extend_from_slice(&bytes);
                    }
                    Err(_) => break,
                }
            }
            {
                let mut reqs = state.requests.lock().await;
                if reqs.len() < MAX_CAPTURED_HTTP_REQUESTS {
                    reqs.push(CapturedHttpRequest {
                        method,
                        version,
                        headers,
                        body: body_bytes,
                    });
                }
            }

            let mode = state.reply.lock().await.clone();
            let (status, content_type, body_out) = match mode {
                HttpReplyMode::SseOk => (
                    200u16,
                    Some("text/event-stream"),
                    concat!(
                        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\n",
                        "data: {\"type\":\"response.output_text.done\",\"text\":\"ok\"}\n\n",
                        "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n",
                    )
                    .as_bytes()
                    .to_vec(),
                ),
                HttpReplyMode::NonSse => {
                    (200, Some("application/json"), b"{\"ok\":true}".to_vec())
                }
                HttpReplyMode::MissingContentType => (
                    200,
                    None,
                    concat!(
                        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\n",
                        "data: {\"type\":\"response.output_text.done\",\"text\":\"ok\"}\n\n",
                        "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n",
                    )
                    .as_bytes()
                    .to_vec(),
                ),
                HttpReplyMode::PartialEof => (
                    200,
                    Some("text/event-stream"),
                    b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n"
                        .to_vec(),
                ),
                HttpReplyMode::MalformedJson => {
                    (200, Some("text/event-stream"), b"data: {not-json\n\n".to_vec())
                }
                HttpReplyMode::MalformedUtf8 => (
                    200,
                    Some("text/event-stream"),
                    vec![0xff, 0xfe, b'd', b'a', b't', b'a', b':', b' ', b'x', b'\n', b'\n'],
                ),
                HttpReplyMode::Status { code, body } => {
                    (code, Some("application/json"), body.into_bytes())
                }
            };
            let mut res = http::Response::builder().status(status);
            if let Some(ct) = content_type {
                res = res.header("content-type", ct);
            }
            let response = res.body(()).unwrap();
            if let Ok(mut send) = respond.send_response(response, false) {
                let _ = send.send_data(Bytes::from(body_out), true);
            }
            state.concurrent.fetch_sub(1, Ordering::SeqCst);
        });
    }
    Ok(())
}

fn runtime_for(
    mode: ResponsesTransportMode,
    http_max: usize,
    wss_max: usize,
    agg: usize,
) -> ResponsesRuntimeConfig {
    ResponsesRuntimeConfig {
        mode,
        http_initial: http_max.clamp(1, 2),
        http_max,
        wss_initial: wss_max.clamp(1, 2),
        wss_max,
        aggregate_max: agg,
        prewarm: 0,
    }
}

async fn client_for(
    mode: ResponsesTransportMode,
    http_url: Option<String>,
    wss_url: Option<String>,
    http_max: usize,
    wss_max: usize,
    agg: usize,
) -> Arc<CodexResponsesClient> {
    client_for_auth(
        mode,
        http_url,
        wss_url,
        http_max,
        wss_max,
        agg,
        Arc::new(StaticCodexAuthSource(test_auth())),
    )
    .await
}

async fn client_for_auth(
    mode: ResponsesTransportMode,
    http_url: Option<String>,
    wss_url: Option<String>,
    http_max: usize,
    wss_max: usize,
    agg: usize,
    auth_source: Arc<dyn CodexAuthSource>,
) -> Arc<CodexResponsesClient> {
    let mut b = CodexResponsesClient::builder()
        .mode(mode)
        .runtime(runtime_for(mode, http_max, wss_max, agg))
        .installation_id(install_id())
        .auth_source(auth_source)
        .auth_ttl(Duration::from_secs(3600));
    if let Some(u) = http_url {
        b = b.http_endpoint(u);
    }
    if let Some(u) = wss_url {
        b = b.wss_endpoint(u);
    }
    b.build().expect("client")
}

fn assert_cleartext_alpn_unknown(err: &RealtimeError) {
    let msg = format!("{err}");
    assert!(
        msg.contains(CLEARTEXT_ALPN_UNKNOWN),
        "expected exact cleartext ALPN Unknown protocol failure, got: {msg}"
    );
    assert_eq!(err.code_or_label(), "protocol");
}

struct RecordingAuthSource {
    load_calls: AtomicUsize,
    force_calls: AtomicUsize,
    auth: CodexAuth,
    force_auth: CodexAuth,
}

impl RecordingAuthSource {
    fn new() -> Self {
        let mut force = test_auth();
        force.access_token = "dual-access-refreshed".into();
        Self {
            load_calls: AtomicUsize::new(0),
            force_calls: AtomicUsize::new(0),
            auth: test_auth(),
            force_auth: force,
        }
    }
}

#[async_trait]
impl CodexAuthSource for RecordingAuthSource {
    async fn load(&self) -> Result<CodexAuth, RealtimeError> {
        self.load_calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.auth.clone())
    }

    async fn force_refresh(
        &self,
        _rejected_access_token: &str,
    ) -> Result<CodexAuth, RealtimeError> {
        self.force_calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.force_auth.clone())
    }
}

// ---------------------------------------------------------------------------
// WSS lane tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn wss_reuses_one_socket_for_sequential_completed_asks() {
    let (url, state, _stop) = spawn_wss_fixture().await;
    *state.handshake_hold.lock().await = Duration::ZERO;
    let client = client_for(ResponsesTransportMode::Wss, None, Some(url), 1, 4, 4).await;
    for _ in 0..3 {
        let text = client.ask_text("s", "u", None).await.unwrap();
        assert_eq!(text, "ok");
    }
    assert_eq!(state.handshakes.load(Ordering::SeqCst), 1);
    assert_eq!(state.creates.load(Ordering::SeqCst), 3);
    assert!(state.max_active.load(Ordering::SeqCst) <= 1);
    let sockets = state.sockets.lock().await;
    assert_eq!(sockets.len(), 1);
    let track = sockets.values().next().unwrap();
    assert_eq!(track.creates, 3);
    assert!(track.max_active <= 1);
    drop(sockets);
    let snap = client.snapshot().await;
    assert_eq!(snap.wss_idle_sockets, 1);
    assert_eq!(snap.wss_active_asks, 0);
}

#[tokio::test]
async fn wss_socket_ownership_is_exclusive() {
    let (url, state, _stop) = spawn_wss_fixture().await;
    *state.handshake_hold.lock().await = Duration::ZERO;
    let client = client_for(ResponsesTransportMode::Wss, None, Some(url), 1, 2, 2).await;
    client.prewarm(2).await;
    assert_eq!(client.snapshot().await.wss_idle_sockets, 2);
    assert_eq!(state.handshakes.load(Ordering::SeqCst), 2);

    let c1 = Arc::clone(&client);
    let c2 = Arc::clone(&client);
    let (a, b) = tokio::join!(c1.ask_text("s", "u1", None), c2.ask_text("s", "u2", None));
    assert_eq!(a.unwrap(), "ok");
    assert_eq!(b.unwrap(), "ok");

    assert_eq!(state.creates.load(Ordering::SeqCst), 2);
    assert_eq!(state.handshakes.load(Ordering::SeqCst), 2);

    let sockets = state.sockets.lock().await;
    let used: Vec<_> = sockets.iter().filter(|(_, t)| t.creates > 0).collect();
    assert_eq!(
        used.len(),
        2,
        "two concurrent asks must use distinct sockets"
    );
    for (_, track) in &used {
        assert!(
            track.max_active <= 1,
            "max one active ask per socket; got {}",
            track.max_active
        );
        assert_eq!(track.creates, 1);
    }
    drop(sockets);

    let snap = client.snapshot().await;
    assert_eq!(snap.wss_active_asks, 0);
    assert_eq!(
        snap.wss_idle_sockets, 2,
        "both healthy sockets must return to idle"
    );
}

#[tokio::test]
async fn wss_incomplete_attempt_is_dropped_not_replayed() {
    let (url, state, _stop) = spawn_wss_fixture().await;
    *state.handshake_hold.lock().await = Duration::ZERO;
    *state.mode.lock().await = WssReplyMode::IncompleteThenClose;
    let client = client_for(ResponsesTransportMode::Wss, None, Some(url), 1, 2, 2).await;
    let err = client.ask_text("s", "u", None).await.unwrap_err();
    assert_eq!(err.code_or_label(), "protocol");
    assert_eq!(state.creates.load(Ordering::SeqCst), 1);
    let bodies = state.create_bodies.lock().await;
    assert_eq!(bodies.len(), 1);
    let id = logical_id_from_create(&bodies[0]);
    assert!(!id.is_empty());
    drop(bodies);
    assert_eq!(client.snapshot().await.wss_idle_sockets, 0);
    assert_eq!(client.snapshot().await.wss_active_asks, 0);
}

#[tokio::test]
async fn wss_malformed_attempt_is_dropped_not_replayed() {
    let (url, state, _stop) = spawn_wss_fixture().await;
    *state.handshake_hold.lock().await = Duration::ZERO;
    *state.mode.lock().await = WssReplyMode::MalformedJson;
    let client = client_for(ResponsesTransportMode::Wss, None, Some(url), 1, 2, 2).await;
    let err = client.ask_text("s", "u", None).await.unwrap_err();
    assert_eq!(err.code_or_label(), "protocol");
    assert_eq!(state.creates.load(Ordering::SeqCst), 1);
    let bodies = state.create_bodies.lock().await;
    assert_eq!(bodies.len(), 1);
    assert!(!logical_id_from_create(&bodies[0]).is_empty());
    drop(bodies);
    assert_eq!(client.snapshot().await.wss_idle_sockets, 0);
    assert_eq!(client.snapshot().await.wss_active_asks, 0);
}

#[tokio::test]
async fn wss_handshakes_never_exceed_eight() {
    let (url, state, _stop) = spawn_wss_fixture().await;
    *state.handshake_hold.lock().await = Duration::from_millis(80);
    let client = client_for(ResponsesTransportMode::Wss, None, Some(url), 1, 64, 64).await;
    let mut joins = Vec::new();
    for i in 0..32 {
        let c = Arc::clone(&client);
        joins.push(tokio::spawn(async move {
            c.ask_text("s", &format!("u{i}"), None).await
        }));
    }
    for j in joins {
        let _ = j.await.unwrap();
    }
    let max_hs = state.max_active_handshakes.load(Ordering::SeqCst);
    assert!(
        max_hs > 1,
        "32 cold asks with handshake hold must overlap; max_active_handshakes={max_hs}"
    );
    assert!(
        max_hs <= 8,
        "HANDSHAKE_GATE must cap concurrent handshakes at 8; observed {max_hs}"
    );
    assert_eq!(state.creates.load(Ordering::SeqCst), 32);
    let end = client.snapshot().await;
    assert_eq!(end.adaptive.aggregate.in_flight, 0);
    assert_eq!(end.wss_active_asks, 0);
}

#[tokio::test]
async fn wss_abort_held_ask_releases_admission_and_skips_dirty_checkin() {
    let (url, state, _stop) = spawn_wss_fixture().await;
    *state.handshake_hold.lock().await = Duration::ZERO;
    *state.mode.lock().await = WssReplyMode::HoldUntilRelease;
    let client = client_for(ResponsesTransportMode::Wss, None, Some(url), 1, 2, 2).await;

    let c = Arc::clone(&client);
    let handle = tokio::spawn(async move { c.ask_text("s", "held", None).await });

    timeout(Duration::from_secs(2), async {
        loop {
            if state.creates.load(Ordering::SeqCst) >= 1
                && client.snapshot().await.wss_active_asks >= 1
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("held ask must become an active WSS ask with one create");

    let mid = client.snapshot().await;
    assert_eq!(mid.adaptive.aggregate.in_flight, 1);
    assert_eq!(mid.wss_active_asks, 1);

    handle.abort();
    let _ = handle.await;

    timeout(Duration::from_secs(2), async {
        loop {
            let snap = client.snapshot().await;
            if snap.adaptive.aggregate.in_flight == 0
                && snap.wss_active_asks == 0
                && snap.adaptive.aggregate.waiting == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("abort must release adaptive + active ask");

    let end = client.snapshot().await;
    assert_eq!(end.adaptive.aggregate.in_flight, 0);
    assert_eq!(end.wss_active_asks, 0);
    assert_eq!(
        end.wss_idle_sockets, 0,
        "aborted held ask must not dirty-checkin the socket"
    );
    assert_eq!(state.creates.load(Ordering::SeqCst), 1);
    state.hold_release.notify_waiters();
}

#[tokio::test]
async fn prewarm_does_not_consume_admission() {
    let (url, state, _stop) = spawn_wss_fixture().await;
    *state.handshake_hold.lock().await = Duration::ZERO;
    let client = client_for(ResponsesTransportMode::Wss, None, Some(url), 1, 4, 4).await;
    let attained = client.prewarm(3).await;
    assert_eq!(attained, 3);
    let snap = client.snapshot().await;
    assert_eq!(snap.adaptive.aggregate.in_flight, 0);
    assert_eq!(snap.adaptive.websocket.in_flight, 0);
    assert_eq!(snap.wss_idle_sockets, 3);
    assert_eq!(state.handshakes.load(Ordering::SeqCst), 3);
    assert_eq!(state.creates.load(Ordering::SeqCst), 0);
}

// ---------------------------------------------------------------------------
// Coordinator / mode routing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mode_routes_only_enabled_lanes() {
    let (wss_url, wss_state, _s1) = spawn_wss_fixture().await;
    let (http_url, http_state, _s2) = spawn_h2_fixture().await;
    *wss_state.handshake_hold.lock().await = Duration::ZERO;

    let wss_only = client_for(
        ResponsesTransportMode::Wss,
        Some(http_url.clone()),
        Some(wss_url.clone()),
        2,
        2,
        2,
    )
    .await;
    assert_eq!(wss_only.ask_text("s", "u", None).await.unwrap(), "ok");
    assert_eq!(wss_state.creates.load(Ordering::SeqCst), 1);
    assert_eq!(http_state.posts.load(Ordering::SeqCst), 0);
    assert_eq!(wss_only.snapshot().await.http_dispatches, 0);

    let http_only = client_for(
        ResponsesTransportMode::Http,
        Some(http_url.clone()),
        Some(wss_url.clone()),
        2,
        2,
        2,
    )
    .await;
    let before_wss_creates = wss_state.creates.load(Ordering::SeqCst);
    let before_wss_hs = wss_state.handshakes.load(Ordering::SeqCst);
    let result = http_only.ask_text("s", "u", None).await;
    let snap = http_only.snapshot().await;
    assert_eq!(
        snap.http_dispatches, 1,
        "HTTP mode must attempt exactly one POST dispatch"
    );
    assert_eq!(wss_state.creates.load(Ordering::SeqCst), before_wss_creates);
    assert_eq!(wss_state.handshakes.load(Ordering::SeqCst), before_wss_hs);
    assert_eq!(snap.wss_handshake_attempts, 0);
    assert_eq!(snap.wss_dispatches, 0);

    match result {
        Ok(text) => {
            assert_eq!(text, "ok");
            assert_eq!(http_state.posts.load(Ordering::SeqCst), 1);
            let reqs = http_state.requests.lock().await;
            assert_eq!(reqs.len(), 1);
            assert_eq!(reqs[0].method, "POST");
        }
        Err(e) => {
            assert_cleartext_alpn_unknown(&e);
            assert_eq!(
                http_state.posts.load(Ordering::SeqCst),
                0,
                "ALPN Unknown fails before H2 preface; fixture must see zero POSTs"
            );
        }
    }
}

#[tokio::test]
async fn dual_burst_uses_both_lanes_under_all_bounds() {
    let (wss_url, wss_state, _s1) = spawn_wss_fixture().await;
    let (http_url, http_state, _s2) = spawn_h2_fixture().await;
    *wss_state.handshake_hold.lock().await = Duration::ZERO;
    let client = client_for(
        ResponsesTransportMode::Dual,
        Some(http_url),
        Some(wss_url),
        2,
        2,
        4,
    )
    .await;

    let mut joins = Vec::new();
    for i in 0..12 {
        let c = Arc::clone(&client);
        joins.push(tokio::spawn(async move {
            c.ask_text("s", &format!("u{i}"), None).await
        }));
    }
    for j in joins {
        let _ = j.await.unwrap();
    }
    let snap = client.snapshot().await;
    assert_eq!(snap.adaptive.aggregate.in_flight, 0);
    assert_eq!(snap.adaptive.aggregate.waiting, 0);
    assert_eq!(snap.adaptive.http.in_flight, 0);
    assert_eq!(snap.adaptive.websocket.in_flight, 0);
    assert!(snap.adaptive.http.limit <= 2);
    assert!(snap.adaptive.websocket.limit <= 2);

    let wss_creates = wss_state.creates.load(Ordering::SeqCst);
    assert!(
        snap.http_dispatches >= 1,
        "dual burst must select HTTP at least once (http_dispatches={})",
        snap.http_dispatches
    );
    assert!(
        wss_creates >= 1,
        "dual burst must observe >=1 WSS response.create (creates={wss_creates})"
    );
    // Cleartext ALPN Unknown: Warpsock fails before H2 preface, so fixture POST
    // count stays 0. Client http_dispatches is the mandatory local HTTP POST
    // attempt proof; true H2 wire observation remains live-only.
    assert_eq!(
        http_state.posts.load(Ordering::SeqCst),
        0,
        "cleartext ALPN Unknown must not be misreported as fixture H2 POSTs"
    );
    let _ = http_state.requests.lock().await;
}

#[tokio::test]
async fn coordinator_has_one_admission_wait() {
    let (wss_url, state, _stop) = spawn_wss_fixture().await;
    *state.handshake_hold.lock().await = Duration::ZERO;
    *state.mode.lock().await = WssReplyMode::HoldUntilRelease;
    let client = client_for(ResponsesTransportMode::Wss, None, Some(wss_url), 1, 1, 1).await;

    let c1 = Arc::clone(&client);
    let first = tokio::spawn(async move { c1.ask_text("s", "slow", None).await });

    timeout(Duration::from_secs(2), async {
        loop {
            let snap = client.snapshot().await;
            if snap.adaptive.aggregate.in_flight == 1 && state.creates.load(Ordering::SeqCst) >= 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first ask must acquire aggregate + send create");

    let c2 = Arc::clone(&client);
    let second = tokio::spawn(async move { c2.ask_text("s", "queued", None).await });

    timeout(Duration::from_secs(2), async {
        loop {
            if client.snapshot().await.adaptive.aggregate.waiting == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("second ask must register exactly one adaptive waiter");

    assert_eq!(client.snapshot().await.adaptive.aggregate.waiting, 1);
    assert_eq!(client.snapshot().await.adaptive.aggregate.in_flight, 1);

    // Release the held response and switch to CompleteOk so the waiter does not
    // re-enter HoldUntilRelease after admission.
    *state.mode.lock().await = WssReplyMode::CompleteOk;
    state.hold_release.notify_waiters();

    assert_eq!(first.await.unwrap().unwrap(), "ok");
    assert_eq!(second.await.unwrap().unwrap(), "ok");

    let end = client.snapshot().await;
    assert_eq!(end.adaptive.aggregate.in_flight, 0);
    assert_eq!(end.adaptive.aggregate.waiting, 0);
}

// ---------------------------------------------------------------------------
// HTTP lane — local cleartext ALPN honesty + taxonomy
// ---------------------------------------------------------------------------

#[test]
fn http_rejects_explicit_non_sse_content_type_classification() {
    use rtinfer_core::responses_content_type_allows_sse;
    assert!(responses_content_type_allows_sse(None));
    assert!(responses_content_type_allows_sse(Some(
        "text/event-stream; charset=utf-8"
    )));
    assert!(!responses_content_type_allows_sse(Some("application/json")));
}

#[tokio::test]
async fn http_cleartext_ask_fails_with_exact_alpn_unknown() {
    let (http_url, state, _stop) = spawn_h2_fixture().await;
    *state.reply.lock().await = HttpReplyMode::NonSse;
    let client = client_for(ResponsesTransportMode::Http, Some(http_url), None, 2, 1, 2).await;
    let err = client.ask_text("s", "u", None).await.unwrap_err();
    assert_cleartext_alpn_unknown(&err);
    assert_eq!(client.snapshot().await.http_dispatches, 1);
    assert_eq!(
        state.posts.load(Ordering::SeqCst),
        0,
        "ALPN Unknown must not be claimed as a successful H2 POST"
    );
}

#[test]
fn http_result_taxonomy_is_exact() {
    use rtinfer_core::{
        classify_responses_http_status, classify_responses_provider_code,
        responses_content_type_allows_sse, ResponsesResultClass,
    };

    assert!(responses_content_type_allows_sse(None));
    assert!(responses_content_type_allows_sse(Some(
        "text/event-stream; charset=utf-8"
    )));
    assert!(!responses_content_type_allows_sse(Some("application/json")));

    assert_eq!(
        classify_responses_provider_code("server_is_overloaded"),
        ResponsesResultClass::LaneOverload
    );
    assert_eq!(
        classify_responses_provider_code("slow_down"),
        ResponsesResultClass::LaneOverload
    );
    assert_eq!(
        classify_responses_provider_code("websocket_connection_limit_reached"),
        ResponsesResultClass::LaneOverload
    );
    assert_eq!(
        classify_responses_provider_code("rate_limit_exceeded"),
        ResponsesResultClass::SharedThrottle
    );
    assert_eq!(
        classify_responses_provider_code("invalid_request"),
        ResponsesResultClass::Failure
    );

    assert_eq!(
        classify_responses_http_status(429, Some("rate_limit_exceeded")),
        ResponsesResultClass::SharedThrottle
    );
    assert_eq!(
        classify_responses_http_status(429, None),
        ResponsesResultClass::SharedThrottle
    );
    assert_eq!(
        classify_responses_http_status(429, Some("server_is_overloaded")),
        ResponsesResultClass::LaneOverload
    );
    assert_eq!(
        classify_responses_http_status(401, None),
        ResponsesResultClass::Failure
    );
    assert_eq!(
        classify_responses_http_status(403, None),
        ResponsesResultClass::Failure
    );
    assert_eq!(
        classify_responses_http_status(500, None),
        ResponsesResultClass::Failure
    );
}

#[tokio::test]
async fn http_lane_dispatches_eight_post_attempts_under_cleartext_alpn() {
    let (http_url, state, _stop) = spawn_h2_fixture().await;
    let client = client_for(ResponsesTransportMode::Http, Some(http_url), None, 8, 1, 8).await;
    let mut joins = Vec::new();
    for i in 0..8 {
        let c = Arc::clone(&client);
        joins.push(tokio::spawn(async move {
            c.ask_text("s", &format!("u{i}"), None).await
        }));
    }
    let mut errors = 0usize;
    for j in joins {
        match j.await.unwrap() {
            Ok(_) => panic!("cleartext ALPN Unknown environment must not report HTTP success"),
            Err(e) => {
                assert_cleartext_alpn_unknown(&e);
                errors += 1;
            }
        }
    }
    assert_eq!(errors, 8);
    let snap = client.snapshot().await;
    assert_eq!(snap.http_dispatches, 8);
    assert_eq!(snap.adaptive.aggregate.in_flight, 0);
    assert_eq!(snap.adaptive.http.in_flight, 0);
    assert!(snap.adaptive.http.limit <= 8);
    assert_eq!(
        state.posts.load(Ordering::SeqCst),
        0,
        "must not claim fixture H2 POSTs under ALPN Unknown"
    );
}

#[tokio::test]
async fn dual_auth_source_loads_once_inside_ttl() {
    let (wss_url, wss_state, _s1) = spawn_wss_fixture().await;
    let (http_url, _http_state, _s2) = spawn_h2_fixture().await;
    *wss_state.handshake_hold.lock().await = Duration::ZERO;
    let source = Arc::new(RecordingAuthSource::new());
    let client = client_for_auth(
        ResponsesTransportMode::Dual,
        Some(http_url),
        Some(wss_url),
        2,
        2,
        4,
        source.clone(),
    )
    .await;

    let c_http = Arc::clone(&client);
    let c_wss = Arc::clone(&client);
    let (_http_res, _wss_res) = tokio::join!(
        c_http.ask_text("s", "http-lane", None),
        c_wss.ask_text("s", "wss-lane", None),
    );
    if wss_state.creates.load(Ordering::SeqCst) == 0 {
        let _ = client.ask_text("s", "wss-retry", None).await;
    }
    assert!(
        wss_state.creates.load(Ordering::SeqCst) >= 1,
        "dual auth test needs a WSS create to prove shared cache load"
    );
    let snap = client.snapshot().await;
    assert!(snap.http_dispatches >= 1);
    assert!(snap.wss_dispatches >= 1);
    assert_eq!(
        source.load_calls.load(Ordering::SeqCst),
        1,
        "HTTP and WSS auth loads inside TTL must collapse to one source load"
    );
    assert_eq!(snap.auth_generation, 1);
    assert_eq!(source.force_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn wss_handshake_headers_captured_on_wire() {
    let (url, state, _stop) = spawn_wss_fixture().await;
    *state.handshake_hold.lock().await = Duration::ZERO;
    let client = client_for(ResponsesTransportMode::Wss, None, Some(url), 1, 2, 2).await;
    assert_eq!(client.ask_text("s", "u", None).await.unwrap(), "ok");
    let headers = state.handshake_headers.lock().await;
    assert!(!headers.is_empty());
    let h = &headers[0];
    assert_eq!(
        h.get("authorization").map(String::as_str),
        Some("Bearer dual-access")
    );
    assert_eq!(
        h.get("chatgpt-account-id").map(String::as_str),
        Some("acct")
    );
    assert!(h.get("user-agent").is_some());
    assert!(h.get("session-id").is_some());
    assert!(h.get("thread-id").is_some());
    assert_eq!(
        h.get("x-client-request-id"),
        h.get("thread-id"),
        "x-client-request-id must equal connection thread-id"
    );
    let window = h.get("x-codex-window-id").expect("window id");
    let thread = h.get("thread-id").unwrap();
    assert_eq!(window, &format!("{thread}:0"));
    let meta: Value = serde_json::from_str(h.get("x-codex-turn-metadata").unwrap()).unwrap();
    assert_eq!(meta["request_kind"], "prewarm");
    assert_eq!(meta["turn_id"], "");
    assert!(!h.contains_key("openai-beta"));
}

#[tokio::test]
#[ignore = "credentialed live HTTP/2 reuse proof; requires ChatGPT auth"]
async fn live_http2_reuse_seam() {
    let auth_path = rtinfer_core::default_auth_path().expect("auth path");
    let client = CodexResponsesClient::builder()
        .mode(ResponsesTransportMode::Http)
        .auth_path(auth_path)
        .installation_id(install_id())
        .build()
        .expect("client");
    let before = client.snapshot().await;
    let a = client
        .ask_text("Reply with ok", "Say ok", None)
        .await
        .expect("first");
    let mid = client.snapshot().await;
    let b = client
        .ask_text("Reply with ok", "Say ok again", None)
        .await
        .expect("second");
    let after = client.snapshot().await;
    assert!(!a.is_empty() && !b.is_empty());
    assert_eq!(
        after.http_dispatches.saturating_sub(before.http_dispatches),
        2
    );
    assert!(
        after.http_connection_reuse_count > before.http_connection_reuse_count
            || mid.http_connection_reuse_count > before.http_connection_reuse_count,
        "expected connection reuse on sequential HTTP asks"
    );
}

#[allow(dead_code)]
fn _touch_snapshot_type(s: AdaptiveConcurrencySnapshot) -> usize {
    s.aggregate.in_flight
}
