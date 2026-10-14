/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

//! `sbq-node`: the N-API addon of `@polyverse/switchboard`. It mirrors the C ABI of the sbq QUIC
//! shim (`sbq-vendor/include/sbq.h`, ABI version 4) function for function, calling the `sbq_*`
//! functions Rust to Rust. The provider and everything above it are TypeScript
//! (`src/node/quic/native-provider.ts`); nothing here knows about Switchboard.
//!
//! Conventions, the same for every function:
//! * handles, counts, codes and lengths are JS numbers (handles are small table ids);
//! * a result is the integer the C function returns: a number, or a BigInt for a result whose
//!   magnitude exceeds 2^53 - 1 (only the code of a reset or STOP_SENDING at or above
//!   2^53 - 16 does), so that no result is ever rounded;
//! * a buffer in is a `Uint8Array` the shim copies during the call; a buffer out is a
//!   `Uint8Array` the call fills, its length the capacity;
//! * `poll` writes events into a `Float64Array`, four numbers each: kind, handle, a, b;
//! * the doorbell is a thread-safe function the shim's notifier thread queues a call to without
//!   waiting, so `shutdown` may be called synchronously from JavaScript at any time. It holds the
//!   event loop open only while `setActive(ctx, true)`: the provider sets it while it has a live
//!   session or listener;
//! * an environment that ends (a worker thread that exits, the process) shuts down the contexts
//!   it created and left open.

#![allow(clippy::too_many_arguments)]

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::sync::{LazyLock, Mutex, MutexGuard};

use napi::bindgen_prelude::{i64n, Either, Float64ArraySlice, Function, Uint8ArraySlice, Unknown};
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi::{sys, Env, Result, Status};
use napi_derive::napi;
use sbq::SbqEvent;

/// A context's doorbell: a weak thread-safe function (it does not hold the event loop open until
/// referenced), called with the context id alone.
type Doorbell = ThreadsafeFunction<f64, Unknown<'static>, f64, Status, false, true, 0>;

struct Bell {
    doorbell: Doorbell,
    referenced: bool,
}

/// The doorbells of the live contexts, by context id, across every environment of the process.
static BELLS: LazyLock<Mutex<HashMap<u64, Bell>>> = LazyLock::new(Default::default);

thread_local! {
    /// The contexts this thread's environment created and has not shut down.
    static OWNED: RefCell<HashSet<u64>> = RefCell::new(HashSet::new());
    /// Whether this thread's environment has the cleanup hook that shuts them down.
    static HOOKED: Cell<bool> = const { Cell::new(false) };
    /// `poll`'s buffer.
    static EVENTS: RefCell<Vec<SbqEvent>> = const { RefCell::new(Vec::new()) };
}

const SBQ_ERR_ARG: i32 = sbq::SBQ_ERR_ARG as i32;
const MAX_SAFE_INTEGER: i64 = (1 << 53) - 1;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// A result of the C ABI, exactly: a number when it is one, else a BigInt.
type Int = Either<f64, i64n>;

fn int(v: i64) -> Int {
    if (-MAX_SAFE_INTEGER..=MAX_SAFE_INTEGER).contains(&v) {
        Either::A(v as f64)
    } else {
        Either::B(i64n(v))
    }
}

/// A handle, code or size from JavaScript (a negative or NaN one is 0, never a valid handle).
fn u(v: f64) -> u64 {
    v as u64
}

/// Called by the shim's notifier thread: queues a call of the context's doorbell on its JS
/// thread, without waiting for it.
extern "C" fn ring(ctx: u64) {
    if let Some(bell) = lock(&BELLS).get(&ctx) {
        bell.doorbell
            .call(ctx as f64, ThreadsafeFunctionCallMode::NonBlocking);
    }
}

/// `sbq_shutdown`, and forgetting the doorbell, which can ring no more.
fn shutdown_ctx(ctx: u64) -> i32 {
    let r = sbq::sbq_shutdown(ctx);
    let bell = lock(&BELLS).remove(&ctx);
    drop(bell);
    OWNED.with(|o| o.borrow_mut().remove(&ctx));
    r
}

/// `sbq_abi_version`.
#[napi]
pub fn abi_version() -> u32 {
    sbq::sbq_abi_version()
}

/// `sbq_init` with `doorbell` (called with the context id) as the context's doorbell; returns
/// the context id, or 0 (then `lastError`). The doorbell does not hold the event loop open
/// until `setActive(ctx, true)`.
#[napi]
pub fn init(
    env: Env,
    doorbell: Function<'_, f64, Unknown<'static>>,
    worker_threads: u32,
) -> Result<f64> {
    let doorbell: Doorbell = doorbell
        .build_threadsafe_function::<f64>()
        .weak::<true>()
        .callee_handled::<false>()
        .build()?;
    if !HOOKED.get() {
        env.add_env_cleanup_hook((), |()| {
            HOOKED.set(false);
            let owned: Vec<u64> = OWNED.with(|o| o.borrow_mut().drain().collect());
            for ctx in owned {
                shutdown_ctx(ctx);
            }
        })?;
        HOOKED.set(true);
    }
    // Held across sbq_init: a doorbell for the new context waits until it is known.
    let mut bells = lock(&BELLS);
    let ctx = sbq::sbq_init(ring, worker_threads);
    if ctx == 0 {
        return Ok(0.0);
    }
    bells.insert(
        ctx,
        Bell {
            doorbell,
            referenced: false,
        },
    );
    drop(bells);
    OWNED.with(|o| o.borrow_mut().insert(ctx));
    Ok(ctx as f64)
}

/// Whether the context's doorbell holds the event loop open (`napi_ref_threadsafe_function`
/// and `napi_unref_threadsafe_function`). Returns whether it does now.
#[napi]
pub fn set_active(env: Env, ctx: f64, active: bool) -> bool {
    let mut bells = lock(&BELLS);
    let Some(bell) = bells.get_mut(&u(ctx)) else {
        return false;
    };
    if bell.referenced != active && !bell.doorbell.aborted() {
        let raw = bell.doorbell.raw();
        let status = unsafe {
            if active {
                sys::napi_ref_threadsafe_function(env.raw(), raw)
            } else {
                sys::napi_unref_threadsafe_function(env.raw(), raw)
            }
        };
        if status == sys::Status::napi_ok {
            bell.referenced = active;
        }
    }
    bell.referenced
}

/// `sbq_shutdown`; the doorbell is released when it returns.
#[napi]
pub fn shutdown(ctx: f64) -> i32 {
    shutdown_ctx(u(ctx))
}

/// `sbq_poll` into `out`, four numbers per event (kind, handle, a, b), as many events as fit;
/// returns how many.
#[napi]
pub fn poll(ctx: f64, mut out: Float64ArraySlice<'_>) -> u32 {
    // SAFETY: JavaScript waits for the call; nothing else uses the buffer meanwhile.
    let out = unsafe { out.as_mut() };
    let cap = out.len() / 4;
    if cap == 0 {
        return 0;
    }
    EVENTS.with(|events| {
        let mut events = events.borrow_mut();
        events.resize(cap, SbqEvent::default());
        let n = unsafe { sbq::sbq_poll(u(ctx), events.as_mut_ptr(), cap) }.min(cap);
        for (i, e) in events[..n].iter().enumerate() {
            out[i * 4] = e.kind as f64;
            out[i * 4 + 1] = e.handle as f64;
            out[i * 4 + 2] = e.a as f64;
            out[i * 4 + 3] = e.b as f64;
        }
        n as u32
    })
}

/// `sbq_last_error` into `out`; returns the full length.
#[napi]
pub fn last_error(mut out: Uint8ArraySlice<'_>) -> Int {
    // SAFETY: JavaScript waits for the call; nothing else uses the buffer meanwhile.
    let out = unsafe { out.as_mut() };
    int(unsafe { sbq::sbq_last_error(out.as_mut_ptr(), out.len()) })
}

/// `sbq_sha256` of `data` into `out` (32 bytes).
#[napi]
pub fn sha256(data: &[u8], mut out: Uint8ArraySlice<'_>) -> i32 {
    // SAFETY: JavaScript waits for the call; nothing else uses the buffer meanwhile.
    let out = unsafe { out.as_mut() };
    if out.len() < 32 {
        return SBQ_ERR_ARG;
    }
    unsafe { sbq::sbq_sha256(data.as_ptr(), data.len(), out.as_mut_ptr()) }
}

/// `sbq_generate_self_signed`: the certificate into `cert_out`, the key into `key_out`, their
/// lengths into `lengths[0]` and `lengths[1]`.
#[napi]
pub fn generate_self_signed(
    subject: &[u8],
    validity_days: u32,
    mut cert_out: Uint8ArraySlice<'_>,
    mut key_out: Uint8ArraySlice<'_>,
    mut lengths: Float64ArraySlice<'_>,
) -> i32 {
    // SAFETY: JavaScript waits for the call; nothing else uses the buffer meanwhile.
    let cert_out = unsafe { cert_out.as_mut() };
    // SAFETY: JavaScript waits for the call; nothing else uses the buffer meanwhile.
    let key_out = unsafe { key_out.as_mut() };
    // SAFETY: JavaScript waits for the call; nothing else uses the buffer meanwhile.
    let lengths = unsafe { lengths.as_mut() };
    if lengths.len() < 2 {
        return SBQ_ERR_ARG;
    }
    let (mut cert_len, mut key_len) = (0usize, 0usize);
    let r = unsafe {
        sbq::sbq_generate_self_signed(
            subject.as_ptr(),
            subject.len(),
            validity_days,
            cert_out.as_mut_ptr(),
            cert_out.len(),
            &mut cert_len,
            key_out.as_mut_ptr(),
            key_out.len(),
            &mut key_len,
        )
    };
    lengths[0] = cert_len as f64;
    lengths[1] = key_len as f64;
    r
}

/// `sbq_client_config`; `pins` holds `pin_count` hashes of 32 bytes.
#[napi]
pub fn client_config(
    alpn: &[u8],
    verify: i32,
    pins: &[u8],
    pin_count: f64,
    max_bidi: f64,
    stream_window: f64,
    conn_window: f64,
    idle_ms: f64,
    keep_alive_ms: f64,
    handshake_ms: f64,
) -> Int {
    let count = u(pin_count) as usize;
    if count.checked_mul(32).is_none_or(|n| n > pins.len()) {
        return int(sbq::SBQ_ERR_ARG);
    }
    int(unsafe {
        sbq::sbq_client_config(
            alpn.as_ptr(),
            alpn.len(),
            verify,
            pins.as_ptr(),
            count,
            u(max_bidi),
            u(stream_window),
            u(conn_window),
            u(idle_ms),
            u(keep_alive_ms),
            u(handshake_ms),
        )
    })
}

/// `sbq_server_config`.
#[napi]
pub fn server_config(
    alpn: &[u8],
    cert: &[u8],
    key: &[u8],
    max_bidi: f64,
    stream_window: f64,
    conn_window: f64,
    idle_ms: f64,
    keep_alive_ms: f64,
    handshake_ms: f64,
) -> Int {
    int(unsafe {
        sbq::sbq_server_config(
            alpn.as_ptr(),
            alpn.len(),
            cert.as_ptr(),
            cert.len(),
            key.as_ptr(),
            key.len(),
            u(max_bidi),
            u(stream_window),
            u(conn_window),
            u(idle_ms),
            u(keep_alive_ms),
            u(handshake_ms),
        )
    })
}

/// `sbq_config_free`.
#[napi]
pub fn config_free(config: f64) -> i32 {
    sbq::sbq_config_free(u(config))
}

/// `sbq_listen`.
#[napi]
pub fn listen(ctx: f64, config: f64, host: &[u8], port: u32) -> Int {
    let Ok(port) = u16::try_from(port) else {
        return int(sbq::SBQ_ERR_ARG);
    };
    int(unsafe { sbq::sbq_listen(u(ctx), u(config), host.as_ptr(), host.len(), port) })
}

/// `sbq_listener_port`.
#[napi]
pub fn listener_port(listener: f64) -> i32 {
    sbq::sbq_listener_port(u(listener))
}

/// `sbq_listener_close`.
#[napi]
pub fn listener_close(listener: f64) -> i32 {
    sbq::sbq_listener_close(u(listener))
}

/// `sbq_connect`.
#[napi]
pub fn connect(ctx: f64, config: f64, host: &[u8], port: u32, server_name: &[u8]) -> Int {
    let Ok(port) = u16::try_from(port) else {
        return int(sbq::SBQ_ERR_ARG);
    };
    int(unsafe {
        sbq::sbq_connect(
            u(ctx),
            u(config),
            host.as_ptr(),
            host.len(),
            port,
            server_name.as_ptr(),
            server_name.len(),
        )
    })
}

/// `sbq_connect_wt`.
#[napi]
pub fn connect_wt(
    ctx: f64,
    config: f64,
    host: &[u8],
    port: u32,
    server_name: &[u8],
    authority: &[u8],
    path: &[u8],
    origin: &[u8],
    protocols: &[u8],
) -> Int {
    let Ok(port) = u16::try_from(port) else {
        return int(sbq::SBQ_ERR_ARG);
    };
    int(unsafe {
        sbq::sbq_connect_wt(
            u(ctx),
            u(config),
            host.as_ptr(),
            host.len(),
            port,
            server_name.as_ptr(),
            server_name.len(),
            authority.as_ptr(),
            authority.len(),
            path.as_ptr(),
            path.len(),
            origin.as_ptr(),
            origin.len(),
            protocols.as_ptr(),
            protocols.len(),
        )
    })
}

/// `sbq_wt_request`: the field into `out`; returns its full length.
#[napi]
pub fn wt_request(session: f64, field: i32, mut out: Uint8ArraySlice<'_>) -> Int {
    // SAFETY: JavaScript waits for the call; nothing else uses the buffer meanwhile.
    let out = unsafe { out.as_mut() };
    int(unsafe { sbq::sbq_wt_request(u(session), field, out.as_mut_ptr(), out.len()) })
}

/// `sbq_wt_accept`.
#[napi]
pub fn wt_accept(session: f64, protocol: &[u8]) -> i32 {
    unsafe { sbq::sbq_wt_accept(u(session), protocol.as_ptr(), protocol.len()) }
}

/// `sbq_wt_reject`.
#[napi]
pub fn wt_reject(session: f64, status: u32) -> i32 {
    sbq::sbq_wt_reject(u(session), status)
}

/// `sbq_wt_drain`.
#[napi]
pub fn wt_drain(session: f64) -> i32 {
    sbq::sbq_wt_drain(u(session))
}

/// `sbq_conn_close`.
#[napi]
pub fn conn_close(conn: f64, code: f64, reason: &[u8]) -> i32 {
    unsafe { sbq::sbq_conn_close(u(conn), u(code), reason.as_ptr(), reason.len()) }
}

/// `sbq_conn_release`.
#[napi]
pub fn conn_release(conn: f64) -> i32 {
    sbq::sbq_conn_release(u(conn))
}

/// `sbq_conn_close_reason` into `out`; returns the full length.
#[napi]
pub fn conn_close_reason(conn: f64, mut out: Uint8ArraySlice<'_>) -> Int {
    // SAFETY: JavaScript waits for the call; nothing else uses the buffer meanwhile.
    let out = unsafe { out.as_mut() };
    int(unsafe { sbq::sbq_conn_close_reason(u(conn), out.as_mut_ptr(), out.len()) })
}

/// `sbq_conn_exporter`: `out.length` bytes into `out`.
#[napi]
pub fn conn_exporter(conn: f64, label: &[u8], context: &[u8], mut out: Uint8ArraySlice<'_>) -> i32 {
    // SAFETY: JavaScript waits for the call; nothing else uses the buffer meanwhile.
    let out = unsafe { out.as_mut() };
    unsafe {
        sbq::sbq_conn_exporter(
            u(conn),
            label.as_ptr(),
            label.len(),
            context.as_ptr(),
            context.len(),
            out.as_mut_ptr(),
            out.len(),
        )
    }
}

/// `sbq_conn_peer_cert_sha256` into `out` (32 bytes).
#[napi]
pub fn conn_peer_cert_sha256(conn: f64, mut out: Uint8ArraySlice<'_>) -> i32 {
    // SAFETY: JavaScript waits for the call; nothing else uses the buffer meanwhile.
    let out = unsafe { out.as_mut() };
    if out.len() < 32 {
        return SBQ_ERR_ARG;
    }
    unsafe { sbq::sbq_conn_peer_cert_sha256(u(conn), out.as_mut_ptr()) }
}

/// `sbq_conn_protocol` into `out`; returns the full length.
#[napi]
pub fn conn_protocol(conn: f64, mut out: Uint8ArraySlice<'_>) -> Int {
    // SAFETY: JavaScript waits for the call; nothing else uses the buffer meanwhile.
    let out = unsafe { out.as_mut() };
    int(unsafe { sbq::sbq_conn_protocol(u(conn), out.as_mut_ptr(), out.len()) })
}

/// `sbq_conn_remote` into `out`; returns the full length.
#[napi]
pub fn conn_remote(conn: f64, mut out: Uint8ArraySlice<'_>) -> Int {
    // SAFETY: JavaScript waits for the call; nothing else uses the buffer meanwhile.
    let out = unsafe { out.as_mut() };
    int(unsafe { sbq::sbq_conn_remote(u(conn), out.as_mut_ptr(), out.len()) })
}

/// `sbq_conn_set_max_bidi`.
#[napi]
pub fn conn_set_max_bidi(conn: f64, count: f64) -> i32 {
    sbq::sbq_conn_set_max_bidi(u(conn), u(count))
}

/// `sbq_conn_stats` into `out`, the nine fields of `SbqStats` in their order.
#[napi]
pub fn conn_stats(conn: f64, mut out: Float64ArraySlice<'_>) -> i32 {
    // SAFETY: JavaScript waits for the call; nothing else uses the buffer meanwhile.
    let out = unsafe { out.as_mut() };
    if out.len() < 9 {
        return SBQ_ERR_ARG;
    }
    let mut s = sbq::SbqStats::default();
    let r = unsafe { sbq::sbq_conn_stats(u(conn), &mut s) };
    for (o, v) in out.iter_mut().zip([
        s.rtt_us,
        s.cwnd,
        s.congestion_events,
        s.lost_packets,
        s.sent_packets,
        s.udp_tx_bytes,
        s.udp_rx_bytes,
        s.udp_tx_datagrams,
        s.udp_rx_datagrams,
    ]) {
        *o = v as f64;
    }
    r
}

/// `sbq_stream_open`.
#[napi]
pub fn stream_open(conn: f64) -> Int {
    int(sbq::sbq_stream_open(u(conn)))
}

/// `sbq_stream_write` of `data`.
#[napi]
pub fn stream_write(stream: f64, data: &[u8]) -> Int {
    int(unsafe { sbq::sbq_stream_write(u(stream), data.as_ptr(), data.len()) })
}

/// `sbq_stream_read` into `buf`, at most `buf.length` bytes.
#[napi]
pub fn stream_read(stream: f64, mut buf: Uint8ArraySlice<'_>) -> Int {
    // SAFETY: JavaScript waits for the call; nothing else uses the buffer meanwhile.
    let buf = unsafe { buf.as_mut() };
    int(unsafe { sbq::sbq_stream_read(u(stream), buf.as_mut_ptr(), buf.len()) })
}

/// `sbq_stream_poll_reset`.
#[napi]
pub fn stream_poll_reset(stream: f64) -> Int {
    int(sbq::sbq_stream_poll_reset(u(stream)))
}

/// `sbq_stream_finish`.
#[napi]
pub fn stream_finish(stream: f64) -> Int {
    int(sbq::sbq_stream_finish(u(stream)))
}

/// `sbq_stream_reset`.
#[napi]
pub fn stream_reset(stream: f64, code: f64) -> Int {
    int(sbq::sbq_stream_reset(u(stream), u(code)))
}

/// `sbq_stream_stop`.
#[napi]
pub fn stream_stop(stream: f64, code: f64) -> Int {
    int(sbq::sbq_stream_stop(u(stream), u(code)))
}

/// `sbq_stream_set_priority`.
#[napi]
pub fn stream_set_priority(stream: f64, priority: i32) -> Int {
    int(sbq::sbq_stream_set_priority(u(stream), priority))
}

/// `sbq_stream_release`.
#[napi]
pub fn stream_release(stream: f64) -> i32 {
    sbq::sbq_stream_release(u(stream))
}
