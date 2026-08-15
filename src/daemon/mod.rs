//! Daemon assembly: singleton lock, Unix socket server with peer-credential
//! checks, the coordinator thread, and the reconcile tick.

mod coordinator;
mod views;

use std::fs;
use std::io::Write;
use std::os::unix::io::AsRawFd;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::io::AsyncReadExt;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};

use crate::config::Config;
use crate::db;
use crate::paths::Paths;
use crate::protocol::{self, ErrorBody, Op, Request, Response, error_codes};

use coordinator::{Coordinator, Msg};

pub fn run(paths: Paths) -> Result<()> {
    // Workers and artifacts must never be group/world readable, and runner
    // children must not inherit daemon descriptors (Rust opens everything
    // close-on-exec; umask covers created files and the socket).
    unsafe {
        libc::umask(0o077);
        // Runners are detached children; auto-reap them.
        libc::signal(libc::SIGCHLD, libc::SIG_IGN);
    }
    let config = Config::load(&paths.config_file)?;
    paths.ensure_dirs()?;

    // Advisory singleton lock in the stable state directory, acquired before
    // touching the socket. Held (leaked) for the daemon's whole life.
    let mut lock_file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(paths.daemon_lock())
        .with_context(|| format!("opening {}", paths.daemon_lock().display()))?;
    let rc = unsafe { libc::flock(lock_file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc != 0 {
        bail!(
            "another mlqd instance holds {} — only one daemon may run",
            paths.daemon_lock().display()
        );
    }
    let _ = lock_file.set_len(0);
    let _ = writeln!(lock_file, "{}", std::process::id());

    let conn = db::open(&paths.db())?;
    tracing::info!(
        "mlqd {} starting (db {}, socket {})",
        env!("CARGO_PKG_VERSION"),
        paths.db().display(),
        paths.socket().display()
    );

    // We hold the singleton lock, so any existing socket file is confirmed
    // stale; a socket path alone is never proof of a live daemon.
    match fs::remove_file(paths.socket()) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err).context("removing stale socket"),
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    runtime.block_on(serve(paths, config, conn))?;
    // The lock file descriptor is dropped here, on clean shutdown only.
    drop(lock_file);
    Ok(())
}

async fn serve(paths: Paths, config: Config, conn: rusqlite::Connection) -> Result<()> {
    let listener = UnixListener::bind(paths.socket())
        .with_context(|| format!("binding {}", paths.socket().display()))?;

    let (tx, rx) = mpsc::channel::<Msg>(1024);
    let coordinator = Coordinator::new(conn, config.clone(), paths.clone())?;
    // Handed to every connection so a subscription learns about committed
    // changes without polling; the sender lives on the coordinator thread and
    // its drop is what ends the streams at shutdown.
    let revisions = coordinator.revisions();
    let coordinator_thread = std::thread::Builder::new()
        .name("coordinator".into())
        .spawn(move || coordinator.run(rx))?;

    let tick_tx = tx.clone();
    let tick_interval = Duration::from_millis(config.tick_interval_ms.max(20));
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tick_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            // A full channel already has work queued; dropping a tick is fine.
            let _ = tick_tx.try_send(Msg::Tick);
        }
    });

    let connections = Arc::new(Semaphore::new(config.max_connections));
    let serving = Serving {
        tx: tx.clone(),
        revisions,
        // Streams outlive a request/response turn, so they get a strict share of
        // the connection budget: a crowd of subscribers can never starve
        // control-plane clients like `mlq submit`.
        subscriptions: Arc::new(Semaphore::new(max_subscriptions(&config))),
        config: config.clone(),
    };
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(pair) => pair,
                    Err(err) => {
                        tracing::warn!("accept failed: {err}");
                        continue;
                    }
                };
                let Ok(permit) = connections.clone().try_acquire_owned() else {
                    // Over the connection limit: drop the connection; clients
                    // retry with backoff.
                    continue;
                };
                let serving = serving.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    if let Err(err) = handle_connection(stream, serving).await {
                        tracing::debug!("connection ended: {err:#}");
                    }
                });
            }
            _ = sigterm.recv() => break,
            _ = sigint.recv() => break,
        }
    }

    tracing::info!("shutting down (workers keep running; restart adopts them)");
    let _ = tx.send(Msg::Shutdown).await;
    drop(tx);
    let _ = tokio::task::spawn_blocking(move || coordinator_thread.join()).await;
    let _ = fs::remove_file(paths.socket());
    Ok(())
}

/// Everything a connection task needs, cloned once per accepted connection.
#[derive(Clone)]
struct Serving {
    tx: mpsc::Sender<Msg>,
    config: Config,
    /// Wakes streaming connections when committed state changes.
    revisions: watch::Receiver<i64>,
    /// Streams hold their slot for as long as the peer stays connected, so they
    /// are budgeted separately from ordinary request/response connections.
    subscriptions: Arc<Semaphore>,
}

/// Streams get a strict quarter of the connection budget, leaving headroom for
/// request/response clients whatever a crowd of subscribers does.
fn max_subscriptions(config: &Config) -> usize {
    (config.max_connections / 4).max(1)
}

async fn handle_connection(stream: UnixStream, serving: Serving) -> Result<()> {
    let Serving { tx, config, revisions, subscriptions } = serving;
    // Reject a different UID before parsing anything.
    let cred = stream.peer_cred().context("reading peer credentials")?;
    let euid = unsafe { libc::geteuid() };
    if cred.uid() != euid {
        bail!("rejecting connection from uid {} (daemon runs as {euid})", cred.uid());
    }

    let idle = Duration::from_millis(config.idle_timeout_ms.max(1_000));
    let (mut reader, mut writer) = stream.into_split();
    loop {
        let frame = match tokio::time::timeout(
            idle,
            protocol::read_frame(&mut reader, config.max_frame_bytes),
        )
        .await
        {
            Err(_) => break, // idle timeout
            Ok(Ok(None)) => break,
            Ok(Ok(Some(frame))) => frame,
            Ok(Err(err)) => {
                // Oversized or broken frame: report once, then close — the
                // stream is no longer in sync.
                let response = Response {
                    request_id: String::new(),
                    reply: None,
                    error: Some(ErrorBody {
                        code: error_codes::MALFORMED_REQUEST.to_string(),
                        message: err.to_string(),
                    }),
                };
                let bytes = serde_json::to_vec(&response)?;
                let _ = protocol::write_frame(&mut writer, &bytes, config.max_frame_bytes).await;
                break;
            }
        };

        let response = match serde_json::from_slice::<Request>(&frame) {
            Ok(request) if matches!(request.op, Op::Subscribe) => {
                match subscriptions.clone().try_acquire_owned() {
                    // One subscribe ends the request/response loop: the rest of
                    // this connection is a snapshot stream, with no idle timeout.
                    Ok(permit) => {
                        return stream_snapshots(
                            reader, writer, tx, config, request, revisions, permit,
                        )
                        .await;
                    }
                    // Refuse in words: a silently dropped connection is
                    // indistinguishable from a dead daemon.
                    Err(_) => Response {
                        request_id: request.request_id,
                        reply: None,
                        error: Some(ErrorBody {
                            code: error_codes::TOO_MANY_SUBSCRIPTIONS.to_string(),
                            message: format!(
                                "all {} subscription slots are in use; retry shortly",
                                max_subscriptions(&config)
                            ),
                        }),
                    },
                }
            }
            Ok(request) => match ask(&tx, request).await {
                Some(response) => response,
                None => break, // coordinator gone: shutting down
            },
            Err(err) => Response {
                request_id: String::new(),
                reply: None,
                error: Some(ErrorBody {
                    code: error_codes::MALFORMED_REQUEST.to_string(),
                    message: format!("invalid request JSON: {err}"),
                }),
            },
        };
        let bytes = serde_json::to_vec(&response)?;
        protocol::write_frame(&mut writer, &bytes, config.max_frame_bytes).await?;
    }
    Ok(())
}

/// Hand one request to the coordinator and wait for its response. `None` means
/// the coordinator is gone (shutdown), which ends the connection.
async fn ask(tx: &mpsc::Sender<Msg>, request: Request) -> Option<Response> {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(Msg::Request { request: Box::new(request), reply: reply_tx }).await.ok()?;
    reply_rx.await.ok()
}

/// Stream complete `Status` snapshots for the life of the connection: one
/// immediately, then one per committed change. Every frame is a full snapshot,
/// so a consumer never replays events.
async fn stream_snapshots(
    mut reader: OwnedReadHalf,
    mut writer: OwnedWriteHalf,
    tx: mpsc::Sender<Msg>,
    config: Config,
    request: Request,
    mut revisions: watch::Receiver<i64>,
    // Released when the stream ends, which is what frees the slot for the next
    // subscriber.
    _permit: OwnedSemaphorePermit,
) -> Result<()> {
    // Claim the current revision before taking the first snapshot: a change
    // committed in between then re-arms `changed()` instead of being lost.
    revisions.mark_unchanged();

    // The read timeout is deliberately absent from this loop — an idle queue is
    // the normal case for a subscriber. A write that stalls this long, however,
    // means the peer stopped reading without closing, and its slot must not be
    // held forever.
    let write_timeout = Duration::from_millis(config.idle_timeout_ms.max(1_000));
    let mut sent: Option<Vec<u8>> = None;
    let mut probe = [0u8; 64];
    loop {
        // Refetch through the coordinator, exactly like `Op::Status`.
        let Some(response) = ask(&tx, request.clone()).await else { return Ok(()) };
        let failed = response.error.is_some();
        let bytes = serde_json::to_vec(&response)?;
        // `watch` already collapses a burst into one wake-up; identical
        // snapshots (a change invisible to `Status`) are dropped here.
        if sent.as_deref() != Some(bytes.as_slice()) {
            tokio::time::timeout(
                write_timeout,
                protocol::write_frame(&mut writer, &bytes, config.max_frame_bytes),
            )
            .await
            .map_err(|_| anyhow::anyhow!("subscriber stopped reading"))??;
            sent = Some(bytes);
        }
        // A rejected subscribe (unsupported protocol version) is reported once
        // and never becomes a stream.
        if failed {
            return Ok(());
        }

        tokio::select! {
            // Err means the coordinator dropped its sender: the daemon is
            // shutting down, so close the stream cleanly.
            changed = revisions.changed() => if changed.is_err() { return Ok(()) },
            // Nothing further is read from a subscription; this only detects
            // the peer going away (or desynchronising the stream by sending).
            read = reader.read(&mut probe) => {
                if let Ok(count) = read
                    && count > 0
                {
                    tracing::debug!("subscriber sent {count} unexpected bytes; closing");
                }
                return Ok(());
            }
        }
    }
}
