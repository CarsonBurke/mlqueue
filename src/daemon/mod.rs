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
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Semaphore, mpsc, oneshot};

use crate::config::Config;
use crate::db;
use crate::paths::Paths;
use crate::protocol::{self, ErrorBody, Request, Response, error_codes};

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
                let tx = tx.clone();
                let config = config.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    if let Err(err) = handle_connection(stream, tx, config).await {
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

async fn handle_connection(
    stream: UnixStream,
    tx: mpsc::Sender<Msg>,
    config: Config,
) -> Result<()> {
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
            Ok(request) => {
                let (reply_tx, reply_rx) = oneshot::channel();
                if tx.send(Msg::Request { request: Box::new(request), reply: reply_tx }).await.is_err() {
                    break;
                }
                match reply_rx.await {
                    Ok(response) => response,
                    Err(_) => break,
                }
            }
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
