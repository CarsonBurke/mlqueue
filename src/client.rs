//! Unix-socket clients. Mutations always carry a durable idempotency key, so
//! transport retries are safe; subscriptions keep one socket without polling.

use std::io;
use std::os::unix::net::UnixStream;
use std::time::Duration;

use anyhow::Context;

use crate::domain::JobId;
use crate::paths::Paths;
use crate::protocol::{
    self, ErrorBody, JobUpdateView, Op, Reply, Request, Response, StatusView, read_frame_sync,
    write_frame_sync,
};

const CLIENT_RETRIES: u32 = 3;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("cannot reach mlqd at {socket}: {source} — start it with `mlqd`")]
    Unavailable { socket: String, source: io::Error },
    #[error("{}: {}", .0.code, .0.message)]
    Daemon(ErrorBody),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Send one operation and return the daemon's reply. Transport failures are
/// retried with the same request (and therefore the same idempotency key);
/// the daemon replays the original result for a repeated mutation.
pub fn request(
    paths: &Paths,
    op: Op,
    idempotency_key: Option<String>,
) -> Result<Reply, ClientError> {
    let request = Request {
        protocol_version: protocol::PROTOCOL_VERSION,
        request_id: uuid::Uuid::new_v4().to_string(),
        idempotency_key,
        op,
    };
    let mut last_io: Option<io::Error> = None;
    for attempt in 0..CLIENT_RETRIES {
        if attempt > 0 {
            std::thread::sleep(Duration::from_millis(100 << attempt));
        }
        match try_once(paths, &request) {
            Ok(response) => {
                if let Some(error) = response.error {
                    return Err(ClientError::Daemon(error));
                }
                return response
                    .reply
                    .context("daemon response carried neither reply nor error")
                    .map_err(ClientError::Other);
            }
            Err(err) => last_io = Some(err),
        }
    }
    Err(ClientError::Unavailable {
        socket: paths.socket().display().to_string(),
        source: last_io.expect("at least one attempt"),
    })
}

fn try_once(paths: &Paths, request: &Request) -> io::Result<Response> {
    let mut stream = UnixStream::connect(paths.socket())?;
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    stream.set_write_timeout(Some(Duration::from_secs(30)))?;
    let bytes = serde_json::to_vec(request)?;
    write_frame_sync(&mut stream, &bytes, protocol::DEFAULT_MAX_FRAME_BYTES)?;
    let frame = read_frame_sync(&mut stream, protocol::DEFAULT_MAX_FRAME_BYTES)?
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "daemon closed connection"))?;
    serde_json::from_slice(&frame)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))
}

/// Open a snapshot subscription and hand every snapshot to `on_snapshot` until
/// the daemon closes the stream (returning `Ok`). Unlike `request`, this never
/// retries: a reconnect would silently skip the changes made while it was
/// disconnected, which is the caller's decision, not the transport's.
pub fn subscribe(
    paths: &Paths,
    mut on_snapshot: impl FnMut(StatusView) -> anyhow::Result<()>,
) -> Result<(), ClientError> {
    let unavailable = |source: io::Error| ClientError::Unavailable {
        socket: paths.socket().display().to_string(),
        source,
    };
    let request = Request {
        protocol_version: protocol::PROTOCOL_VERSION,
        request_id: uuid::Uuid::new_v4().to_string(),
        idempotency_key: None,
        op: Op::Subscribe,
    };
    let mut stream = UnixStream::connect(paths.socket()).map_err(unavailable)?;
    // No read timeout: a quiet queue sends nothing for as long as it likes.
    stream
        .set_write_timeout(Some(Duration::from_secs(30)))
        .map_err(unavailable)?;
    let bytes = serde_json::to_vec(&request).expect("subscribe request serializes");
    write_frame_sync(&mut stream, &bytes, protocol::DEFAULT_MAX_FRAME_BYTES)
        .map_err(unavailable)?;
    loop {
        let frame = match read_frame_sync(&mut stream, protocol::DEFAULT_MAX_FRAME_BYTES) {
            Ok(Some(frame)) => frame,
            // Clean daemon shutdown.
            Ok(None) => return Ok(()),
            Err(err) => return Err(unavailable(err)),
        };
        let response: Response = serde_json::from_slice(&frame).map_err(|err| {
            ClientError::Other(anyhow::anyhow!("daemon sent invalid JSON: {err}"))
        })?;
        if let Some(error) = response.error {
            return Err(ClientError::Daemon(error));
        }
        match response.reply {
            Some(Reply::Status(view)) => on_snapshot(view).map_err(ClientError::Other)?,
            _ => return Err(ClientError::Other(unexpected("status"))),
        }
    }
}

/// One asynchronous, job-scoped subscription. Quiet streams have no read
/// timeout; EOF is an error, never evidence that the job completed.
pub struct JobSubscription {
    stream: tokio::net::UnixStream,
    socket: String,
    request_id: String,
    job: JobId,
    cursor: Option<i64>,
}

impl JobSubscription {
    pub async fn connect(
        paths: &Paths,
        job: JobId,
        after: Option<i64>,
    ) -> Result<Self, ClientError> {
        let socket = paths.socket().display().to_string();
        let unavailable = |source| ClientError::Unavailable {
            socket: socket.clone(),
            source,
        };
        let request = Request {
            protocol_version: protocol::PROTOCOL_VERSION,
            request_id: uuid::Uuid::new_v4().to_string(),
            idempotency_key: None,
            op: Op::SubscribeJob { job, after },
        };
        let bytes = serde_json::to_vec(&request).expect("subscribe request serializes");
        let stream = tokio::time::timeout(Duration::from_secs(30), async {
            let mut stream = tokio::net::UnixStream::connect(paths.socket()).await?;
            protocol::write_frame(&mut stream, &bytes, protocol::DEFAULT_MAX_FRAME_BYTES).await?;
            Ok::<_, io::Error>(stream)
        })
        .await
        .map_err(|_| {
            unavailable(io::Error::new(
                io::ErrorKind::TimedOut,
                "timed out opening job subscription",
            ))
        })?
        .map_err(unavailable)?;
        Ok(Self {
            stream,
            socket,
            request_id: request.request_id,
            job,
            cursor: after,
        })
    }

    /// Keep this future pinned across timer/select ticks: cancelling a frame
    /// read partway through would discard its partial framing state.
    pub async fn next(&mut self) -> Result<JobUpdateView, ClientError> {
        let frame = protocol::read_frame(&mut self.stream, protocol::DEFAULT_MAX_FRAME_BYTES)
            .await
            .and_then(|frame| {
                frame.ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "daemon closed job subscription",
                    )
                })
            })
            .map_err(|source| ClientError::Unavailable {
                socket: self.socket.clone(),
                source,
            })?;
        let response: Response = serde_json::from_slice(&frame).map_err(|err| {
            ClientError::Other(anyhow::anyhow!("daemon sent invalid JSON: {err}"))
        })?;
        if response.request_id != self.request_id {
            return Err(ClientError::Other(anyhow::anyhow!(
                "subscription response request id mismatch"
            )));
        }
        if let Some(error) = response.error {
            return Err(ClientError::Daemon(error));
        }
        let Some(Reply::JobUpdate(update)) = response.reply else {
            return Err(ClientError::Other(unexpected("job_update")));
        };
        let mut cursor = self.cursor.unwrap_or(0);
        if update.job.id != self.job
            || update.cursor < cursor
            || (update.has_more && update.events.is_empty())
        {
            return Err(ClientError::Other(anyhow::anyhow!(
                "invalid job subscription snapshot"
            )));
        }
        for event in &update.events {
            if event.job != Some(self.job) || event.id <= cursor || event.id > update.cursor {
                return Err(ClientError::Other(anyhow::anyhow!(
                    "invalid job subscription event cursor"
                )));
            }
            cursor = event.id;
        }
        self.cursor = Some(update.cursor);
        Ok(update)
    }
}

pub fn unexpected(expected: &str) -> anyhow::Error {
    anyhow::anyhow!("daemon returned an unexpected reply (expected {expected})")
}
