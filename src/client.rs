//! Synchronous Unix-socket client used by the CLI. Mutations always carry a
//! durable idempotency key, so transport retries are safe.

use std::io;
use std::os::unix::net::UnixStream;
use std::time::Duration;

use anyhow::Context;

use crate::paths::Paths;
use crate::protocol::{
    self, ErrorBody, Op, Reply, Request, Response, read_frame_sync, write_frame_sync,
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
pub fn request(paths: &Paths, op: Op, idempotency_key: Option<String>) -> Result<Reply, ClientError> {
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

pub fn unexpected(expected: &str) -> anyhow::Error {
    anyhow::anyhow!("daemon returned an unexpected reply (expected {expected})")
}
