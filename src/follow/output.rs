//! Process-scoped output workers keep the observer responsive to cancellation.

use std::io::{self, Write};
use std::os::fd::{FromRawFd, RawFd};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

pub(super) struct EventWriter {
    sender: mpsc::Sender<WriteRequest>,
}

struct WriteRequest {
    bytes: Vec<u8>,
    complete: oneshot::Sender<io::Result<()>>,
}

impl EventWriter {
    pub(super) fn stdout() -> io::Result<Self> {
        Self::descriptor(libc::STDOUT_FILENO)
    }

    fn descriptor(descriptor: RawFd) -> io::Result<Self> {
        // Preserve inherited descriptor permissions, offsets and status flags.
        let fd = unsafe { libc::fcntl(descriptor, libc::F_DUPFD_CLOEXEC, 0) };
        if fd == -1 {
            return Err(io::Error::last_os_error());
        }
        let mut file = unsafe { std::fs::File::from_raw_fd(fd) };
        let (sender, mut receiver) = mpsc::channel::<WriteRequest>(1);
        // A private thread, rather than Tokio's blocking pool, is intentional:
        // runtime shutdown must not join a write stuck behind an idle reader.
        // Dropping the sender ends an idle worker. A blocked worker is detached
        // until this CLI process exits; its active and queued buffers are bounded.
        let _worker = std::thread::Builder::new()
            .name("follow-output".into())
            .spawn(move || {
                while let Some(request) = receiver.blocking_recv() {
                    let result = file.write_all(&request.bytes);
                    if request.complete.send(result).is_err() {
                        break;
                    }
                }
            })?;
        Ok(Self { sender })
    }

    pub(super) async fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        let (complete, completed) = oneshot::channel();
        self.sender
            .send(WriteRequest {
                bytes: bytes.to_vec(),
                complete,
            })
            .await
            .map_err(|_| io::Error::other("follow output worker stopped"))?;
        completed
            .await
            .map_err(|_| io::Error::other("follow output worker stopped"))?
    }
}

/// stderr may share stdout's stalled pipe. Deliver normal diagnostics, but bound
/// the wait so reporting a timeout or hook failure cannot disable cancellation.
pub(super) async fn diagnostic(message: &str) {
    if let Ok(mut writer) = EventWriter::descriptor(libc::STDERR_FILENO) {
        let _ = tokio::time::timeout(
            Duration::from_millis(50),
            writer.write_all(message.as_bytes()),
        )
        .await;
    }
}
