//! Backpressure on an observer's output must not disable cancellation.

use std::io::Read;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::net::UnixListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use mlqueue::protocol::{self, Request};
use serde_json::json;

struct Follower(Option<Child>);

impl Drop for Follower {
    fn drop(&mut self) {
        if let Some(child) = &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn wait_until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !condition() {
        assert!(
            Instant::now() < deadline,
            "follower did not respond before the deadline"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn daemon(directory: &std::path::Path, terminal: bool) -> std::thread::JoinHandle<()> {
    let listener = UnixListener::bind(directory.join("mlqd.sock")).unwrap();
    std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let bytes = protocol::read_frame_sync(&mut stream, protocol::DEFAULT_MAX_FRAME_BYTES)
            .unwrap()
            .unwrap();
        let request: Request = serde_json::from_slice(&bytes).unwrap();
        let job = json!({
            "id": 1, "name": "output test", "state": if terminal { "succeeded" } else { "running" },
            "maxParallelRuns": 1, "cwd": "/", "args": ["true"], "maxAttempts": 300,
            "retryDelayMs": 0, "attemptCount": 1, "createdAt": 0, "updatedAt": 0, "attempts": [],
        });
        for page in 0..if terminal { 1 } else { 2 } {
            let events = if page == 0 {
                Vec::new()
            } else {
                (1..=256)
                    .map(|id| {
                        json!({
                            "id": id, "timestamp": 0, "job": 1, "attemptNumber": id,
                            "eventType": "attempt_running",
                        })
                    })
                    .collect()
            };
            let bytes = serde_json::to_vec(&json!({
                "request_id": request.request_id,
                "reply": { "type": "job_update", "job": job, "cursor": if page == 0 { 0 } else { 256 },
                    "events": events, "hasMore": false },
            })).unwrap();
            protocol::write_frame_sync(&mut stream, &bytes, protocol::DEFAULT_MAX_FRAME_BYTES)
                .unwrap();
        }
    })
}

#[test]
fn stalled_output_still_obeys_timeout_and_sigterm_without_changing_shared_flags() {
    for terminate in [false, true] {
        let directory = tempfile::Builder::new()
            .prefix("mlq-output-")
            .tempdir_in("/tmp")
            .unwrap();
        let server = daemon(directory.path(), false);
        let mut fds = [-1; 2];
        assert_eq!(unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
        let reader = unsafe { OwnedFd::from_raw_fd(fds[0]) };
        let writer = unsafe { OwnedFd::from_raw_fd(fds[1]) };
        assert_eq!(
            unsafe { libc::fcntl(writer.as_raw_fd(), libc::F_SETPIPE_SZ, 4096) },
            4096
        );
        let original_flags = unsafe { libc::fcntl(writer.as_raw_fd(), libc::F_GETFL) };
        let child = Command::new(env!("CARGO_BIN_EXE_mlq"))
            .args([
                "follow",
                "1",
                "--json",
                "--events",
                "started",
                "--timeout",
                if terminate { "30s" } else { "200ms" },
            ])
            .env("MLQUEUE_RUNTIME_DIR", directory.path())
            .stdout(Stdio::from(writer.try_clone().unwrap()))
            .stderr(Stdio::from(writer.try_clone().unwrap()))
            .spawn()
            .unwrap();
        let mut follower = Follower(Some(child));
        if terminate {
            wait_until(|| {
                let mut available = 0;
                assert_eq!(
                    unsafe { libc::ioctl(reader.as_raw_fd(), libc::FIONREAD, &mut available) },
                    0
                );
                if available >= 4000 {
                    assert_eq!(
                        unsafe { libc::fcntl(writer.as_raw_fd(), libc::F_GETFL) },
                        original_flags
                    );
                    true
                } else {
                    false
                }
            });
            assert_eq!(
                unsafe { libc::kill(follower.0.as_ref().unwrap().id() as i32, libc::SIGTERM) },
                0
            );
        }
        wait_until(|| follower.0.as_mut().unwrap().try_wait().unwrap().is_some());
        let output = follower.0.take().unwrap().wait_with_output().unwrap();
        assert_eq!(
            output.status.code(),
            Some(if terminate { 143 } else { 124 }),
            "{output:?}"
        );
        assert_eq!(
            unsafe { libc::fcntl(writer.as_raw_fd(), libc::F_GETFL) },
            original_flags
        );
        server.join().unwrap();
        // Keep the reader alive and unread until the follower has exited.
        drop(reader);
    }
}

#[test]
fn final_output_can_be_redirected_to_a_regular_file() {
    let directory = tempfile::Builder::new()
        .prefix("mlq-output-")
        .tempdir_in("/tmp")
        .unwrap();
    let server = daemon(directory.path(), true);
    let path = directory.path().join("output.json");
    let output = Command::new(env!("CARGO_BIN_EXE_mlq"))
        .args(["follow", "1", "--json"])
        .env("MLQUEUE_RUNTIME_DIR", directory.path())
        .stdout(std::fs::File::create(&path).unwrap())
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let event: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    assert_eq!(event["event"], "finished");
    assert_eq!(event["exitCode"], 0);
    server.join().unwrap();
}

#[test]
fn terminal_output_obeys_the_deadline_with_both_outputs_in_a_full_pipe() {
    let directory = tempfile::Builder::new()
        .prefix("mlq-output-")
        .tempdir_in("/tmp")
        .unwrap();
    let server = daemon(directory.path(), true);
    let mut fds = [-1; 2];
    assert_eq!(unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
    let reader = unsafe { OwnedFd::from_raw_fd(fds[0]) };
    let writer = unsafe { OwnedFd::from_raw_fd(fds[1]) };
    let capacity = unsafe { libc::fcntl(writer.as_raw_fd(), libc::F_GETPIPE_SZ) };
    let fill = vec![b'x'; capacity as usize];
    assert_eq!(
        unsafe { libc::write(writer.as_raw_fd(), fill.as_ptr().cast(), fill.len()) },
        capacity as isize
    );
    let child = Command::new(env!("CARGO_BIN_EXE_mlq"))
        .args(["follow", "1", "--json", "--timeout", "100ms"])
        .env("MLQUEUE_RUNTIME_DIR", directory.path())
        .stdout(Stdio::from(writer.try_clone().unwrap()))
        .stderr(Stdio::from(writer))
        .spawn()
        .unwrap();
    let mut follower = Follower(Some(child));
    wait_until(|| follower.0.as_mut().unwrap().try_wait().unwrap().is_some());
    assert_eq!(follower.0.take().unwrap().wait().unwrap().code(), Some(124));
    server.join().unwrap();
    drop(reader);
}

#[test]
fn already_closed_output_exits_with_the_broken_pipe_status() {
    let directory = tempfile::Builder::new()
        .prefix("mlq-output-")
        .tempdir_in("/tmp")
        .unwrap();
    let server = daemon(directory.path(), true);
    let mut fds = [-1; 2];
    assert_eq!(unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
    drop(unsafe { OwnedFd::from_raw_fd(fds[0]) });
    let writer = unsafe { OwnedFd::from_raw_fd(fds[1]) };
    let output = Command::new(env!("CARGO_BIN_EXE_mlq"))
        .args(["follow", "1", "--timeout", "100ms"])
        .env("MLQUEUE_RUNTIME_DIR", directory.path())
        .stdout(Stdio::from(writer))
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(141), "{output:?}");
    server.join().unwrap();
}

#[test]
fn stalled_socket_output_is_cancellable_without_changing_shared_flags() {
    let directory = tempfile::Builder::new()
        .prefix("mlq-output-")
        .tempdir_in("/tmp")
        .unwrap();
    let server = daemon(directory.path(), true);
    let (reader, writer) = std::os::unix::net::UnixStream::pair().unwrap();
    let original_flags = unsafe { libc::fcntl(writer.as_raw_fd(), libc::F_GETFL) };
    let fill = [b'x'; 4096];
    loop {
        let count = unsafe {
            libc::send(
                writer.as_raw_fd(),
                fill.as_ptr().cast(),
                fill.len(),
                libc::MSG_DONTWAIT,
            )
        };
        if count == -1 {
            assert_eq!(
                std::io::Error::last_os_error().kind(),
                std::io::ErrorKind::WouldBlock
            );
            break;
        }
    }
    let stdout: OwnedFd = writer.try_clone().unwrap().into();
    let stderr: OwnedFd = writer.try_clone().unwrap().into();
    let child = Command::new(env!("CARGO_BIN_EXE_mlq"))
        .args(["follow", "1", "--json", "--timeout", "100ms"])
        .env("MLQUEUE_RUNTIME_DIR", directory.path())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .unwrap();
    let mut follower = Follower(Some(child));
    wait_until(|| {
        assert_eq!(
            unsafe { libc::fcntl(writer.as_raw_fd(), libc::F_GETFL) },
            original_flags
        );
        follower.0.as_mut().unwrap().try_wait().unwrap().is_some()
    });
    assert_eq!(follower.0.take().unwrap().wait().unwrap().code(), Some(124));
    server.join().unwrap();
    drop(reader);
}

#[test]
fn inherited_writable_output_does_not_require_permission_to_reopen_it() {
    let directory = tempfile::Builder::new()
        .prefix("mlq-output-")
        .tempdir_in("/tmp")
        .unwrap();
    let server = daemon(directory.path(), true);
    let mut fds = [-1; 2];
    assert_eq!(unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
    let reader = unsafe { OwnedFd::from_raw_fd(fds[0]) };
    let writer = unsafe { OwnedFd::from_raw_fd(fds[1]) };
    assert_eq!(unsafe { libc::fchmod(writer.as_raw_fd(), 0) }, 0);
    let output = Command::new(env!("CARGO_BIN_EXE_mlq"))
        .args(["follow", "1", "--json"])
        .env("MLQUEUE_RUNTIME_DIR", directory.path())
        .stdout(Stdio::from(writer))
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let mut text = String::new();
    std::fs::File::from(reader)
        .read_to_string(&mut text)
        .unwrap();
    let event: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(event["event"], "finished");
    server.join().unwrap();
}
