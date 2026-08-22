//! End-to-end tests: a real daemon, real runners, and the real CLI, all
//! isolated in a per-test state directory.

use std::path::PathBuf;
use std::os::unix::fs::PermissionsExt;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

fn wait_for(what: &str, timeout: Duration, mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    loop {
        if check() {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

struct TestQueue {
    dir: tempfile::TempDir,
    daemon: Option<Child>,
}

struct KillOnDrop(Option<Child>);

impl KillOnDrop {
    fn stop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        self.stop();
    }
}

impl TestQueue {
    fn new() -> Self {
        Self::with_runner_poll_ms(20)
    }

    fn with_runner_poll_ms(runner_poll_ms: u64) -> Self {
        Self::with_config(runner_poll_ms, "")
    }

    /// `extra` appends config keys for one test; TOML rejects duplicates, so
    /// they must not already be set below.
    fn with_config(runner_poll_ms: u64, extra: &str) -> Self {
        // Keep the tempdir shallow: the Unix socket path must stay under the
        // 108-byte sockaddr_un limit regardless of TMPDIR.
        let dir = tempfile::Builder::new().prefix("mlq-e2e").tempdir_in("/tmp").unwrap();
        std::fs::write(
            dir.path().join("config.toml"),
            format!(
                "version = 1\n\
                 tick_interval_ms = 40\n\
                 runner_poll_ms = {runner_poll_ms}\n\
                 cancel_grace_ms = 800\n\
                 runner_identity_grace_ms = 5000\n\
                 idle_timeout_ms = 200\n\
                 {extra}"
            ),
        )
        .unwrap();
        let mut queue = Self { dir, daemon: None };
        queue.start_daemon();
        queue
    }

    fn socket(&self) -> PathBuf {
        self.dir.path().join("runtime/mlqd.sock")
    }

    fn state_dir(&self) -> PathBuf {
        self.dir.path().join("state")
    }

    fn apply_env(&self, cmd: &mut Command) {
        cmd.env("MLQUEUE_STATE_DIR", self.state_dir())
            .env("MLQUEUE_RUNTIME_DIR", self.dir.path().join("runtime"))
            .env("MLQUEUE_CONFIG_FILE", self.dir.path().join("config.toml"));
    }

    fn spawn_daemon(&self) -> Child {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_mlqd"));
        self.apply_env(&mut cmd);
        cmd.stdout(Stdio::null()).stderr(Stdio::null());
        cmd.spawn().unwrap()
    }

    fn start_daemon(&mut self) {
        assert!(self.daemon.is_none());
        self.daemon = Some(self.spawn_daemon());
        wait_for("daemon to accept connections", Duration::from_secs(10), || {
            self.try_cli(&["daemon", "status"]).status.success()
        });
    }

    /// Simulate a crash: SIGKILL, so nothing shuts down cleanly.
    fn kill_daemon(&mut self) {
        let mut child = self.daemon.take().expect("daemon running");
        child.kill().unwrap();
        child.wait().unwrap();
    }

    /// Graceful shutdown: SIGTERM, then wait for the daemon to exit.
    fn stop_daemon(&mut self) {
        let mut child = self.daemon.take().expect("daemon running");
        assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGTERM) }, 0);
        child.wait().unwrap();
    }

    fn try_cli(&self, args: &[&str]) -> Output {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_mlq"));
        self.apply_env(&mut cmd);
        cmd.args(args);
        cmd.output().unwrap()
    }

    fn cli(&self, args: &[&str]) -> String {
        let output = self.try_cli(args);
        assert!(
            output.status.success(),
            "mlq {args:?} failed:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).into_owned()
    }

    fn cli_json(&self, args: &[&str]) -> serde_json::Value {
        serde_json::from_str(&self.cli(args)).expect("CLI produced valid JSON")
    }

    /// Submit a command; extra flags first, command after `--`.
    fn submit(&self, flags: &[&str], command: &[&str]) -> i64 {
        let mut args = vec!["submit", "--json"];
        args.extend_from_slice(flags);
        args.push("--cwd");
        let cwd = self.dir.path().to_str().unwrap();
        args.push(cwd);
        args.push("--");
        args.extend_from_slice(command);
        self.cli_json(&args)["id"].as_i64().expect("submit returns a job id")
    }

    fn job(&self, id: i64) -> serde_json::Value {
        self.cli_json(&["show", &id.to_string(), "--json"])
    }

    fn job_state(&self, id: i64) -> String {
        self.job(id)["state"].as_str().unwrap().to_string()
    }

    fn wait_state(&self, id: i64, state: &str, timeout: Duration) {
        wait_for(&format!("job {id} to be {state}"), timeout, || self.job_state(id) == state);
    }

    fn status(&self) -> serde_json::Value {
        self.cli_json(&["status", "--json"])
    }

    /// Start `mlq subscribe` and read its NDJSON stream on a helper thread, so
    /// a test can wait for the next snapshot with a deadline.
    fn subscribe(&self) -> Subscriber {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_mlq"));
        self.apply_env(&mut cmd);
        cmd.arg("subscribe").stdout(Stdio::piped()).stderr(Stdio::null());
        let mut child = cmd.spawn().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (tx, lines) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            use std::io::BufRead;
            for line in std::io::BufReader::new(stdout).lines() {
                if tx.send(line.unwrap()).is_err() {
                    return;
                }
            }
        });
        Subscriber { child: KillOnDrop(Some(child)), lines }
    }
}

impl Drop for TestQueue {
    fn drop(&mut self) {
        if let Some(mut child) = self.daemon.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// A live `mlq subscribe` process plus its decoded snapshot stream.
struct Subscriber {
    child: KillOnDrop,
    lines: std::sync::mpsc::Receiver<String>,
}

impl Subscriber {
    fn next_snapshot(&self, timeout: Duration) -> serde_json::Value {
        let line = self.lines.recv_timeout(timeout).expect("a snapshot before the deadline");
        serde_json::from_str(&line).expect("every snapshot line is valid JSON")
    }

    /// Read frames until one satisfies `matches`; the frames skipped on the way
    /// are reported if the deadline passes first.
    fn wait_snapshot(
        &self,
        what: &str,
        timeout: Duration,
        mut matches: impl FnMut(&serde_json::Value) -> bool,
    ) -> serde_json::Value {
        let deadline = Instant::now() + timeout;
        let mut skipped = Vec::new();
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let line = self
                .lines
                .recv_timeout(remaining)
                .unwrap_or_else(|err| panic!("no snapshot showed {what} ({err}); saw {skipped:#?}"));
            let snapshot: serde_json::Value =
                serde_json::from_str(&line).expect("every snapshot line is valid JSON");
            if matches(&snapshot) {
                return snapshot;
            }
            skipped.push(snapshot);
        }
    }

    /// An unchanged queue must produce no frames while keeping the stream open.
    fn expect_silence(&self, within: Duration) {
        use std::sync::mpsc::RecvTimeoutError;
        match self.lines.recv_timeout(within) {
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => panic!("the subscription was closed while idle"),
            Ok(line) => panic!("unexpected snapshot on an unchanged queue: {line}"),
        }
    }

    /// The daemon shutting down must end the stream and exit `mlq` cleanly.
    fn expect_clean_end(&mut self, timeout: Duration) {
        use std::sync::mpsc::RecvTimeoutError;
        let deadline = Instant::now() + timeout;
        // Drain any final snapshot still in flight; `Disconnected` is EOF.
        while let Ok(_) | Err(RecvTimeoutError::Timeout) =
            self.lines.recv_timeout(Duration::from_millis(50))
        {
            assert!(Instant::now() < deadline, "the subscription did not end");
        }
        let child = self.child.0.as_mut().expect("subscriber running");
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            assert!(Instant::now() < deadline, "mlq subscribe did not exit");
            std::thread::sleep(Duration::from_millis(20));
        };
        assert_eq!(status.code(), Some(0), "a clean daemon shutdown is not a client error");
    }
}

#[test]
fn subscribe_streams_snapshots_outlives_idle_and_ends_on_shutdown() {
    let mut q = TestQueue::new();
    let mut subscriber = q.subscribe();

    // One complete snapshot arrives immediately, before anything changes.
    let initial = subscriber.next_snapshot(Duration::from_secs(10));
    assert!(initial["jobs"].as_array().unwrap().is_empty(), "{initial}");
    assert_eq!(initial["activeLeases"], 0);
    assert_eq!(initial["admissionBlocked"], false);

    // Idle for longer than the configured idle timeout (200ms, floored to 1s by
    // the daemon): an unchanged queue must stay silent and stay connected.
    subscriber.expect_silence(Duration::from_millis(1_500));

    // Work submitted on another connection is pushed to this one, and a
    // runner-driven transition arrives without any polling.
    let blocker = q.submit(&["--name", "blocker"], &["sleep", "30"]);
    let running =
        subscriber.wait_snapshot("the blocker running", Duration::from_secs(10), |snapshot| {
            job_in(snapshot, blocker)["state"] == "running"
        });
    assert_eq!(running["activeLeases"], 1, "{running}");
    assert_eq!(running["effectiveLimit"], 1, "{running}");

    // Client-driven transitions arrive in order, each as a whole snapshot.
    let waiting = q.submit(&["--name", "waiting"], &["sh", "-c", "true"]);
    subscriber.wait_snapshot("the second job queued", Duration::from_secs(10), |snapshot| {
        job_in(snapshot, waiting)["state"] == "queued"
    });
    q.cli(&["hold", &waiting.to_string()]);
    subscriber.wait_snapshot("the job held", Duration::from_secs(10), |snapshot| {
        job_in(snapshot, waiting)["state"] == "held"
    });
    q.cli(&["release", &waiting.to_string()]);
    subscriber.wait_snapshot("the job released", Duration::from_secs(10), |snapshot| {
        job_in(snapshot, waiting)["state"] == "queued"
    });

    // Freeing the lease lets the queued job run through to its outcome.
    q.cli(&["cancel", &blocker.to_string()]);
    let done =
        subscriber.wait_snapshot("the job succeeding", Duration::from_secs(20), |snapshot| {
            job_in(snapshot, waiting)["state"] == "succeeded"
        });
    assert_eq!(job_in(&done, blocker)["state"], "cancelled", "{done}");

    // Shutting the daemon down closes the stream instead of hanging.
    q.stop_daemon();
    subscriber.expect_clean_end(Duration::from_secs(10));
}

/// The job with `id` in a status snapshot, or `null` when it is absent.
fn job_in(snapshot: &serde_json::Value, id: i64) -> &serde_json::Value {
    snapshot["jobs"]
        .as_array()
        .expect("snapshots carry a jobs array")
        .iter()
        .find(|job| job["id"] == id)
        .unwrap_or(&serde_json::Value::Null)
}

/// The exact bytes a non-Rust consumer sends to open a subscription.
const RAW_SUBSCRIBE: &[u8] =
    br#"{"protocol_version":8,"request_id":"raw-1","op":{"type":"subscribe"}}"#;

fn raw_subscribe(socket: &std::path::Path) -> (std::os::unix::net::UnixStream, serde_json::Value) {
    try_raw_subscribe(socket).expect("the daemon answered the subscribe request")
}

/// `None` when the daemon closed the connection without answering, which is what
/// being over the *connection* budget looks like.
fn try_raw_subscribe(
    socket: &std::path::Path,
) -> Option<(std::os::unix::net::UnixStream, serde_json::Value)> {
    use mlqueue::protocol::{DEFAULT_MAX_FRAME_BYTES, read_frame_sync, write_frame_sync};

    let mut stream = std::os::unix::net::UnixStream::connect(socket).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    write_frame_sync(&mut stream, RAW_SUBSCRIBE, DEFAULT_MAX_FRAME_BYTES).ok()?;
    let frame = read_frame_sync(&mut stream, DEFAULT_MAX_FRAME_BYTES).ok()??;
    let response = serde_json::from_slice(&frame).expect("frames carry JSON responses");
    Some((stream, response))
}

fn read_response(stream: &mut std::os::unix::net::UnixStream) -> serde_json::Value {
    use mlqueue::protocol::{DEFAULT_MAX_FRAME_BYTES, read_frame_sync};

    let frame = read_frame_sync(stream, DEFAULT_MAX_FRAME_BYTES)
        .expect("a readable frame")
        .expect("the daemon kept the stream open");
    serde_json::from_slice(&frame).expect("frames carry JSON responses")
}

#[test]
fn raw_subscribe_echoes_the_request_id_and_budgets_its_slot() {
    // One stream slot (a quarter of the connection budget, at least one), so the
    // refusal and the release of a slot are both deterministic.
    let q = TestQueue::with_config(20, "max_connections = 4\n");

    let (mut first_stream, initial) = raw_subscribe(&q.socket());
    assert_eq!(initial["request_id"], "raw-1", "the subscribe request id is echoed");
    assert_eq!(initial["reply"]["type"], "status", "{initial}");
    assert!(initial["error"].is_null(), "{initial}");

    let job = q.submit(&["--name", "raw"], &["sh", "-c", "true"]);
    let pushed = read_response(&mut first_stream);
    assert_eq!(pushed["request_id"], "raw-1", "every frame echoes the same id");
    assert_eq!(job_in(&pushed["reply"], job)["id"], job, "{pushed}");

    // A second stream does not fit, and is told so instead of being dropped.
    let (_refused_stream, refused) = raw_subscribe(&q.socket());
    assert_eq!(refused["error"]["code"], "too_many_subscriptions", "{refused}");
    assert!(refused["reply"].is_null(), "{refused}");
    // Ordinary request/response clients still have headroom.
    assert!(q.try_cli(&["daemon", "status"]).status.success());

    // Disconnecting frees the slot again.
    drop(first_stream);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some((_stream, response)) = try_raw_subscribe(&q.socket())
            && response["error"].is_null()
        {
            assert_eq!(response["reply"]["type"], "status", "{response}");
            break;
        }
        assert!(Instant::now() < deadline, "the subscription slot was never released");
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn submit_runs_captures_logs_and_reports() {
    let q = TestQueue::new();
    let job = q.submit(&["--name", "hello"], &["sh", "-c", "echo out-marker; echo err-marker >&2"]);
    q.wait_state(job, "succeeded", Duration::from_secs(10));

    let detail = q.job(job);
    assert_eq!(detail["maxParallelRuns"], 1, "default limit is the conservative 1");
    assert_eq!(detail["name"], "hello");
    let attempt = &detail["attempts"][0];
    assert_eq!(attempt["state"], "succeeded");
    assert_eq!(attempt["exitCode"], 0);

    let stdout = q.cli(&["logs", &job.to_string()]);
    assert!(stdout.contains("out-marker"), "stdout log: {stdout:?}");
    let stderr = q.cli(&["logs", &job.to_string(), "--stderr"]);
    assert!(stderr.contains("err-marker"), "stderr log: {stderr:?}");
}

#[test]
fn time_limit_excludes_queue_wait_but_bounds_the_whole_attempt() {
    let q = TestQueue::new();

    // This job waits behind the blocker longer than its own time limit, but
    // should still succeed because the clock begins only when its command is
    // spawned.
    let blocker = q.submit(&["--name", "blocker"], &["sleep", "0.6"]);
    let queued = q.submit(
        &["--name", "queued-limit", "--time-limit", "250ms"],
        &["sleep", "0.05"],
    );
    q.wait_state(blocker, "succeeded", Duration::from_secs(10));
    q.wait_state(queued, "succeeded", Duration::from_secs(10));
    assert_eq!(q.job(queued)["timeLimitMs"], 250);

    // The limit covers the complete submitted command and is hard: a group
    // that ignores SIGTERM is escalated after the configured grace period.
    let bounded = q.submit(
        &["--name", "bounded", "--time-limit", "200ms"],
        &["sh", "-c", "trap '' TERM; exec sleep 30"],
    );
    q.wait_state(bounded, "failed", Duration::from_secs(10));
    let detail = q.job(bounded);
    let attempt = &detail["attempts"][0];
    assert_eq!(attempt["state"], "failed");
    assert_eq!(attempt["termSignal"], libc::SIGKILL);
    assert!(
        attempt["message"]
            .as_str()
            .is_some_and(|message| message.contains("time limit of 200 ms exceeded")),
        "attempt detail: {attempt}"
    );
}

#[test]
fn detached_runner_keeps_the_time_limit_across_daemon_restart() {
    let mut q = TestQueue::new();
    let job = q.submit(
        &["--name", "restart-limit", "--time-limit", "300ms"],
        &["sleep", "30"],
    );
    q.wait_state(job, "running", Duration::from_secs(10));

    q.kill_daemon();
    std::thread::sleep(Duration::from_millis(600));
    q.start_daemon();

    q.wait_state(job, "failed", Duration::from_secs(10));
    let detail = q.job(job);
    assert!(
        detail["attempts"][0]["message"]
            .as_str()
            .is_some_and(|message| message.contains("time limit of 300 ms exceeded")),
        "job detail: {detail}"
    );
}

#[test]
fn an_expired_time_limit_wins_over_a_later_cancellation() {
    // A long runner poll makes the ordering deterministic: the deadline and
    // cancellation are both first observed by the same supervision pass.
    let q = TestQueue::with_runner_poll_ms(500);
    let job = q.submit(
        &["--name", "timeout-race", "--time-limit", "200ms"],
        &["sleep", "30"],
    );
    q.wait_state(job, "running", Duration::from_secs(10));
    std::thread::sleep(Duration::from_millis(300));
    q.cli(&["cancel", &job.to_string()]);

    q.wait_state(job, "failed", Duration::from_secs(10));
    let detail = q.job(job);
    assert!(
        detail["attempts"][0]["message"]
            .as_str()
            .is_some_and(|message| message.contains("time limit of 200 ms exceeded")),
        "job detail: {detail}"
    );
}

#[test]
fn daemon_enforces_the_time_limit_after_the_runner_dies() {
    let q = TestQueue::new();
    let job = q.submit(
        &["--name", "orphan-limit", "--time-limit", "300ms"],
        &["sh", "-c", "trap '' TERM; exec sleep 30"],
    );
    q.wait_state(job, "running", Duration::from_secs(10));

    let detail = q.job(job);
    let attempt_dir = detail["attempts"][0]["logDir"].as_str().unwrap();
    let identity: serde_json::Value = serde_json::from_slice(
        &std::fs::read(PathBuf::from(attempt_dir).join("identity.json")).unwrap(),
    )
    .unwrap();
    let runner_pid = identity["runner_pid"].as_i64().unwrap() as i32;
    assert_eq!(unsafe { libc::kill(runner_pid, libc::SIGKILL) }, 0);

    q.wait_state(job, "failed", Duration::from_secs(10));
    let detail = q.job(job);
    assert!(
        detail["attempts"][0]["message"]
            .as_str()
            .is_some_and(|message| message.contains("time limit exceeded")),
        "job detail: {detail}"
    );
}

#[test]
fn an_early_cancel_does_not_disable_runner_timeout_escalation() {
    let q = TestQueue::new();
    let job = q.submit(
        &["--name", "early-cancel", "--time-limit", "300ms"],
        &["sh", "-c", "trap '' TERM; exec sleep 30"],
    );
    q.wait_state(job, "running", Duration::from_secs(10));
    q.cli(&["cancel", &job.to_string()]);

    // Cancellation remains the terminal classification, but the later hard
    // deadline must still escalate the TERM-ignoring process group.
    q.wait_state(job, "cancelled", Duration::from_secs(10));
    assert_eq!(q.job(job)["attempts"][0]["termSignal"], libc::SIGKILL);
}

#[test]
fn an_early_cancel_does_not_disable_orphan_timeout_escalation() {
    let q = TestQueue::new();
    let job = q.submit(
        &["--name", "orphan-early-cancel", "--time-limit", "300ms"],
        &["sh", "-c", "trap '' TERM; exec sleep 30"],
    );
    q.wait_state(job, "running", Duration::from_secs(10));
    q.cli(&["cancel", &job.to_string()]);

    let detail = q.job(job);
    let attempt_dir = detail["attempts"][0]["logDir"].as_str().unwrap();
    let identity: serde_json::Value = serde_json::from_slice(
        &std::fs::read(PathBuf::from(attempt_dir).join("identity.json")).unwrap(),
    )
    .unwrap();
    let runner_pid = identity["runner_pid"].as_i64().unwrap() as i32;
    assert_eq!(unsafe { libc::kill(runner_pid, libc::SIGKILL) }, 0);

    q.wait_state(job, "cancelled", Duration::from_secs(10));
}

#[test]
fn repeated_cancel_preserves_the_first_request_timestamp() {
    let q = TestQueue::with_runner_poll_ms(500);
    let job = q.submit(
        &["--name", "repeat-cancel", "--time-limit", "300ms"],
        &["sh", "-c", "trap '' TERM; exec sleep 30"],
    );
    q.wait_state(job, "running", Duration::from_secs(10));
    q.cli(&["cancel", &job.to_string()]);

    let attempt_dir = q.job(job)["attempts"][0]["logDir"].as_str().unwrap().to_string();
    let cancel_path = PathBuf::from(&attempt_dir).join("cancel.json");
    let first: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&cancel_path).unwrap()).unwrap();
    std::thread::sleep(Duration::from_millis(350));
    q.cli(&["cancel", &job.to_string(), "--force"]);
    let second: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&cancel_path).unwrap()).unwrap();
    assert_eq!(first["requested_at"], second["requested_at"]);
    assert_eq!(second["force"], true);

    q.wait_state(job, "cancelled", Duration::from_secs(10));
}

#[test]
fn late_observation_of_a_drained_orphan_is_lost_not_timed_out() {
    let mut q = TestQueue::new();
    let marker = q.dir.path().join("orphan-drained-marker");
    let marker_arg = format!("echo run >> {}; sleep 0.1", marker.display());
    let job = q.submit(
        &[
            "--name",
            "drained-orphan",
            "--time-limit",
            "1s",
            "--max-attempts",
            "2",
        ],
        &["sh", "-c", &marker_arg],
    );
    q.wait_state(job, "running", Duration::from_secs(10));

    let detail = q.job(job);
    let attempt_dir = detail["attempts"][0]["logDir"].as_str().unwrap();
    let identity: serde_json::Value = serde_json::from_slice(
        &std::fs::read(PathBuf::from(attempt_dir).join("identity.json")).unwrap(),
    )
    .unwrap();
    let runner_pid = identity["runner_pid"].as_i64().unwrap() as i32;
    q.kill_daemon();
    assert_eq!(unsafe { libc::kill(runner_pid, libc::SIGKILL) }, 0);

    // The command drains before its limit, but recovery does not observe the
    // empty group until after the nominal deadline.
    std::thread::sleep(Duration::from_millis(1_200));
    q.start_daemon();
    q.wait_state(job, "lost", Duration::from_secs(10));
    assert_eq!(q.job(job)["attemptCount"], 1, "lost work must not auto-retry");
    assert_eq!(
        std::fs::read_to_string(&marker)
            .unwrap()
            .lines()
            .count(),
        1,
        "the unknown command must not be duplicated"
    );
}

#[test]
fn submit_resolves_relative_executables_against_the_job_cwd() {
    let q = TestQueue::new();
    // Only the job's --cwd contains the script; the daemon's own working
    // directory (the crate root under `cargo test`) does not. A relative
    // command launches from the job cwd, so validation must look there.
    let bin = q.dir.path().join("bin");
    std::fs::create_dir(&bin).unwrap();
    let script = bin.join("hello.sh");
    std::fs::write(&script, "#!/bin/sh\necho cwd-relative-marker\n").unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();

    let job = q.submit(&["--name", "relative-exe"], &["bin/hello.sh"]);
    q.wait_state(job, "succeeded", Duration::from_secs(10));
    let stdout = q.cli(&["logs", &job.to_string()]);
    assert!(stdout.contains("cwd-relative-marker"), "stdout log: {stdout:?}");

    // A genuinely missing relative executable is still rejected, and the
    // message names the resolved path so the mistake is visible.
    let cwd = q.dir.path().to_str().unwrap();
    let rejected = q.try_cli(&[
        "submit", "--name", "missing-exe", "--cwd", cwd, "--", "bin/nope.sh",
    ]);
    assert!(!rejected.status.success(), "missing executable must be rejected");
    let stderr = String::from_utf8_lossy(&rejected.stderr);
    assert!(
        stderr.contains("bin/nope.sh") && stderr.contains(cwd),
        "rejection should name the resolved path: {stderr}"
    );
}

#[test]
fn status_displays_long_job_names() {
    let q = TestQueue::new();
    let name = "long-job-name-now-fits-in-status";
    q.submit(&["--name", name], &["sleep", "1"]);

    let status = q.cli(&["status"]);
    assert!(status.contains(name), "long job name was truncated: {status}");
    let header = status.lines().find(|line| line.starts_with("JOB ")).unwrap();
    assert!(header.chars().count() <= 80, "status header is wider than 80 columns: {header}");
}

#[test]
fn follow_tts_announces_completion_and_the_next_running_job() {
    let mut q = TestQueue::new();
    let fake_bin = q.dir.path().join("fake-bin");
    std::fs::create_dir(&fake_bin).unwrap();
    let fake_tts = fake_bin.join("tts");
    std::fs::write(
        &fake_tts,
        "#!/bin/sh\nprintf 'ARGS: %s\\n' \"$*\" >> \"$MLQ_TTS_LOG\"\ncat >> \"$MLQ_TTS_LOG\"\nprintf '\\n' >> \"$MLQ_TTS_LOG\"\n",
    )
    .unwrap();
    std::fs::set_permissions(&fake_tts, std::fs::Permissions::from_mode(0o755)).unwrap();
    let tts_log = q.dir.path().join("tts.log");

    let mut command = Command::new(env!("CARGO_BIN_EXE_mlq"));
    q.apply_env(&mut command);
    command
        .arg("follow-tts")
        .env("MLQ_TTS_LOG", &tts_log)
        .env(
            "PATH",
            format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap_or_default()),
        )
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut follower = KillOnDrop(Some(command.spawn().unwrap()));
    wait_for("initial TTS announcement", Duration::from_secs(5), || {
        std::fs::read_to_string(&tts_log)
            .is_ok_and(|text| text.contains("Queue monitor started"))
    });
    let duplicate = q.try_cli(&["follow-tts"]);
    assert!(!duplicate.status.success());
    assert!(
        String::from_utf8_lossy(&duplicate.stderr).contains("already running"),
        "{}",
        String::from_utf8_lossy(&duplicate.stderr)
    );

    let first = q.submit(&["--name", "tts-first"], &["sleep", "0.4"]);
    let next = q.submit(&["--name", "tts-next"], &["sleep", "2"]);
    q.wait_state(next, "running", Duration::from_secs(10));
    let expected_finished = format!("job {first}, tts first finished successfully");
    let expected_running = format!("Now running: job {next}, tts next");
    let deadline = Instant::now() + Duration::from_secs(10);
    let spoken = loop {
        let spoken = std::fs::read_to_string(&tts_log).unwrap_or_default();
        if spoken.contains(&expected_finished) && spoken.contains(&expected_running) {
            break spoken;
        }
        if Instant::now() >= deadline {
            break spoken;
        }
        std::thread::sleep(Duration::from_millis(50));
    };

    // The durable event cursor survives a daemon restart while the follower
    // and managed command remain alive.
    q.kill_daemon();
    q.start_daemon();
    q.wait_state(next, "succeeded", Duration::from_secs(10));
    let expected_next_finished = format!("job {next}, tts next finished successfully");
    wait_for("post-restart TTS announcement", Duration::from_secs(10), || {
        std::fs::read_to_string(&tts_log)
            .is_ok_and(|text| text.contains(&expected_next_finished))
    });

    follower.stop();
    assert!(spoken.contains(&expected_finished), "missing finish announcement:\n{spoken}");
    assert!(spoken.contains(&expected_running), "missing running announcement:\n{spoken}");
    assert!(spoken.contains("ARGS: speak --daemon --level status --text-stdin"), "{spoken}");
    assert!(!spoken.contains("--backend"), "default TTS backend was overridden: {spoken}");
}

#[test]
fn default_limit_serializes_jobs() {
    let q = TestQueue::new();
    let slow = q.submit(&["--name", "slow"], &["sleep", "1.5"]);
    let fast = q.submit(&["--name", "fast"], &["sh", "-c", "true"]);
    q.wait_state(slow, "running", Duration::from_secs(10));

    // While the exclusive job runs, the second must wait. As the blocked
    // queue head it immediately becomes the protected job.
    let fast_view = q.job(fast);
    assert_eq!(fast_view["state"], "queued", "second maxParallelRuns=1 job must wait");
    assert!(
        fast_view["eligibility"].as_str().unwrap().starts_with("protected_drain"),
        "unexpected eligibility: {fast_view}"
    );

    q.wait_state(slow, "succeeded", Duration::from_secs(10));
    q.wait_state(fast, "succeeded", Duration::from_secs(10));
}

#[test]
fn three_wide_jobs_share_and_a_fourth_waits() {
    let q = TestQueue::new();
    let jobs: Vec<i64> = (0..4)
        .map(|i| {
            q.submit(&["--max-parallel-runs", "3", "--name", &format!("cleanrl-{i}")], &[
                "sleep", "1.2",
            ])
        })
        .collect();

    // Exactly three run together; the fourth queues behind the formula.
    wait_for("three concurrent leases", Duration::from_secs(10), || {
        q.status()["activeLeases"].as_u64() == Some(3)
    });
    let running: Vec<String> = jobs.iter().map(|&j| q.job_state(j)).collect();
    assert_eq!(running.iter().filter(|s| *s == "running" || *s == "starting").count(), 3);
    assert_eq!(running.iter().filter(|s| *s == "queued").count(), 1);

    // The invariant holds throughout: never more than three leases.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let leases = q.status()["activeLeases"].as_u64().unwrap();
        assert!(leases <= 3, "over-admission: {leases} active leases");
        if jobs.iter().all(|&j| q.job_state(j) == "succeeded") {
            break;
        }
        assert!(Instant::now() < deadline, "jobs did not finish");
        std::thread::sleep(Duration::from_millis(60));
    }
}

#[test]
fn restrictive_job_is_protected_and_the_backfill_window_stays_open() {
    let q = TestQueue::new();
    // Three permissive jobs fill the 3-wide set; `gate` finishes first.
    let a = q.submit(&["--max-parallel-runs", "3", "--name", "a"], &["sleep", "4"]);
    let b = q.submit(&["--max-parallel-runs", "3", "--name", "b"], &["sleep", "4"]);
    let gate = q.submit(&["--max-parallel-runs", "3", "--name", "gate"], &["sleep", "1"]);
    for job in [a, b, gate] {
        q.wait_state(job, "running", Duration::from_secs(10));
    }

    // The exclusive job is the eligible head and cannot start beside the
    // 3-wide set: it becomes the protected job.
    let exclusive =
        q.submit(&["--max-parallel-runs", "1", "--name", "exclusive"], &["sh", "-c", "true"]);
    wait_for("reservation to exist", Duration::from_secs(5), || {
        q.status()["reservation"]["protectedJob"].as_i64() == Some(exclusive)
    });

    // Submitted after protection, but the original blockers are still
    // running: the open window lets these permissive jobs use slots in FIFO
    // order rather than idling them.
    let backfill = q.submit(
        &["--max-parallel-runs", "3", "--name", "backfill"],
        &["sleep", "1"],
    );
    let late = q.submit(&["--max-parallel-runs", "3", "--name", "late"], &["sleep", "1"]);
    let status = q.status();
    assert_eq!(status["reservation"]["backfillWindowOpen"], true, "window must be open: {status}");
    assert!(status["reservation"]["backfillCutoff"].is_null(), "no cutoff while open: {status}");
    let late_view = q.job(late);
    assert!(
        late_view["eligibility"].as_str().unwrap().starts_with("backfill_window_open"),
        "late job must be bypass-eligible in the open window: {late_view}"
    );

    // The earlier backfill job takes the first open slot; the late job follows
    // when another slot opens, all while the protected job keeps waiting.
    q.wait_state(backfill, "running", Duration::from_secs(10));
    assert_eq!(q.job_state(exclusive), "queued");
    q.wait_state(late, "running", Duration::from_secs(10));
    assert_eq!(q.job_state(exclusive), "queued");
    let status = q.status();
    for consumed in [backfill, late] {
        assert!(
            status["reservation"]["consumedBypasses"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!(consumed)),
            "bypass of job {consumed} must be recorded: {status}"
        );
    }

    // Full drain: the protected job starts alone and succeeds.
    q.wait_state(exclusive, "succeeded", Duration::from_secs(20));
    assert!(q.job(exclusive)["finishedAt"].as_i64() >= q.job(late)["finishedAt"].as_i64());
}

#[test]
fn frontier_freezes_once_the_original_blockers_drain() {
    let q = TestQueue::new();
    // Long enough that both blockers are still running through the submits
    // below even on a loaded machine; the freeze is then deterministic.
    let a = q.submit(&["--max-parallel-runs", "3", "--name", "a"], &["sleep", "4"]);
    let b = q.submit(&["--max-parallel-runs", "3", "--name", "b"], &["sleep", "4"]);
    for job in [a, b] {
        q.wait_state(job, "running", Duration::from_secs(10));
    }
    let exclusive =
        q.submit(&["--max-parallel-runs", "1", "--name", "exclusive"], &["sh", "-c", "true"]);
    wait_for("reservation to exist", Duration::from_secs(5), || {
        q.status()["reservation"]["protectedJob"].as_i64() == Some(exclusive)
    });

    // Open window: the post-protection job takes the third slot immediately.
    let occupant = q.submit(&["--max-parallel-runs", "3", "--name", "occupant"], &["sleep", "20"]);
    q.wait_state(occupant, "running", Duration::from_secs(10));

    // The original blockers drain; the occupant (a mere backfill) does not
    // hold the window open, so the frontier freezes.
    q.wait_state(a, "succeeded", Duration::from_secs(10));
    q.wait_state(b, "succeeded", Duration::from_secs(10));
    wait_for("frontier to freeze", Duration::from_secs(5), || {
        q.status()["reservation"]["backfillWindowOpen"] == serde_json::json!(false)
    });
    let cutoff = q.status()["reservation"]["backfillCutoff"].as_i64().unwrap();
    assert!(cutoff >= occupant, "cutoff must cover everything submitted before the freeze");

    // Submitted after the freeze: may no longer pass the protected job, even
    // though two slots are open beside the occupant.
    let frozen_out =
        q.submit(&["--max-parallel-runs", "3", "--name", "frozen-out"], &["sh", "-c", "true"]);
    let frozen_view = q.job(frozen_out);
    assert!(
        frozen_view["eligibility"].as_str().unwrap().starts_with("behind_backfill_cutoff"),
        "post-freeze submission must wait: {frozen_view}"
    );
    assert_eq!(q.job_state(exclusive), "queued");

    // Drain the occupant: the protected job runs alone, then the frozen-out
    // job follows.
    q.cli(&["cancel", &occupant.to_string()]);
    q.wait_state(exclusive, "succeeded", Duration::from_secs(20));
    q.wait_state(frozen_out, "succeeded", Duration::from_secs(20));
    assert!(q.job(frozen_out)["finishedAt"].as_i64() >= q.job(exclusive)["finishedAt"].as_i64());
}

#[test]
fn reservation_and_consumed_bypasses_survive_daemon_restart() {
    let mut q = TestQueue::new();
    // Same shape as the open-window test, but long enough to straddle a
    // daemon crash while the reservation and one consumed bypass exist.
    let a = q.submit(&["--max-parallel-runs", "3", "--name", "a"], &["sleep", "8"]);
    let b = q.submit(&["--max-parallel-runs", "3", "--name", "b"], &["sleep", "8"]);
    let gate = q.submit(&["--max-parallel-runs", "3", "--name", "gate"], &["sleep", "1"]);
    for job in [a, b, gate] {
        q.wait_state(job, "running", Duration::from_secs(10));
    }
    let exclusive =
        q.submit(&["--max-parallel-runs", "1", "--name", "exclusive"], &["sh", "-c", "true"]);
    wait_for("reservation to exist", Duration::from_secs(5), || {
        q.status()["reservation"]["protectedJob"].as_i64() == Some(exclusive)
    });
    let backfill =
        q.submit(&["--max-parallel-runs", "3", "--name", "backfill"], &["sleep", "2"]);
    let late = q.submit(&["--max-parallel-runs", "3", "--name", "late"], &["sh", "-c", "true"]);

    // The bypass is consumed, then the daemon crashes.
    q.wait_state(backfill, "running", Duration::from_secs(10));
    q.kill_daemon();
    q.start_daemon();

    // The restarted daemon restores the reservation and the consumed set
    // from the database, and re-derives the open window from the still-live
    // initial blockers rather than recomputing fairness from events.
    let status = q.status();
    assert_eq!(status["reservation"]["protectedJob"].as_i64(), Some(exclusive));
    assert_eq!(status["reservation"]["backfillWindowOpen"], true, "blockers still run: {status}");
    assert!(
        status["reservation"]["consumedBypasses"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!(backfill)),
        "consumed bypass must survive the restart: {status}"
    );
    assert_eq!(q.job_state(exclusive), "queued");
    assert!(
        q.job(late)["eligibility"].as_str().unwrap().starts_with("backfill_window_open"),
        "the window stays open across the restart"
    );

    // The late job takes a slot once the backfill finishes, while the
    // original blockers still run and the protected job keeps waiting.
    q.wait_state(late, "succeeded", Duration::from_secs(30));
    assert_eq!(q.job_state(a), "running");
    assert_eq!(q.job_state(exclusive), "queued");

    // Drain everything: the protected job still starts.
    q.cli(&["cancel", &a.to_string()]);
    q.cli(&["cancel", &b.to_string()]);
    q.wait_state(exclusive, "succeeded", Duration::from_secs(30));
}

#[test]
fn cancelling_the_protected_job_invalidates_the_reservation() {
    let q = TestQueue::new();
    let a = q.submit(&["--max-parallel-runs", "3", "--name", "a"], &["sleep", "6"]);
    let b = q.submit(&["--max-parallel-runs", "3", "--name", "b"], &["sleep", "6"]);
    for job in [a, b] {
        q.wait_state(job, "running", Duration::from_secs(10));
    }
    let exclusive =
        q.submit(&["--max-parallel-runs", "1", "--name", "exclusive"], &["sleep", "5"]);
    wait_for("reservation to exist", Duration::from_secs(5), || {
        q.status()["reservation"]["protectedJob"].as_i64() == Some(exclusive)
    });

    // The open window admits the late job into the third slot; its bypass is
    // consumed under the standing reservation.
    let late = q.submit(&["--max-parallel-runs", "3", "--name", "late"], &["sleep", "6"]);
    q.wait_state(late, "running", Duration::from_secs(10));
    assert_eq!(q.job_state(exclusive), "queued");

    // Cancelling the protected job invalidates the reservation entirely:
    // later submissions no longer answer to any frontier.
    q.cli(&["cancel", &exclusive.to_string()]);
    wait_for("reservation to be invalidated", Duration::from_secs(5), || {
        q.status()["reservation"].is_null()
    });
    let free = q.submit(&["--max-parallel-runs", "4", "--name", "free"], &["sh", "-c", "true"]);
    let free_view = q.job(free);
    assert!(
        free_view["eligibility"].as_str().is_none_or(|reason| !reason.contains("backfill")),
        "no reservation may constrain the queue after invalidation: {free_view}"
    );
    assert_eq!(q.job_state(a), "running");
    assert_eq!(q.job_state(b), "running");
}

#[test]
fn unsupported_reservation_blocks_admission_until_resolved() {
    let mut q = TestQueue::new();
    let a = q.submit(&["--max-parallel-runs", "3", "--name", "a"], &["sleep", "10"]);
    let b = q.submit(&["--max-parallel-runs", "3", "--name", "b"], &["sleep", "10"]);
    for job in [a, b] {
        q.wait_state(job, "running", Duration::from_secs(10));
    }
    let exclusive =
        q.submit(&["--max-parallel-runs", "1", "--name", "exclusive"], &["sh", "-c", "true"]);
    wait_for("reservation to exist", Duration::from_secs(5), || {
        q.status()["reservation"]["protectedJob"].as_i64() == Some(exclusive)
    });

    // Rewrite the active reservation to a semantics version this daemon does
    // not implement, as a bad upgrade or downgrade would leave behind.
    q.kill_daemon();
    {
        let conn = mlqueue::db::open(&q.state_dir().join("mlqueue.db")).unwrap();
        conn.execute(
            "UPDATE scheduler_reservation SET semantics_version = 99 WHERE status = 'active'",
            [],
        )
        .unwrap();
    }
    q.start_daemon();

    // The daemon must refuse ordinary admission rather than guess: a new
    // compatible job queues even though a slot is open beside a and b.
    wait_for("admission to block", Duration::from_secs(5), || {
        q.status()["admissionBlocked"] == serde_json::json!(true)
    });
    let stuck = q.submit(&["--max-parallel-runs", "3", "--name", "stuck"], &["sh", "-c", "true"]);
    std::thread::sleep(Duration::from_millis(400));
    assert_eq!(q.job_state(stuck), "queued");

    // Cancelling the protected job resolves the uninterpretable reservation;
    // admission must heal without a daemon restart.
    q.cli(&["cancel", &exclusive.to_string()]);
    q.wait_state(stuck, "succeeded", Duration::from_secs(10));
    assert_eq!(q.status()["admissionBlocked"], serde_json::json!(false));
}

#[test]
fn cancel_terminates_and_frees_the_lease() {
    let q = TestQueue::new();
    let job = q.submit(&["--name", "long"], &["sleep", "60"]);
    q.wait_state(job, "running", Duration::from_secs(10));

    // Cancel of live work is async: report intent, not a false "is now running".
    let out = q.cli(&["cancel", &job.to_string()]);
    assert!(
        out.contains("cancel requested") && !out.contains("is now running"),
        "cancel of a live job should report async cancel intent, got: {out:?}"
    );
    q.wait_state(job, "cancelled", Duration::from_secs(10));

    // The lease is gone: an exclusive job starts immediately.
    let next = q.submit(&["--name", "next"], &["sh", "-c", "true"]);
    q.wait_state(next, "succeeded", Duration::from_secs(10));
}

#[test]
fn force_cancel_escalates_to_sigkill_after_grace() {
    let q = TestQueue::new();
    // Ignores SIGTERM; only SIGKILL can end the shell loop.
    let job = q.submit(&["--name", "stubborn"], &[
        "sh",
        "-c",
        "trap '' TERM; while true; do sleep 0.2; done",
    ]);
    q.wait_state(job, "running", Duration::from_secs(10));

    q.cli(&["cancel", &job.to_string(), "--force"]);
    q.wait_state(job, "cancelled", Duration::from_secs(15));
}

#[test]
fn idempotency_key_replays_and_conflicts() {
    let q = TestQueue::new();
    let args = |name: &str| {
        vec![
            "--idempotency-key".to_string(),
            "test-key-1".to_string(),
            "submit".to_string(),
            "--json".to_string(),
            "--name".to_string(),
            name.to_string(),
            "--cwd".to_string(),
            q.dir.path().to_str().unwrap().to_string(),
            "--".to_string(),
            "sh".to_string(),
            "-c".to_string(),
            "true".to_string(),
        ]
    };
    let run = |name: &str| -> Output {
        let owned = args(name);
        let refs: Vec<&str> = owned.iter().map(String::as_str).collect();
        q.try_cli(&refs)
    };
    let parse = |output: &Output| -> serde_json::Value {
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        serde_json::from_slice(&output.stdout).unwrap()
    };
    let first = parse(&run("same"));
    let second = parse(&run("same"));
    assert_eq!(first["id"], second["id"], "identical retry must not duplicate the job");

    // Same key, different payload: stable conflict, no third job.
    let output = run("different");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("idempotency_conflict"), "stderr: {stderr}");
}

#[test]
fn retry_policy_reattempts_then_fails() {
    let q = TestQueue::new();
    let job = q.submit(
        &["--name", "flaky", "--max-attempts", "2", "--retry-delay", "100ms"],
        &["sh", "-c", "exit 1"],
    );
    q.wait_state(job, "failed", Duration::from_secs(15));
    let detail = q.job(job);
    assert_eq!(detail["attemptCount"], 2, "retry policy must run a second attempt");
    assert_eq!(detail["attempts"][1]["state"], "failed");

    // Manual retry grants one more attempt.
    q.cli(&["retry", &job.to_string()]);
    q.wait_state(job, "failed", Duration::from_secs(15));
    assert_eq!(q.job(job)["attemptCount"], 3);
}

#[test]
fn failed_dependencies_cascade_skips_and_run_terminal_cleanup() {
    let q = TestQueue::new();
    let parent = q.submit(
        &["--name", "parent", "--max-attempts", "2", "--retry-delay", "800ms"],
        &["sh", "-c", "exit 3"],
    );
    let child = q.submit(
        &["--name", "child", "--after-success", &parent.to_string()],
        &["sh", "-c", "true"],
    );
    let grandchild = q.submit(
        &["--name", "grandchild", "--after-success", &child.to_string()],
        &["sh", "-c", "true"],
    );
    let cleanup = q.submit(
        &["--name", "cleanup", "--after-terminal", &parent.to_string()],
        &["sh", "-c", "true"],
    );

    // The parent is not terminal between automatic attempts, so neither
    // dependency condition resolves during the retry window.
    wait_for("parent retry to be scheduled", Duration::from_secs(10), || {
        let view = q.job(parent);
        view["state"] == "queued" && view["attemptCount"] == 1
    });
    assert_eq!(q.job_state(child), "queued");
    assert!(
        q.job(child)["eligibility"]
            .as_str()
            .unwrap()
            .starts_with("waiting_for_dependencies")
    );
    assert_eq!(q.job_state(cleanup), "queued");

    q.wait_state(parent, "failed", Duration::from_secs(15));
    q.wait_state(child, "skipped", Duration::from_secs(10));
    q.wait_state(grandchild, "skipped", Duration::from_secs(10));
    q.wait_state(cleanup, "succeeded", Duration::from_secs(10));
    assert_eq!(
        q.try_cli(&["wait", &child.to_string()]).status.code(),
        Some(1),
        "waiting on a skipped job returns a stable generic failure"
    );

    let child_view = q.job(child);
    assert_eq!(child_view["dependencies"][0]["condition"], "succeeded");
    assert_eq!(child_view["dependencies"][0]["resolution"], "violated");
    assert_eq!(child_view["dependencies"][0]["parentOutcome"], "failed");
    let cleanup_view = q.job(cleanup);
    assert_eq!(cleanup_view["dependencies"][0]["condition"], "terminal");
    assert_eq!(cleanup_view["dependencies"][0]["resolution"], "satisfied");

    // Retrying the parent cannot change latched descendant outcomes.
    q.cli(&["retry", &parent.to_string()]);
    q.wait_state(parent, "failed", Duration::from_secs(15));
    assert_eq!(q.job_state(child), "skipped");
    assert_eq!(q.job_state(cleanup), "succeeded");
}

#[test]
fn successful_dependencies_persist_across_daemon_restart() {
    let mut q = TestQueue::new();
    let parent = q.submit(&["--name", "parent"], &["sh", "-c", "sleep 1; true"]);
    let child = q.submit(
        &["--name", "child", "--after-success", &parent.to_string()],
        &["sh", "-c", "true"],
    );
    assert!(
        q.job(child)["eligibility"]
            .as_str()
            .unwrap()
            .starts_with("waiting_for_dependencies")
    );

    q.kill_daemon();
    q.start_daemon();
    q.wait_state(parent, "succeeded", Duration::from_secs(15));
    q.wait_state(child, "succeeded", Duration::from_secs(10));
}

#[test]
fn dependency_submission_rejects_a_missing_parent_atomically() {
    let q = TestQueue::new();
    let before = q.status()["jobs"].as_array().unwrap().len();
    let output = q.try_cli(&[
        "submit",
        "--after-success",
        "999999",
        "--cwd",
        q.dir.path().to_str().unwrap(),
        "--",
        "sh",
        "-c",
        "true",
    ]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("not_found"));
    assert_eq!(q.status()["jobs"].as_array().unwrap().len(), before);
}

#[test]
fn daemon_crash_neither_kills_nor_duplicates_running_work() {
    let q = TestQueue::new();
    let marker = q.dir.path().join("ran-count");
    let script = format!("sleep 1.5; echo done >> {}", marker.display());
    let job = q.submit(&["--name", "survivor"], &["sh", "-c", &script]);
    q.wait_state(job, "running", Duration::from_secs(10));

    let mut q = q;
    q.kill_daemon();
    std::thread::sleep(Duration::from_millis(300));
    q.start_daemon();

    // The worker kept running through the crash; the restarted daemon adopts
    // it and finalizes the durable result exactly once.
    q.wait_state(job, "succeeded", Duration::from_secs(15));
    wait_for("marker file", Duration::from_secs(5), || marker.exists());
    let content = std::fs::read_to_string(&marker).unwrap();
    assert_eq!(content, "done\n", "command must have run exactly once");

    // Queue still fully operational after recovery.
    let next = q.submit(&["--name", "post-restart"], &["sh", "-c", "true"]);
    q.wait_state(next, "succeeded", Duration::from_secs(10));
}

#[test]
fn hold_excludes_from_scheduling_until_release() {
    let q = TestQueue::new();
    let blocker = q.submit(&["--name", "blocker"], &["sleep", "1"]);
    let held = q.submit(&["--name", "held"], &["sh", "-c", "true"]);
    q.cli(&["hold", &held.to_string()]);
    q.wait_state(blocker, "succeeded", Duration::from_secs(10));

    // Slot is free but the held job must not start.
    std::thread::sleep(Duration::from_millis(400));
    let view = q.job(held);
    assert_eq!(view["state"], "held");
    assert_eq!(view["eligibility"], "held");

    q.cli(&["release", &held.to_string()]);
    q.wait_state(held, "succeeded", Duration::from_secs(10));
}

#[test]
fn pause_blocks_new_starts_until_unpause() {
    let mut q = TestQueue::new();

    let running = q.submit(&["--name", "running"], &["sleep", "2"]);
    q.wait_state(running, "running", Duration::from_secs(5));

    assert_eq!(q.cli(&["pause"]).trim(), "queue paused");
    assert_eq!(q.status()["paused"], serde_json::json!(true));
    assert_eq!(q.cli_json(&["daemon", "status", "--json"])["paused"], true);
    assert_eq!(q.cli_json(&["pause", "--json"])["paused"], true);

    let queued = q.submit(&["--name", "queued"], &["sh", "-c", "true"]);
    q.wait_state(running, "succeeded", Duration::from_secs(10));
    std::thread::sleep(Duration::from_millis(400));
    let view = q.job(queued);
    assert_eq!(view["state"], "queued");
    assert_eq!(view["eligibility"], "paused");

    q.kill_daemon();
    q.start_daemon();
    std::thread::sleep(Duration::from_millis(400));
    assert_eq!(q.job_state(queued), "queued");
    assert_eq!(q.status()["paused"], serde_json::json!(true));

    assert_eq!(q.cli(&["unpause"]).trim(), "queue unpaused");
    q.wait_state(queued, "succeeded", Duration::from_secs(10));
    assert_eq!(q.status()["paused"], serde_json::json!(false));
}

#[test]
fn second_daemon_instance_is_refused() {
    let q = TestQueue::new();
    let mut second = q.spawn_daemon();
    let status = second.wait().unwrap();
    assert!(!status.success(), "second daemon must refuse to start while the lock is held");
}

#[test]
fn set_max_parallel_runs_updates_queued_and_live_jobs_safely() {
    let q = TestQueue::new();
    let first = q.submit(
        &["--max-parallel-runs", "2", "--name", "first"],
        &["sleep", "30"],
    );
    let second = q.submit(
        &["--max-parallel-runs", "2", "--name", "second"],
        &["sleep", "30"],
    );
    for job in [first, second] {
        q.wait_state(job, "running", Duration::from_secs(10));
    }

    let queued = q.submit(
        &["--max-parallel-runs", "2", "--name", "tune-me"],
        &["sleep", "30"],
    );

    // Reapplying the same value is a true no-op, including for this protected
    // queue head; it must not perturb the active reservation.
    q.cli(&["set-max-parallel-runs", &queued.to_string(), "2"]);
    assert_eq!(q.status()["reservation"]["protectedJob"], queued);

    q.cli(&["set-max-parallel-runs", &queued.to_string(), "3"]);
    assert_eq!(q.job(queued)["maxParallelRuns"], 3);
    // Both active declarations still cap the machine at two, so changing only
    // the queued candidate cannot admit it.
    assert_eq!(q.job_state(queued), "queued");

    // Raising one live declaration is not enough; raising both opens the third
    // slot and the queued job starts on the mutation-triggered scheduler pass.
    let message = q.cli(&["set-max-parallel-runs", &first.to_string(), "3"]);
    assert!(
        message.contains("maxParallelRuns is now 3"),
        "message: {message}"
    );
    assert_eq!(q.job_state(queued), "queued");
    q.cli(&["set-max-parallel-runs", &second.to_string(), "3"]);
    q.wait_state(queued, "running", Duration::from_secs(10));

    // Job, attempt, and lease views all reflect the live change.
    for job in [first, second] {
        let detail = q.job(job);
        assert_eq!(detail["maxParallelRuns"], 3);
        assert_eq!(detail["attempts"][0]["maxParallelRuns"], 3);
    }
    assert_eq!(q.status()["effectiveLimit"], 3);

    // Three leases are active. Lowering one declaration to 2 makes the set
    // temporarily oversubscribed (no preemption); the limit still applies to
    // future admissions, and the job/attempt/lease copies all update now.
    q.cli(&["set-max-parallel-runs", &first.to_string(), "2"]);
    let detail = q.job(first);
    assert_eq!(detail["maxParallelRuns"], 2);
    assert_eq!(detail["attempts"][0]["maxParallelRuns"], 2);
    assert_eq!(q.status()["effectiveLimit"], 2);
    assert_eq!(q.status()["activeLeases"], 3);

    // A fourth job must stay queued while three leases remain and the
    // effective declaration is only 2.
    let fourth = q.submit(
        &["--max-parallel-runs", "3", "--name", "must-wait"],
        &["sleep", "30"],
    );
    std::thread::sleep(Duration::from_millis(400));
    assert_eq!(q.job_state(fourth), "queued");

    // After the third lease drains, two remain at effective limit 2; still no
    // room for a third start until someone else finishes or limits rise.
    q.cli(&["cancel", &queued.to_string()]);
    q.wait_state(queued, "cancelled", Duration::from_secs(10));
    assert_eq!(q.status()["activeLeases"], 2);
    assert_eq!(q.status()["effectiveLimit"], 2);
    std::thread::sleep(Duration::from_millis(400));
    assert_eq!(q.job_state(fourth), "queued");

    // Raise the tight declaration so three can coexist again; the waiter
    // admits on the mutation-triggered scheduler pass.
    q.cli(&["set-max-parallel-runs", &first.to_string(), "3"]);
    q.wait_state(fourth, "running", Duration::from_secs(10));
    assert_eq!(q.status()["activeLeases"], 3);
    assert_eq!(q.status()["effectiveLimit"], 3);

    for job in [first, second, fourth] {
        q.cli(&["cancel", &job.to_string()]);
        q.wait_state(job, "cancelled", Duration::from_secs(10));
    }
}

#[test]
fn set_priority_updates_queued_jobs_and_rejects_running() {
    let q = TestQueue::new();
    let blocker = q.submit(&["--name", "blocker"], &["sleep", "30"]);
    q.wait_state(blocker, "running", Duration::from_secs(10));

    let first = q.submit(&["--name", "first", "--priority", "0"], &["sleep", "30"]);
    let second = q.submit(&["--name", "second", "--priority", "0"], &["sleep", "30"]);
    assert_eq!(
        q.status()["reservation"]["protectedJob"].as_i64(),
        Some(first),
        "FIFO protection while priorities are equal"
    );

    // Same value is a no-op that leaves the protected reservation alone.
    let message = q.cli(&["set-priority", &first.to_string(), "0"]);
    assert!(
        message.contains("priority is now 0"),
        "message: {message}"
    );
    assert_eq!(
        q.status()["reservation"]["protectedJob"].as_i64(),
        Some(first)
    );

    // Raising a later job above the protected head supersedes the reservation.
    let message = q.cli(&["set-priority", &second.to_string(), "2"]);
    assert!(
        message.contains("priority is now 2"),
        "message: {message}"
    );
    assert_eq!(q.job(second)["priority"], 2);
    assert_eq!(
        q.status()["reservation"]["protectedJob"].as_i64(),
        Some(second)
    );

    // Held jobs may also be retuned before release.
    q.cli(&["hold", &first.to_string()]);
    q.cli(&["set-priority", &first.to_string(), "-1"]);
    assert_eq!(q.job(first)["priority"], -1);
    assert_eq!(q.job_state(first), "held");

    // Running work cannot change priority (no preemption).
    let output = q.try_cli(&["set-priority", &blocker.to_string(), "9"]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("queued or held"),
        "stderr: {stderr}"
    );
    assert_eq!(q.job(blocker)["priority"], 0);

    // Negative priorities parse as a positional value.
    q.cli(&["release", &first.to_string()]);
    q.cli(&["set-priority", &first.to_string(), "-4"]);
    assert_eq!(q.job(first)["priority"], -4);

    q.cli(&["cancel", &blocker.to_string()]);
    q.wait_state(blocker, "cancelled", Duration::from_secs(10));
    q.wait_state(second, "running", Duration::from_secs(10));
    assert_eq!(q.job_state(first), "queued");

    q.cli(&["cancel", &second.to_string()]);
    q.wait_state(second, "cancelled", Duration::from_secs(10));
    q.wait_state(first, "running", Duration::from_secs(10));
    q.cli(&["cancel", &first.to_string()]);
    q.wait_state(first, "cancelled", Duration::from_secs(10));
}

#[test]
fn signed_priorities_strictly_order_queued_jobs_without_preempting_running_work() {
    let q = TestQueue::new();
    let blocker = q.submit(
        &["--priority", "-5", "--name", "running-low-priority"],
        &["sleep", "30"],
    );
    q.wait_state(blocker, "running", Duration::from_secs(10));

    let low = q.submit(&["--priority", "-1", "--name", "low"], &["sleep", "30"]);
    let default = q.submit(&["--name", "default"], &["sleep", "30"]);
    let high = q.submit(&["--priority", "1", "--name", "high"], &["sleep", "30"]);

    // Higher-priority arrivals supersede queued reservations, not active
    // leases. The already-running negative-priority job remains untouched.
    assert_eq!(q.job_state(blocker), "running");
    assert_eq!(q.job(high)["priority"], 1);
    assert_eq!(q.job(default)["priority"], 0);
    assert_eq!(q.job(low)["priority"], -1);
    assert_eq!(
        q.status()["reservation"]["protectedJob"].as_i64(),
        Some(high)
    );
    assert!(
        q.job(default)["eligibility"]
            .as_str()
            .unwrap()
            .starts_with("waiting_for_higher_priority"),
        "default-priority work should wait behind priority 1"
    );
    let status = q.cli(&["status"]);
    assert!(status.contains("PRI"), "status header: {status}");

    q.cli(&["cancel", &blocker.to_string()]);
    q.wait_state(blocker, "cancelled", Duration::from_secs(10));
    q.wait_state(high, "running", Duration::from_secs(10));
    assert_eq!(q.job_state(default), "queued");
    assert_eq!(q.job_state(low), "queued");

    q.cli(&["cancel", &high.to_string()]);
    q.wait_state(high, "cancelled", Duration::from_secs(10));
    q.wait_state(default, "running", Duration::from_secs(10));
    assert_eq!(q.job_state(low), "queued");

    q.cli(&["cancel", &default.to_string()]);
    q.wait_state(default, "cancelled", Duration::from_secs(10));
    q.wait_state(low, "running", Duration::from_secs(10));
    q.cli(&["cancel", &low.to_string()]);
    q.wait_state(low, "cancelled", Duration::from_secs(10));
}

#[test]
fn wait_blocks_until_terminal_and_exits_with_the_outcome() {
    let q = TestQueue::new();

    let ok = q.submit(&["--name", "ok"], &["sh", "-c", "sleep 0.3; true"]);
    let output = q.try_cli(&["wait", &ok.to_string()]);
    assert!(output.status.success(), "wait on a succeeding job must exit 0");
    assert!(String::from_utf8_lossy(&output.stdout).contains("succeeded"));

    let bad = q.submit(&["--name", "bad"], &["sh", "-c", "exit 3"]);
    let output = q.try_cli(&["wait", &bad.to_string()]);
    assert_eq!(output.status.code(), Some(3), "wait must propagate the command's exit code");

    let hurt = q.submit(&["--name", "hurt"], &["sh", "-c", "kill -SEGV $$"]);
    let output = q.try_cli(&["wait", &hurt.to_string()]);
    assert_eq!(output.status.code(), Some(128 + 11), "signal deaths map to 128+N");

    let slow = q.submit(&["--name", "slow"], &["sleep", "5"]);
    let output = q.try_cli(&["wait", &slow.to_string(), "--timeout", "300ms"]);
    assert_eq!(output.status.code(), Some(124), "wait --timeout expiry must exit 124");
    // Cancelling before launch would finalize without a signal; the 128+15
    // mapping is only defined for a delivered SIGTERM.
    q.wait_state(slow, "running", Duration::from_secs(10));
    q.cli(&["cancel", &slow.to_string()]);
    let output = q.try_cli(&["wait", &slow.to_string()]);
    assert_eq!(output.status.code(), Some(128 + 15), "cancellation by SIGTERM maps to 128+15");
}

#[test]
fn logs_follow_reports_the_terminal_outcome() {
    let q = TestQueue::new();
    // Tail a live job so the follow loop crosses the running→terminal edge.
    let job = q.submit(&["--name", "tail-me"], &["sh", "-c", "echo body-marker; sleep 0.8; exit 7"]);
    q.wait_state(job, "running", Duration::from_secs(10));

    let output = q.try_cli(&["logs", &job.to_string(), "--follow"]);
    assert_eq!(output.status.code(), Some(7), "follow must reflect the attempt outcome");
    assert!(String::from_utf8_lossy(&output.stdout).contains("body-marker"));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("attempt 1 failed"), "missing terminal report: {stderr}");

    // Without --follow the command only prints current logs; a failed job
    // must not turn that read into a failure.
    let output = q.try_cli(&["logs", &job.to_string()]);
    assert!(output.status.success());

    // Following a succeeding job drains, reports, and exits 0.
    let fine = q.submit(&["--name", "tail-fine"], &["sh", "-c", "echo fine-marker; sleep 0.5"]);
    q.wait_state(fine, "running", Duration::from_secs(10));
    let output = q.try_cli(&["logs", &fine.to_string(), "--follow"]);
    assert!(output.status.success(), "follow on success must exit 0");
    assert!(String::from_utf8_lossy(&output.stdout).contains("fine-marker"));
    assert!(String::from_utf8_lossy(&output.stderr).contains("attempt 1 succeeded"));
}

#[test]
fn see_prints_detail_and_latest_stdout() {
    let q = TestQueue::new();
    let empty = q.try_cli(&["see"]);
    assert!(!empty.status.success());
    assert!(
        String::from_utf8_lossy(&empty.stderr).contains("no jobs"),
        "empty queue: {}",
        String::from_utf8_lossy(&empty.stderr)
    );

    let older = q.submit(
        &["--name", "older", "--env", "MLQ_SEE_MARK=older-stdout-only"],
        &["sh", "-c", "echo \"$MLQ_SEE_MARK\""],
    );
    q.wait_state(older, "succeeded", Duration::from_secs(10));
    let job = q.submit(
        &["--name", "see-me", "--env", "MLQ_SEE_MARK=see-stdout-only"],
        &["sh", "-c", "echo \"$MLQ_SEE_MARK\""],
    );
    q.wait_state(job, "succeeded", Duration::from_secs(10));

    let shown = q.cli(&["see", &job.to_string()]);
    assert!(shown.contains(&format!("job {job} [see-me]")), "{shown}");
    assert!(shown.contains("see-stdout-only"), "{shown}");
    assert!(!shown.contains("older-stdout-only"), "{shown}");

    let latest = q.cli(&["see"]);
    assert!(latest.contains(&format!("job {job} [see-me]")), "{latest}");
    assert!(latest.contains("see-stdout-only"), "{latest}");
    assert!(!latest.contains("older-stdout-only"), "{latest}");

    let json = q.cli_json(&["see", "--json"]);
    assert_eq!(json["id"], job);
    assert_eq!(json["name"], "see-me");

    let missing = q.try_cli(&["see", "999"]);
    assert!(!missing.status.success());
    assert!(
        String::from_utf8_lossy(&missing.stderr).contains("not found"),
        "missing job: {}",
        String::from_utf8_lossy(&missing.stderr)
    );
}
