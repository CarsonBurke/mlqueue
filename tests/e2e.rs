//! End-to-end tests: a real daemon, real runners, and the real CLI, all
//! isolated in a per-test state directory.

use std::path::PathBuf;
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

impl TestQueue {
    fn new() -> Self {
        // Keep the tempdir shallow: the Unix socket path must stay under the
        // 108-byte sockaddr_un limit regardless of TMPDIR.
        let dir = tempfile::Builder::new().prefix("mlq-e2e").tempdir_in("/tmp").unwrap();
        std::fs::write(
            dir.path().join("config.toml"),
            "version = 1\n\
             tick_interval_ms = 40\n\
             runner_poll_ms = 20\n\
             cancel_grace_ms = 800\n\
             runner_identity_grace_ms = 5000\n",
        )
        .unwrap();
        let mut queue = Self { dir, daemon: None };
        queue.start_daemon();
        queue
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
}

impl Drop for TestQueue {
    fn drop(&mut self) {
        if let Some(mut child) = self.daemon.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
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
fn restrictive_job_is_protected_and_frontier_is_frozen() {
    let q = TestQueue::new();
    // Three permissive jobs fill the 3-wide set; `gate` finishes first.
    let a = q.submit(&["--max-parallel-runs", "3", "--name", "a"], &["sleep", "3"]);
    let b = q.submit(&["--max-parallel-runs", "3", "--name", "b"], &["sleep", "3"]);
    let gate = q.submit(&["--max-parallel-runs", "3", "--name", "gate"], &["sleep", "1"]);
    for job in [a, b, gate] {
        q.wait_state(job, "running", Duration::from_secs(10));
    }

    // Dependency-delayed permissive job: ineligible now, but submitted before
    // protection, so it belongs to the frozen frontier once eligible.
    let backfill = q.submit(
        &["--max-parallel-runs", "3", "--name", "backfill", "--after-completion", &gate.to_string()],
        &["sleep", "1"],
    );

    // The exclusive job is the eligible head and cannot start beside the
    // 3-wide set: it becomes the protected job.
    let exclusive =
        q.submit(&["--max-parallel-runs", "1", "--name", "exclusive"], &["sh", "-c", "true"]);
    wait_for("reservation to exist", Duration::from_secs(5), || {
        q.status()["reservation"]["protectedJob"].as_i64() == Some(exclusive)
    });

    // Submitted after protection: frozen out until the protected job runs.
    let late = q.submit(&["--max-parallel-runs", "3", "--name", "late"], &["sh", "-c", "true"]);
    let late_view = q.job(late);
    assert!(
        late_view["eligibility"].as_str().unwrap().starts_with("behind_backfill_cutoff"),
        "late job must be frozen out: {late_view}"
    );

    // When `gate` drains, the pre-cutoff job becomes eligible and takes the
    // open slot as a backfill while the protected job keeps waiting and the
    // post-cutoff job stays frozen.
    q.wait_state(backfill, "running", Duration::from_secs(10));
    assert_eq!(q.job_state(exclusive), "queued");
    assert_eq!(q.job_state(late), "queued");
    let status = q.status();
    assert!(
        status["reservation"]["consumedBypasses"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!(backfill)),
        "backfill bypass must be recorded: {status}"
    );

    // Full drain: the protected job starts (alone), then the late job.
    q.wait_state(exclusive, "succeeded", Duration::from_secs(20));
    q.wait_state(late, "succeeded", Duration::from_secs(20));
    assert!(q.job(late)["finishedAt"].as_i64() >= q.job(exclusive)["finishedAt"].as_i64());
}

#[test]
fn reservation_and_consumed_bypasses_survive_daemon_restart() {
    let mut q = TestQueue::new();
    // Same shape as the frozen-frontier test, but long enough to straddle a
    // daemon crash while the reservation and one consumed bypass exist.
    let a = q.submit(&["--max-parallel-runs", "3", "--name", "a"], &["sleep", "5"]);
    let b = q.submit(&["--max-parallel-runs", "3", "--name", "b"], &["sleep", "5"]);
    let gate = q.submit(&["--max-parallel-runs", "3", "--name", "gate"], &["sleep", "1"]);
    for job in [a, b, gate] {
        q.wait_state(job, "running", Duration::from_secs(10));
    }
    let backfill = q.submit(
        &["--max-parallel-runs", "3", "--name", "backfill", "--after-completion", &gate.to_string()],
        &["sleep", "3"],
    );
    let exclusive =
        q.submit(&["--max-parallel-runs", "1", "--name", "exclusive"], &["sh", "-c", "true"]);
    wait_for("reservation to exist", Duration::from_secs(5), || {
        q.status()["reservation"]["protectedJob"].as_i64() == Some(exclusive)
    });
    let late = q.submit(&["--max-parallel-runs", "3", "--name", "late"], &["sh", "-c", "true"]);

    // The bypass is consumed, then the daemon crashes.
    q.wait_state(backfill, "running", Duration::from_secs(10));
    q.kill_daemon();
    q.start_daemon();

    // The restarted daemon restores the reservation, its cutoff semantics,
    // and the consumed set from the database rather than recomputing them.
    let status = q.status();
    assert_eq!(status["reservation"]["protectedJob"].as_i64(), Some(exclusive));
    assert!(
        status["reservation"]["consumedBypasses"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!(backfill)),
        "consumed bypass must survive the restart: {status}"
    );
    assert_eq!(q.job_state(exclusive), "queued");
    assert!(
        q.job(late)["eligibility"].as_str().unwrap().starts_with("behind_backfill_cutoff"),
        "post-cutoff job must stay frozen after restart"
    );

    // Ordering guarantees still hold through the drain.
    q.wait_state(exclusive, "succeeded", Duration::from_secs(30));
    q.wait_state(late, "succeeded", Duration::from_secs(30));
    assert!(q.job(late)["finishedAt"].as_i64() >= q.job(exclusive)["finishedAt"].as_i64());
}

#[test]
fn cancelling_the_protected_job_invalidates_the_reservation() {
    let q = TestQueue::new();
    let a = q.submit(&["--max-parallel-runs", "3", "--name", "a"], &["sleep", "4"]);
    let b = q.submit(&["--max-parallel-runs", "3", "--name", "b"], &["sleep", "4"]);
    let gate = q.submit(&["--max-parallel-runs", "3", "--name", "gate"], &["sleep", "1"]);
    for job in [a, b, gate] {
        q.wait_state(job, "running", Duration::from_secs(10));
    }
    let exclusive =
        q.submit(&["--max-parallel-runs", "1", "--name", "exclusive"], &["sleep", "5"]);
    wait_for("reservation to exist", Duration::from_secs(5), || {
        q.status()["reservation"]["protectedJob"].as_i64() == Some(exclusive)
    });
    let late = q.submit(&["--max-parallel-runs", "3", "--name", "late"], &["sleep", "1"]);

    // `gate` drains, but the frozen frontier keeps the open slot away from
    // the post-cutoff job while the protected job waits.
    q.wait_state(gate, "succeeded", Duration::from_secs(10));
    assert!(
        q.job(late)["eligibility"].as_str().unwrap().starts_with("behind_backfill_cutoff"),
        "late job must be frozen while the reservation stands"
    );

    // Cancelling the protected job invalidates the reservation, releasing
    // the frontier: the late job takes the open slot beside a and b.
    q.cli(&["cancel", &exclusive.to_string()]);
    wait_for("reservation to be invalidated", Duration::from_secs(5), || {
        q.status()["reservation"].is_null()
    });
    q.wait_state(late, "running", Duration::from_secs(10));
    assert_eq!(q.job_state(a), "running");
    assert_eq!(q.job_state(b), "running");
}

#[test]
fn cancel_terminates_and_frees_the_lease() {
    let q = TestQueue::new();
    let job = q.submit(&["--name", "long"], &["sleep", "60"]);
    q.wait_state(job, "running", Duration::from_secs(10));

    q.cli(&["cancel", &job.to_string()]);
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
fn failed_prerequisite_skips_descendants_but_completion_children_run() {
    let q = TestQueue::new();
    let parent = q.submit(&["--name", "parent"], &["sh", "-c", "exit 3"]);
    let child = q.submit(
        &["--name", "child", "--after-success", &parent.to_string()],
        &["sh", "-c", "true"],
    );
    let grandchild = q.submit(
        &["--name", "grandchild", "--after-success", &child.to_string()],
        &["sh", "-c", "true"],
    );
    let cleanup = q.submit(
        &["--name", "cleanup", "--after-completion", &parent.to_string()],
        &["sh", "-c", "true"],
    );

    q.wait_state(parent, "failed", Duration::from_secs(10));
    q.wait_state(child, "skipped", Duration::from_secs(10));
    q.wait_state(grandchild, "skipped", Duration::from_secs(10));
    q.wait_state(cleanup, "succeeded", Duration::from_secs(10));

    let detail = q.job(parent);
    assert_eq!(detail["attempts"][0]["exitCode"], 3);
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
fn second_daemon_instance_is_refused() {
    let q = TestQueue::new();
    let mut second = q.spawn_daemon();
    let status = second.wait().unwrap();
    assert!(!status.success(), "second daemon must refuse to start while the lock is held");
}

#[test]
fn set_max_parallel_runs_only_while_queued() {
    let q = TestQueue::new();
    let blocker = q.submit(&["--name", "blocker"], &["sleep", "1.5"]);
    q.wait_state(blocker, "running", Duration::from_secs(10));
    let queued = q.submit(&["--name", "tune-me"], &["sh", "-c", "true"]);

    q.cli(&["set-max-parallel-runs", &queued.to_string(), "3"]);
    assert_eq!(q.job(queued)["maxParallelRuns"], 3);
    // Now compatible with the running blocker? No: blocker declared 1, so the
    // queued job still waits — the lower limit wins.
    assert_eq!(q.job_state(queued), "queued");

    // Immutable once running.
    let output = q.try_cli(&["set-max-parallel-runs", &blocker.to_string(), "2"]);
    assert!(!output.status.success());

    q.wait_state(queued, "succeeded", Duration::from_secs(10));
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
