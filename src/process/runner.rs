//! The attempt runner: a small blocking supervisor that outlives the daemon.
//!
//! The daemon spawns one runner per attempt in a fresh session. The runner
//! waits for durable launch authorization, forks the command into its own
//! process group, supervises it, delivers cancellation signals from outside
//! that group, and publishes an atomic terminal result only once the whole
//! command process group is provably empty.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use crate::domain::now_ms;
use crate::process::artifacts::{
    self, CANCEL_FILE, COMMAND_FILE, CancelFile, CommandFile, ExecFile, IDENTITY_FILE,
    IdentityFile, RESULT_FILE, RUNNER_LOCK_FILE, ResultFile, ResultOutcome, START_FILE,
    STDERR_LOG_FILE, STDOUT_LOG_FILE, StartFile,
};
use crate::process::identity;

/// Entry point for `mlqueued __runner --attempt-dir DIR`. Returns the process
/// exit code; failures before `command.json` is readable cannot publish a
/// result and surface only through runner death.
pub fn runner_main(attempt_dir: &Path) -> i32 {
    match run(attempt_dir) {
        Ok(()) => 0,
        Err(err) => {
            log_line(attempt_dir, &format!("runner failed: {err:#}"));
            1
        }
    }
}

fn log_line(dir: &Path, message: &str) {
    if let Ok(mut file) = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(dir.join(artifacts::RUNNER_LOG_FILE))
    {
        let _ = writeln!(file, "[{}] {message}", now_ms());
    }
}

fn run(dir: &Path) -> anyhow::Result<()> {
    // The daemon ignores SIGCHLD to auto-reap detached runners, and ignored
    // dispositions survive exec: restore the default so waitpid on our own
    // command child works (and so the command inherits a clean disposition).
    unsafe {
        libc::signal(libc::SIGCHLD, libc::SIG_DFL);
    }

    // A per-attempt exclusive lock prevents duplicate runners for one
    // attempt directory; held for the runner's whole life.
    let lock = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .mode(0o600)
        .open(dir.join(RUNNER_LOCK_FILE))?;
    let rc = unsafe { libc::flock(std::os::unix::io::AsRawFd::as_raw_fd(&lock), libc::LOCK_EX | libc::LOCK_NB) };
    if rc != 0 {
        anyhow::bail!("another runner already owns this attempt directory");
    }

    let command: CommandFile = artifacts::read_artifact(&dir.join(COMMAND_FILE))?
        .ok_or_else(|| anyhow::anyhow!("missing {COMMAND_FILE}"))?;
    let poll = Duration::from_millis(command.poll_ms.max(10));

    // Publish verified runner identity for the daemon to attach.
    let self_pid = std::process::id() as i32;
    let self_stat = identity::proc_stat(self_pid)
        .ok_or_else(|| anyhow::anyhow!("cannot read own /proc stat"))?;
    let identity_record = IdentityFile {
        token: command.token.clone(),
        runner_pid: self_pid,
        runner_start_time: self_stat.start_time,
        boot_id: identity::boot_id()?,
    };
    // Non-exclusive: a recovery runner replacing a dead predecessor (the
    // flock serializes them) must be able to re-announce itself over the
    // predecessor's stale identity.
    artifacts::write_artifact(dir, IDENTITY_FILE, &identity_record, false)?;
    log_line(dir, &format!("runner {self_pid} waiting for authorization"));

    // Wait for durable launch authorization or committed cancellation.
    let deadline = Instant::now() + Duration::from_millis(command.start_wait_ms);
    loop {
        if let Some(cancel) = read_cancel(dir, &command.token) {
            let _ = cancel;
            log_line(dir, "cancelled before launch authorization");
            return publish_result(dir, &command, ResultOutcome::Cancelled, None, None,
                Some("cancelled before launch".to_string()));
        }
        match artifacts::read_artifact::<StartFile>(&dir.join(START_FILE)) {
            Ok(Some(start)) if start.token == command.token => break,
            Ok(Some(_)) => {
                return publish_result(dir, &command, ResultOutcome::LaunchFailed, None, None,
                    Some("authorization token mismatch".to_string()));
            }
            Ok(None) => {}
            // A partially visible artifact is unexpected (writes are atomic);
            // treat as fatal rather than guessing.
            Err(err) => {
                return publish_result(dir, &command, ResultOutcome::LaunchFailed, None, None,
                    Some(format!("corrupt start artifact: {err}")));
            }
        }
        if Instant::now() >= deadline {
            log_line(dir, "timed out waiting for launch authorization");
            return publish_result(dir, &command, ResultOutcome::AuthorizationTimeout, None, None,
                Some(format!("no authorization within {} ms", command.start_wait_ms)));
        }
        std::thread::sleep(poll);
    }

    // Launch the command in its own process group. std::process reports
    // execve/chdir failure through its internal pipe, so a spawn error means
    // the command never ran.
    let stdout = log_file(dir, STDOUT_LOG_FILE)?;
    let stderr = log_file(dir, STDERR_LOG_FILE)?;
    let mut cmd = Command::new(&command.argv[0]);
    cmd.args(&command.argv[1..])
        .current_dir(&command.cwd)
        .env_clear()
        .envs(&command.env)
        .stdin(Stdio::null())
        .stdout(stdout)
        .stderr(stderr);
    unsafe {
        cmd.pre_exec(|| {
            if libc::setpgid(0, 0) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = match cmd.spawn() {
        Ok(child) => child,
        Err(err) => {
            log_line(dir, &format!("launch failed: {err}"));
            return publish_result(dir, &command, ResultOutcome::LaunchFailed, None, None,
                Some(format!("launch failed: {err}")));
        }
    };
    let child_pid = child.id() as i32;
    let pgid = child_pid;
    let child_start_time =
        identity::proc_stat(child_pid).map(|stat| stat.start_time).unwrap_or_default();
    artifacts::write_artifact(
        dir,
        artifacts::EXEC_FILE,
        &ExecFile {
            token: command.token.clone(),
            pid: child_pid,
            pgid,
            start_time: child_start_time,
            boot_id: identity::boot_id()?,
        },
        true,
    )?;
    log_line(dir, &format!("command running: pid {child_pid} pgid {pgid}"));

    // Supervision loop. Linearization of cancellation against natural
    // completion: a signal delivered before the exit was observed makes the
    // terminal result `cancelled`; a cancel that arrives after natural exit
    // is recorded as too late.
    let mut exit_status: Option<std::process::ExitStatus> = None;
    let mut term_sent = false;
    let mut cancelled_before_exit = false;
    let mut cancel_too_late = false;
    let mut kill_at: Option<Instant> = None;
    let mut kill_sent = false;
    let mut empty_scans = 0u32;
    let grace = Duration::from_millis(command.cancel_grace_ms);
    loop {
        // Observe a natural exit before processing cancellation, so a command
        // that already finished in the last poll window wins the race and a
        // simultaneous cancel is recorded as too late rather than rewriting a
        // genuine completion.
        if exit_status.is_none()
            && let Some(status) = peek_exit(child_pid)?
        {
            // WNOWAIT peek: the leader stays a zombie, pinning its PID so the
            // numeric pgid cannot be recycled by an unrelated new group while
            // we scan for surviving members.
            log_line(dir, &format!("command leader exited: {status}"));
            exit_status = Some(status);
        }
        if let Some(cancel) = read_cancel(dir, &command.token) {
            if !term_sent {
                log_line(dir, &format!("delivering SIGTERM to group {pgid}"));
                signal_group(pgid, libc::SIGTERM);
                term_sent = true;
                if exit_status.is_none() {
                    cancelled_before_exit = true;
                } else {
                    cancel_too_late = true;
                }
            }
            if cancel.force && kill_at.is_none() && !kill_sent {
                kill_at = Some(Instant::now() + grace);
            }
        }
        if let Some(at) = kill_at
            && Instant::now() >= at
        {
            log_line(dir, &format!("grace expired; delivering SIGKILL to group {pgid}"));
            signal_group(pgid, libc::SIGKILL);
            kill_sent = true;
            kill_at = None;
        }
        // The lease is not releasable until the complete process group is
        // gone, including reparented descendants the runner cannot waitpid.
        // Two consecutive empty scans close the window where a member forks
        // and exits between /proc directory reads.
        if exit_status.is_some() && identity::group_empty_except(pgid, child_pid) {
            empty_scans += 1;
            if empty_scans >= 2 {
                break;
            }
        } else {
            empty_scans = 0;
        }
        std::thread::sleep(poll);
    }

    // Only now reap the zombie leader; the group is provably empty.
    unsafe {
        let mut status: libc::c_int = 0;
        libc::waitpid(child_pid, &mut status, 0);
    }
    drop(child);
    let status = exit_status.expect("loop exits only after leader exit");
    let (outcome, exit_code, term_signal, message) = if cancelled_before_exit {
        (
            ResultOutcome::Cancelled,
            status.code(),
            status.signal(),
            Some("cancelled by request".to_string()),
        )
    } else {
        let message = cancel_too_late.then(|| "cancellation_too_late".to_string());
        (ResultOutcome::Exited, status.code(), status.signal(), message)
    };
    publish_result(dir, &command, outcome, exit_code, term_signal, message)
}

/// Non-destructive exit-status check: `WNOWAIT` reports the status while
/// leaving the child as a reapable zombie, so its PID (and therefore the
/// command's pgid) stays pinned until we have finished scanning the group.
fn peek_exit(pid: i32) -> std::io::Result<Option<std::process::ExitStatus>> {
    // WNOWAIT is a waitid-only flag (waitpid rejects it with EINVAL).
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    let rc = unsafe {
        libc::waitid(
            libc::P_PID,
            pid as libc::id_t,
            &mut info,
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // With WNOHANG, "no state change yet" is rc == 0 with si_pid left zero.
    if unsafe { info.si_pid() } == 0 {
        return Ok(None);
    }
    let status = unsafe { info.si_status() };
    let raw = match info.si_code {
        libc::CLD_EXITED => (status & 0xff) << 8,
        libc::CLD_DUMPED => status | 0x80,
        _ => status, // CLD_KILLED: the terminating signal number
    };
    Ok(Some(std::process::ExitStatus::from_raw(raw)))
}

fn log_file(dir: &Path, name: &str) -> std::io::Result<File> {
    OpenOptions::new().create(true).append(true).mode(0o600).open(dir.join(name))
}

fn read_cancel(dir: &Path, token: &str) -> Option<CancelFile> {
    match artifacts::read_artifact::<CancelFile>(&dir.join(CANCEL_FILE)) {
        Ok(Some(cancel)) if cancel.token == token => Some(cancel),
        _ => None,
    }
}

fn signal_group(pgid: i32, signal: i32) {
    // ESRCH (already empty) is fine; anything else is unexpected but the
    // /proc scan remains the source of truth for emptiness.
    unsafe {
        libc::killpg(pgid, signal);
    }
}

fn publish_result(
    dir: &Path,
    command: &CommandFile,
    outcome: ResultOutcome,
    exit_code: Option<i32>,
    term_signal: Option<i32>,
    message: Option<String>,
) -> anyhow::Result<()> {
    let result = ResultFile {
        token: command.token.clone(),
        outcome,
        exit_code,
        term_signal,
        message,
        finished_at: now_ms(),
    };
    artifacts::write_artifact(dir, RESULT_FILE, &result, true)?;
    log_line(dir, &format!("result published: {outcome:?} code={exit_code:?} signal={term_signal:?}"));
    Ok(())
}
