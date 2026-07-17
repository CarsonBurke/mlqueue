//! The `mlq` CLI: argument parsing, environment capture, idempotency-key
//! generation, and human/JSON rendering of daemon replies.

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};

use crate::client::{self, ClientError};
use crate::domain::{AttemptState, JobId, JobState};
use crate::paths::Paths;
use crate::protocol::{
    EventView, JobView, LogPathsView, Op, Reply, ResolveAs, StatusView, SubmitParams,
};

/// Environment variables captured by default at submission. Everything else
/// must be passed explicitly via `--env` or `--inherit-env`.
const BASELINE_ENV: &[&str] = &["PATH", "HOME", "USER", "LOGNAME", "SHELL", "LANG", "TMPDIR"];

const SENSITIVE_MARKERS: &[&str] =
    &["TOKEN", "SECRET", "PASSWORD", "PASSWD", "APIKEY", "API_KEY", "CREDENTIAL", "PRIVATE"];

const STATUS_NAME_WIDTH: usize = 32;
const STATUS_QUEUE_LIMIT: usize = 10;
const STATUS_FINISHED_LIMIT: usize = 10;
const STATUS_HELD_LIMIT: usize = 10;

#[derive(Parser)]
#[command(
    name = "mlq",
    version,
    about = "Machine-wide queue for local ML commands (talks to mlqd)"
)]
struct Cli {
    /// Durable idempotency key; pass the same key to safely rerun a whole
    /// invocation after losing its output. Generated automatically otherwise.
    #[arg(long, global = true)]
    idempotency_key: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Submit a command to the queue.
    Submit(SubmitArgs),
    /// Show queue state, concurrency declarations, and admission reasons.
    Status {
        #[arg(long)]
        watch: bool,
        #[arg(long)]
        json: bool,
    },
    /// Announce queue transitions through the local `tts` command.
    FollowTts {
        /// Override the backend selected by the local `tts` configuration.
        #[arg(long)]
        tts_backend: Option<String>,
    },
    /// Show one job in detail, including its attempts.
    Show {
        job: JobId,
        #[arg(long)]
        json: bool,
    },
    /// Print (or follow) an attempt's captured output.
    Logs {
        job: JobId,
        /// Attempt number (defaults to the latest attempt).
        #[arg(long)]
        attempt: Option<i64>,
        /// Read stderr instead of stdout.
        #[arg(long)]
        stderr: bool,
        #[arg(long, short = 'f')]
        follow: bool,
    },
    /// Block until a job reaches a terminal state, then exit with its
    /// outcome: 0 on success, the command's exit code (or 128+signal) on
    /// failure, 124 on --timeout.
    Wait {
        job: JobId,
        /// Give up after this long (e.g. "90s", "2h") and exit 124.
        #[arg(long, value_parser = humantime::parse_duration)]
        timeout: Option<Duration>,
        #[arg(long)]
        json: bool,
    },
    /// Cancel a job. Running work receives SIGTERM; --force escalates to
    /// SIGKILL after the configured grace period.
    Cancel {
        job: JobId,
        #[arg(long)]
        force: bool,
        #[arg(long)]
        json: bool,
    },
    /// Hold a queued job so the scheduler skips it.
    Hold {
        job: JobId,
        #[arg(long)]
        json: bool,
    },
    /// Release a held job back to the queue.
    Release {
        job: JobId,
        #[arg(long)]
        json: bool,
    },
    /// Requeue a failed or lost job for one more attempt.
    Retry {
        job: JobId,
        #[arg(long)]
        json: bool,
    },
    /// Change a queued job's concurrency declaration.
    SetMaxParallelRuns {
        job: JobId,
        max_parallel_runs: u32,
        #[arg(long)]
        json: bool,
    },
    /// Daemon operations.
    Daemon {
        #[command(subcommand)]
        command: DaemonCommand,
    },
    /// Inspect and resolve attempts that need operator attention.
    Recover {
        #[command(subcommand)]
        command: RecoverCommand,
    },
}

#[derive(Subcommand)]
enum DaemonCommand {
    /// Report daemon liveness and counters.
    Status {
        #[arg(long)]
        json: bool,
    },
    /// Run mlqd in the foreground (this process becomes the daemon).
    Run,
    /// Install mlqd as a systemd user service and start it.
    Install {
        /// Write the unit file only; do not enable or start the service.
        #[arg(long)]
        no_enable: bool,
    },
    /// Stop, disable, and remove the systemd user service.
    Uninstall,
}

#[derive(Subcommand)]
enum RecoverCommand {
    /// List orphaned and quarantined attempts.
    List {
        #[arg(long)]
        json: bool,
    },
    /// Resolve an orphaned/quarantined attempt once its processes are gone.
    Resolve {
        job: JobId,
        #[arg(long)]
        attempt: i64,
        #[arg(long = "as", value_parser = parse_resolve_as)]
        resolve_as: ResolveAsArg,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Clone, Copy)]
enum ResolveAsArg {
    Lost,
    Cancelled,
}

fn parse_resolve_as(value: &str) -> Result<ResolveAsArg, String> {
    match value {
        "lost" => Ok(ResolveAsArg::Lost),
        "cancelled" => Ok(ResolveAsArg::Cancelled),
        other => Err(format!("expected 'lost' or 'cancelled', got {other:?}")),
    }
}

#[derive(Args)]
struct SubmitArgs {
    /// Job name (defaults to the executable's basename).
    #[arg(long)]
    name: Option<String>,
    /// Working directory (defaults to the current directory), canonicalized.
    #[arg(long)]
    cwd: Option<PathBuf>,
    /// This job is safe only while the total number of concurrent managed
    /// jobs, including itself, stays at or below this value.
    #[arg(long, default_value_t = 1)]
    max_parallel_runs: u32,
    /// Total attempts allowed before the job fails permanently.
    #[arg(long, default_value_t = 1)]
    max_attempts: u32,
    /// Delay before an automatic retry (e.g. "30s", "5m").
    #[arg(long, value_parser = humantime::parse_duration)]
    retry_delay: Option<Duration>,
    /// Start only after this job succeeds (repeatable).
    #[arg(long = "after-success")]
    after_success: Vec<JobId>,
    /// Start only after this job reaches any terminal state (repeatable).
    #[arg(long = "after-completion")]
    after_completion: Vec<JobId>,
    /// Explicit KEY=VALUE environment entry (repeatable).
    #[arg(long = "env")]
    env: Vec<String>,
    /// Capture this variable from the submitting environment (repeatable).
    #[arg(long = "inherit-env")]
    inherit_env: Vec<String>,
    #[arg(long)]
    json: bool,
    /// The command to run, after `--`.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true, required = true)]
    command: Vec<String>,
}

pub fn main() -> Result<()> {
    let cli = Cli::parse();
    let paths = Paths::resolve()?;
    // One key per invocation: transport retries inside `client::request` and
    // user-supplied reruns with an explicit key both stay idempotent.
    let key = cli.idempotency_key.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    match cli.command {
        Command::Submit(args) => submit(&paths, args, key),
        Command::Status { watch, json } => status(&paths, watch, json),
        Command::FollowTts { tts_backend } => follow_tts(&paths, tts_backend.as_deref()),
        Command::Show { job, json } => {
            let reply = send(&paths, Op::Show { job }, None)?;
            let Reply::Job { job } = reply else { bail!(client::unexpected("job")) };
            if json {
                println!("{}", serde_json::to_string_pretty(&job)?);
            } else {
                print_job_detail(&job);
            }
            Ok(())
        }
        Command::Logs { job, attempt, stderr, follow } => logs(&paths, job, attempt, stderr, follow),
        Command::Wait { job, timeout, json } => wait(&paths, job, timeout, json),
        Command::Cancel { job, force, json } => {
            mutate_job(&paths, Op::Cancel { job, force }, key, json)
        }
        Command::Hold { job, json } => mutate_job(&paths, Op::Hold { job }, key, json),
        Command::Release { job, json } => mutate_job(&paths, Op::Release { job }, key, json),
        Command::Retry { job, json } => mutate_job(&paths, Op::Retry { job }, key, json),
        Command::SetMaxParallelRuns { job, max_parallel_runs, json } => {
            mutate_job(&paths, Op::SetMaxParallelRuns { job, max_parallel_runs }, key, json)
        }
        Command::Daemon { command: DaemonCommand::Status { json } } => {
            let reply = send(&paths, Op::DaemonStatus, None)?;
            let Reply::DaemonStatus(view) = reply else {
                bail!(client::unexpected("daemon status"))
            };
            if json {
                println!("{}", serde_json::to_string_pretty(&view)?);
            } else {
                println!("mlqd {} (pid {})", view.version, view.pid);
                println!("  socket:         {}", view.socket_path);
                println!("  database:       {}", view.db_path);
                println!("  active leases:  {}", view.active_leases);
                println!("  queued jobs:    {}", view.queued_jobs);
                if view.admission_blocked {
                    println!("  ADMISSION BLOCKED: operator attention required");
                }
            }
            Ok(())
        }
        Command::Daemon { command: DaemonCommand::Run } => daemon_run(),
        Command::Daemon { command: DaemonCommand::Install { no_enable } } => {
            daemon_install(no_enable)
        }
        Command::Daemon { command: DaemonCommand::Uninstall } => daemon_uninstall(),
        Command::Recover { command } => match command {
            RecoverCommand::List { json } => {
                let reply = send(&paths, Op::RecoverList, None)?;
                let Reply::RecoverList { attempts } = reply else {
                    bail!(client::unexpected("recover list"))
                };
                if json {
                    println!("{}", serde_json::to_string_pretty(&attempts)?);
                } else if attempts.is_empty() {
                    println!("no attempts need recovery");
                } else {
                    println!("{:<6} {:<9} {:<12} MESSAGE", "JOB", "ATTEMPT", "STATE");
                    for attempt in attempts {
                        println!(
                            "{:<6} {:<9} {:<12} {}",
                            attempt.job_id,
                            attempt.number,
                            attempt.state,
                            attempt.message.as_deref().unwrap_or("-")
                        );
                    }
                }
                Ok(())
            }
            RecoverCommand::Resolve { job, attempt, resolve_as, json } => {
                let resolve_as = match resolve_as {
                    ResolveAsArg::Lost => ResolveAs::Lost,
                    ResolveAsArg::Cancelled => ResolveAs::Cancelled,
                };
                mutate_job(&paths, Op::RecoverResolve { job, attempt, resolve_as }, key, json)
            }
        },
    }
}

fn send(paths: &Paths, op: Op, key: Option<String>) -> Result<Reply> {
    match client::request(paths, op, key) {
        Ok(reply) => Ok(reply),
        Err(ClientError::Daemon(body)) => bail!("{}: {}", body.code, body.message),
        Err(err) => Err(err.into()),
    }
}

fn mutate_job(paths: &Paths, op: Op, key: String, json: bool) -> Result<()> {
    let reply = send(paths, op, Some(key))?;
    let Reply::Job { job } = reply else { bail!(client::unexpected("job")) };
    if json {
        println!("{}", serde_json::to_string_pretty(&job)?);
    } else {
        let extra = match (&job.eligibility, &job.state_reason) {
            (_, Some(reason)) => format!(" ({reason})"),
            (Some(reason), _) => format!(" ({reason})"),
            _ => String::new(),
        };
        println!("job {} [{}] is now {}{}", job.id, job.name, job.state, extra);
    }
    Ok(())
}

fn submit(paths: &Paths, args: SubmitArgs, key: String) -> Result<()> {
    if args.command.is_empty() {
        bail!("a command is required after `--`");
    }
    let cwd = match args.cwd {
        Some(dir) => dir,
        None => std::env::current_dir()?,
    };
    let cwd = cwd
        .canonicalize()
        .with_context(|| format!("canonicalizing working directory {}", cwd.display()))?;

    // Documented baseline, then client-resolved --inherit-env, then explicit
    // --env (highest precedence). The daemon persists exactly this map.
    let mut env: BTreeMap<String, String> = BTreeMap::new();
    for name in BASELINE_ENV {
        if let Ok(value) = std::env::var(name) {
            env.insert((*name).to_string(), value);
        }
    }
    for name in &args.inherit_env {
        let value = std::env::var(name)
            .with_context(|| format!("--inherit-env {name}: variable is not set"))?;
        env.insert(name.clone(), value);
    }
    for entry in &args.env {
        let (name, value) = entry
            .split_once('=')
            .with_context(|| format!("--env {entry:?} is not KEY=VALUE"))?;
        env.insert(name.to_string(), value.to_string());
    }
    for name in env.keys() {
        let upper = name.to_uppercase();
        if SENSITIVE_MARKERS.iter().any(|marker| upper.contains(marker)) {
            eprintln!(
                "warning: environment variable {name} looks sensitive; it will be stored \
                 in plaintext in the queue database. Prefer a credential file read by the \
                 workload at launch."
            );
        }
    }

    let name = match args.name {
        Some(name) => name,
        None => PathBuf::from(&args.command[0])
            .file_name()
            .map(|base| base.to_string_lossy().into_owned())
            .unwrap_or_else(|| args.command[0].clone()),
    };
    let params = SubmitParams {
        name,
        cwd: cwd.display().to_string(),
        args: args.command,
        env,
        max_parallel_runs: args.max_parallel_runs,
        max_attempts: args.max_attempts,
        retry_delay_ms: args.retry_delay.map(|delay| delay.as_millis() as u64).unwrap_or(0),
        after_success: args.after_success,
        after_completion: args.after_completion,
    };
    let reply = send(paths, Op::Submit(params), Some(key))?;
    let Reply::Submitted { job } = reply else { bail!(client::unexpected("submitted job")) };
    if args.json {
        println!("{}", serde_json::to_string_pretty(&job)?);
    } else {
        println!(
            "submitted job {} [{}] with maxParallelRuns {}",
            job.id, job.name, job.max_parallel_runs
        );
    }
    Ok(())
}

fn status(paths: &Paths, watch: bool, json: bool) -> Result<()> {
    loop {
        let reply = send(paths, Op::Status, None)?;
        let Reply::Status(view) = reply else { bail!(client::unexpected("status")) };
        if json {
            println!("{}", serde_json::to_string_pretty(&view)?);
        } else {
            if watch {
                // Clear screen and home the cursor.
                print!("\x1b[2J\x1b[H");
            }
            print_status(&view);
            std::io::stdout().flush()?;
        }
        if !watch {
            return Ok(());
        }
        std::thread::sleep(Duration::from_secs(2));
    }
}

fn follow_tts(paths: &Paths, tts_backend: Option<&str>) -> Result<()> {
    use std::os::fd::AsRawFd;

    paths.ensure_dirs()?;
    let mut follower_lock = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(paths.follow_tts_lock())
        .with_context(|| format!("opening {}", paths.follow_tts_lock().display()))?;
    let lock_result = unsafe {
        libc::flock(follower_lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB)
    };
    if lock_result != 0 {
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::WouldBlock {
            bail!("another mlq follow-tts process is already running");
        }
        return Err(error).context("locking the follow-tts singleton");
    }
    follower_lock.set_len(0)?;
    writeln!(follower_lock, "{}", std::process::id())?;

    let reply = send(paths, Op::FollowTtsSnapshot, None)?;
    let Reply::FollowTtsSnapshot(snapshot) = reply else {
        bail!(client::unexpected("follow TTS snapshot"))
    };
    let mut cursor = snapshot.latest_event_id;

    println!("following queue with TTS announcements (Ctrl-C to stop)");
    let mut running = snapshot
        .running_jobs
        .iter()
        .map(|job| spoken_job_name(job.id, &job.name))
        .collect::<Vec<_>>();
    if snapshot.additional_running_jobs > 0 {
        running.push(format!("{} more", snapshot.additional_running_jobs));
    }
    let body = if running.is_empty() {
        "Queue monitor started. No runs are currently active.".to_string()
    } else {
        format!("Queue monitor started. Currently running: {}.", running.join(", "))
    };
    speak_tts(tts_backend, "ML queue monitor", &body);

    let mut daemon_unavailable = false;
    loop {
        std::thread::sleep(Duration::from_secs(1));
        let mut events = match follow_tts_events(paths, &mut cursor) {
            Ok(events) => {
                if daemon_unavailable {
                    eprintln!("mlqd is reachable again; resumed TTS monitoring");
                    daemon_unavailable = false;
                }
                events
            }
            Err(error) => {
                if !daemon_unavailable {
                    eprintln!("warning: lost contact with mlqd: {error:#}; retrying");
                    daemon_unavailable = true;
                }
                continue;
            }
        };
        if events.is_empty() {
            continue;
        }
        // Give the scheduler one tick to pair a completion with the work it
        // admits next, producing one useful utterance instead of two.
        std::thread::sleep(Duration::from_millis(350));
        match follow_tts_events(paths, &mut cursor) {
            Ok(more) => events.extend(more),
            Err(error) => {
                eprintln!("warning: lost contact with mlqd: {error:#}; retrying");
                daemon_unavailable = true;
            }
        }
        if let Some(body) = follow_tts_announcement(&events) {
            println!("{body}");
            speak_tts(tts_backend, "ML queue update", &body);
        }
    }
}

fn follow_tts_events(paths: &Paths, cursor: &mut i64) -> Result<Vec<EventView>> {
    const PAGE_SIZE: u32 = 256;

    let mut all = Vec::new();
    let mut next_cursor = *cursor;
    loop {
        let reply = send(paths, Op::Events { after: next_cursor, limit: PAGE_SIZE }, None)?;
        let Reply::Events { events } = reply else { bail!(client::unexpected("events")) };
        let full_page = events.len() == PAGE_SIZE as usize;
        if let Some(last) = events.last() {
            next_cursor = last.id;
        }
        all.extend(events);
        if !full_page {
            *cursor = next_cursor;
            return Ok(all);
        }
    }
}

fn follow_tts_announcement(events: &[EventView]) -> Option<String> {
    let mut outcomes = Vec::new();
    let mut started = BTreeMap::new();
    for event in events {
        let name = || spoken_event_job(event);
        match event.event_type.as_str() {
            "job_succeeded" => {
                outcomes.push(format!("{} finished successfully", name()));
                started.remove(&event.job);
            }
            "job_failed" => {
                outcomes.push(format!("{} failed", name()));
                started.remove(&event.job);
            }
            "job_cancelled" => {
                outcomes.push(format!("{} was cancelled", name()));
                started.remove(&event.job);
            }
            "job_lost" => {
                outcomes.push(format!("{} was lost", name()));
                started.remove(&event.job);
            }
            "job_skipped" => {
                outcomes.push(format!("{} was skipped", name()));
                started.remove(&event.job);
            }
            "retry_scheduled" => {
                outcomes.push(match event.attempt_number {
                    Some(attempt) => {
                        format!("Attempt {attempt} for {} failed and a retry is queued", name())
                    }
                    None => format!("An attempt for {} failed and a retry is queued", name()),
                });
                started.remove(&event.job);
            }
            "attempt_orphaned" | "attempt_quarantined" => {
                outcomes.push(format!("{} needs operator attention", name()));
                started.remove(&event.job);
            }
            "attempt_running" => {
                started.insert(event.job, name());
            }
            _ => {}
        }
    }
    if outcomes.is_empty() && started.is_empty() {
        return None;
    }

    const MAX_SPOKEN_OUTCOMES: usize = 8;
    const MAX_SPOKEN_STARTS: usize = 5;

    let omitted_outcomes = outcomes.len().saturating_sub(MAX_SPOKEN_OUTCOMES);
    let mut sentences = outcomes.into_iter().take(MAX_SPOKEN_OUTCOMES).collect::<Vec<_>>();
    if omitted_outcomes > 0 {
        sentences.push(format!("{omitted_outcomes} more queue updates"));
    }
    if !started.is_empty() {
        let omitted_starts = started.len().saturating_sub(MAX_SPOKEN_STARTS);
        let mut running = started.into_values().take(MAX_SPOKEN_STARTS).collect::<Vec<_>>();
        if omitted_starts > 0 {
            running.push(format!("{omitted_starts} more"));
        }
        sentences.push(format!(
            "Now running: {}",
            running.join(", ")
        ));
    }
    Some(format!("{}.", sentences.join(". ")))
}

fn spoken_event_job(event: &EventView) -> String {
    match (event.job, event.job_name.as_deref()) {
        (Some(id), Some(name)) => spoken_job_name(id, name),
        (Some(id), None) => format!("job {id}"),
        (None, Some(name)) => {
            let name = normalized_spoken_name(name);
            if name.is_empty() { "an unnamed job".to_string() } else { name }
        }
        (None, None) => "an unknown job".to_string(),
    }
}

fn spoken_job_name(id: JobId, name: &str) -> String {
    let normalized = normalized_spoken_name(name);
    if normalized.is_empty() {
        format!("job {id}")
    } else {
        format!("job {id}, {normalized}")
    }
}

fn normalized_spoken_name(name: &str) -> String {
    let normalized = name
        .chars()
        .map(|character| match character {
            '_' | '-' => ' ',
            character if character.is_control() => ' ',
            character => character,
        })
        .collect::<String>();
    let normalized = normalized.split_whitespace().collect::<Vec<_>>().join(" ");
    truncate(&normalized, 60)
}

fn speak_tts(backend: Option<&str>, title: &str, body: &str) {
    const TTS_TIMEOUT: Duration = Duration::from_secs(60);

    let result = tts_command(backend)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .spawn();
    let mut child = match result {
        Ok(child) => child,
        Err(error) => {
            eprintln!("warning: could not run tts: {error}; continuing to follow the queue");
            return;
        }
    };
    let text = format!("{title}. {body}");
    if let Some(mut stdin) = child.stdin.take()
        && let Err(error) = stdin.write_all(text.as_bytes())
    {
        eprintln!("warning: could not send text to tts: {error}; continuing to follow the queue");
        let _ = child.kill();
    }
    let deadline = Instant::now() + TTS_TIMEOUT;
    loop {
        match child.try_wait() {
            Ok(Some(status)) if status.success() => return,
            Ok(Some(status)) => {
                eprintln!("warning: tts exited with {status}; continuing to follow the queue");
                return;
            }
            Ok(None) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(100));
            }
            Ok(None) => {
                eprintln!("warning: tts timed out after 60 seconds; continuing to follow the queue");
                let _ = child.kill();
                let _ = child.wait();
                return;
            }
            Err(error) => {
                eprintln!("warning: could not wait for tts: {error}; continuing to follow the queue");
                let _ = child.kill();
                let _ = child.wait();
                return;
            }
        }
    }
}

fn tts_command(backend: Option<&str>) -> std::process::Command {
    let mut command = std::process::Command::new("tts");
    command.args(["speak", "--daemon"]);
    if let Some(backend) = backend {
        command.args(["--backend", backend]);
    }
    command.args(["--level", "status", "--text-stdin"]);
    command
}

fn print_status(view: &StatusView) {
    print!("{}", render_status(view));
}

fn render_status(view: &StatusView) -> String {
    use std::fmt::Write as _;

    let mut output = String::new();
    match view.effective_limit {
        Some(limit) => writeln!(
            output,
            "active leases: {} (effective minimum limit {})",
            view.active_leases, limit
        )
        .unwrap(),
        None => writeln!(output, "active leases: 0").unwrap(),
    }
    if let Some(res) = &view.reservation {
        let frontier = match res.backfill_cutoff {
            _ if res.backfill_window_open => "backfill window open".to_string(),
            Some(cutoff) => format!("backfill cutoff at job {cutoff}"),
            None => "backfill frontier unavailable".to_string(),
        };
        writeln!(
            output,
            "protected job: {} ({frontier}; blocked by attempts {:?}; consumed bypasses {:?})",
            res.protected_job, res.blocking_attempts, res.consumed_bypasses
        )
        .unwrap();
    }
    if view.admission_blocked {
        writeln!(output, "ADMISSION BLOCKED: operator attention required (see daemon logs)")
            .unwrap();
    }
    if view.jobs.is_empty() {
        writeln!(output, "no jobs").unwrap();
        return output;
    }

    let (live, queued, held, finished) = status_sections(view);
    if !live.is_empty() || !queued.is_empty() {
        write_status_header(&mut output);
        for job in live.into_iter().chain(queued) {
            write_status_job(&mut output, job);
        }
    }
    if !finished.is_empty() {
        writeln!(output).unwrap();
        writeln!(output, "Finished Runs").unwrap();
        write_status_header(&mut output);
        for job in finished {
            write_status_job(&mut output, job);
        }
    }
    if !held.is_empty() {
        writeln!(output).unwrap();
        writeln!(output, "Held Jobs").unwrap();
        write_status_header(&mut output);
        for job in held {
            write_status_job(&mut output, job);
        }
    }
    output
}

fn status_sections(
    view: &StatusView,
) -> (Vec<&JobView>, Vec<&JobView>, Vec<&JobView>, Vec<&JobView>) {
    let mut live = Vec::new();
    let mut queued = Vec::new();
    let mut held = Vec::new();
    let mut finished = Vec::new();
    for job in &view.jobs {
        if job.finished_at.is_some() {
            finished.push(job);
        } else if job.state == "queued" {
            queued.push(job);
        } else if job.state == "held" {
            held.push(job);
        } else {
            // Starting and recovery states are live operational work too;
            // keep unknown future non-terminal states visible here as well.
            live.push(job);
        }
    }

    live.sort_by_key(|job| {
        let rank = match job.state.as_str() {
            "running" => 0,
            "starting" => 1,
            "needs_attention" => 2,
            _ => 3,
        };
        (rank, job.id)
    });
    queued.sort_by_key(|job| job.id);
    queued.truncate(STATUS_QUEUE_LIMIT);
    held.sort_by_key(|job| job.id);
    held.truncate(STATUS_HELD_LIMIT);
    finished.sort_by_key(|job| Reverse((job.finished_at, job.id)));
    finished.truncate(STATUS_FINISHED_LIMIT);
    (live, queued, held, finished)
}

fn write_status_header(output: &mut String) {
    use std::fmt::Write as _;

    writeln!(
        output,
        "{:<6} {:<width$} {:<15} {:<6} {:<9} REASON",
        "JOB",
        "NAME",
        "STATE",
        "LIMIT",
        "ATTEMPTS",
        width = STATUS_NAME_WIDTH,
    )
    .unwrap();
}

fn write_status_job(output: &mut String, job: &JobView) {
    use std::fmt::Write as _;

    let reason = job
        .eligibility
        .as_deref()
        .or(job.state_reason.as_deref())
        .unwrap_or("-");
    writeln!(
        output,
        "{:<6} {:<width$} {:<15} {:<6} {:<9} {}",
        job.id,
        truncate(&job.name, STATUS_NAME_WIDTH),
        job.state,
        job.max_parallel_runs,
        format!("{}/{}", job.attempt_count, job.max_attempts),
        reason,
        width = STATUS_NAME_WIDTH,
    )
    .unwrap();
}

fn print_job_detail(job: &JobView) {
    println!("job {} [{}]", job.id, job.name);
    println!("  state:            {}{}", job.state, match &job.state_reason {
        Some(reason) => format!(" ({reason})"),
        None => String::new(),
    });
    if let Some(eligibility) = &job.eligibility {
        println!("  eligibility:      {eligibility}");
    }
    if job.cancel_requested == Some(true) {
        println!("  cancel requested: yes");
    }
    println!("  maxParallelRuns:  {}", job.max_parallel_runs);
    println!("  cwd:              {}", job.cwd);
    println!("  command:          {}", shell_join(&job.args));
    println!("  attempts:         {}/{}", job.attempt_count, job.max_attempts);
    if !job.dependencies.is_empty() {
        for dep in &job.dependencies {
            println!(
                "  depends on:       job {} ({}, {})",
                dep.parent,
                dep.requirement,
                if dep.satisfied { "satisfied" } else { "unsatisfied" }
            );
        }
    }
    for attempt in &job.attempts {
        let outcome = match (attempt.exit_code, attempt.term_signal) {
            (Some(code), _) => format!(" exit {code}"),
            (None, Some(signal)) => format!(" signal {signal}"),
            _ => String::new(),
        };
        println!(
            "  attempt {:<3} {}{}{}",
            attempt.number,
            attempt.state,
            outcome,
            match &attempt.message {
                Some(message) => format!(" — {message}"),
                None => String::new(),
            }
        );
        println!("    logs: {}", attempt.log_dir);
    }
}

fn logs(paths: &Paths, job: JobId, attempt: Option<i64>, stderr: bool, follow: bool) -> Result<()> {
    let fetch = |attempt| -> Result<LogPathsView> {
        let reply = send(paths, Op::Logs { job, attempt }, None)?;
        let Reply::LogPaths(view) = reply else { bail!(client::unexpected("log paths")) };
        Ok(view)
    };
    let view = fetch(attempt)?;
    let path = PathBuf::from(if stderr { &view.stderr } else { &view.stdout });

    // Client and daemon share the machine and user; logs are read straight
    // from the attempt directory.
    let mut offset: u64 = 0;
    let mut recovery_notice_shown = false;
    let stdout = std::io::stdout();
    loop {
        if path.exists() {
            let mut file = std::fs::File::open(&path)?;
            let len = file.metadata()?.len();
            if len > offset {
                file.seek(SeekFrom::Start(offset))?;
                let mut chunk = Vec::new();
                file.read_to_end(&mut chunk)?;
                offset = len;
                stdout.lock().write_all(&chunk)?;
                stdout.lock().flush()?;
            }
        }
        if !follow {
            return Ok(());
        }
        // Stop following once the attempt is terminal and everything written
        // has been drained.
        let latest = fetch(Some(view.attempt_number))?;
        let state = latest.attempt_state.parse::<AttemptState>().ok();
        // Orphaned/quarantined attempts may still have a live command
        // writing logs; keep following, but say why this never ends.
        if matches!(state, Some(AttemptState::Orphaned | AttemptState::Quarantined))
            && !recovery_notice_shown
        {
            recovery_notice_shown = true;
            eprintln!(
                "[mlq] attempt is {}: the command may still be running; \
                 following until it drains (Ctrl-C to stop, see `mlq recover list`)",
                latest.attempt_state
            );
        }
        if state.is_some_and(AttemptState::is_terminal) {
            if let Ok(meta) = std::fs::metadata(&path)
                && meta.len() > offset
            {
                continue;
            }
            // Report why the stream ended, and reflect the outcome in this
            // process's exit code so scripted tails can branch on it.
            eprintln!(
                "[mlq] attempt {} {}{}",
                latest.attempt_number,
                latest.attempt_state,
                latest.message.as_deref().map(|message| format!(" — {message}")).unwrap_or_default()
            );
            let code = outcome_exit_code(
                state == Some(AttemptState::Succeeded),
                latest.exit_code,
                latest.term_signal,
            );
            if code != 0 {
                std::process::exit(code);
            }
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(300));
    }
}

/// Exit code for waiting on a job that never reached a terminal state
/// within `--timeout`, mirroring timeout(1).
const EXIT_TIMED_OUT: i32 = 124;

/// Map a terminal outcome onto a process exit code: 0 for success, 128+N
/// for a death by signal N, the command's own non-zero exit code where one
/// exists, and 1 otherwise (skipped, lost, cancelled or failed pre-launch).
fn outcome_exit_code(succeeded: bool, exit_code: Option<i32>, term_signal: Option<i32>) -> i32 {
    if succeeded {
        return 0;
    }
    match (term_signal, exit_code) {
        (Some(signal), _) => 128 + signal,
        (None, Some(code)) if code != 0 => code,
        _ => 1,
    }
}

fn wait(paths: &Paths, job: JobId, timeout: Option<Duration>, json: bool) -> Result<()> {
    // A deadline beyond Instant's range means "wait forever", which is what
    // an absurdly large --timeout asks for anyway.
    let deadline = timeout.and_then(|timeout| Instant::now().checked_add(timeout));
    let mut recovery_notice_shown = false;
    loop {
        let reply = send(paths, Op::Show { job }, None)?;
        let Reply::Job { job: view } = reply else { bail!(client::unexpected("job")) };
        let last = view.attempts.last();
        let state = view.state.parse::<JobState>().ok();
        if state.is_some_and(JobState::is_terminal) {
            // Attempt data feeds the exit code only when that attempt produced
            // the job's outcome — a job cancelled while queued for a retry
            // still carries its predecessor's failed attempt.
            let outcome_attempt = last.filter(|a| a.state == view.state);
            let code = outcome_exit_code(
                state == Some(JobState::Succeeded),
                outcome_attempt.and_then(|a| a.exit_code),
                outcome_attempt.and_then(|a| a.term_signal),
            );
            if json {
                println!("{}", serde_json::to_string_pretty(&view)?);
            } else {
                let reason = view
                    .state_reason
                    .as_deref()
                    .or(outcome_attempt.and_then(|a| a.message.as_deref()));
                println!("job {} [{}] {}{}", view.id, view.name, view.state, match reason {
                    Some(reason) => format!(" ({reason})"),
                    None => String::new(),
                });
            }
            std::process::exit(code);
        }
        // Orphaned/quarantined attempts resolve through an operator, not the
        // scheduler; say so once rather than blocking silently.
        if !recovery_notice_shown
            && let Some(attempt) = last
            && matches!(attempt.state.as_str(), "orphaned" | "quarantined")
        {
            recovery_notice_shown = true;
            eprintln!(
                "[mlq] attempt is {}: waiting on operator recovery (see `mlq recover list`)",
                attempt.state
            );
        }
        if let Some(deadline) = deadline
            && Instant::now() >= deadline
        {
            eprintln!("[mlq] timed out waiting for job {job} (state: {})", view.state);
            std::process::exit(EXIT_TIMED_OUT);
        }
        std::thread::sleep(Duration::from_millis(300));
    }
}

// ---------------------------------------------------------------------------
// Daemon lifecycle: foreground run and systemd user-service management
// ---------------------------------------------------------------------------

const SERVICE_NAME: &str = "mlqd.service";

/// Locate the `mlqd` binary: preferring the one installed alongside this
/// `mlq` binary (they are built and versioned together), then PATH.
fn mlqd_path() -> Result<PathBuf> {
    if let Ok(exe) = std::env::current_exe()
        && let Some(dir) = exe.parent()
    {
        let sibling = dir.join("mlqd");
        if sibling.is_file() {
            return Ok(sibling.canonicalize()?);
        }
    }
    let path = std::env::var_os("PATH").unwrap_or_default();
    for dir in std::env::split_paths(&path) {
        if dir.as_os_str().is_empty() {
            continue;
        }
        let candidate = dir.join("mlqd");
        if candidate.is_file() {
            return Ok(candidate.canonicalize()?);
        }
    }
    bail!("mlqd not found next to mlq or on PATH; install with `cargo install --path .`")
}

fn daemon_run() -> Result<()> {
    use std::os::unix::process::CommandExt;
    let path = mlqd_path()?;
    // exec only returns on failure.
    let err = std::process::Command::new(&path).exec();
    Err(err).with_context(|| format!("executing {}", path.display()))
}

fn systemd_user_unit_path() -> Result<PathBuf> {
    let config_home = match std::env::var_os("XDG_CONFIG_HOME").filter(|v| !v.is_empty()) {
        Some(xdg) => PathBuf::from(xdg),
        None => {
            let home = std::env::var_os("HOME")
                .filter(|v| !v.is_empty())
                .context("HOME is not set; cannot locate the systemd user unit directory")?;
            PathBuf::from(home).join(".config")
        }
    };
    Ok(config_home.join("systemd/user").join(SERVICE_NAME))
}

fn systemctl_user(args: &[&str]) -> Result<()> {
    let status = std::process::Command::new("systemctl")
        .arg("--user")
        .args(args)
        .status()
        .context("running systemctl (is this a systemd system?)")?;
    if !status.success() {
        bail!("`systemctl --user {}` failed", args.join(" "));
    }
    Ok(())
}

fn daemon_install(no_enable: bool) -> Result<()> {
    let mlqd = mlqd_path()?;
    let unit_path = systemd_user_unit_path()?;
    // Mirrors contrib/systemd/mlqd.service, with the resolved binary.
    let unit = format!(
        "[Unit]\n\
         Description=mlqueue machine-wide ML job queue daemon\n\
         \n\
         [Service]\n\
         Type=simple\n\
         ExecStart={}\n\
         Restart=on-failure\n\
         RestartSec=2\n\
         # Workers must survive daemon restarts: kill only the daemon itself.\n\
         KillMode=process\n\
         \n\
         [Install]\n\
         WantedBy=default.target\n",
        mlqd.display()
    );
    let dir = unit_path.parent().expect("unit path has a parent");
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    std::fs::write(&unit_path, unit).with_context(|| format!("writing {}", unit_path.display()))?;
    println!("wrote {}", unit_path.display());
    if no_enable {
        println!("not enabled; start with: systemctl --user enable --now {SERVICE_NAME}");
        return Ok(());
    }
    systemctl_user(&["daemon-reload"])?;
    systemctl_user(&["enable", "--now", SERVICE_NAME])?;
    println!("mlqd enabled and started (systemctl --user status {SERVICE_NAME})");
    Ok(())
}

fn daemon_uninstall() -> Result<()> {
    let unit_path = systemd_user_unit_path()?;
    if !unit_path.exists() {
        bail!("{} is not installed (expected {})", SERVICE_NAME, unit_path.display());
    }
    // Stopping the daemon never kills workers (KillMode=process); a later
    // daemon start adopts anything still running. A unit that was installed
    // with --no-enable was never loaded, so disable failing is not fatal.
    if let Err(err) = systemctl_user(&["disable", "--now", SERVICE_NAME]) {
        eprintln!("warning: {err:#} (removing the unit file anyway)");
    }
    std::fs::remove_file(&unit_path)
        .with_context(|| format!("removing {}", unit_path.display()))?;
    systemctl_user(&["daemon-reload"])?;
    println!("removed {}", unit_path.display());
    Ok(())
}

fn truncate(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        text.to_string()
    } else {
        let cut: String = text.chars().take(max.saturating_sub(1)).collect();
        format!("{cut}…")
    }
}

fn shell_join(args: &[String]) -> String {
    args.iter()
        .map(|arg| {
            if arg.is_empty() || arg.contains(|c: char| c.is_whitespace() || "'\"$`\\".contains(c))
            {
                format!("{arg:?}")
            } else {
                arg.clone()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(id: i64, job: JobId, name: &str, event_type: &str) -> EventView {
        EventView {
            id,
            timestamp: id,
            job: Some(job),
            job_name: Some(name.into()),
            attempt: None,
            attempt_number: None,
            event_type: event_type.into(),
        }
    }

    fn status_job(id: JobId, state: &str, finished_at: Option<i64>) -> JobView {
        JobView {
            id,
            name: format!("{state}-{id}"),
            state: state.into(),
            eligibility: None,
            state_reason: None,
            max_parallel_runs: 1,
            cwd: "/tmp".into(),
            args: vec!["true".into()],
            max_attempts: 1,
            retry_delay_ms: 0,
            retry_not_before: None,
            attempt_count: finished_at.is_some() as i64,
            created_at: id,
            updated_at: finished_at.unwrap_or(id),
            finished_at,
            dependencies: vec![],
            attempts: vec![],
            cancel_requested: None,
        }
    }

    #[test]
    fn status_prioritizes_live_work_and_limits_queue_and_finished_sections() {
        let mut jobs = vec![
            status_job(2, "starting", None),
            status_job(1, "needs_attention", None),
            status_job(3, "running", None),
        ];
        jobs.extend((50..62).rev().map(|id| status_job(id, "held", None)));
        jobs.extend((100..112).rev().map(|id| status_job(id, "queued", None)));
        jobs.extend((0..12).map(|index| {
            let finished_at = (index * 7) % 12;
            status_job(200 + index, "succeeded", Some(finished_at))
        }));
        let view = StatusView {
            jobs,
            active_leases: 1,
            effective_limit: Some(1),
            reservation: None,
            admission_blocked: false,
        };

        let (live, queued, held, finished) = status_sections(&view);
        assert_eq!(live.iter().map(|job| job.id).collect::<Vec<_>>(), vec![3, 2, 1]);
        assert_eq!(
            queued.iter().map(|job| job.id).collect::<Vec<_>>(),
            (100..110).collect::<Vec<_>>()
        );
        assert_eq!(
            held.iter().map(|job| job.id).collect::<Vec<_>>(),
            (50..60).collect::<Vec<_>>()
        );
        assert_eq!(
            finished.iter().map(|job| job.id).collect::<Vec<_>>(),
            vec![205, 210, 203, 208, 201, 206, 211, 204, 209, 202]
        );

        let output = render_status(&view);
        assert_eq!(output.matches("JOB    NAME").count(), 3, "{output}");
        let finished_header = output.find("Finished Runs").unwrap();
        let held_header = output.find("Held Jobs").unwrap();
        assert!(output.find("running-3").unwrap() < output.find("queued-100").unwrap());
        assert!(output.find("queued-109").unwrap() < finished_header);
        assert!(finished_header < output.find("succeeded-205").unwrap());
        assert!(output.find("succeeded-202").unwrap() < held_header);
        assert!(held_header < output.find("held-50").unwrap());
        assert!(!output.contains("queued-110"));
        assert!(!output.contains("queued-111"));
        assert!(!output.contains("held-60"));
        assert!(!output.contains("held-61"));
        assert!(!output.contains("succeeded-200"));
        assert!(!output.contains("succeeded-207"));
    }

    #[test]
    fn follow_tts_batches_outcomes_retries_and_started_work() {
        let mut retry = event(2, 2, "retry_job", "retry_scheduled");
        retry.attempt = Some(20);
        retry.attempt_number = Some(1);
        let events = vec![
            event(1, 1, "finished-job", "job_succeeded"),
            retry,
            event(3, 3, "next-job", "attempt_running"),
        ];

        assert_eq!(
            follow_tts_announcement(&events).as_deref(),
            Some(
                "job 1, finished job finished successfully. Attempt 1 for job 2, retry job failed and a retry is queued. Now running: job 3, next job."
            )
        );
    }

    #[test]
    fn follow_tts_ignores_irrelevant_events_and_sanitizes_names() {
        assert!(follow_tts_announcement(&[event(1, 1, "secret", "job_submitted")]).is_none());
        assert_eq!(spoken_job_name(7, "  odd\nname_with-control\t"), "job 7, odd name with control");
    }

    #[test]
    fn follow_tts_bounds_large_announcement_batches() {
        let events = (1..=10)
            .map(|id| event(id, id, &format!("failed-{id}"), "job_failed"))
            .collect::<Vec<_>>();
        let announcement = follow_tts_announcement(&events).unwrap();
        assert!(announcement.contains("job 8, failed 8 failed"));
        assert!(announcement.contains("2 more queue updates"));
        assert!(!announcement.contains("job 9, failed 9 failed"));
    }

    #[test]
    fn follow_tts_inherits_the_default_backend_unless_overridden() {
        let args = |backend| {
            tts_command(backend)
                .get_args()
                .map(|arg| arg.to_string_lossy().into_owned())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            args(None),
            ["speak", "--daemon", "--level", "status", "--text-stdin"]
        );
        assert_eq!(
            args(Some("system")),
            [
                "speak",
                "--daemon",
                "--backend",
                "system",
                "--level",
                "status",
                "--text-stdin",
            ]
        );
    }
}
