//! Quiet, job-scoped subscriptions. Local checks and notification hooks are opt-in.

use std::io::Write;
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::{Args, ValueEnum};
use serde_json::{Value, json};

use crate::client::JobSubscription;
use crate::domain::{JobId, JobState};
use crate::follow_checks::{self, CheckRunner, FollowConfig, HookConfig};
use crate::paths::Paths;
use crate::protocol::JobView;

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub(crate) enum EventKind {
    Started,
    Warning,
    Retry,
    Attention,
    Finished,
}

impl EventKind {
    fn name(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Warning => "warning",
            Self::Retry => "retry",
            Self::Attention => "attention",
            Self::Finished => "finished",
        }
    }
}

#[derive(Args)]
pub(crate) struct FollowArgs {
    pub job: JobId,
    /// Opt into intermediate events (comma-separated). Final outcome is always included.
    #[arg(long, value_enum, value_delimiter = ',')]
    events: Option<Vec<EventKind>>,
    /// Explicit TOML configuration; no configuration is discovered automatically.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Override configured events: no intermediate output, checks, or intermediate hooks.
    #[arg(long)]
    only_finish: bool,
    /// Disable all configured notification commands, including the finish hook.
    #[arg(long)]
    no_hooks: bool,
    /// Disable all configured checks.
    #[arg(long)]
    no_checks: bool,
    /// Stop following after this long and exit 124; never cancel the job.
    #[arg(long, default_value = "55m", value_parser = humantime::parse_duration)]
    pub timeout: Duration,
    /// Emit compact NDJSON events instead of one-line text.
    #[arg(long)]
    pub json: bool,
}

impl FollowArgs {
    fn configure(&self) -> Result<FollowConfig> {
        let mut config = match &self.config {
            Some(path) => follow_checks::load(path)?,
            None => FollowConfig::default(),
        };
        if self.only_finish {
            config.events = Some(vec!["finished".into()]);
        } else if let Some(events) = &self.events {
            config.events = Some(events.iter().map(|event| event.name().to_owned()).collect());
        }
        let events = config.events.get_or_insert_with(Vec::new);
        if !events.iter().any(|event| event == "finished") {
            events.push("finished".into());
        }
        if self.no_checks || !events.iter().any(|event| event == "warning") {
            config.checks.clear();
        }
        if self.no_hooks {
            config.hooks.clear();
        }
        config.validate()?;
        Ok(config)
    }
}

pub(crate) fn run(paths: &Paths, args: FollowArgs) -> Result<i32> {
    let config = args.configure()?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        // Default signal termination skips Rust destructors. Handle it here so
        // cancelling the observer also cancels its subprocess groups.
        let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
        let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        // mlq normally restores SIGPIPE's default disposition. A follower owns
        // subprocess groups, so a closed output pipe must run cleanup too.
        let mut pipe = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::pipe())?;
        let following = async {
            let watching = follow(paths, &args, config);
            // Timeout bounds observation, not notification delivery after a
            // terminal outcome is already known.
            let deadline = Instant::now()
                .checked_add(args.timeout)
                .ok_or_else(|| anyhow::anyhow!("follow timeout is too large"))?;
            let (job, mut output) = match tokio::time::timeout_at(deadline.into(), watching).await {
                Ok(result) => result?,
                Err(_) => {
                    eprintln!("follow timed out after {} minutes", args.timeout.as_secs_f64() / 60.0);
                    return Ok(124);
                }
            };
            finish(&mut output, job).await
        };
        tokio::select! {
            biased;
            _ = interrupt.recv() => Ok(130),
            _ = terminate.recv() => Ok(143),
            _ = pipe.recv() => Ok(141),
            result = following => match result {
                Err(error) if error.chain().any(|cause| cause.downcast_ref::<std::io::Error>()
                    .is_some_and(|error| error.kind() == std::io::ErrorKind::BrokenPipe)) => Ok(141),
                result => result,
            },
        }
    })
}

async fn follow(
    paths: &Paths,
    args: &FollowArgs,
    config: FollowConfig,
) -> Result<(JobView, Output)> {
    let mut subscription = JobSubscription::connect(paths, args.job, None).await?;
    let mut checks = CheckRunner::new(config.checks);
    let mut output = Output {
        json: args.json,
        events: config.events.unwrap_or_default(),
        hooks: HookWorker::new(config.hooks),
        suppressed_hooks: 0,
        warnings: WarningBudget::default(),
    };
    let mut previous_attempt = None;
    let mut initial = true;
    let mut update = subscription.next().await?;
    loop {
        let attempt = update.job.attempts.last();
        let attempt_id = attempt.map(|attempt| attempt.id);
        if attempt_id != previous_attempt {
            checks.reset_attempt();
            previous_attempt = attempt_id;
        }
        for event in &update.events {
            let kind = match event.event_type.as_str() {
                "attempt_running" => EventKind::Started,
                "retry_scheduled" => EventKind::Retry,
                "attempt_orphaned" | "attempt_quarantined" => EventKind::Attention,
                // Final output comes from the snapshot, once the event backlog is drained.
                _ => continue,
            };
            if !output.selected(kind) {
                continue;
            }
            output.emit(
                kind,
                &json!({
                    "event": kind.name(), "job": args.job, "attempt": event.attempt_number,
                    "eventId": event.id,
                }),
            )?;
        }
        if initial
            && output.selected(EventKind::Attention)
            && attempt.is_some_and(|a| matches!(a.state.as_str(), "orphaned" | "quarantined"))
        {
            output.emit(
                EventKind::Attention,
                &json!({
                    "event": "attention", "job": args.job, "attempt": attempt.map(|a| a.number),
                    "message": "operator recovery required",
                }),
            )?;
        }
        initial = false;
        if !update.has_more && terminal(&update.job) {
            return Ok((update.job, output));
        }
        // A partially received frame must survive timer ticks. Dropping next() in
        // a select loop would lose read_exact's partially consumed frame bytes.
        let next = subscription.next();
        tokio::pin!(next);
        update = loop {
            let due = if update.job.state == "running" && !update.has_more {
                checks.next_deadline()
            } else {
                None
            };
            tokio::select! {
                biased;
                received = &mut next => break received?,
                _ = sleep_until(due) => {
                    let context = check_context(&update.job);
                    // A transition preempts checks, so a slow checker cannot delay
                    // completion or report findings against an obsolete attempt.
                    let findings = tokio::select! {
                        biased;
                        received = &mut next => break received?,
                        findings = checks.run_due(&context) => findings,
                    };
                    let (accepted, suppressed) = output.warnings.admit(findings.len(), Instant::now());
                    if accepted != 0 {
                        let findings = findings.into_iter().take(accepted).map(|mut finding| {
                            finding.message = one_line(&finding.message, 240);
                            finding
                        }).collect::<Vec<_>>();
                        output.emit(EventKind::Warning, &json!({
                            "event": "warning", "job": args.job,
                            "attempt": update.job.attempts.last().map(|a| a.number),
                            "findings": findings,
                            "suppressed": suppressed,
                        }))?;
                    }
                }
            }
        };
    }
}

fn terminal(job: &JobView) -> bool {
    job.state
        .parse::<JobState>()
        .is_ok_and(JobState::is_terminal)
}

async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
        None => std::future::pending().await,
    }
}

fn check_context(job: &JobView) -> Value {
    let attempt = job.attempts.last();
    json!({
        "job": job.id, "state": job.state, "cwd": job.cwd,
        "attempt": attempt.map(|a| a.number), "logDir": attempt.map(|a| &a.log_dir),
    })
}

async fn finish(output: &mut Output, job: JobView) -> Result<i32> {
    // Cancellation while queued for a retry must not inherit the previous
    // attempt's failure code or signal.
    let attempt = job
        .attempts
        .last()
        .filter(|attempt| attempt.state == job.state);
    let code = crate::cli::outcome_exit_code(
        job.state == "succeeded",
        attempt.and_then(|a| a.exit_code),
        attempt.and_then(|a| a.term_signal),
    );
    let reason = job
        .state_reason
        .as_deref()
        .or(attempt.and_then(|a| a.message.as_deref()));
    let mut event =
        json!({"event": "finished", "job": job.id, "state": job.state, "exitCode": code});
    if let Some(reason) = reason {
        event["message"] = Value::String(one_line(reason, 240));
    }
    if output.warnings.suppressed != 0 {
        event["suppressedWarnings"] = json!(output.warnings.suppressed);
    }
    if output.suppressed_hooks != 0 {
        event["suppressedHooks"] = json!(output.suppressed_hooks);
    }
    output.emit(EventKind::Finished, &event)?;
    if let Some(hooks) = &mut output.hooks {
        hooks.finish(&event).await;
    }
    Ok(code)
}

struct Output {
    json: bool,
    events: Vec<String>,
    hooks: Option<HookWorker>,
    suppressed_hooks: u64,
    warnings: WarningBudget,
}

impl Output {
    fn selected(&self, kind: EventKind) -> bool {
        self.events.iter().any(|event| event == kind.name())
    }

    fn emit(&mut self, kind: EventKind, payload: &Value) -> Result<()> {
        if !self.selected(kind) {
            return Ok(());
        }
        {
            let mut out = std::io::stdout().lock();
            if self.json {
                serde_json::to_writer(&mut out, payload)?;
                writeln!(out)?;
            } else {
                write!(out, "job {} {}", payload["job"], kind.name())?;
                if let Some(state) = payload["state"].as_str() {
                    write!(out, " {state} (exit {})", payload["exitCode"])?;
                } else if let Some(attempt) = payload["attempt"].as_i64() {
                    write!(out, " attempt {attempt}")?;
                }
                if let Some(message) = payload["message"].as_str() {
                    write!(out, ": {}", one_line(message, 240))?;
                }
                if let Some(findings) = payload["findings"].as_array() {
                    for finding in findings {
                        write!(
                            out,
                            "; {}: {}",
                            one_line(finding["key"].as_str().unwrap_or("check"), 80),
                            one_line(finding["message"].as_str().unwrap_or(""), 240)
                        )?;
                    }
                }
                if let Some(count) = payload["suppressedWarnings"].as_u64() {
                    write!(out, "; {count} warnings suppressed")?;
                }
                if let Some(count) = payload["suppressedHooks"].as_u64() {
                    write!(out, "; {count} hook deliveries suppressed")?;
                }
                writeln!(out)?;
            }
            out.flush()?;
        }
        if kind != EventKind::Finished
            && let Some(hooks) = &self.hooks
            && hooks
                .hooks
                .iter()
                .any(|hook| hook.events.iter().any(|event| event == kind.name()))
            && hooks.sender.try_send((kind, payload.clone())).is_err()
        {
            self.suppressed_hooks = self.suppressed_hooks.saturating_add(1);
        }
        Ok(())
    }
}

/// Notification delivery cannot block observation. A small bounded mailbox
/// absorbs bursts; final delivery preempts obsolete intermediate notifications.
struct HookWorker {
    hooks: Arc<Vec<HookConfig>>,
    sender: tokio::sync::mpsc::Sender<(EventKind, Value)>,
    task: Option<tokio::task::JoinHandle<()>>,
    error_shown: Arc<AtomicBool>,
}

impl HookWorker {
    fn new(hooks: Vec<HookConfig>) -> Option<Self> {
        if hooks.is_empty() {
            return None;
        }
        let hooks = Arc::new(hooks);
        let error_shown = Arc::new(AtomicBool::new(false));
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<(EventKind, Value)>(8);
        let worker_hooks = Arc::clone(&hooks);
        let worker_errors = Arc::clone(&error_shown);
        let task = tokio::spawn(async move {
            while let Some((kind, payload)) = receiver.recv().await {
                let errors = follow_checks::run_hooks(&worker_hooks, kind.name(), &payload).await;
                report_hook_errors(&worker_errors, &errors);
            }
        });
        Some(Self {
            hooks,
            sender,
            task: Some(task),
            error_shown,
        })
    }

    async fn finish(&mut self, payload: &Value) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
        let errors = follow_checks::run_hooks(&self.hooks, "finished", payload).await;
        report_hook_errors(&self.error_shown, &errors);
    }
}

impl Drop for HookWorker {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

fn report_hook_errors(shown: &AtomicBool, errors: &[String]) {
    if let Some(error) = errors.first()
        && !shown.swap(true, Ordering::Relaxed)
    {
        eprintln!(
            "[mlq] notification hook failed: {}; further hook errors suppressed",
            one_line(error, 240)
        );
    }
}

/// Hard per-follower bound across distinct finding keys/checks. Stable-key
/// suppression and rearming live in CheckRunner; this also limits noisy scripts
/// that manufacture a fresh key on every invocation. No periodic reminders.
#[derive(Default)]
struct WarningBudget {
    last: Option<Instant>,
    suppressed: u64,
}

impl WarningBudget {
    fn admit(&mut self, count: usize, now: Instant) -> (usize, usize) {
        const COOLDOWN: Duration = Duration::from_secs(60);
        const MAX_BATCH: usize = 8;
        let accepted = if self
            .last
            .is_none_or(|last| now.duration_since(last) >= COOLDOWN)
        {
            count.min(MAX_BATCH)
        } else {
            0
        };
        if accepted != 0 {
            self.last = Some(now);
        }
        let suppressed = count - accepted;
        self.suppressed = self.suppressed.saturating_add(suppressed as u64);
        (accepted, suppressed)
    }
}

fn one_line(text: &str, limit: usize) -> String {
    text.chars()
        .take(limit)
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn warnings_are_bounded_across_distinct_keys_without_reminders() {
        let mut budget = WarningBudget::default();
        let start = Instant::now();
        assert_eq!(budget.admit(12, start), (8, 4));
        assert_eq!(budget.admit(3, start + Duration::from_secs(59)), (0, 3));
        assert_eq!(budget.admit(0, start + Duration::from_secs(60)), (0, 0));
        assert_eq!(budget.admit(1, start + Duration::from_secs(61)), (1, 0));
        assert_eq!(budget.suppressed, 7);
    }
}
