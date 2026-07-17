//! The single coordinator task. All scheduling decisions and database writes
//! are serialized here; socket handlers validate input and send commands, and
//! never launch work independently. No transaction or lock is held across
//! socket I/O, filesystem waits, or process spawning.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::os::unix::fs::DirBuilderExt;
use std::os::unix::process::CommandExt;
use std::process::Stdio;
use std::time::Instant;

use anyhow::{Context, Result};
use rusqlite::{Connection, Transaction};
use serde_json::json;
use tokio::sync::{mpsc, oneshot};

use crate::config::Config;
use crate::db::{self, AttemptRow};
use crate::domain::{
    AttemptId, AttemptState, JobId, JobState, SCHEDULER_SEMANTICS_VERSION, now_ms,
};
use crate::paths::Paths;
use crate::process::artifacts::{
    self, CANCEL_FILE, COMMAND_FILE, CancelFile, CommandFile, EXEC_FILE, ExecFile, IDENTITY_FILE,
    IdentityFile, RESULT_FILE, RUNNER_LOG_FILE, ResultFile, ResultOutcome, START_FILE, StartFile,
};
use crate::process::identity;
use crate::protocol::{
    self, DaemonStatusView, ErrorBody, LogPathsView, Op, Reply, Request, ResolveAs, Response,
    SubmitParams, error_codes,
};
use crate::scheduler::{self, ReservationSnapshot};

use super::views;

pub enum Msg {
    Request { request: Box<Request>, reply: oneshot::Sender<Response> },
    Tick,
    Shutdown,
}

#[derive(Debug)]
pub struct ApiError {
    pub code: &'static str,
    pub message: String,
}

impl ApiError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self { code, message: message.into() }
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(err: anyhow::Error) -> Self {
        Self::new(error_codes::INTERNAL, format!("{err:#}"))
    }
}

impl From<rusqlite::Error> for ApiError {
    fn from(err: rusqlite::Error) -> Self {
        Self::new(error_codes::INTERNAL, err.to_string())
    }
}

type ApiResult<T> = Result<T, ApiError>;

/// Side effects that must happen after (never inside) the mutation
/// transaction. They are idempotently re-derived from committed state by
/// reconcile, so a crash between commit and action loses nothing.
enum PostAction {
    PublishCancel { attempt_id: AttemptId, token: String, force: bool },
    SignalGroup { pgid: i32, signal: i32 },
}

pub struct Coordinator {
    conn: Connection,
    config: Config,
    paths: Paths,
    started_at: i64,
    /// Set when persisted scheduler semantics cannot be interpreted; blocks
    /// all ordinary admission until an operator intervenes.
    admission_blocked: Option<String>,
    /// Attempts whose runner this daemon process has spawned, with the spawn
    /// instant (for the identity grace period).
    spawned: HashMap<AttemptId, Instant>,
    /// Orphaned attempts that already received a daemon-delivered SIGTERM.
    orphan_term_sent: HashSet<AttemptId>,
}

impl Coordinator {
    pub fn new(conn: Connection, config: Config, paths: Paths) -> Result<Self> {
        // Fail fast if /proc identity primitives are unavailable; every
        // recovery decision depends on them.
        identity::boot_id().context("reading boot id")?;
        Ok(Self {
            conn,
            config,
            paths,
            started_at: now_ms(),
            admission_blocked: None,
            spawned: HashMap::new(),
            orphan_term_sent: HashSet::new(),
        })
    }

    pub fn run(mut self, mut rx: mpsc::Receiver<Msg>) {
        // Startup recovery is the same reconcile/schedule cycle as a tick:
        // adopt live runners, finalize durable results, respawn unlaunched
        // prepared attempts, orphan/quarantine the rest, restore the
        // reservation before ordinary admission.
        self.tick();
        while let Some(msg) = rx.blocking_recv() {
            match msg {
                Msg::Request { request, reply } => {
                    let response = self.handle_request(*request);
                    let _ = reply.send(response);
                }
                Msg::Tick => self.tick(),
                Msg::Shutdown => break,
            }
        }
    }

    fn tick(&mut self) {
        if let Err(err) = self.reconcile_all() {
            tracing::error!("reconcile failed: {err:#}");
        }
        if let Err(err) = self.propagate_skips() {
            tracing::error!("skip propagation failed: {err:#}");
        }
        if let Err(err) = self.schedule() {
            tracing::error!("scheduling pass failed: {err:#}");
        }
    }

    // -----------------------------------------------------------------------
    // Request handling
    // -----------------------------------------------------------------------

    fn handle_request(&mut self, request: Request) -> Response {
        let request_id = request.request_id.clone();
        let result = if request.protocol_version != protocol::PROTOCOL_VERSION {
            Err(ApiError::new(
                error_codes::UNSUPPORTED_PROTOCOL,
                format!(
                    "protocol version {} is not supported (daemon speaks {})",
                    request.protocol_version,
                    protocol::PROTOCOL_VERSION
                ),
            ))
        } else if request.op.is_mutation() {
            match &request.idempotency_key {
                Some(key) if !key.is_empty() => self.handle_mutation(&request.op, key),
                _ => Err(ApiError::new(
                    error_codes::MISSING_IDEMPOTENCY_KEY,
                    "mutating operations require a durable idempotency key",
                )),
            }
        } else {
            self.handle_read(&request.op)
        };
        match result {
            Ok(reply) => Response { request_id, reply: Some(reply), error: None },
            Err(err) => Response {
                request_id,
                reply: None,
                error: Some(ErrorBody { code: err.code.to_string(), message: err.message }),
            },
        }
    }

    fn handle_read(&mut self, op: &Op) -> ApiResult<Reply> {
        match op {
            Op::Status => Ok(Reply::Status(views::status_view(
                &self.conn,
                &self.paths,
                self.admission_blocked.is_some(),
            )?)),
            Op::Show { job } => {
                let row = db::job_row(&self.conn, *job)?
                    .ok_or_else(|| ApiError::new(error_codes::NOT_FOUND, format!("job {job} not found")))?;
                Ok(Reply::Job { job: views::job_view(&self.conn, &self.paths, &row, true)? })
            }
            Op::Logs { job, attempt } => {
                let attempt_row = match attempt {
                    Some(number) => db::attempt_by_number(&self.conn, *job, *number)?,
                    None => db::attempts_for_job(&self.conn, *job)?.into_iter().next_back(),
                }
                .ok_or_else(|| {
                    ApiError::new(
                        error_codes::NOT_FOUND,
                        format!("job {job} has no matching attempt (has it started?)"),
                    )
                })?;
                let dir = self.paths.attempt_dir(attempt_row.id);
                Ok(Reply::LogPaths(LogPathsView {
                    job: *job,
                    attempt: attempt_row.id,
                    attempt_number: attempt_row.number,
                    attempt_state: attempt_row.state.as_str().to_string(),
                    stdout: dir.join(artifacts::STDOUT_LOG_FILE).display().to_string(),
                    stderr: dir.join(artifacts::STDERR_LOG_FILE).display().to_string(),
                }))
            }
            Op::DaemonStatus => Ok(Reply::DaemonStatus(DaemonStatusView {
                pid: std::process::id(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                protocol_version: protocol::PROTOCOL_VERSION,
                scheduler_semantics_version: SCHEDULER_SEMANTICS_VERSION,
                started_at: self.started_at,
                db_path: self.paths.db().display().to_string(),
                socket_path: self.paths.socket().display().to_string(),
                active_leases: db::active_leases(&self.conn)?.len() as u32,
                queued_jobs: db::count_jobs_in_state(&self.conn, JobState::Queued)?,
                admission_blocked: self.admission_blocked.is_some(),
            })),
            Op::RecoverList => {
                let attempts = db::non_terminal_attempts(&self.conn)?
                    .into_iter()
                    .filter(|a| {
                        matches!(a.state, AttemptState::Orphaned | AttemptState::Quarantined)
                    })
                    .map(|a| views::attempt_view(&self.paths, &a))
                    .collect();
                Ok(Reply::RecoverList { attempts })
            }
            _ => Err(ApiError::new(error_codes::INTERNAL, "mutation routed to read handler")),
        }
    }

    /// Insert the operation record and the mutation in one transaction. An
    /// identical retry returns the original result; a reused key with a
    /// different payload conflicts.
    fn handle_mutation(&mut self, op: &Op, key: &str) -> ApiResult<Reply> {
        let hash = op.request_hash();
        if let Some((stored_hash, stored_response)) = db::lookup_operation(&self.conn, key)? {
            if stored_hash != hash {
                return Err(ApiError::new(
                    error_codes::IDEMPOTENCY_CONFLICT,
                    format!("idempotency key {key:?} was already used with a different request"),
                ));
            }
            let reply = serde_json::from_str(&stored_response).map_err(|err| {
                ApiError::new(error_codes::INTERNAL, format!("stored response corrupt: {err}"))
            })?;
            return Ok(reply);
        }

        // Argument and filesystem validation runs before the write
        // transaction opens: a slow or hung cwd (NFS, FUSE) must never stall
        // the transaction and every mutation queued behind it.
        if let Op::Submit(params) = op {
            validate_submission_fs(params)?;
        }

        let now = now_ms();
        let (reply, actions) = {
            let tx = self.conn.transaction().map_err(ApiError::from)?;
            let (reply, actions) = apply_mutation(&tx, &self.paths, op, now)?;
            let encoded = serde_json::to_string(&reply)
                .map_err(|err| ApiError::new(error_codes::INTERNAL, err.to_string()))?;
            db::insert_operation(&tx, key, op.kind(), &hash, &encoded, now)?;
            tx.commit().map_err(ApiError::from)?;
            (reply, actions)
        };
        for action in actions {
            self.perform(action);
        }
        // Mutations change eligibility; run the same cycle as a tick.
        self.tick();
        Ok(reply)
    }

    fn perform(&mut self, action: PostAction) {
        match action {
            PostAction::PublishCancel { attempt_id, token, force } => {
                let dir = self.paths.attempt_dir(attempt_id);
                if let Err(err) = fs::DirBuilder::new().recursive(true).mode(0o700).create(&dir) {
                    tracing::warn!("creating {} for cancel intent: {err}", dir.display());
                }
                if let Err(err) =
                    artifacts::write_artifact(&dir, CANCEL_FILE, &CancelFile { token, force }, false)
                {
                    // Reconcile republishes committed cancellation intent.
                    tracing::warn!("publishing cancel for attempt {attempt_id}: {err}");
                }
            }
            PostAction::SignalGroup { pgid, signal } => {
                unsafe {
                    libc::killpg(pgid, signal);
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Reconciliation: artifacts + /proc -> database state
    // -----------------------------------------------------------------------

    fn reconcile_all(&mut self) -> Result<()> {
        let attempts = db::non_terminal_attempts(&self.conn)?;
        // Attempts finalized by mutation paths (free functions that cannot
        // reach these maps) are pruned here instead.
        let live: std::collections::HashSet<AttemptId> =
            attempts.iter().map(|attempt| attempt.id).collect();
        self.spawned.retain(|id, _| live.contains(id));
        self.orphan_term_sent.retain(|id| live.contains(id));
        for attempt in attempts {
            if let Err(err) = self.reconcile_attempt(&attempt) {
                tracing::error!("reconciling attempt {}: {err:#}", attempt.id);
            }
        }
        Ok(())
    }

    fn reconcile_attempt(&mut self, attempt: &AttemptRow) -> Result<()> {
        let dir = self.paths.attempt_dir(attempt.id);

        // A valid terminal result always wins, whatever the current state:
        // the runner only publishes it after the command group is provably
        // gone (or was never created).
        match artifacts::read_artifact::<ResultFile>(&dir.join(RESULT_FILE)) {
            Ok(Some(result)) if result.token == attempt.launch_token => {
                return self.finalize_from_result(attempt, &result);
            }
            Ok(Some(_)) => return self.quarantine(attempt, "result artifact token mismatch"),
            Err(err) => return self.quarantine(attempt, &format!("corrupt result artifact: {err}")),
            Ok(None) => {}
        }

        // Republish committed cancellation intent (crash between commit and
        // publication, or daemon restart).
        if attempt.cancel_requested
            && !attempt.state.is_terminal()
            && !matches!(
                artifacts::read_artifact::<CancelFile>(&dir.join(CANCEL_FILE)),
                Ok(Some(_))
            )
        {
            let _ = artifacts::write_artifact(
                &dir,
                CANCEL_FILE,
                &CancelFile { token: attempt.launch_token.clone(), force: attempt.cancel_force },
                false,
            );
        }

        match attempt.state {
            AttemptState::Prepared => self.reconcile_prepared(attempt, &dir),
            AttemptState::Authorized => self.reconcile_authorized(attempt, &dir),
            AttemptState::Running => self.reconcile_running(attempt),
            AttemptState::Orphaned => self.reconcile_orphaned(attempt),
            // Quarantine holds its lease until an operator resolves it (or a
            // valid result appears above).
            AttemptState::Quarantined => Ok(()),
            _ => Ok(()),
        }
    }

    fn reconcile_prepared(&mut self, attempt: &AttemptRow, dir: &std::path::Path) -> Result<()> {
        // Not spawned by this daemon process: crash-recovery. The per-attempt
        // runner flock makes a duplicate spawn race safe.
        let Some(spawned_at) = self.spawned.get(&attempt.id).copied() else {
            self.launch(attempt.id);
            return Ok(());
        };

        match artifacts::read_artifact::<IdentityFile>(&dir.join(IDENTITY_FILE)) {
            Ok(Some(identity_file)) => {
                if identity_file.token != attempt.launch_token {
                    return self.quarantine(attempt, "identity artifact token mismatch");
                }
                if attempt.cancel_requested {
                    // Cancellation of a prepared attempt finalizes it in the
                    // cancel mutation; reaching here means only the flag
                    // raced, and the runner will exit on cancel.json.
                    return Ok(());
                }
                // A stale identity left by a runner that died before this
                // daemon spawned its replacement must not be authorized: the
                // replacement republishes its own identity (the write is
                // non-exclusive under the per-attempt flock). Since the
                // attempt is still `prepared`, no `start` was ever published
                // and failing after the grace period is safe.
                if !identity::identity_alive(
                    identity_file.runner_pid,
                    identity_file.runner_start_time,
                    &identity_file.boot_id,
                ) {
                    if spawned_at.elapsed().as_millis() as u64
                        > self.config.runner_identity_grace_ms
                    {
                        self.finalize_attempt_and_job(
                            attempt,
                            AttemptState::Failed,
                            None,
                            None,
                            Some("runner died before launch authorization"),
                            "daemon",
                        )?;
                    }
                    return Ok(());
                }
                self.authorize(attempt, &identity_file)
            }
            Ok(None) => {
                // Never authorized and no runner identity: if the runner does
                // not appear within the grace period it is dead, and without
                // `start` no command can ever have run — safe to fail.
                if spawned_at.elapsed().as_millis() as u64 > self.config.runner_identity_grace_ms {
                    self.finalize_attempt_and_job(
                        attempt,
                        AttemptState::Failed,
                        None,
                        None,
                        Some("runner never published its identity"),
                        "daemon",
                    )?;
                }
                Ok(())
            }
            Err(err) => self.quarantine(attempt, &format!("corrupt identity artifact: {err}")),
        }
    }

    /// Commit the launch authorization, then publish the durable `start`
    /// marker. Cancellation is serialized against this because both run on
    /// the coordinator.
    fn authorize(&mut self, attempt: &AttemptRow, identity_file: &IdentityFile) -> Result<()> {
        let now = now_ms();
        let job = db::job_row(&self.conn, attempt.job_id)?.context("job of live attempt")?;
        if job.state != JobState::Starting {
            // Revalidation failed (e.g. cancel raced); leave the runner
            // waiting — cancellation paths stop it.
            return Ok(());
        }
        {
            let tx = self.conn.transaction()?;
            db::set_attempt_runner_identity(
                &tx,
                attempt.id,
                identity_file.runner_pid,
                identity_file.runner_start_time,
                &identity_file.boot_id,
            )?;
            db::set_attempt_authorized(&tx, attempt.id, now)?;
            db::append_event(
                &tx,
                Some(attempt.job_id),
                Some(attempt.id),
                "attempt_authorized",
                "daemon",
                Some(&json!({ "runnerPid": identity_file.runner_pid }).to_string()),
            )?;
            tx.commit()?;
        }
        let dir = self.paths.attempt_dir(attempt.id);
        match artifacts::write_artifact(
            &dir,
            START_FILE,
            &StartFile { token: attempt.launch_token.clone() },
            true,
        ) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
            // Committed but unpublished: the authorized reconcile branch
            // republishes on the next tick.
            Err(err) => {
                tracing::warn!("publishing start for attempt {}: {err}", attempt.id);
                Ok(())
            }
        }
    }

    fn reconcile_authorized(&mut self, attempt: &AttemptRow, dir: &std::path::Path) -> Result<()> {
        let runner_dead = match (attempt.runner_pid, attempt.runner_start_time, &attempt.runner_boot_id)
        {
            (Some(pid), Some(start_time), Some(boot)) => {
                !identity::identity_alive(pid, start_time, boot)
            }
            _ => false,
        };
        let start_exists = dir.join(START_FILE).exists();

        // Dead runner that never saw a published authorization: the command
        // provably never ran, so the lease is safe to release.
        if runner_dead && !start_exists {
            return self.finalize_attempt_and_job(
                attempt,
                AttemptState::Failed,
                None,
                None,
                Some("runner died before launch authorization was published"),
                "daemon",
            );
        }

        // Republish a missing authorization marker; the committed state
        // still allows it.
        if !start_exists {
            match artifacts::write_artifact(
                dir,
                START_FILE,
                &StartFile { token: attempt.launch_token.clone() },
                true,
            ) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(err) => tracing::warn!("republishing start for attempt {}: {err}", attempt.id),
            }
        }

        match artifacts::read_artifact::<ExecFile>(&dir.join(EXEC_FILE)) {
            Ok(Some(exec)) if exec.token == attempt.launch_token => {
                let now = now_ms();
                let tx = self.conn.transaction()?;
                db::set_attempt_running(&tx, attempt.id, exec.pid, exec.pgid, exec.start_time, now)?;
                db::update_job_state(&tx, attempt.job_id, JobState::Running, None, now)?;
                db::append_event(
                    &tx,
                    Some(attempt.job_id),
                    Some(attempt.id),
                    "attempt_running",
                    "runner",
                    Some(&json!({ "pid": exec.pid, "pgid": exec.pgid }).to_string()),
                )?;
                tx.commit()?;
                Ok(())
            }
            Ok(Some(_)) => self.quarantine(attempt, "exec artifact token mismatch"),
            Err(err) => self.quarantine(attempt, &format!("corrupt exec artifact: {err}")),
            Ok(None) if runner_dead => {
                // Authorization was visible but the runner died before the
                // exec handshake: a command process group may or may not
                // exist, and its pgid is unknown. Never guess.
                self.quarantine(
                    attempt,
                    "runner died between authorization and exec handshake; command state unknown",
                )
            }
            Ok(None) => Ok(()),
        }
    }

    fn reconcile_running(&mut self, attempt: &AttemptRow) -> Result<()> {
        let runner_alive = match (attempt.runner_pid, attempt.runner_start_time, &attempt.runner_boot_id)
        {
            (Some(pid), Some(start_time), Some(boot)) => {
                identity::identity_alive(pid, start_time, boot)
            }
            _ => false,
        };
        if runner_alive {
            return Ok(());
        }
        // Runner died mid-supervision. The command group may still be live;
        // the lease is retained until it is proven empty.
        let now = now_ms();
        let tx = self.conn.transaction()?;
        db::set_attempt_state(&tx, attempt.id, AttemptState::Orphaned, Some("runner died"))?;
        db::update_job_state(
            &tx,
            attempt.job_id,
            JobState::NeedsAttention,
            Some("attempt orphaned: runner died while the command may still run"),
            now,
        )?;
        db::append_event(
            &tx,
            Some(attempt.job_id),
            Some(attempt.id),
            "attempt_orphaned",
            "daemon",
            None,
        )?;
        tx.commit()?;
        Ok(())
    }

    fn reconcile_orphaned(&mut self, attempt: &AttemptRow) -> Result<()> {
        let (Some(pgid), Some(boot)) = (attempt.cmd_pgid, attempt.runner_boot_id.as_deref()) else {
            // Orphaned is only entered from running, which has exec identity.
            return self.quarantine(attempt, "orphaned attempt lacks command identity");
        };
        if !identity::group_possibly_alive(pgid, boot, attempt.cmd_start_time) {
            let (state, message) = if attempt.cancel_requested {
                (AttemptState::Cancelled, "cancelled; orphaned process group drained")
            } else {
                (AttemptState::Lost, "runner died; command exit status unknown")
            };
            return self.finalize_attempt_and_job(attempt, state, None, None, Some(message), "daemon");
        }
        // The group is still live and there is no runner: the daemon itself
        // delivers committed cancellation signals from outside the group.
        if attempt.cancel_requested {
            if self.orphan_term_sent.insert(attempt.id) {
                unsafe {
                    libc::killpg(pgid, libc::SIGTERM);
                }
            }
            let grace_elapsed = attempt
                .cancel_requested_at
                .is_some_and(|at| now_ms() - at >= self.config.cancel_grace_ms as i64);
            if attempt.cancel_force && grace_elapsed {
                unsafe {
                    libc::killpg(pgid, libc::SIGKILL);
                }
            }
        }
        Ok(())
    }

    fn quarantine(&mut self, attempt: &AttemptRow, reason: &str) -> Result<()> {
        if attempt.state == AttemptState::Quarantined {
            return Ok(());
        }
        tracing::error!("quarantining attempt {}: {reason}", attempt.id);
        let now = now_ms();
        let tx = self.conn.transaction()?;
        db::set_attempt_state(&tx, attempt.id, AttemptState::Quarantined, Some(reason))?;
        db::update_job_state(
            &tx,
            attempt.job_id,
            JobState::NeedsAttention,
            Some(&format!("attempt quarantined: {reason}")),
            now,
        )?;
        db::append_event(
            &tx,
            Some(attempt.job_id),
            Some(attempt.id),
            "attempt_quarantined",
            "daemon",
            Some(&json!({ "reason": reason }).to_string()),
        )?;
        tx.commit()?;
        Ok(())
    }

    fn finalize_from_result(&mut self, attempt: &AttemptRow, result: &ResultFile) -> Result<()> {
        let (state, message) = match result.outcome {
            ResultOutcome::Exited => {
                if result.exit_code == Some(0) {
                    (AttemptState::Succeeded, result.message.clone())
                } else {
                    let describe = match (result.exit_code, result.term_signal) {
                        (Some(code), _) => format!("command exited with code {code}"),
                        (None, Some(signal)) => format!("command terminated by signal {signal}"),
                        (None, None) => "command exited abnormally".to_string(),
                    };
                    (AttemptState::Failed, Some(match &result.message {
                        Some(extra) => format!("{describe} ({extra})"),
                        None => describe,
                    }))
                }
            }
            ResultOutcome::LaunchFailed | ResultOutcome::AuthorizationTimeout => {
                (AttemptState::Failed, result.message.clone())
            }
            ResultOutcome::Cancelled => (AttemptState::Cancelled, result.message.clone()),
        };
        self.finalize_attempt_and_job(
            attempt,
            state,
            result.exit_code,
            result.term_signal,
            message.as_deref(),
            "runner",
        )
    }

    /// One transaction: terminal attempt state, lease release, job
    /// transition (including automatic retry policy), and events.
    fn finalize_attempt_and_job(
        &mut self,
        attempt: &AttemptRow,
        state: AttemptState,
        exit_code: Option<i32>,
        term_signal: Option<i32>,
        message: Option<&str>,
        actor: &str,
    ) -> Result<()> {
        let now = now_ms();
        {
            let tx = self.conn.transaction()?;
            db::finalize_attempt(&tx, attempt.id, state, exit_code, term_signal, message, now)?;
            db::release_lease(&tx, attempt.id, now)?;
            db::append_event(
                &tx,
                Some(attempt.job_id),
                Some(attempt.id),
                &format!("attempt_{}", state.as_str()),
                actor,
                message.map(|m| json!({ "message": m }).to_string()).as_deref(),
            )?;
            let job = db::job_row(&tx, attempt.job_id)?.context("job of finalized attempt")?;
            if !job.state.is_terminal() {
                match state {
                    AttemptState::Succeeded => {
                        db::update_job_state(&tx, job.id, JobState::Succeeded, None, now)?;
                        db::append_event(&tx, Some(job.id), None, "job_succeeded", actor, None)?;
                    }
                    AttemptState::Cancelled => {
                        db::update_job_state(&tx, job.id, JobState::Cancelled, message, now)?;
                        db::append_event(&tx, Some(job.id), None, "job_cancelled", actor, None)?;
                    }
                    AttemptState::Lost => {
                        db::update_job_state(&tx, job.id, JobState::Lost, message, now)?;
                        db::append_event(&tx, Some(job.id), None, "job_lost", actor, None)?;
                    }
                    AttemptState::Failed => {
                        if job.attempt_count < job.max_attempts && !attempt.cancel_requested {
                            let not_before =
                                (job.retry_delay_ms > 0).then(|| now + job.retry_delay_ms);
                            db::update_job_state(&tx, job.id, JobState::Queued, message, now)?;
                            db::set_job_retry_not_before(&tx, job.id, not_before, now)?;
                            db::append_event(
                                &tx,
                                Some(job.id),
                                Some(attempt.id),
                                "retry_scheduled",
                                "daemon",
                                Some(
                                    &json!({
                                        "attemptsUsed": job.attempt_count,
                                        "maxAttempts": job.max_attempts,
                                        "notBefore": not_before,
                                    })
                                    .to_string(),
                                ),
                            )?;
                        } else {
                            db::update_job_state(&tx, job.id, JobState::Failed, message, now)?;
                            db::append_event(&tx, Some(job.id), None, "job_failed", actor, None)?;
                        }
                    }
                    _ => unreachable!("finalize called with non-terminal state"),
                }
            }
            tx.commit()?;
        }
        self.spawned.remove(&attempt.id);
        self.orphan_term_sent.remove(&attempt.id);
        Ok(())
    }

    /// Skip queued/held jobs whose success dependency terminally failed;
    /// cascades until a fixed point.
    fn propagate_skips(&mut self) -> Result<()> {
        loop {
            let violated = db::jobs_with_violated_dependencies(&self.conn)?;
            if violated.is_empty() {
                return Ok(());
            }
            let now = now_ms();
            let tx = self.conn.transaction()?;
            for (job, parent) in violated {
                db::update_job_state(
                    &tx,
                    job,
                    JobState::Skipped,
                    Some(&format!("prerequisite job {parent} did not succeed")),
                    now,
                )?;
                db::append_event(
                    &tx,
                    Some(job),
                    None,
                    "job_skipped",
                    "daemon",
                    Some(&json!({ "failedPrerequisite": parent }).to_string()),
                )?;
            }
            tx.commit()?;
        }
    }

    // -----------------------------------------------------------------------
    // Scheduling
    // -----------------------------------------------------------------------

    fn schedule(&mut self) -> Result<()> {
        if self.admission_blocked.is_some() {
            return Ok(());
        }
        let now = now_ms();
        let leases = db::active_leases(&self.conn)?;
        let reservation = db::active_reservation(&self.conn)?;
        if let Some(res) = &reservation
            && res.semantics_version != SCHEDULER_SEMANTICS_VERSION
        {
            let reason = format!(
                "active reservation {} uses scheduler semantics version {} (daemon implements {}); \
                 admission blocked until an operator migrates or resolves it",
                res.id, res.semantics_version, SCHEDULER_SEMANTICS_VERSION
            );
            tracing::error!("{reason}");
            self.admission_blocked = Some(reason);
            return Ok(());
        }
        let eligible = db::eligible_candidates(&self.conn, now)?;
        let max_seq = db::max_job_seq(&self.conn)?;
        let active: Vec<_> = leases.iter().map(|(_, lease)| lease.clone()).collect();
        let snapshot = reservation.as_ref().map(|res| ReservationSnapshot {
            job_id: res.job_id,
            cutoff_seq: res.cutoff_seq,
            consumed: res.consumed.clone(),
        });
        let outcome = scheduler::plan_pass(&active, &eligible, snapshot.as_ref(), max_seq);
        if outcome.starts.is_empty()
            && !outcome.satisfy_reservation
            && outcome.invalidate_reservation.is_none()
            && outcome.create_reservation.is_none()
        {
            return Ok(());
        }

        let lease_attempt_by_job: BTreeMap<JobId, AttemptId> =
            leases.iter().map(|(attempt, lease)| (lease.job_id, *attempt)).collect();
        let mut launched: Vec<AttemptId> = Vec::new();
        {
            let tx = self.conn.transaction()?;
            // Revalidate and create every attempt + lease first so backfill
            // and seeding rows can reference attempt IDs.
            let mut batch: BTreeMap<JobId, AttemptId> = BTreeMap::new();
            for start in &outcome.starts {
                let job = db::job_row(&tx, start.job_id)?.context("scheduled job exists")?;
                anyhow::ensure!(
                    job.state == JobState::Queued,
                    "revalidation failed: job {} is {}",
                    job.id,
                    job.state
                );
                let token = uuid::Uuid::new_v4().to_string();
                let attempt = db::create_attempt_with_lease(&tx, &job, &token, now)?;
                db::append_event(
                    &tx,
                    Some(job.id),
                    Some(attempt.id),
                    "attempt_prepared",
                    "daemon",
                    Some(
                        &json!({
                            "number": attempt.number,
                            "maxParallelRuns": job.max_parallel_runs,
                            "backfill": start.consumes_bypass(),
                        })
                        .to_string(),
                    ),
                )?;
                batch.insert(job.id, attempt.id);
                launched.push(attempt.id);
            }

            let mut current_reservation = reservation.as_ref().map(|res| (res.id, res.job_id));
            if outcome.satisfy_reservation {
                let (id, job) = current_reservation.take().context("satisfy without reservation")?;
                db::resolve_reservation(&tx, id, "satisfied", "protected job started", now)?;
                db::append_event(&tx, Some(job), None, "reservation_satisfied", "daemon", None)?;
            }
            if let Some(reason) = outcome.invalidate_reservation {
                let (id, job) =
                    current_reservation.take().context("invalidate without reservation")?;
                db::resolve_reservation(&tx, id, "invalidated", reason, now)?;
                db::append_event(
                    &tx,
                    Some(job),
                    None,
                    "reservation_invalidated",
                    "daemon",
                    Some(&json!({ "reason": reason }).to_string()),
                )?;
            }
            if let Some(new_res) = &outcome.create_reservation {
                // Blockers: the shadow set when protection was created —
                // active leases plus this batch's pre-protection admissions.
                let blockers: Vec<AttemptId> = leases
                    .iter()
                    .map(|(attempt, _)| *attempt)
                    .chain(outcome.starts.iter().filter_map(|start| {
                        (start.bypasses != Some(new_res.job_id))
                            .then(|| batch.get(&start.job_id).copied())
                            .flatten()
                    }))
                    .collect();
                let rid = db::create_reservation(
                    &tx,
                    new_res.job_id,
                    new_res.cutoff_seq,
                    SCHEDULER_SEMANTICS_VERSION,
                    &blockers,
                    now,
                )?;
                for job in &new_res.initial_consumed {
                    let attempt =
                        lease_attempt_by_job.get(job).or_else(|| batch.get(job)).copied();
                    db::record_backfill(&tx, rid, *job, "initial_active", attempt, now)?;
                }
                db::append_event(
                    &tx,
                    Some(new_res.job_id),
                    None,
                    "reservation_created",
                    "daemon",
                    Some(
                        &json!({
                            "backfillCutoff": new_res.cutoff_seq,
                            "initialConsumed": new_res.initial_consumed,
                        })
                        .to_string(),
                    ),
                )?;
                current_reservation = Some((rid, new_res.job_id));
            }
            for start in &outcome.starts {
                let Some(bypassed) = start.bypasses else { continue };
                // A pass can consume bypasses of the old reservation (before
                // it was satisfied) and of a newly created one; attribute by
                // protected job.
                let old_reservation = reservation.as_ref().map(|res| (res.id, res.job_id));
                let (rid, _) = [current_reservation, old_reservation]
                    .into_iter()
                    .flatten()
                    .find(|(_, job)| *job == bypassed)
                    .context("backfill without matching reservation")?;
                db::record_backfill(&tx, rid, start.job_id, "admitted", batch.get(&start.job_id).copied(), now)?;
                db::append_event(
                    &tx,
                    Some(start.job_id),
                    batch.get(&start.job_id).copied(),
                    "backfill_admitted",
                    "daemon",
                    Some(&json!({ "bypassedProtectedJob": bypassed }).to_string()),
                )?;
            }
            tx.commit()?;
        }

        // Spawning happens strictly after the commit; a crash in between is
        // recovered by the prepared-attempt respawn path.
        for attempt in launched {
            self.launch(attempt);
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Runner launch
    // -----------------------------------------------------------------------

    fn launch(&mut self, attempt_id: AttemptId) {
        self.spawned.insert(attempt_id, Instant::now());
        let result = self.try_launch(attempt_id);
        if let Err(err) = result {
            tracing::error!("launching attempt {attempt_id}: {err:#}");
            if let Ok(Some(attempt)) = db::attempt_row(&self.conn, attempt_id)
                && !attempt.state.is_terminal()
                && let Err(finalize_err) = self.finalize_attempt_and_job(
                    &attempt,
                    AttemptState::Failed,
                    None,
                    None,
                    Some(&format!("runner spawn failed: {err:#}")),
                    "daemon",
                )
            {
                tracing::error!("finalizing failed launch {attempt_id}: {finalize_err:#}");
            }
        }
    }

    fn try_launch(&mut self, attempt_id: AttemptId) -> Result<()> {
        let attempt =
            db::attempt_row(&self.conn, attempt_id)?.context("attempt to launch exists")?;
        let job = db::job_row(&self.conn, attempt.job_id)?.context("job of attempt exists")?;
        let dir = self.paths.attempt_dir(attempt.id);
        if !dir.exists() {
            fs::DirBuilder::new().recursive(true).mode(0o700).create(&dir)?;
        }

        match artifacts::read_artifact::<CommandFile>(&dir.join(COMMAND_FILE)) {
            // Crash-recovery respawn of the same attempt: reuse the identical
            // command record.
            Ok(Some(existing)) if existing.token == attempt.launch_token => {}
            Ok(Some(_)) => anyhow::bail!("attempt directory reused with a different token"),
            Err(err) => anyhow::bail!("corrupt command artifact: {err}"),
            Ok(None) => {
                // A fresh attempt directory must not contain protocol
                // artifacts from a previous life.
                for stale in [IDENTITY_FILE, START_FILE, EXEC_FILE, RESULT_FILE] {
                    anyhow::ensure!(
                        !dir.join(stale).exists(),
                        "attempt directory contains stale {stale} without a command record"
                    );
                }
                artifacts::write_artifact(
                    &dir,
                    COMMAND_FILE,
                    &CommandFile {
                        attempt_id: attempt.id,
                        job_id: job.id,
                        token: attempt.launch_token.clone(),
                        argv: job.args.clone(),
                        cwd: job.cwd.clone(),
                        env: job.env.clone(),
                        start_wait_ms: self.config.runner_start_wait_ms,
                        cancel_grace_ms: self.config.cancel_grace_ms,
                        poll_ms: self.config.runner_poll_ms,
                    },
                    true,
                )?;
            }
        }

        let runner_log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join(RUNNER_LOG_FILE))?;
        let runner_log_err = runner_log.try_clone()?;
        let exe = std::env::current_exe().context("resolving mlqd executable")?;
        let mut cmd = std::process::Command::new(exe);
        cmd.arg("__runner")
            .arg("--attempt-dir")
            .arg(&dir)
            .stdin(Stdio::null())
            .stdout(runner_log)
            .stderr(runner_log_err);
        unsafe {
            // A fresh session detaches the runner from the daemon's lifetime
            // so daemon restart/upgrade never kills workers.
            cmd.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        // SIGCHLD is SIG_IGN in the daemon, so the runner is auto-reaped;
        // dropping the child handle does not kill it.
        cmd.spawn().context("spawning attempt runner")?;
        db::append_event(
            &self.conn,
            Some(job.id),
            Some(attempt.id),
            "runner_spawned",
            "daemon",
            None,
        )?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Mutations (free functions so they can run inside the coordinator's
// transaction without borrowing the whole coordinator)
// ---------------------------------------------------------------------------

fn api_not_found(job: JobId) -> ApiError {
    ApiError::new(error_codes::NOT_FOUND, format!("job {job} not found"))
}

fn invalidate_reservation_if_protected(
    tx: &Transaction<'_>,
    job: JobId,
    reason: &str,
    now: i64,
) -> ApiResult<()> {
    if let Some(res) = db::active_reservation(tx)?
        && res.job_id == job
    {
        db::resolve_reservation(tx, res.id, "invalidated", reason, now)?;
        db::append_event(
            tx,
            Some(job),
            None,
            "reservation_invalidated",
            "client",
            Some(&json!({ "reason": reason }).to_string()),
        )?;
    }
    Ok(())
}

fn job_reply(tx: &Transaction<'_>, paths: &Paths, job: JobId) -> ApiResult<Reply> {
    let row = db::job_row(tx, job)?.ok_or_else(|| api_not_found(job))?;
    Ok(Reply::Job { job: views::job_view(tx, paths, &row, true)? })
}

fn apply_mutation(
    tx: &Transaction<'_>,
    paths: &Paths,
    op: &Op,
    now: i64,
) -> ApiResult<(Reply, Vec<PostAction>)> {
    match op {
        Op::Submit(params) => {
            validate_submission_parents(tx, params)?;
            let job = db::insert_job(tx, params, now)?;
            db::append_event(
                tx,
                Some(job),
                None,
                "job_submitted",
                "client",
                Some(
                    &json!({
                        "name": params.name,
                        "maxParallelRuns": params.max_parallel_runs,
                    })
                    .to_string(),
                ),
            )?;
            let row = db::job_row(tx, job)?.ok_or_else(|| api_not_found(job))?;
            Ok((
                Reply::Submitted { job: views::job_view(tx, paths, &row, false)? },
                Vec::new(),
            ))
        }
        Op::Cancel { job, force } => {
            let actions = cancel_job(tx, *job, *force, now)?;
            Ok((job_reply(tx, paths, *job)?, actions))
        }
        Op::Hold { job } => {
            let row = db::job_row(tx, *job)?.ok_or_else(|| api_not_found(*job))?;
            if row.state != JobState::Queued {
                return Err(ApiError::new(
                    error_codes::INVALID_STATE,
                    format!("only queued jobs can be held; job {} is {}", row.id, row.state),
                ));
            }
            invalidate_reservation_if_protected(tx, row.id, "protected job was held", now)?;
            db::update_job_state(tx, row.id, JobState::Held, None, now)?;
            db::append_event(tx, Some(row.id), None, "job_held", "client", None)?;
            Ok((job_reply(tx, paths, *job)?, Vec::new()))
        }
        Op::Release { job } => {
            let row = db::job_row(tx, *job)?.ok_or_else(|| api_not_found(*job))?;
            if row.state != JobState::Held {
                return Err(ApiError::new(
                    error_codes::INVALID_STATE,
                    format!("only held jobs can be released; job {} is {}", row.id, row.state),
                ));
            }
            db::update_job_state(tx, row.id, JobState::Queued, None, now)?;
            db::append_event(tx, Some(row.id), None, "job_released", "client", None)?;
            Ok((job_reply(tx, paths, *job)?, Vec::new()))
        }
        Op::Retry { job } => {
            let row = db::job_row(tx, *job)?.ok_or_else(|| api_not_found(*job))?;
            if !row.state.is_retryable() {
                return Err(ApiError::new(
                    error_codes::INVALID_STATE,
                    format!(
                        "only failed or lost jobs can be retried; job {} is {}",
                        row.id, row.state
                    ),
                ));
            }
            db::update_job_state(tx, row.id, JobState::Queued, None, now)?;
            db::set_job_retry_not_before(tx, row.id, None, now)?;
            db::append_event(tx, Some(row.id), None, "job_retried", "client", None)?;
            Ok((job_reply(tx, paths, *job)?, Vec::new()))
        }
        Op::SetMaxParallelRuns { job, max_parallel_runs } => {
            if *max_parallel_runs == 0 {
                return Err(ApiError::new(
                    error_codes::INVALID_ARGUMENT,
                    "maxParallelRuns must be a positive integer",
                ));
            }
            let row = db::job_row(tx, *job)?.ok_or_else(|| api_not_found(*job))?;
            if !matches!(row.state, JobState::Queued | JobState::Held) {
                return Err(ApiError::new(
                    error_codes::INVALID_STATE,
                    format!(
                        "maxParallelRuns is immutable after launch preparation; job {} is {}",
                        row.id, row.state
                    ),
                ));
            }
            invalidate_reservation_if_protected(
                tx,
                row.id,
                "protected job concurrency limit was changed",
                now,
            )?;
            db::set_job_max_parallel_runs(tx, row.id, *max_parallel_runs, now)?;
            db::append_event(
                tx,
                Some(row.id),
                None,
                "max_parallel_runs_changed",
                "client",
                Some(
                    &json!({ "from": row.max_parallel_runs, "to": max_parallel_runs }).to_string(),
                ),
            )?;
            Ok((job_reply(tx, paths, *job)?, Vec::new()))
        }
        Op::RecoverResolve { job, attempt, resolve_as } => {
            let attempt_row = db::attempt_by_number(tx, *job, *attempt)?.ok_or_else(|| {
                ApiError::new(
                    error_codes::NOT_FOUND,
                    format!("job {job} has no attempt number {attempt}"),
                )
            })?;
            if !matches!(attempt_row.state, AttemptState::Orphaned | AttemptState::Quarantined) {
                return Err(ApiError::new(
                    error_codes::INVALID_STATE,
                    format!(
                        "attempt {} is {}, not awaiting recovery",
                        attempt_row.id, attempt_row.state
                    ),
                ));
            }
            // Refuse to release the lease while known containment is live.
            if let (Some(pgid), Some(boot)) =
                (attempt_row.cmd_pgid, attempt_row.runner_boot_id.as_deref())
                && identity::group_possibly_alive(pgid, boot, attempt_row.cmd_start_time)
            {
                return Err(ApiError::new(
                    error_codes::UNSAFE_RESOLUTION,
                    format!(
                        "command process group {pgid} still has live members {:?}; \
                         resolve after they exit or are killed",
                        identity::group_members(pgid)
                    ),
                ));
            }
            let (attempt_state, job_state) = match resolve_as {
                ResolveAs::Lost => (AttemptState::Lost, JobState::Lost),
                ResolveAs::Cancelled => (AttemptState::Cancelled, JobState::Cancelled),
            };
            // With no exec handshake the command's pgid was never learned, so
            // the safety gate above could not check anything: the release is
            // on the operator's judgement, and the record says so.
            let message = if attempt_row.cmd_pgid.is_none() {
                "resolved by operator via recover resolve (containment unknown: \
                 no exec handshake was recorded; verify no stray processes remain)"
            } else {
                "resolved by operator via recover resolve"
            };
            db::finalize_attempt(tx, attempt_row.id, attempt_state, None, None, Some(message), now)?;
            db::release_lease(tx, attempt_row.id, now)?;
            db::update_job_state(tx, *job, job_state, Some(message), now)?;
            db::append_event(
                tx,
                Some(*job),
                Some(attempt_row.id),
                "recover_resolved",
                "client",
                Some(&json!({ "as": attempt_state.as_str() }).to_string()),
            )?;
            Ok((job_reply(tx, paths, *job)?, Vec::new()))
        }
        _ => Err(ApiError::new(error_codes::INTERNAL, "read op routed to mutation handler")),
    }
}

fn cancel_job(tx: &Transaction<'_>, job: JobId, force: bool, now: i64) -> ApiResult<Vec<PostAction>> {
    let row = db::job_row(tx, job)?.ok_or_else(|| api_not_found(job))?;
    match row.state {
        JobState::Queued | JobState::Held => {
            invalidate_reservation_if_protected(tx, row.id, "protected job was cancelled", now)?;
            db::update_job_state(tx, row.id, JobState::Cancelled, Some("cancelled before start"), now)?;
            db::append_event(tx, Some(row.id), None, "job_cancelled", "client", None)?;
            Ok(Vec::new())
        }
        JobState::Starting | JobState::Running | JobState::NeedsAttention => {
            let attempt = db::live_attempt_for_job(tx, row.id)?.ok_or_else(|| {
                ApiError::new(
                    error_codes::INVALID_STATE,
                    format!("job {} has no live attempt to cancel", row.id),
                )
            })?;
            db::set_attempt_cancel_requested(tx, attempt.id, force, now)?;
            db::append_event(
                tx,
                Some(row.id),
                Some(attempt.id),
                "cancel_requested",
                "client",
                Some(&json!({ "force": force }).to_string()),
            )?;
            match attempt.state {
                // Never authorized: `start` will never be published, so the
                // command can never run. Finalize immediately and release the
                // lease; the waiting runner exits on cancel.json.
                AttemptState::Prepared => {
                    db::finalize_attempt(
                        tx,
                        attempt.id,
                        AttemptState::Cancelled,
                        None,
                        None,
                        Some("cancelled before launch authorization"),
                        now,
                    )?;
                    db::release_lease(tx, attempt.id, now)?;
                    db::update_job_state(
                        tx,
                        row.id,
                        JobState::Cancelled,
                        Some("cancelled before launch"),
                        now,
                    )?;
                    db::append_event(tx, Some(row.id), None, "job_cancelled", "client", None)?;
                    Ok(vec![PostAction::PublishCancel {
                        attempt_id: attempt.id,
                        token: attempt.launch_token.clone(),
                        force,
                    }])
                }
                AttemptState::Authorized | AttemptState::Running => {
                    Ok(vec![PostAction::PublishCancel {
                        attempt_id: attempt.id,
                        token: attempt.launch_token.clone(),
                        force,
                    }])
                }
                // No runner remains; the daemon delivers the signal itself
                // and reconcile finalizes once the group drains.
                AttemptState::Orphaned => Ok(attempt
                    .cmd_pgid
                    .map(|pgid| PostAction::SignalGroup { pgid, signal: libc::SIGTERM })
                    .into_iter()
                    .collect()),
                AttemptState::Quarantined => Err(ApiError::new(
                    error_codes::UNSAFE_RESOLUTION,
                    format!(
                        "attempt {} is quarantined with unknown process containment; \
                         inspect and use `mlq recover resolve`",
                        attempt.id
                    ),
                )),
                _ => Err(ApiError::new(
                    error_codes::INVALID_STATE,
                    format!("attempt {} is already {}", attempt.id, attempt.state),
                )),
            }
        }
        state if state.is_terminal() => Err(ApiError::new(
            error_codes::INVALID_STATE,
            format!("job {} is already {state}", row.id),
        )),
        state => Err(ApiError::new(
            error_codes::INVALID_STATE,
            format!("job {} is {state} and cannot be cancelled", row.id),
        )),
    }
}

/// Argument and filesystem validation with no database dependency; runs on
/// the coordinator before the write transaction opens.
fn validate_submission_fs(params: &SubmitParams) -> ApiResult<()> {
    let invalid = |message: String| ApiError::new(error_codes::INVALID_ARGUMENT, message);
    if params.name.is_empty() || params.name.contains('\0') {
        return Err(invalid("job name must be non-empty UTF-8 without NUL".into()));
    }
    if params.max_parallel_runs == 0 {
        return Err(invalid("maxParallelRuns must be a positive integer".into()));
    }
    if params.max_attempts == 0 {
        return Err(invalid("maxAttempts must be a positive integer".into()));
    }
    if params.args.is_empty() {
        return Err(invalid("a command argument vector is required".into()));
    }
    for arg in &params.args {
        if arg.contains('\0') {
            return Err(invalid("command arguments must not contain NUL".into()));
        }
    }
    for (key, value) in &params.env {
        if key.is_empty() || key.contains('\0') || key.contains('=') || value.contains('\0') {
            return Err(invalid(format!("invalid environment entry {key:?}")));
        }
    }
    let cwd = std::path::Path::new(&params.cwd);
    if !cwd.is_absolute() || params.cwd.contains('\0') {
        return Err(invalid("working directory must be an absolute path".into()));
    }
    // Fast feedback only; the launch path revalidates because queued
    // filesystem state can change.
    if !cwd.is_dir() {
        return Err(invalid(format!("working directory {} is not a directory", params.cwd)));
    }
    let executable = &params.args[0];
    if executable.contains('/') {
        if !std::path::Path::new(executable).is_file() {
            return Err(invalid(format!("executable {executable} does not exist")));
        }
    } else {
        let path = params.env.get("PATH").map(String::as_str).unwrap_or("");
        let found = std::env::split_paths(path)
            .any(|dir| !dir.as_os_str().is_empty() && dir.join(executable).is_file());
        if !found {
            return Err(invalid(format!(
                "executable {executable} not found on the persisted PATH"
            )));
        }
    }
    Ok(())
}

fn validate_submission_parents(tx: &Transaction<'_>, params: &SubmitParams) -> ApiResult<()> {
    for parent in params.after_success.iter().chain(&params.after_completion) {
        if !db::job_exists(tx, *parent)? {
            return Err(ApiError::new(
                error_codes::NOT_FOUND,
                format!("dependency parent job {parent} does not exist"),
            ));
        }
    }
    Ok(())
}
