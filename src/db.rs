//! SQLite persistence: pragmas, embedded monotonic migrations, and the
//! transactional queries the coordinator composes. Every state transition and
//! its event are committed together by the caller.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};

use crate::domain::{
    AttemptId, AttemptState, DepRequirement, JobId, JobState, SCHEDULER_SEMANTICS_VERSION, now_ms,
};
use crate::protocol::SubmitParams;
use crate::scheduler::{ActiveLease, Candidate};

const MIGRATIONS: &[&str] = &[
    // v1: initial schema.
    r#"
    CREATE TABLE jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        state TEXT NOT NULL,
        cwd TEXT NOT NULL,
        args TEXT NOT NULL,
        env TEXT NOT NULL,
        max_parallel_runs INTEGER NOT NULL CHECK (max_parallel_runs >= 1),
        max_attempts INTEGER NOT NULL,
        retry_delay_ms INTEGER NOT NULL,
        retry_not_before INTEGER,
        attempt_count INTEGER NOT NULL DEFAULT 0,
        state_reason TEXT,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        finished_at INTEGER
    );
    CREATE INDEX jobs_state ON jobs(state);

    CREATE TABLE dependencies (
        parent_id INTEGER NOT NULL REFERENCES jobs(id),
        child_id INTEGER NOT NULL REFERENCES jobs(id),
        requirement TEXT NOT NULL,
        PRIMARY KEY (parent_id, child_id)
    ) WITHOUT ROWID;
    CREATE INDEX dependencies_child ON dependencies(child_id);

    CREATE TABLE attempts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER NOT NULL REFERENCES jobs(id),
        number INTEGER NOT NULL,
        state TEXT NOT NULL,
        launch_token TEXT NOT NULL,
        max_parallel_runs INTEGER NOT NULL,
        runner_pid INTEGER,
        runner_start_time INTEGER,
        runner_boot_id TEXT,
        cmd_pid INTEGER,
        cmd_pgid INTEGER,
        cmd_start_time INTEGER,
        exit_code INTEGER,
        term_signal INTEGER,
        cancel_requested INTEGER NOT NULL DEFAULT 0,
        cancel_force INTEGER NOT NULL DEFAULT 0,
        cancel_requested_at INTEGER,
        message TEXT,
        created_at INTEGER NOT NULL,
        authorized_at INTEGER,
        running_at INTEGER,
        finished_at INTEGER,
        UNIQUE (job_id, number)
    );
    CREATE INDEX attempts_job ON attempts(job_id);
    CREATE INDEX attempts_state ON attempts(state);

    CREATE TABLE run_leases (
        attempt_id INTEGER PRIMARY KEY REFERENCES attempts(id),
        job_id INTEGER NOT NULL REFERENCES jobs(id),
        max_parallel_runs INTEGER NOT NULL,
        acquired_at INTEGER NOT NULL,
        released_at INTEGER
    );
    CREATE INDEX run_leases_open ON run_leases(attempt_id) WHERE released_at IS NULL;

    CREATE TABLE scheduler_reservation (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER NOT NULL REFERENCES jobs(id),
        cutoff_seq INTEGER NOT NULL,
        semantics_version INTEGER NOT NULL,
        status TEXT NOT NULL,
        reason TEXT,
        initial_blockers TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        resolved_at INTEGER
    );
    CREATE UNIQUE INDEX reservation_single_active
        ON scheduler_reservation(status) WHERE status = 'active';

    CREATE TABLE scheduler_reservation_backfills (
        reservation_id INTEGER NOT NULL REFERENCES scheduler_reservation(id),
        job_id INTEGER NOT NULL REFERENCES jobs(id),
        reason TEXT NOT NULL,
        attempt_id INTEGER REFERENCES attempts(id),
        created_at INTEGER NOT NULL,
        PRIMARY KEY (reservation_id, job_id)
    ) WITHOUT ROWID;

    CREATE TABLE operations (
        key TEXT PRIMARY KEY,
        op_type TEXT NOT NULL,
        request_hash TEXT NOT NULL,
        response TEXT NOT NULL,
        created_at INTEGER NOT NULL
    );

    CREATE TABLE events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER NOT NULL,
        job_id INTEGER,
        attempt_id INTEGER,
        type TEXT NOT NULL,
        actor TEXT NOT NULL,
        details TEXT
    );
    CREATE INDEX events_job ON events(job_id);
    "#,
];

pub fn open(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)
        .with_context(|| format!("opening database {}", path.display()))?;
    configure(&conn)?;
    migrate(&conn)?;
    Ok(conn)
}

pub fn open_in_memory() -> Result<Connection> {
    let conn = Connection::open_in_memory()?;
    configure(&conn)?;
    migrate(&conn)?;
    Ok(conn)
}

fn configure(conn: &Connection) -> Result<()> {
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "FULL")?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    conn.busy_timeout(std::time::Duration::from_secs(10))?;
    Ok(())
}

pub fn migrate(conn: &Connection) -> Result<()> {
    let current: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    for (idx, migration) in MIGRATIONS.iter().enumerate() {
        let version = idx as i64 + 1;
        if version <= current {
            continue;
        }
        conn.execute_batch(&format!(
            "BEGIN IMMEDIATE;\n{migration}\nPRAGMA user_version = {version};\nCOMMIT;"
        ))
        .with_context(|| format!("applying migration {version}"))?;
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct JobRow {
    pub id: JobId,
    pub name: String,
    pub state: JobState,
    pub cwd: String,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub max_parallel_runs: u32,
    pub max_attempts: i64,
    pub retry_delay_ms: i64,
    pub retry_not_before: Option<i64>,
    pub attempt_count: i64,
    pub state_reason: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub finished_at: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct AttemptRow {
    pub id: AttemptId,
    pub job_id: JobId,
    pub number: i64,
    pub state: AttemptState,
    pub launch_token: String,
    pub max_parallel_runs: u32,
    pub runner_pid: Option<i32>,
    pub runner_start_time: Option<i64>,
    pub runner_boot_id: Option<String>,
    pub cmd_pid: Option<i32>,
    pub cmd_pgid: Option<i32>,
    pub cmd_start_time: Option<i64>,
    pub exit_code: Option<i32>,
    pub term_signal: Option<i32>,
    pub cancel_requested: bool,
    pub cancel_force: bool,
    pub cancel_requested_at: Option<i64>,
    pub message: Option<String>,
    pub created_at: i64,
    pub authorized_at: Option<i64>,
    pub running_at: Option<i64>,
    pub finished_at: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct DependencyRow {
    pub parent_id: JobId,
    pub child_id: JobId,
    pub requirement: DepRequirement,
}

#[derive(Debug, Clone)]
pub struct ReservationRow {
    pub id: i64,
    pub job_id: JobId,
    pub cutoff_seq: i64,
    pub semantics_version: i64,
    pub created_at: i64,
    pub initial_blockers: Vec<AttemptId>,
    pub consumed: BTreeSet<JobId>,
}

const JOB_COLUMNS: &str = "id, name, state, cwd, args, env, max_parallel_runs, max_attempts, \
     retry_delay_ms, retry_not_before, attempt_count, state_reason, created_at, updated_at, \
     finished_at";

fn job_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<JobRow> {
    let state: String = row.get(2)?;
    let args: String = row.get(4)?;
    let env: String = row.get(5)?;
    Ok(JobRow {
        id: row.get(0)?,
        name: row.get(1)?,
        state: state.parse().expect("valid job state in database"),
        cwd: row.get(3)?,
        args: serde_json::from_str(&args).expect("valid args JSON in database"),
        env: serde_json::from_str(&env).expect("valid env JSON in database"),
        max_parallel_runs: row.get(6)?,
        max_attempts: row.get(7)?,
        retry_delay_ms: row.get(8)?,
        retry_not_before: row.get(9)?,
        attempt_count: row.get(10)?,
        state_reason: row.get(11)?,
        created_at: row.get(12)?,
        updated_at: row.get(13)?,
        finished_at: row.get(14)?,
    })
}

const ATTEMPT_COLUMNS: &str = "id, job_id, number, state, launch_token, max_parallel_runs, \
     runner_pid, runner_start_time, runner_boot_id, cmd_pid, cmd_pgid, cmd_start_time, \
     exit_code, term_signal, cancel_requested, cancel_force, cancel_requested_at, message, \
     created_at, authorized_at, running_at, finished_at";

fn attempt_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<AttemptRow> {
    let state: String = row.get(3)?;
    Ok(AttemptRow {
        id: row.get(0)?,
        job_id: row.get(1)?,
        number: row.get(2)?,
        state: state.parse().expect("valid attempt state in database"),
        launch_token: row.get(4)?,
        max_parallel_runs: row.get(5)?,
        runner_pid: row.get(6)?,
        runner_start_time: row.get(7)?,
        runner_boot_id: row.get(8)?,
        cmd_pid: row.get(9)?,
        cmd_pgid: row.get(10)?,
        cmd_start_time: row.get(11)?,
        exit_code: row.get(12)?,
        term_signal: row.get(13)?,
        cancel_requested: row.get::<_, i64>(14)? != 0,
        cancel_force: row.get::<_, i64>(15)? != 0,
        cancel_requested_at: row.get(16)?,
        message: row.get(17)?,
        created_at: row.get(18)?,
        authorized_at: row.get(19)?,
        running_at: row.get(20)?,
        finished_at: row.get(21)?,
    })
}

// ---------------------------------------------------------------------------
// Jobs
// ---------------------------------------------------------------------------

/// Insert a job and its dependency edges. Edges only point from existing
/// parents to the brand-new child, so a cycle is structurally impossible at
/// submission time; parents are validated to exist by the caller.
pub fn insert_job(conn: &Connection, params: &SubmitParams, now: i64) -> Result<JobId> {
    conn.execute(
        "INSERT INTO jobs (name, state, cwd, args, env, max_parallel_runs, max_attempts, \
         retry_delay_ms, attempt_count, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 0, ?9, ?9)",
        params![
            params.name,
            JobState::Queued.as_str(),
            params.cwd,
            serde_json::to_string(&params.args)?,
            serde_json::to_string(&params.env)?,
            params.max_parallel_runs,
            params.max_attempts,
            params.retry_delay_ms as i64,
            now,
        ],
    )?;
    let job_id = conn.last_insert_rowid();
    // Declaring the same parent under both flags keeps the stricter
    // requirement: success implies completion, never the reverse.
    let mut edges: std::collections::BTreeMap<JobId, DepRequirement> = std::collections::BTreeMap::new();
    for parent in &params.after_completion {
        edges.insert(*parent, DepRequirement::Completion);
    }
    for parent in &params.after_success {
        edges.insert(*parent, DepRequirement::Success);
    }
    for (parent, requirement) in edges {
        conn.execute(
            "INSERT INTO dependencies (parent_id, child_id, requirement) VALUES (?1, ?2, ?3)",
            params![parent, job_id, requirement.as_str()],
        )?;
    }
    Ok(job_id)
}

pub fn job_row(conn: &Connection, id: JobId) -> Result<Option<JobRow>> {
    Ok(conn
        .query_row(
            &format!("SELECT {JOB_COLUMNS} FROM jobs WHERE id = ?1"),
            [id],
            job_from_row,
        )
        .optional()?)
}

pub fn job_exists(conn: &Connection, id: JobId) -> Result<bool> {
    Ok(conn
        .query_row("SELECT 1 FROM jobs WHERE id = ?1", [id], |_| Ok(()))
        .optional()?
        .is_some())
}

/// Jobs shown by `status`: all non-terminal plus the most recent terminal.
pub fn status_jobs(conn: &Connection, recent_terminal: u32) -> Result<Vec<JobRow>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {JOB_COLUMNS} FROM jobs WHERE id IN (
             SELECT id FROM jobs WHERE finished_at IS NULL
             UNION
             SELECT id FROM (
                 SELECT id FROM jobs WHERE finished_at IS NOT NULL
                 ORDER BY finished_at DESC LIMIT ?1
             )
         ) ORDER BY id"
    ))?;
    let rows = stmt.query_map([recent_terminal], job_from_row)?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn update_job_state(
    conn: &Connection,
    id: JobId,
    state: JobState,
    reason: Option<&str>,
    now: i64,
) -> Result<()> {
    // Entering a terminal state stamps finished_at; leaving one (retry)
    // clears it.
    let finished_at = state.is_terminal().then_some(now);
    conn.execute(
        "UPDATE jobs SET state = ?2, state_reason = ?3, updated_at = ?4, finished_at = ?5 \
         WHERE id = ?1",
        params![id, state.as_str(), reason, now, finished_at],
    )?;
    Ok(())
}

pub fn set_job_retry_not_before(conn: &Connection, id: JobId, at: Option<i64>, now: i64) -> Result<()> {
    conn.execute(
        "UPDATE jobs SET retry_not_before = ?2, updated_at = ?3 WHERE id = ?1",
        params![id, at, now],
    )?;
    Ok(())
}

pub fn set_job_max_parallel_runs(conn: &Connection, id: JobId, value: u32, now: i64) -> Result<()> {
    conn.execute(
        "UPDATE jobs SET max_parallel_runs = ?2, updated_at = ?3 WHERE id = ?1",
        params![id, value, now],
    )?;
    Ok(())
}

pub fn max_job_seq(conn: &Connection) -> Result<i64> {
    // sqlite_sequence records the highest id ever allocated, so a future
    // retention cleanup deleting the newest rows can never lower the frozen
    // backfill cutoff. The row is absent until the first job is inserted.
    Ok(conn
        .query_row("SELECT seq FROM sqlite_sequence WHERE name = 'jobs'", [], |row| row.get(0))
        .optional()?
        .unwrap_or(0))
}

pub fn count_jobs_in_state(conn: &Connection, state: JobState) -> Result<u32> {
    Ok(conn.query_row(
        "SELECT COUNT(*) FROM jobs WHERE state = ?1",
        [state.as_str()],
        |row| row.get(0),
    )?)
}


// ---------------------------------------------------------------------------
// Dependencies
// ---------------------------------------------------------------------------

pub fn dependencies_of(conn: &Connection, child: JobId) -> Result<Vec<DependencyRow>> {
    let mut stmt = conn.prepare(
        "SELECT parent_id, child_id, requirement FROM dependencies WHERE child_id = ?1 \
         ORDER BY parent_id",
    )?;
    let rows = stmt.query_map([child], |row| {
        let requirement: String = row.get(2)?;
        Ok(DependencyRow {
            parent_id: row.get(0)?,
            child_id: row.get(1)?,
            requirement: requirement.parse().expect("valid requirement in database"),
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

/// Queued/held jobs with a dependency that can never be satisfied (a parent
/// terminal in a non-qualifying state). One iteration of skip propagation;
/// the caller loops until it returns nothing.
pub fn jobs_with_violated_dependencies(conn: &Connection) -> Result<Vec<(JobId, JobId)>> {
    let mut stmt = conn.prepare(
        "SELECT DISTINCT j.id, p.id FROM jobs j
         JOIN dependencies d ON d.child_id = j.id
         JOIN jobs p ON p.id = d.parent_id
         WHERE j.state IN ('queued', 'held')
           AND d.requirement = 'success'
           AND p.state IN ('failed', 'cancelled', 'lost', 'skipped')",
    )?;
    let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

/// FIFO-ordered jobs eligible for admission right now.
pub fn eligible_candidates(conn: &Connection, now: i64) -> Result<Vec<Candidate>> {
    let mut stmt = conn.prepare(
        "SELECT j.id, j.max_parallel_runs FROM jobs j
         WHERE j.state = 'queued'
           AND (j.retry_not_before IS NULL OR j.retry_not_before <= ?1)
           AND NOT EXISTS (
               SELECT 1 FROM dependencies d
               JOIN jobs p ON p.id = d.parent_id
               WHERE d.child_id = j.id
                 AND NOT (
                     (d.requirement = 'success' AND p.state = 'succeeded')
                     OR (d.requirement = 'completion'
                         AND p.state IN ('succeeded','failed','cancelled','lost','skipped'))
                 )
           )
         ORDER BY j.id",
    )?;
    let rows = stmt.query_map([now], |row| {
        let id: JobId = row.get(0)?;
        Ok(Candidate { job_id: id, seq: id, limit: row.get(1)? })
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

// ---------------------------------------------------------------------------
// Attempts and leases
// ---------------------------------------------------------------------------

/// Create a `prepared` attempt and its run lease atomically and move the job
/// to `starting`. The caller wraps this in the pass-commit transaction.
pub fn create_attempt_with_lease(
    conn: &Connection,
    job: &JobRow,
    launch_token: &str,
    now: i64,
) -> Result<AttemptRow> {
    let number = job.attempt_count + 1;
    conn.execute(
        "INSERT INTO attempts (job_id, number, state, launch_token, max_parallel_runs, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            job.id,
            number,
            AttemptState::Prepared.as_str(),
            launch_token,
            job.max_parallel_runs,
            now
        ],
    )?;
    let attempt_id = conn.last_insert_rowid();
    conn.execute(
        "INSERT INTO run_leases (attempt_id, job_id, max_parallel_runs, acquired_at) \
         VALUES (?1, ?2, ?3, ?4)",
        params![attempt_id, job.id, job.max_parallel_runs, now],
    )?;
    conn.execute(
        "UPDATE jobs SET state = ?2, attempt_count = ?3, updated_at = ?4, state_reason = NULL \
         WHERE id = ?1",
        params![job.id, JobState::Starting.as_str(), number, now],
    )?;
    attempt_row(conn, attempt_id)?.context("attempt row just inserted")
}

pub fn attempt_row(conn: &Connection, id: AttemptId) -> Result<Option<AttemptRow>> {
    Ok(conn
        .query_row(
            &format!("SELECT {ATTEMPT_COLUMNS} FROM attempts WHERE id = ?1"),
            [id],
            attempt_from_row,
        )
        .optional()?)
}

pub fn attempt_by_number(conn: &Connection, job: JobId, number: i64) -> Result<Option<AttemptRow>> {
    Ok(conn
        .query_row(
            &format!("SELECT {ATTEMPT_COLUMNS} FROM attempts WHERE job_id = ?1 AND number = ?2"),
            params![job, number],
            attempt_from_row,
        )
        .optional()?)
}

pub fn attempts_for_job(conn: &Connection, job: JobId) -> Result<Vec<AttemptRow>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {ATTEMPT_COLUMNS} FROM attempts WHERE job_id = ?1 ORDER BY number"
    ))?;
    let rows = stmt.query_map([job], attempt_from_row)?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

/// The single non-terminal attempt of a job, if any. The serialized
/// coordinator plus the unique lease per attempt guarantee at most one.
pub fn live_attempt_for_job(conn: &Connection, job: JobId) -> Result<Option<AttemptRow>> {
    Ok(conn
        .query_row(
            &format!(
                "SELECT {ATTEMPT_COLUMNS} FROM attempts WHERE job_id = ?1 \
                 AND state IN ('prepared','authorized','running','orphaned','quarantined') \
                 ORDER BY number DESC LIMIT 1"
            ),
            [job],
            attempt_from_row,
        )
        .optional()?)
}

pub fn non_terminal_attempts(conn: &Connection) -> Result<Vec<AttemptRow>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {ATTEMPT_COLUMNS} FROM attempts \
         WHERE state IN ('prepared','authorized','running','orphaned','quarantined') ORDER BY id"
    ))?;
    let rows = stmt.query_map([], attempt_from_row)?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn active_leases(conn: &Connection) -> Result<Vec<(AttemptId, ActiveLease)>> {
    let mut stmt = conn.prepare(
        "SELECT attempt_id, job_id, max_parallel_runs FROM run_leases \
         WHERE released_at IS NULL ORDER BY attempt_id",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, AttemptId>(0)?, ActiveLease { job_id: row.get(1)?, limit: row.get(2)? }))
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn release_lease(conn: &Connection, attempt: AttemptId, now: i64) -> Result<()> {
    conn.execute(
        "UPDATE run_leases SET released_at = ?2 WHERE attempt_id = ?1 AND released_at IS NULL",
        params![attempt, now],
    )?;
    Ok(())
}

pub fn set_attempt_runner_identity(
    conn: &Connection,
    attempt: AttemptId,
    pid: i32,
    start_time: i64,
    boot_id: &str,
) -> Result<()> {
    conn.execute(
        "UPDATE attempts SET runner_pid = ?2, runner_start_time = ?3, runner_boot_id = ?4 \
         WHERE id = ?1",
        params![attempt, pid, start_time, boot_id],
    )?;
    Ok(())
}

pub fn set_attempt_authorized(conn: &Connection, attempt: AttemptId, now: i64) -> Result<()> {
    conn.execute(
        "UPDATE attempts SET state = ?2, authorized_at = ?3 WHERE id = ?1",
        params![attempt, AttemptState::Authorized.as_str(), now],
    )?;
    Ok(())
}

pub fn set_attempt_running(
    conn: &Connection,
    attempt: AttemptId,
    cmd_pid: i32,
    cmd_pgid: i32,
    cmd_start_time: i64,
    now: i64,
) -> Result<()> {
    conn.execute(
        "UPDATE attempts SET state = ?2, cmd_pid = ?3, cmd_pgid = ?4, cmd_start_time = ?5, \
         running_at = ?6 WHERE id = ?1",
        params![attempt, AttemptState::Running.as_str(), cmd_pid, cmd_pgid, cmd_start_time, now],
    )?;
    Ok(())
}

pub fn set_attempt_state(
    conn: &Connection,
    attempt: AttemptId,
    state: AttemptState,
    message: Option<&str>,
) -> Result<()> {
    conn.execute(
        "UPDATE attempts SET state = ?2, message = COALESCE(?3, message) WHERE id = ?1",
        params![attempt, state.as_str(), message],
    )?;
    Ok(())
}

pub fn finalize_attempt(
    conn: &Connection,
    attempt: AttemptId,
    state: AttemptState,
    exit_code: Option<i32>,
    term_signal: Option<i32>,
    message: Option<&str>,
    now: i64,
) -> Result<()> {
    debug_assert!(state.is_terminal());
    conn.execute(
        "UPDATE attempts SET state = ?2, exit_code = ?3, term_signal = ?4, \
         message = COALESCE(?5, message), finished_at = ?6 WHERE id = ?1",
        params![attempt, state.as_str(), exit_code, term_signal, message, now],
    )?;
    Ok(())
}

pub fn set_attempt_cancel_requested(
    conn: &Connection,
    attempt: AttemptId,
    force: bool,
    now: i64,
) -> Result<()> {
    conn.execute(
        "UPDATE attempts SET cancel_requested = 1, cancel_force = MAX(cancel_force, ?2), \
         cancel_requested_at = COALESCE(cancel_requested_at, ?3) WHERE id = ?1",
        params![attempt, force as i64, now],
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Reservation
// ---------------------------------------------------------------------------

/// Sentinel `cutoff_seq` for a reservation whose backfill window is still
/// open. Job sequences start at 1, so 0 is unambiguous, and it keeps the
/// column NOT NULL: the freeze can then be made idempotent (pin the real
/// cutoff exactly once, when the window closes) without a schema change.
pub const RESERVATION_CUTOFF_UNFROZEN: i64 = 0;

pub fn active_reservation(conn: &Connection) -> Result<Option<ReservationRow>> {
    let row = conn
        .query_row(
            "SELECT id, job_id, cutoff_seq, semantics_version, created_at, initial_blockers \
             FROM scheduler_reservation WHERE status = 'active'",
            [],
            |row| {
                let blockers: String = row.get(5)?;
                Ok(ReservationRow {
                    id: row.get(0)?,
                    job_id: row.get(1)?,
                    cutoff_seq: row.get(2)?,
                    semantics_version: row.get(3)?,
                    created_at: row.get(4)?,
                    initial_blockers: serde_json::from_str(&blockers)
                        .expect("valid blockers JSON in database"),
                    consumed: BTreeSet::new(),
                })
            },
        )
        .optional()?;
    let Some(mut reservation) = row else { return Ok(None) };
    let mut stmt = conn.prepare(
        "SELECT job_id FROM scheduler_reservation_backfills WHERE reservation_id = ?1",
    )?;
    let consumed = stmt.query_map([reservation.id], |row| row.get::<_, JobId>(0))?;
    reservation.consumed = consumed.collect::<rusqlite::Result<BTreeSet<_>>>()?;
    Ok(Some(reservation))
}

/// Create an active reservation with an open backfill window
/// ([`RESERVATION_CUTOFF_UNFROZEN`]); the cutoff is pinned later by
/// [`freeze_reservation_cutoff`] when the initial blockers stop advancing.
pub fn create_reservation(
    conn: &Connection,
    job: JobId,
    semantics_version: i64,
    initial_blockers: &[AttemptId],
    now: i64,
) -> Result<i64> {
    conn.execute(
        "INSERT INTO scheduler_reservation \
         (job_id, cutoff_seq, semantics_version, status, initial_blockers, created_at) \
         VALUES (?1, ?2, ?3, 'active', ?4, ?5)",
        params![
            job,
            RESERVATION_CUTOFF_UNFROZEN,
            semantics_version,
            serde_json::to_string(initial_blockers)?,
            now
        ],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Pin the frozen backfill cutoff. Guarded on the sentinel so a repeated
/// call (crash between commit and observation, or a racing recomputation)
/// can never advance an already-frozen frontier.
pub fn freeze_reservation_cutoff(conn: &Connection, id: i64, cutoff_seq: i64) -> Result<()> {
    debug_assert!(cutoff_seq != RESERVATION_CUTOFF_UNFROZEN);
    conn.execute(
        "UPDATE scheduler_reservation SET cutoff_seq = ?2 \
         WHERE id = ?1 AND cutoff_seq = ?3",
        params![id, cutoff_seq, RESERVATION_CUTOFF_UNFROZEN],
    )?;
    Ok(())
}

/// The backfill frontier of an active reservation, as scheduling interprets
/// it right now. Single source of truth for the coordinator and the views.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackfillFrontier {
    /// An initial blocker is still advancing: any eligible job whose bypass
    /// is unconsumed may pass the protected job once.
    OpenWindow,
    /// The window closed but the sentinel is still stored; the next pass
    /// plans against this cutoff and pins it.
    FreezePending(i64),
    /// Pinned: jobs submitted after the cutoff may not pass.
    Frozen(i64),
    /// Unknown semantics version; ordinary admission must stay blocked.
    Unsupported(i64),
}

pub fn backfill_frontier(conn: &Connection, res: &ReservationRow) -> Result<BackfillFrontier> {
    match res.semantics_version {
        // v1 froze the cutoff at creation; a permanently frozen frontier at
        // the stored cutoff remains its exact meaning.
        1 => Ok(BackfillFrontier::Frozen(res.cutoff_seq)),
        SCHEDULER_SEMANTICS_VERSION => {
            if res.cutoff_seq != RESERVATION_CUTOFF_UNFROZEN {
                Ok(BackfillFrontier::Frozen(res.cutoff_seq))
            } else if any_blockers_advancing(conn, &res.initial_blockers)? {
                Ok(BackfillFrontier::OpenWindow)
            } else {
                Ok(BackfillFrontier::FreezePending(max_job_seq(conn)?))
            }
        }
        version => Ok(BackfillFrontier::Unsupported(version)),
    }
}

/// True while any of the given attempts still holds an unreleased run lease
/// in a live-advancing state. Orphaned and quarantined attempts retain their
/// leases until resolved, but no longer advance: counting them would hold a
/// protected job's backfill window open indefinitely, admitting an unbounded
/// stream of later arrivals past it.
pub fn any_blockers_advancing(conn: &Connection, attempts: &[AttemptId]) -> Result<bool> {
    if attempts.is_empty() {
        return Ok(false);
    }
    let placeholders = vec!["?"; attempts.len()].join(",");
    let mut stmt = conn.prepare(&format!(
        "SELECT EXISTS (
             SELECT 1 FROM run_leases l
             JOIN attempts a ON a.id = l.attempt_id
             WHERE l.released_at IS NULL
               AND a.state IN ('prepared','authorized','running')
               AND l.attempt_id IN ({placeholders})
         )"
    ))?;
    Ok(stmt.query_row(rusqlite::params_from_iter(attempts.iter()), |row| row.get(0))?)
}

pub fn resolve_reservation(
    conn: &Connection,
    id: i64,
    status: &str,
    reason: &str,
    now: i64,
) -> Result<()> {
    debug_assert!(status == "satisfied" || status == "invalidated");
    conn.execute(
        "UPDATE scheduler_reservation SET status = ?2, reason = ?3, resolved_at = ?4 \
         WHERE id = ?1",
        params![id, status, reason, now],
    )?;
    Ok(())
}

/// Record a consumed bypass. The unique reservation/job key enforces one
/// bypass per job across retries and restarts.
pub fn record_backfill(
    conn: &Connection,
    reservation: i64,
    job: JobId,
    reason: &str,
    attempt: Option<AttemptId>,
    now: i64,
) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO scheduler_reservation_backfills \
         (reservation_id, job_id, reason, attempt_id, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![reservation, job, reason, attempt, now],
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Operations (idempotency)
// ---------------------------------------------------------------------------

pub fn lookup_operation(conn: &Connection, key: &str) -> Result<Option<(String, String)>> {
    Ok(conn
        .query_row(
            "SELECT request_hash, response FROM operations WHERE key = ?1",
            [key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?)
}

pub fn insert_operation(
    conn: &Connection,
    key: &str,
    op_type: &str,
    request_hash: &str,
    response: &str,
    now: i64,
) -> Result<()> {
    conn.execute(
        "INSERT INTO operations (key, op_type, request_hash, response, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![key, op_type, request_hash, response, now],
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

pub fn append_event(
    conn: &Connection,
    job: Option<JobId>,
    attempt: Option<AttemptId>,
    event_type: &str,
    actor: &str,
    details: Option<&str>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO events (ts, job_id, attempt_id, type, actor, details) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![now_ms(), job, attempt, event_type, actor, details],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn submit_params(name: &str, limit: u32) -> SubmitParams {
        SubmitParams {
            name: name.into(),
            cwd: "/tmp".into(),
            args: vec!["true".into()],
            env: BTreeMap::new(),
            max_parallel_runs: limit,
            max_attempts: 1,
            retry_delay_ms: 0,
            after_success: vec![],
            after_completion: vec![],
        }
    }

    #[test]
    fn empty_database_migrates_to_current() {
        let conn = open_in_memory().unwrap();
        let version: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0)).unwrap();
        assert_eq!(version, MIGRATIONS.len() as i64);
        // Idempotent.
        migrate(&conn).unwrap();
    }

    #[test]
    fn file_database_migrates_and_persists() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mlqueue.db");
        let id;
        {
            let conn = open(&path).unwrap();
            id = insert_job(&conn, &submit_params("a", 1), 1000).unwrap();
        }
        let conn = open(&path).unwrap();
        let job = job_row(&conn, id).unwrap().unwrap();
        assert_eq!(job.name, "a");
        assert_eq!(job.state, JobState::Queued);
    }

    #[test]
    fn attempt_and_lease_lifecycle() {
        let conn = open_in_memory().unwrap();
        let id = insert_job(&conn, &submit_params("a", 3), 1000).unwrap();
        let job = job_row(&conn, id).unwrap().unwrap();
        let attempt = create_attempt_with_lease(&conn, &job, "token", 1001).unwrap();
        assert_eq!(attempt.number, 1);
        assert_eq!(attempt.max_parallel_runs, 3);
        assert_eq!(active_leases(&conn).unwrap().len(), 1);
        assert_eq!(job_row(&conn, id).unwrap().unwrap().state, JobState::Starting);

        release_lease(&conn, attempt.id, 1002).unwrap();
        assert!(active_leases(&conn).unwrap().is_empty());
    }

    #[test]
    fn eligible_candidates_respect_dependencies_and_retry_delay() {
        let conn = open_in_memory().unwrap();
        let parent = insert_job(&conn, &submit_params("parent", 1), 1000).unwrap();
        let mut child_params = submit_params("child", 1);
        child_params.after_success = vec![parent];
        let child = insert_job(&conn, &child_params, 1001).unwrap();

        let ids: Vec<JobId> =
            eligible_candidates(&conn, 2000).unwrap().iter().map(|c| c.job_id).collect();
        assert_eq!(ids, vec![parent], "child must wait for parent");

        update_job_state(&conn, parent, JobState::Succeeded, None, 3000).unwrap();
        let ids: Vec<JobId> =
            eligible_candidates(&conn, 3000).unwrap().iter().map(|c| c.job_id).collect();
        assert_eq!(ids, vec![child]);

        set_job_retry_not_before(&conn, child, Some(9000), 3000).unwrap();
        assert!(eligible_candidates(&conn, 3000).unwrap().is_empty());
        assert_eq!(eligible_candidates(&conn, 9000).unwrap().len(), 1);
    }

    #[test]
    fn violated_dependencies_are_reported_for_skipping() {
        let conn = open_in_memory().unwrap();
        let parent = insert_job(&conn, &submit_params("parent", 1), 1000).unwrap();
        let mut child_params = submit_params("child", 1);
        child_params.after_success = vec![parent];
        let child = insert_job(&conn, &child_params, 1001).unwrap();
        let mut completion_params = submit_params("completion-child", 1);
        completion_params.after_completion = vec![parent];
        let completion_child = insert_job(&conn, &completion_params, 1002).unwrap();

        update_job_state(&conn, parent, JobState::Failed, None, 2000).unwrap();
        let violated = jobs_with_violated_dependencies(&conn).unwrap();
        assert_eq!(violated, vec![(child, parent)]);

        // after-completion child becomes eligible instead.
        let ids: Vec<JobId> =
            eligible_candidates(&conn, 3000).unwrap().iter().map(|c| c.job_id).collect();
        assert!(ids.contains(&completion_child));
    }

    #[test]
    fn single_active_reservation_enforced_and_restored() {
        let conn = open_in_memory().unwrap();
        let a = insert_job(&conn, &submit_params("a", 1), 1000).unwrap();
        let b = insert_job(&conn, &submit_params("b", 1), 1001).unwrap();
        let res = create_reservation(&conn, a, SCHEDULER_SEMANTICS_VERSION, &[7], 1002).unwrap();
        assert!(create_reservation(&conn, b, SCHEDULER_SEMANTICS_VERSION, &[], 1003).is_err());

        record_backfill(&conn, res, b, "admitted", None, 1004).unwrap();
        // Second consumption attempt is ignored, not duplicated.
        record_backfill(&conn, res, b, "admitted", None, 1005).unwrap();

        let restored = active_reservation(&conn).unwrap().unwrap();
        assert_eq!(restored.job_id, a);
        assert_eq!(restored.cutoff_seq, RESERVATION_CUTOFF_UNFROZEN);
        assert_eq!(restored.initial_blockers, vec![7]);
        assert_eq!(restored.consumed, BTreeSet::from([b]));

        resolve_reservation(&conn, res, "satisfied", "protected job started", 2000).unwrap();
        assert!(active_reservation(&conn).unwrap().is_none());
    }

    #[test]
    fn freezing_the_cutoff_is_idempotent() {
        let conn = open_in_memory().unwrap();
        let a = insert_job(&conn, &submit_params("a", 1), 1000).unwrap();
        let res = create_reservation(&conn, a, SCHEDULER_SEMANTICS_VERSION, &[7], 1001).unwrap();

        freeze_reservation_cutoff(&conn, res, 9).unwrap();
        assert_eq!(active_reservation(&conn).unwrap().unwrap().cutoff_seq, 9);
        // A later attempt (crash replay, racing recomputation) must never
        // advance an already-frozen frontier.
        freeze_reservation_cutoff(&conn, res, 42).unwrap();
        assert_eq!(active_reservation(&conn).unwrap().unwrap().cutoff_seq, 9);
    }

    #[test]
    fn backfill_frontier_interprets_versions_and_window_state() {
        let conn = open_in_memory().unwrap();
        let blocker_job = insert_job(&conn, &submit_params("blocker", 2), 1000).unwrap();
        let row = job_row(&conn, blocker_job).unwrap().unwrap();
        let attempt = create_attempt_with_lease(&conn, &row, "token", 1001).unwrap();
        let protected = insert_job(&conn, &submit_params("protected", 1), 1002).unwrap();

        // Native version with a live blocker: the window is open.
        let rid =
            create_reservation(&conn, protected, SCHEDULER_SEMANTICS_VERSION, &[attempt.id], 1003)
                .unwrap();
        let res = active_reservation(&conn).unwrap().unwrap();
        assert_eq!(backfill_frontier(&conn, &res).unwrap(), BackfillFrontier::OpenWindow);

        // Blocker drained: a freeze at the current maximum sequence is
        // pending, then pinning makes it durable.
        release_lease(&conn, attempt.id, 1004).unwrap();
        assert_eq!(
            backfill_frontier(&conn, &res).unwrap(),
            BackfillFrontier::FreezePending(protected)
        );
        freeze_reservation_cutoff(&conn, rid, protected).unwrap();
        let res = active_reservation(&conn).unwrap().unwrap();
        assert_eq!(backfill_frontier(&conn, &res).unwrap(), BackfillFrontier::Frozen(protected));
        resolve_reservation(&conn, rid, "invalidated", "test", 1005).unwrap();

        // A v1 reservation stays frozen at its stored cutoff even while a
        // blocker still runs: it never gains an open window retroactively.
        let row = job_row(&conn, blocker_job).unwrap().unwrap();
        let live = create_attempt_with_lease(&conn, &row, "token-2", 1006).unwrap();
        let rid = create_reservation(&conn, protected, 1, &[live.id], 1007).unwrap();
        freeze_reservation_cutoff(&conn, rid, 1).unwrap();
        let res = active_reservation(&conn).unwrap().unwrap();
        assert!(any_blockers_advancing(&conn, &res.initial_blockers).unwrap());
        assert_eq!(backfill_frontier(&conn, &res).unwrap(), BackfillFrontier::Frozen(1));
        resolve_reservation(&conn, rid, "invalidated", "test", 1008).unwrap();

        // Unknown versions must surface as unsupported, never as a guess.
        create_reservation(&conn, protected, 99, &[], 1009).unwrap();
        let res = active_reservation(&conn).unwrap().unwrap();
        assert_eq!(backfill_frontier(&conn, &res).unwrap(), BackfillFrontier::Unsupported(99));
    }

    #[test]
    fn blockers_advance_only_while_leased_in_a_live_state() {
        let conn = open_in_memory().unwrap();
        let job = insert_job(&conn, &submit_params("blocker", 2), 1000).unwrap();
        let row = job_row(&conn, job).unwrap().unwrap();
        let attempt = create_attempt_with_lease(&conn, &row, "token", 1001).unwrap();

        assert!(!any_blockers_advancing(&conn, &[]).unwrap());
        assert!(any_blockers_advancing(&conn, &[attempt.id]).unwrap());

        // Orphaned/quarantined attempts keep their lease but stop advancing:
        // the backfill window must not stay open on their account.
        set_attempt_state(&conn, attempt.id, AttemptState::Orphaned, None).unwrap();
        assert!(!any_blockers_advancing(&conn, &[attempt.id]).unwrap());

        set_attempt_state(&conn, attempt.id, AttemptState::Running, None).unwrap();
        assert!(any_blockers_advancing(&conn, &[attempt.id]).unwrap());
        release_lease(&conn, attempt.id, 2000).unwrap();
        assert!(!any_blockers_advancing(&conn, &[attempt.id]).unwrap());
    }

    #[test]
    fn duplicate_dependency_declaration_keeps_the_stricter_requirement() {
        let conn = open_in_memory().unwrap();
        let parent = insert_job(&conn, &submit_params("parent", 1), 1000).unwrap();
        let mut child_params = submit_params("child", 1);
        child_params.after_success = vec![parent];
        child_params.after_completion = vec![parent];
        let child = insert_job(&conn, &child_params, 1001).unwrap();

        let deps = dependencies_of(&conn, child).unwrap();
        assert_eq!(deps.len(), 1);
        assert_eq!(deps[0].requirement, DepRequirement::Success);
    }

    #[test]
    fn job_sequence_never_decreases_after_deleting_the_newest_job() {
        let conn = open_in_memory().unwrap();
        insert_job(&conn, &submit_params("a", 1), 1000).unwrap();
        let b = insert_job(&conn, &submit_params("b", 1), 1001).unwrap();
        assert_eq!(max_job_seq(&conn).unwrap(), b);
        // Retention-style deletion of the highest row must not lower the
        // frozen backfill cutoff source.
        conn.execute("DELETE FROM jobs WHERE id = ?1", [b]).unwrap();
        assert_eq!(max_job_seq(&conn).unwrap(), b);
        let c = insert_job(&conn, &submit_params("c", 1), 1002).unwrap();
        assert!(c > b, "ids are never reused");
    }

    #[test]
    fn operations_are_idempotent_and_conflict_on_reuse() {
        let conn = open_in_memory().unwrap();
        insert_operation(&conn, "k1", "submit", "hash-a", "{\"jobId\":1}", 1000).unwrap();
        let (hash, response) = lookup_operation(&conn, "k1").unwrap().unwrap();
        assert_eq!(hash, "hash-a");
        assert_eq!(response, "{\"jobId\":1}");
        // Reusing the key is a constraint violation; the coordinator turns a
        // hash mismatch into a stable conflict error before inserting.
        assert!(insert_operation(&conn, "k1", "submit", "hash-b", "{}", 1001).is_err());
        assert!(lookup_operation(&conn, "missing").unwrap().is_none());
    }

}
