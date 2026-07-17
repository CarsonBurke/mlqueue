//! Builders for the stable public JSON views. Status must explain the
//! declared limits, the effective minimum, protection, and why each queued
//! job is not running.

use anyhow::Result;
use rusqlite::Connection;

use crate::db::{self, AttemptRow, EventRow, JobRow, ReservationRow};
use crate::domain::{JobState, now_ms};
use crate::paths::Paths;
use crate::protocol::{
    AttemptView, DependencyView, EventView, FollowTtsJobView, FollowTtsSnapshotView, JobView,
    ReservationView, StatusView,
};

const TTS_NAME_LIMIT: usize = 80;

fn tts_name(name: &str) -> String {
    if name.chars().count() <= TTS_NAME_LIMIT {
        name.to_string()
    } else {
        format!("{}…", name.chars().take(TTS_NAME_LIMIT - 1).collect::<String>())
    }
}

pub fn event_view(event: &EventRow) -> EventView {
    EventView {
        id: event.id,
        timestamp: event.timestamp,
        job: event.job_id,
        job_name: event.job_name.as_deref().map(tts_name),
        attempt: event.attempt_id,
        attempt_number: event.attempt_number,
        event_type: event.event_type.clone(),
    }
}

pub fn attempt_view(paths: &Paths, attempt: &AttemptRow) -> AttemptView {
    AttemptView {
        id: attempt.id,
        job_id: attempt.job_id,
        number: attempt.number,
        state: attempt.state.as_str().to_string(),
        max_parallel_runs: attempt.max_parallel_runs,
        exit_code: attempt.exit_code,
        term_signal: attempt.term_signal,
        message: attempt.message.clone(),
        created_at: attempt.created_at,
        finished_at: attempt.finished_at,
        log_dir: paths.attempt_dir(attempt.id).display().to_string(),
    }
}

fn dependency_views(conn: &Connection, job: &JobRow) -> Result<Vec<DependencyView>> {
    let mut views = Vec::new();
    for dep in db::dependencies_of(conn, job.id)? {
        let parent_state = db::job_row(conn, dep.parent_id)?
            .map(|parent| parent.state)
            .unwrap_or(JobState::Lost);
        views.push(DependencyView {
            parent: dep.parent_id,
            requirement: dep.requirement.as_str().to_string(),
            satisfied: dep.requirement.satisfied_by(parent_state),
        });
    }
    Ok(views)
}

/// Derived (never persisted) explanation for why a non-terminal job is not
/// running right now.
fn eligibility_reason(
    conn: &Connection,
    job: &JobRow,
    reservation: Option<&ReservationRow>,
    now: i64,
) -> Result<Option<String>> {
    let reason = match job.state {
        JobState::Held => Some("held".to_string()),
        JobState::Starting => Some("launching".to_string()),
        JobState::NeedsAttention => {
            Some("attempt needs recovery; see mlq recover list".to_string())
        }
        JobState::Queued => {
            let deps = dependency_views(conn, job)?;
            if deps.iter().any(|d| !d.satisfied) {
                Some("waiting_for_dependency".to_string())
            } else if job.retry_not_before.is_some_and(|at| at > now) {
                Some("waiting_for_retry_delay".to_string())
            } else if let Some(res) = reservation {
                if res.job_id == job.id {
                    Some("protected_drain: waiting for active jobs to drain".to_string())
                } else if res.consumed.contains(&job.id) {
                    Some(format!(
                        "backfill_bypass_consumed: already bypassed protected job {} once",
                        res.job_id
                    ))
                } else {
                    match db::backfill_frontier(conn, res)? {
                        db::BackfillFrontier::OpenWindow => Some(format!(
                            "backfill_window_open: may bypass protected job {} once while \
                             its original blockers run",
                            res.job_id
                        )),
                        db::BackfillFrontier::Frozen(cutoff)
                        | db::BackfillFrontier::FreezePending(cutoff) => {
                            if job.id > cutoff {
                                Some(format!(
                                    "behind_backfill_cutoff: submitted after job {}'s backfill \
                                     frontier froze",
                                    res.job_id
                                ))
                            } else {
                                Some(format!(
                                    "backfill_eligible: may bypass protected job {} once when \
                                     a slot opens",
                                    res.job_id
                                ))
                            }
                        }
                        db::BackfillFrontier::Unsupported(_) => Some(
                            "admission_blocked: reservation needs operator attention".to_string(),
                        ),
                    }
                }
            } else {
                Some("waiting_for_slot".to_string())
            }
        }
        _ => None,
    };
    Ok(reason)
}

pub fn job_view(
    conn: &Connection,
    paths: &Paths,
    job: &JobRow,
    include_attempts: bool,
) -> Result<JobView> {
    let reservation = db::active_reservation(conn)?;
    let attempts = if include_attempts {
        db::attempts_for_job(conn, job.id)?
            .iter()
            .map(|attempt| attempt_view(paths, attempt))
            .collect()
    } else {
        Vec::new()
    };
    let cancel_requested = db::live_attempt_for_job(conn, job.id)?
        .map(|attempt| attempt.cancel_requested)
        .filter(|&requested| requested);
    Ok(JobView {
        id: job.id,
        name: job.name.clone(),
        state: job.state.as_str().to_string(),
        eligibility: eligibility_reason(conn, job, reservation.as_ref(), now_ms())?,
        state_reason: job.state_reason.clone(),
        max_parallel_runs: job.max_parallel_runs,
        cwd: job.cwd.clone(),
        args: job.args.clone(),
        max_attempts: job.max_attempts as u32,
        retry_delay_ms: job.retry_delay_ms as u64,
        retry_not_before: job.retry_not_before,
        attempt_count: job.attempt_count,
        created_at: job.created_at,
        updated_at: job.updated_at,
        finished_at: job.finished_at,
        dependencies: dependency_views(conn, job)?,
        attempts,
        cancel_requested,
    })
}

pub fn status_view(conn: &Connection, paths: &Paths, admission_blocked: bool) -> Result<StatusView> {
    let leases = db::active_leases(conn)?;
    let reservation = db::active_reservation(conn)?;
    let jobs = db::status_jobs(conn, 25)?
        .iter()
        .map(|job| job_view(conn, paths, job, false))
        .collect::<Result<Vec<_>>>()?;
    Ok(StatusView {
        jobs,
        active_leases: leases.len() as u32,
        effective_limit: leases.iter().map(|(_, lease)| lease.limit).min(),
        reservation: match reservation {
            Some(res) => {
                let (window_open, cutoff) = match db::backfill_frontier(conn, &res)? {
                    db::BackfillFrontier::OpenWindow => (true, None),
                    db::BackfillFrontier::Frozen(cutoff)
                    | db::BackfillFrontier::FreezePending(cutoff) => (false, Some(cutoff)),
                    db::BackfillFrontier::Unsupported(_) => (false, None),
                };
                Some(ReservationView {
                    protected_job: res.job_id,
                    backfill_window_open: window_open,
                    backfill_cutoff: cutoff,
                    created_at: res.created_at,
                    blocking_attempts: leases.iter().map(|(attempt, _)| *attempt).collect(),
                    consumed_bypasses: res.consumed.iter().copied().collect(),
                })
            }
            None => None,
        },
        admission_blocked,
    })
}

pub fn follow_tts_snapshot(conn: &Connection) -> Result<FollowTtsSnapshotView> {
    const MAX_INITIAL_RUNNING: usize = 5;

    let running_count = db::count_jobs_in_state(conn, JobState::Running)?;
    let additional_running_jobs = running_count.saturating_sub(MAX_INITIAL_RUNNING as u32);
    let running_jobs = db::bounded_job_names_in_state(
        conn,
        JobState::Running,
        TTS_NAME_LIMIT as u32,
        MAX_INITIAL_RUNNING as u32,
    )?
        .into_iter()
        .map(|(id, name)| FollowTtsJobView { id, name: tts_name(&name) })
        .collect();
    Ok(FollowTtsSnapshotView {
        running_jobs,
        additional_running_jobs,
        latest_event_id: db::latest_event_id(conn)?,
    })
}
