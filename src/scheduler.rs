//! Pure scheduling policy: the symmetric `maxParallelRuns` compatibility
//! formula plus frozen-frontier protected-job backfill.
//!
//! This module is deliberately independent of Tokio, SQLite, and process
//! code. The coordinator feeds it a snapshot and commits its decisions in one
//! transaction.

use std::collections::BTreeSet;

use crate::domain::JobId;

/// An attempt that currently holds a run lease.
#[derive(Debug, Clone)]
pub struct ActiveLease {
    pub job_id: JobId,
    pub limit: u32,
}

/// A job eligible to start: queued, not held, dependencies satisfied, retry
/// delay elapsed. Must be supplied in FIFO submission order.
#[derive(Debug, Clone)]
pub struct Candidate {
    pub job_id: JobId,
    pub seq: i64,
    pub limit: u32,
}

/// The persisted protected-job reservation, if one is active.
#[derive(Debug, Clone)]
pub struct ReservationSnapshot {
    pub job_id: JobId,
    pub cutoff_seq: i64,
    /// Jobs whose single backfill bypass is already consumed (seeded with the
    /// jobs active at creation plus every backfill admitted since).
    pub consumed: BTreeSet<JobId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Start {
    pub job_id: JobId,
    /// When this start is a backfill that bypasses a protected job, the
    /// protected job's ID. One pass can satisfy an old reservation and create
    /// a new one, so the flag alone would not say which reservation consumed
    /// the bypass.
    pub bypasses: Option<JobId>,
}

impl Start {
    pub fn consumes_bypass(&self) -> bool {
        self.bypasses.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewReservation {
    pub job_id: JobId,
    pub cutoff_seq: i64,
    /// Jobs seeded as already-consumed: everything holding a (shadow) lease
    /// when protection was created, so their retries cannot become fresh
    /// backfills.
    pub initial_consumed: BTreeSet<JobId>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PassOutcome {
    /// Jobs to start, in admission order.
    pub starts: Vec<Start>,
    /// The pre-existing reservation's protected job started this pass.
    pub satisfy_reservation: bool,
    /// The pre-existing reservation is no longer meaningful.
    pub invalidate_reservation: Option<&'static str>,
    /// A new reservation to persist (the head job could not start).
    pub create_reservation: Option<NewReservation>,
}

/// The admission formula: candidate `j` may acquire a lease exactly when
/// `|R| + 1 <= min(j.limit, r.limit for r in R)`.
pub fn can_admit(active_limits: &[u32], candidate_limit: u32) -> bool {
    let resulting = active_limits.len() as u64 + 1;
    resulting <= u64::from(candidate_limit)
        && active_limits.iter().all(|&l| resulting <= u64::from(l))
}

/// One greedy scheduling pass over a consistent snapshot.
///
/// `eligible` must be sorted by ascending `seq`. `max_seq` is the largest
/// submission sequence that exists at all (including ineligible jobs); it
/// becomes the frozen backfill cutoff when protection is created.
pub fn plan_pass(
    active: &[ActiveLease],
    eligible: &[Candidate],
    reservation: Option<&ReservationSnapshot>,
    max_seq: i64,
) -> PassOutcome {
    debug_assert!(eligible.windows(2).all(|w| w[0].seq < w[1].seq));

    let mut outcome = PassOutcome::default();
    let mut shadow_limits: Vec<u32> = active.iter().map(|l| l.limit).collect();
    let mut shadow_jobs: BTreeSet<JobId> = active.iter().map(|l| l.job_id).collect();
    let mut started: BTreeSet<JobId> = BTreeSet::new();

    // Local mutable view of the reservation for this pass. `existing` tracks
    // whether it is the persisted one (whose satisfaction/invalidation must be
    // reported) or one created earlier in this same pass.
    struct Res {
        job_id: JobId,
        cutoff_seq: i64,
        consumed: BTreeSet<JobId>,
        existing: bool,
    }

    let mut res: Option<Res> = reservation.map(|r| Res {
        job_id: r.job_id,
        cutoff_seq: r.cutoff_seq,
        consumed: r.consumed.clone(),
        existing: true,
    });

    // A persisted reservation whose protected job is no longer eligible
    // (cancelled, held, or dependency-ineligible again) is defensively
    // invalidated; mutations normally do this with a precise reason.
    if let Some(r) = &res
        && !eligible.iter().any(|c| c.job_id == r.job_id)
    {
        outcome.invalidate_reservation = Some("protected job is no longer eligible");
        res = None;
    }

    loop {
        match &mut res {
            Some(r) => {
                // Step 2: the protected job starts the moment it fits,
                // before any backfill is reconsidered.
                let protected = eligible
                    .iter()
                    .find(|c| c.job_id == r.job_id)
                    .expect("protected job verified eligible");
                if can_admit(&shadow_limits, protected.limit) {
                    outcome.starts.push(Start { job_id: protected.job_id, bypasses: None });
                    started.insert(protected.job_id);
                    shadow_limits.push(protected.limit);
                    shadow_jobs.insert(protected.job_id);
                    if r.existing {
                        outcome.satisfy_reservation = true;
                    } else {
                        // A reservation created this pass can never be
                        // satisfied in the same pass: the shadow set only
                        // grows, so a job that did not fit cannot fit later.
                        unreachable!("reservation created and satisfied in one pass");
                    }
                    res = None;
                    continue;
                }

                // Step 3: restrict candidates to the frozen frontier —
                // eligible pre-cutoff jobs whose single bypass is unconsumed.
                let backfill = eligible.iter().find(|c| {
                    c.seq <= r.cutoff_seq
                        && c.job_id != r.job_id
                        && !r.consumed.contains(&c.job_id)
                        && !started.contains(&c.job_id)
                        && can_admit(&shadow_limits, c.limit)
                });
                match backfill {
                    Some(c) => {
                        outcome.starts.push(Start { job_id: c.job_id, bypasses: Some(r.job_id) });
                        started.insert(c.job_id);
                        shadow_limits.push(c.limit);
                        shadow_jobs.insert(c.job_id);
                        r.consumed.insert(c.job_id);
                    }
                    // Step 7: nothing fits right now. "Currently full" is not
                    // an exhausted frontier; the next pass rechecks.
                    None => break,
                }
            }
            None => {
                // Step 4: without a reservation, consider FIFO order; the
                // first job either starts or becomes protected.
                let Some(head) = eligible.iter().find(|c| !started.contains(&c.job_id)) else {
                    break;
                };
                if can_admit(&shadow_limits, head.limit) {
                    outcome.starts.push(Start { job_id: head.job_id, bypasses: None });
                    started.insert(head.job_id);
                    shadow_limits.push(head.limit);
                    shadow_jobs.insert(head.job_id);
                    continue;
                }
                // Incompatible only because of active concurrency limits:
                // protect it. Seed the consumed set with every current shadow
                // lease holder (initially active attempts plus jobs admitted
                // earlier in this batch).
                let initial_consumed = shadow_jobs.clone();
                outcome.create_reservation = Some(NewReservation {
                    job_id: head.job_id,
                    cutoff_seq: max_seq,
                    initial_consumed: initial_consumed.clone(),
                });
                res = Some(Res {
                    job_id: head.job_id,
                    cutoff_seq: max_seq,
                    consumed: initial_consumed,
                    existing: false,
                });
            }
        }
    }

    outcome
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leases(limits: &[u32]) -> Vec<ActiveLease> {
        limits
            .iter()
            .enumerate()
            .map(|(i, &limit)| ActiveLease { job_id: -(i as i64 + 1), limit })
            .collect()
    }

    fn cand(job_id: JobId, limit: u32) -> Candidate {
        Candidate { job_id, seq: job_id, limit }
    }

    fn start_ids(outcome: &PassOutcome) -> Vec<JobId> {
        outcome.starts.iter().map(|s| s.job_id).collect()
    }

    #[test]
    fn formula_matches_documented_table() {
        // none + LLM(1) -> starts alone
        assert!(can_admit(&[], 1));
        // two CleanRL(3) + CleanRL(3) -> starts as third
        assert!(can_admit(&[3, 3], 3));
        // two CleanRL(3) + LLM(1) -> waits
        assert!(!can_admit(&[3, 3], 1));
        // LLM(1) running + CleanRL(3) -> waits
        assert!(!can_admit(&[1], 3));
        // one job(2) + one job(4) -> starts; lower limit of 2 wins
        assert!(can_admit(&[2], 4));
        // three at 3-wide is full for everyone
        assert!(!can_admit(&[3, 3, 3], 3));
    }

    #[test]
    fn empty_queue_does_nothing() {
        let outcome = plan_pass(&leases(&[3]), &[], None, 10);
        assert_eq!(outcome, PassOutcome::default());
    }

    #[test]
    fn batch_admission_is_cumulative() {
        // Four 3-wide candidates on an idle machine: exactly three start and
        // the fourth becomes protected (not silently over-admitted).
        let eligible = vec![cand(1, 3), cand(2, 3), cand(3, 3), cand(4, 3)];
        let outcome = plan_pass(&[], &eligible, None, 4);
        assert_eq!(start_ids(&outcome), vec![1, 2, 3]);
        let res = outcome.create_reservation.expect("fourth job is protected");
        assert_eq!(res.job_id, 4);
        assert_eq!(res.cutoff_seq, 4);
        assert_eq!(res.initial_consumed, BTreeSet::from([1, 2, 3]));
    }

    #[test]
    fn restrictive_head_is_protected_and_backfill_is_bounded() {
        // Motivating example: two CleanRL(3) active, LLM(1) first in line,
        // five CleanRL(3) behind it.
        let active = vec![
            ActiveLease { job_id: 100, limit: 3 },
            ActiveLease { job_id: 101, limit: 3 },
        ];
        let eligible = vec![cand(1, 1), cand(2, 3), cand(3, 3), cand(4, 3), cand(5, 3), cand(6, 3)];
        let outcome = plan_pass(&active, &eligible, None, 6);

        // LLM protected; exactly one CleanRL backfills the empty third slot.
        assert_eq!(outcome.starts, vec![Start { job_id: 2, bypasses: Some(1) }]);
        let res = outcome.create_reservation.unwrap();
        assert_eq!(res.job_id, 1);
        assert_eq!(res.cutoff_seq, 6);
        assert_eq!(res.initial_consumed, BTreeSet::from([100, 101]));
    }

    #[test]
    fn post_cutoff_jobs_never_pass_protection() {
        // Reservation frozen at cutoff 6; job 7 arrived later. A slot is
        // open, but job 7 must not take it and pre-cutoff job 3 may.
        let active = vec![
            ActiveLease { job_id: 100, limit: 3 },
            ActiveLease { job_id: 101, limit: 3 },
        ];
        let res = ReservationSnapshot {
            job_id: 1,
            cutoff_seq: 6,
            consumed: BTreeSet::from([100, 101, 2]),
        };
        let eligible = vec![cand(1, 1), cand(3, 3), cand(7, 3)];
        let outcome = plan_pass(&active, &eligible, Some(&res), 7);
        assert_eq!(outcome.starts, vec![Start { job_id: 3, bypasses: Some(1) }]);

        // Same but job 3's bypass is already consumed: nothing starts even
        // though a slot is open — post-cutoff job 7 still may not pass.
        let res2 = ReservationSnapshot {
            job_id: 1,
            cutoff_seq: 6,
            consumed: BTreeSet::from([100, 101, 2, 3]),
        };
        let outcome2 = plan_pass(&active, &eligible, Some(&res2), 7);
        assert!(outcome2.starts.is_empty());
        assert!(!outcome2.satisfy_reservation);
        assert!(outcome2.create_reservation.is_none());
    }

    #[test]
    fn protected_job_starts_before_backfills_when_drained() {
        // Everything drained: the protected LLM(1) starts alone; eligible
        // pre-cutoff CleanRL jobs must not start beside it.
        let res = ReservationSnapshot { job_id: 1, cutoff_seq: 6, consumed: BTreeSet::new() };
        let eligible = vec![cand(1, 1), cand(3, 3), cand(4, 3)];
        let outcome = plan_pass(&[], &eligible, Some(&res), 7);
        assert_eq!(outcome.starts, vec![Start { job_id: 1, bypasses: None }]);
        assert!(outcome.satisfy_reservation);
        // After the exclusive job is admitted, the rest wait (head job 3
        // becomes the next protected job).
        assert_eq!(outcome.create_reservation.unwrap().job_id, 3);
    }

    #[test]
    fn satisfying_one_reservation_can_create_the_next() {
        // Protected job (2-wide) starts; one companion fits; the next head is
        // 1-wide and becomes the new protected job.
        let res = ReservationSnapshot { job_id: 1, cutoff_seq: 5, consumed: BTreeSet::new() };
        let eligible = vec![cand(1, 2), cand(2, 2), cand(3, 1)];
        let outcome = plan_pass(&[], &eligible, Some(&res), 5);
        assert_eq!(start_ids(&outcome), vec![1, 2]);
        assert!(outcome.satisfy_reservation);
        let new_res = outcome.create_reservation.unwrap();
        assert_eq!(new_res.job_id, 3);
        assert_eq!(new_res.initial_consumed, BTreeSet::from([1, 2]));
    }

    #[test]
    fn ineligible_protected_job_invalidates_reservation() {
        let res = ReservationSnapshot { job_id: 1, cutoff_seq: 5, consumed: BTreeSet::new() };
        let eligible = vec![cand(2, 3), cand(3, 3)];
        let outcome = plan_pass(&[], &eligible, Some(&res), 5);
        assert!(outcome.invalidate_reservation.is_some());
        // Ordinary FIFO admission resumes.
        assert_eq!(start_ids(&outcome), vec![2, 3]);
    }

    #[test]
    fn lower_limit_always_wins_in_mixed_sets() {
        // active {4}: a 2-wide candidate fits (resulting 2 <= min(4, 2)).
        let outcome = plan_pass(&leases(&[4]), &[cand(1, 2)], None, 1);
        assert_eq!(start_ids(&outcome), vec![1]);
        // active {4, 2}: a 4-wide candidate would make 3 > 2 — protected.
        let outcome = plan_pass(&leases(&[4, 2]), &[cand(1, 4)], None, 1);
        assert!(outcome.starts.is_empty());
        assert_eq!(outcome.create_reservation.unwrap().job_id, 1);
    }

    /// Simulation oracle: replay plan_pass decisions against randomized job
    /// streams and completions, asserting the invariants after every step.
    #[test]
    fn randomized_streams_never_violate_invariants() {
        // Small deterministic xorshift so the test needs no rand dependency.
        let mut rng_state: u64 = 0x9E3779B97F4A7C15;
        let mut rng = move |bound: u64| {
            rng_state ^= rng_state << 13;
            rng_state ^= rng_state >> 7;
            rng_state ^= rng_state << 17;
            rng_state % bound
        };

        for _round in 0..200 {
            let mut next_seq: i64 = 0;
            let mut queued: Vec<Candidate> = Vec::new();
            let mut active: Vec<ActiveLease> = Vec::new();
            let mut reservation: Option<ReservationSnapshot> = None;
            // Sequence at which each job was protected, to check precedence.
            let mut protected_since: Option<(JobId, i64)> = None;

            for _step in 0..60 {
                match rng(3) {
                    // Submit 1-3 jobs.
                    0 => {
                        for _ in 0..=rng(2) {
                            next_seq += 1;
                            let limit = [1, 1, 2, 3, 3, 4][rng(6) as usize];
                            queued.push(Candidate { job_id: next_seq, seq: next_seq, limit });
                        }
                    }
                    // Finish a random active job.
                    1 if !active.is_empty() => {
                        let idx = rng(active.len() as u64) as usize;
                        active.remove(idx);
                    }
                    _ => {}
                }

                let outcome = plan_pass(&active, &queued, reservation.as_ref(), next_seq);

                // Apply decisions exactly as the coordinator would.
                if outcome.satisfy_reservation || outcome.invalidate_reservation.is_some() {
                    reservation = None;
                    protected_since = None;
                }
                for start in &outcome.starts {
                    let pos = queued.iter().position(|c| c.job_id == start.job_id).unwrap();
                    let c = queued.remove(pos);

                    // Invariant: admission never violates any member's limit.
                    let limits: Vec<u32> = active.iter().map(|l| l.limit).collect();
                    assert!(
                        can_admit(&limits, c.limit),
                        "over-admission: active {limits:?} candidate {}",
                        c.limit
                    );

                    // Invariant: nothing submitted after a protection event
                    // starts while that reservation is active.
                    if let Some(r) = &reservation {
                        assert!(start.consumes_bypass() || c.job_id == r.job_id);
                        assert!(
                            c.seq <= r.cutoff_seq || c.job_id == r.job_id,
                            "post-cutoff job {} bypassed protected job {}",
                            c.job_id,
                            r.job_id
                        );
                        if let Some((pjob, _)) = protected_since {
                            assert_ne!(c.job_id, pjob, "protected job started via backfill path");
                        }
                    }

                    active.push(ActiveLease { job_id: c.job_id, limit: c.limit });
                    if let Some(r) = &mut reservation
                        && start.consumes_bypass()
                    {
                        assert!(
                            r.consumed.insert(c.job_id),
                            "job {} consumed its bypass twice",
                            c.job_id
                        );
                    }
                }
                if let Some(new_res) = outcome.create_reservation {
                    assert!(reservation.is_none());
                    protected_since = Some((new_res.job_id, next_seq));
                    reservation = Some(ReservationSnapshot {
                        job_id: new_res.job_id,
                        cutoff_seq: new_res.cutoff_seq,
                        consumed: new_res.initial_consumed,
                    });
                }

                // Global invariant: current active set satisfies everyone.
                let count = active.len() as u32;
                for lease in &active {
                    assert!(count <= lease.limit, "active set violates a member declaration");
                }
            }
        }
    }
}
