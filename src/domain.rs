//! Core identifiers, states, and invariant-bearing value types.

use std::fmt;
use std::num::NonZeroU32;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

pub type JobId = i64;
pub type AttemptId = i64;

/// Version stamped on persisted scheduler reservations. If a future upgrade
/// cannot interpret an active reservation's version exactly, it must block
/// ordinary admission instead of silently clearing the reservation.
///
/// Version history:
/// - 1: backfill cutoff frozen at reservation creation. An active v1
///   reservation is still interpreted exactly, as a permanently frozen
///   frontier at its stored cutoff.
/// - 2: blocker-scoped backfill window. The cutoff starts as the unfrozen
///   sentinel and is pinned once, when the initial blockers stop advancing.
/// - 3: signed strict priority. Higher-priority eligible jobs supersede lower
///   protected jobs, and only equal-priority jobs may backfill a reservation.
/// - 4: FIFO and frozen backfill cutoffs use durable readiness order rather
///   than submission order. Dependency-delayed jobs join the queue only once
///   their prerequisites resolve successfully.
pub const SCHEDULER_SEMANTICS_VERSION: i64 = 4;

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_millis() as i64
}

/// The single admission declaration: this job is safe to run only when the
/// total number of concurrent managed jobs, including itself, is no greater
/// than this value. Zero is unrepresentable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MaxParallelRuns(NonZeroU32);

impl MaxParallelRuns {
    pub const ONE: MaxParallelRuns = MaxParallelRuns(NonZeroU32::MIN);

    pub fn new(value: u32) -> Option<Self> {
        NonZeroU32::new(value).map(Self)
    }

    pub fn get(self) -> u32 {
        self.0.get()
    }
}

impl fmt::Display for MaxParallelRuns {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

macro_rules! str_enum {
    ($name:ident { $($variant:ident => $text:literal),+ $(,)? }) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(rename_all = "snake_case")]
        pub enum $name {
            $($variant),+
        }

        impl $name {
            pub fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $text),+
                }
            }
        }

        impl FromStr for $name {
            type Err = String;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match s {
                    $($text => Ok(Self::$variant),)+
                    other => Err(format!(concat!("unknown ", stringify!($name), " {:?}"), other)),
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}

str_enum!(JobState {
    Held => "held",
    Queued => "queued",
    Starting => "starting",
    Running => "running",
    Succeeded => "succeeded",
    Failed => "failed",
    Cancelled => "cancelled",
    Lost => "lost",
    // Read-only legacy state retained so pre-v3 history remains truthful.
    Skipped => "skipped",
    NeedsAttention => "needs_attention",
});

impl JobState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::Lost | Self::Skipped
        )
    }

    /// Terminal states an idempotent `retry` may leave.
    pub fn is_retryable(self) -> bool {
        matches!(self, Self::Failed | Self::Lost)
    }
}

str_enum!(AttemptState {
    Prepared => "prepared",
    Authorized => "authorized",
    Running => "running",
    Orphaned => "orphaned",
    Quarantined => "quarantined",
    Succeeded => "succeeded",
    Failed => "failed",
    Cancelled => "cancelled",
    Lost => "lost",
});

str_enum!(DependencyCondition {
    Succeeded => "succeeded",
    Terminal => "terminal",
});

str_enum!(DependencyResolution {
    Satisfied => "satisfied",
    Violated => "violated",
});

/// Whether a cancellation timestamp loses to a time-limit deadline.
///
/// `limit_set_at` is when the current limit was installed. `None` means the
/// attempt launched with it. A cancel that predates a later edit keeps the
/// classification it had under the limit already in force: moving the
/// deadline backward does not turn that cancel into a timeout.
pub fn timeout_wins_over_cancel(
    cancelled_at: i64,
    deadline: i64,
    limit_set_at: Option<i64>,
) -> bool {
    cancelled_at > deadline && limit_set_at.is_none_or(|set_at| cancelled_at > set_at)
}

/// When SIGKILL may follow a timeout's SIGTERM.
///
/// `observed_at` is the instant a supervisor already armed the grace. Without
/// one, a limit installed after its own deadline starts grace at installation,
/// while a limit that was already in force is measured from the deadline so a
/// late enforcer still catches up.
pub fn timeout_kill_anchor(
    deadline: i64,
    limit_set_at: Option<i64>,
    observed_at: Option<i64>,
) -> i64 {
    if let Some(observed_at) = observed_at {
        return observed_at;
    }
    match limit_set_at {
        Some(set_at) if deadline < set_at => set_at,
        _ => deadline,
    }
}

impl AttemptState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::Lost
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_parallel_runs_rejects_zero() {
        assert!(MaxParallelRuns::new(0).is_none());
        assert_eq!(MaxParallelRuns::new(3).unwrap().get(), 3);
    }

    #[test]
    fn timeout_does_not_rewrite_a_cancel_that_predates_the_limit_edit() {
        assert!(timeout_wins_over_cancel(12, 10, None));
        assert!(!timeout_wins_over_cancel(8, 10, None));
        // Cancel at 5, then the limit is moved so the deadline is 1.
        assert!(!timeout_wins_over_cancel(5, 1, Some(6)));
        // The edit happened first; the cancel came after the new deadline.
        assert!(timeout_wins_over_cancel(8, 4, Some(6)));
    }

    #[test]
    fn timeout_grace_starts_at_installation_when_the_deadline_was_backdated() {
        assert_eq!(timeout_kill_anchor(1_000, None, None), 1_000);
        assert_eq!(timeout_kill_anchor(1_000, Some(5_000), None), 5_000);
        assert_eq!(timeout_kill_anchor(9_000, Some(5_000), None), 9_000);
        assert_eq!(timeout_kill_anchor(1_000, Some(5_000), Some(7_000)), 7_000);
    }

    #[test]
    fn state_round_trips() {
        for state in [
            JobState::Held,
            JobState::Queued,
            JobState::Starting,
            JobState::Running,
            JobState::Succeeded,
            JobState::Failed,
            JobState::Cancelled,
            JobState::Lost,
            JobState::Skipped,
            JobState::NeedsAttention,
        ] {
            assert_eq!(state.as_str().parse::<JobState>().unwrap(), state);
        }
        assert_eq!(
            "prepared".parse::<AttemptState>().unwrap(),
            AttemptState::Prepared
        );
        assert!("bogus".parse::<JobState>().is_err());
    }
}
