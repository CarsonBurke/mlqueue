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
pub const SCHEDULER_SEMANTICS_VERSION: i64 = 1;

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

impl AttemptState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled | Self::Lost)
    }
}

str_enum!(DepRequirement {
    Success => "success",
    Completion => "completion",
});

impl DepRequirement {
    /// Whether a terminal parent state satisfies this requirement.
    pub fn satisfied_by(self, parent: JobState) -> bool {
        match self {
            Self::Success => parent == JobState::Succeeded,
            Self::Completion => parent.is_terminal(),
        }
    }

    /// Whether a terminal parent state makes this requirement permanently
    /// unsatisfiable, which skips the child.
    pub fn violated_by(self, parent: JobState) -> bool {
        parent.is_terminal() && !self.satisfied_by(parent)
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
        assert_eq!("prepared".parse::<AttemptState>().unwrap(), AttemptState::Prepared);
        assert!("bogus".parse::<JobState>().is_err());
    }

    #[test]
    fn dependency_requirements() {
        assert!(DepRequirement::Success.satisfied_by(JobState::Succeeded));
        assert!(!DepRequirement::Success.satisfied_by(JobState::Failed));
        assert!(DepRequirement::Success.violated_by(JobState::Failed));
        assert!(DepRequirement::Success.violated_by(JobState::Skipped));
        assert!(!DepRequirement::Success.violated_by(JobState::Running));
        assert!(DepRequirement::Completion.satisfied_by(JobState::Failed));
        assert!(!DepRequirement::Completion.violated_by(JobState::Failed));
    }
}
