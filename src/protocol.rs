//! Framed Unix-socket protocol: big-endian u32 length-prefixed JSON with a
//! hard frame limit checked before allocation, plus the stable public JSON
//! views (camelCase, per the plan's `maxParallelRuns` convention).

use std::collections::BTreeMap;
use std::io::{self, Read, Write};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::domain::{AttemptId, JobId};

pub const PROTOCOL_VERSION: u32 = 1;
pub const DEFAULT_MAX_FRAME_BYTES: u32 = 1 << 20;

// Stable error codes.
pub mod error_codes {
    pub const UNSUPPORTED_PROTOCOL: &str = "unsupported_protocol";
    pub const MALFORMED_REQUEST: &str = "malformed_request";
    pub const MISSING_IDEMPOTENCY_KEY: &str = "missing_idempotency_key";
    pub const IDEMPOTENCY_CONFLICT: &str = "idempotency_conflict";
    pub const NOT_FOUND: &str = "not_found";
    pub const INVALID_STATE: &str = "invalid_state";
    pub const INVALID_ARGUMENT: &str = "invalid_argument";
    pub const UNSAFE_RESOLUTION: &str = "unsafe_resolution";
    pub const ADMISSION_BLOCKED: &str = "admission_blocked";
    pub const INTERNAL: &str = "internal";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub protocol_version: u32,
    pub request_id: String,
    /// Required for every mutating operation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    pub op: Op,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Op {
    Submit(SubmitParams),
    Status,
    Show { job: JobId },
    Logs { job: JobId, attempt: Option<i64> },
    Cancel { job: JobId, force: bool },
    Hold { job: JobId },
    Release { job: JobId },
    Retry { job: JobId },
    SetMaxParallelRuns { job: JobId, max_parallel_runs: u32 },
    DaemonStatus,
    RecoverList,
    RecoverResolve { job: JobId, attempt: i64, resolve_as: ResolveAs },
}

impl Op {
    pub fn is_mutation(&self) -> bool {
        matches!(
            self,
            Op::Submit(_)
                | Op::Cancel { .. }
                | Op::Hold { .. }
                | Op::Release { .. }
                | Op::Retry { .. }
                | Op::SetMaxParallelRuns { .. }
                | Op::RecoverResolve { .. }
        )
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Op::Submit(_) => "submit",
            Op::Status => "status",
            Op::Show { .. } => "show",
            Op::Logs { .. } => "logs",
            Op::Cancel { .. } => "cancel",
            Op::Hold { .. } => "hold",
            Op::Release { .. } => "release",
            Op::Retry { .. } => "retry",
            Op::SetMaxParallelRuns { .. } => "set_max_parallel_runs",
            Op::DaemonStatus => "daemon_status",
            Op::RecoverList => "recover_list",
            Op::RecoverResolve { .. } => "recover_resolve",
        }
    }

    /// Canonical request hash for idempotency-key conflict detection. Struct
    /// field order is deterministic and `env` is a BTreeMap, so equal payloads
    /// serialize identically.
    pub fn request_hash(&self) -> String {
        let canonical = serde_json::to_vec(self).expect("op serializes");
        hex::encode(Sha256::digest(&canonical))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubmitParams {
    pub name: String,
    pub cwd: String,
    /// Exact argument vector; argv[0] is the executable.
    pub args: Vec<String>,
    /// Explicit persisted environment (baseline plus client-resolved values).
    pub env: BTreeMap<String, String>,
    pub max_parallel_runs: u32,
    pub max_attempts: u32,
    pub retry_delay_ms: u64,
    #[serde(default)]
    pub after_success: Vec<JobId>,
    #[serde(default)]
    pub after_completion: Vec<JobId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolveAs {
    Lost,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply: Option<Reply>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorBody>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorBody {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Reply {
    Submitted { job: JobView },
    Job { job: JobView },
    Status(StatusView),
    LogPaths(LogPathsView),
    DaemonStatus(DaemonStatusView),
    RecoverList { attempts: Vec<AttemptView> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobView {
    pub id: JobId,
    pub name: String,
    pub state: String,
    /// Why a non-terminal job is not running right now (derived, not stored).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eligibility: Option<String>,
    /// Extra detail for terminal or attention states.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_reason: Option<String>,
    pub max_parallel_runs: u32,
    pub cwd: String,
    pub args: Vec<String>,
    pub max_attempts: u32,
    pub retry_delay_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_not_before: Option<i64>,
    pub attempt_count: i64,
    pub created_at: i64,
    pub updated_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<i64>,
    pub dependencies: Vec<DependencyView>,
    pub attempts: Vec<AttemptView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cancel_requested: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DependencyView {
    pub parent: JobId,
    pub requirement: String,
    pub satisfied: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AttemptView {
    pub id: AttemptId,
    pub job_id: JobId,
    pub number: i64,
    pub state: String,
    pub max_parallel_runs: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub term_signal: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    pub created_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<i64>,
    pub log_dir: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogPathsView {
    pub job: JobId,
    pub attempt: AttemptId,
    pub attempt_number: i64,
    pub attempt_state: String,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatusView {
    pub jobs: Vec<JobView>,
    pub active_leases: u32,
    /// `min` of active lease limits: the strictest declaration in force.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effective_limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reservation: Option<ReservationView>,
    pub admission_blocked: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReservationView {
    pub protected_job: JobId,
    pub backfill_cutoff: i64,
    pub created_at: i64,
    /// Attempts currently preventing the protected job from starting.
    pub blocking_attempts: Vec<AttemptId>,
    /// Jobs whose single backfill bypass is consumed.
    pub consumed_bypasses: Vec<JobId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DaemonStatusView {
    pub pid: u32,
    pub version: String,
    pub protocol_version: u32,
    pub scheduler_semantics_version: i64,
    pub started_at: i64,
    pub db_path: String,
    pub socket_path: String,
    pub active_leases: u32,
    pub queued_jobs: u32,
    pub admission_blocked: bool,
}

fn frame_len(bytes_len: usize, max: u32) -> io::Result<u32> {
    let len = u32::try_from(bytes_len)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "frame too large"))?;
    if len > max {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("frame of {len} bytes exceeds limit {max}"),
        ));
    }
    Ok(len)
}

pub async fn read_frame<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
    max: u32,
) -> io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err),
    }
    let len = u32::from_be_bytes(len_buf);
    if len > max {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame of {len} bytes exceeds limit {max}"),
        ));
    }
    let mut buf = vec![0u8; len as usize];
    reader.read_exact(&mut buf).await?;
    Ok(Some(buf))
}

pub async fn write_frame<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    bytes: &[u8],
    max: u32,
) -> io::Result<()> {
    let len = frame_len(bytes.len(), max)?;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(bytes).await?;
    writer.flush().await
}

pub fn read_frame_sync<R: Read>(reader: &mut R, max: u32) -> io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf) {
        Ok(_) => {}
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err),
    }
    let len = u32::from_be_bytes(len_buf);
    if len > max {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame of {len} bytes exceeds limit {max}"),
        ));
    }
    let mut buf = vec![0u8; len as usize];
    reader.read_exact(&mut buf)?;
    Ok(Some(buf))
}

pub fn write_frame_sync<W: Write>(writer: &mut W, bytes: &[u8], max: u32) -> io::Result<()> {
    let len = frame_len(bytes.len(), max)?;
    writer.write_all(&len.to_be_bytes())?;
    writer.write_all(bytes)?;
    writer.flush()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_hash_is_stable_and_payload_sensitive() {
        let mk = |limit| {
            Op::Submit(SubmitParams {
                name: "x".into(),
                cwd: "/tmp".into(),
                args: vec!["true".into()],
                env: BTreeMap::new(),
                max_parallel_runs: limit,
                max_attempts: 1,
                retry_delay_ms: 0,
                after_success: vec![],
                after_completion: vec![],
            })
        };
        assert_eq!(mk(1).request_hash(), mk(1).request_hash());
        assert_ne!(mk(1).request_hash(), mk(2).request_hash());
    }

    #[test]
    fn sync_framing_round_trips_and_enforces_limit() {
        let mut buf = Vec::new();
        write_frame_sync(&mut buf, b"hello", 1024).unwrap();
        let mut cursor = std::io::Cursor::new(buf);
        assert_eq!(read_frame_sync(&mut cursor, 1024).unwrap().unwrap(), b"hello");
        assert!(read_frame_sync(&mut cursor, 1024).unwrap().is_none());

        let mut oversized = Vec::new();
        oversized.extend_from_slice(&100u32.to_be_bytes());
        oversized.extend_from_slice(&[0u8; 100]);
        let mut cursor = std::io::Cursor::new(oversized);
        assert!(read_frame_sync(&mut cursor, 10).is_err());
    }

    #[test]
    fn stable_json_uses_camel_case_max_parallel_runs() {
        let view = AttemptView {
            id: 1,
            job_id: 2,
            number: 1,
            state: "running".into(),
            max_parallel_runs: 3,
            exit_code: None,
            term_signal: None,
            message: None,
            created_at: 0,
            finished_at: None,
            log_dir: "/x".into(),
        };
        let json = serde_json::to_string(&view).unwrap();
        assert!(json.contains("\"maxParallelRuns\":3"), "{json}");
    }
}
