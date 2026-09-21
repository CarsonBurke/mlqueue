//! Explicit, local-only follow checks and event hooks. Commands are argv, never shell text.

use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::path::Path;
use std::process::Stdio;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, Command};

const MAX_BYTES: usize = 64 * 1024;
const MAX_CHECKS: usize = 32;
const MAX_HOOKS: usize = 32;
const MAX_FINDINGS: usize = 64;
const MAX_ACTIVE: usize = 256;
const MAX_MESSAGE_BYTES: usize = 2048;
const ERROR_KEY: &str = "__mlq_check_error";
const EVENTS: &[&str] = &["started", "warning", "retry", "attention", "finished"];

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FollowConfig {
    pub events: Option<Vec<String>>,
    pub checks: Vec<CheckConfig>,
    pub hooks: Vec<HookConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckConfig {
    pub id: String,
    pub command: Vec<String>,
    #[serde(default = "default_every", deserialize_with = "duration")]
    pub every: Duration,
    #[serde(default = "default_timeout", deserialize_with = "duration")]
    pub timeout: Duration,
    #[serde(default = "default_clear_after")]
    pub clear_after: u32,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookConfig {
    pub events: Vec<String>,
    pub command: Vec<String>,
    #[serde(default = "default_timeout", deserialize_with = "duration")]
    pub timeout: Duration,
}

fn default_every() -> Duration {
    Duration::from_secs(60)
}

fn default_timeout() -> Duration {
    Duration::from_secs(5)
}

fn default_clear_after() -> u32 {
    2
}

fn duration<'de, D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Duration, D::Error> {
    let value = String::deserialize(deserializer)?;
    humantime::parse_duration(&value).map_err(serde::de::Error::custom)
}

/// Missing or invalid explicit configuration is an error, never an implicit fallback.
pub fn load(path: &Path) -> Result<FollowConfig> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("opening follow config {}", path.display()))?;
    let mut bytes = Vec::new();
    file.take((MAX_BYTES + 1) as u64).read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= MAX_BYTES, "follow config exceeds 64 KiB");
    let mut config: FollowConfig = toml::from_str(std::str::from_utf8(&bytes)?)
        .with_context(|| format!("parsing follow config {}", path.display()))?;
    config.validate()?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    for command in config
        .checks
        .iter_mut()
        .map(|check| &mut check.command)
        .chain(config.hooks.iter_mut().map(|hook| &mut hook.command))
    {
        let executable = Path::new(&command[0]);
        if command[0].contains('/') && executable.is_relative() {
            // Keep bare names as PATH lookups; only executable paths are config-relative.
            let resolved = std::env::current_dir()?.join(parent).join(executable);
            command[0] = resolved
                .into_os_string()
                .into_string()
                .map_err(|_| anyhow::anyhow!("follow executable path is not UTF-8"))?;
        }
    }
    Ok(config)
}

impl FollowConfig {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.checks.len() <= MAX_CHECKS,
            "at most {MAX_CHECKS} follow checks are allowed"
        );
        ensure!(
            self.hooks.len() <= MAX_HOOKS,
            "at most {MAX_HOOKS} follow hooks are allowed"
        );
        if let Some(events) = &self.events {
            validate_events(events)?;
        }
        let mut ids = HashSet::new();
        for check in &self.checks {
            ensure!(
                !check.id.is_empty()
                    && check.id.len() <= 64
                    && check
                        .id
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c)),
                "check IDs must be 1..64 ASCII letters, digits, '-', '_' or '.'"
            );
            ensure!(ids.insert(&check.id), "duplicate check ID: {}", check.id);
            validate_command(&check.command)?;
            ensure!(
                check.every >= Duration::from_secs(1)
                    && check.every <= Duration::from_secs(7 * 24 * 3600),
                "check every must be between 1s and 7days"
            );
            validate_timeout(check.timeout)?;
            ensure!(
                (1..=1000).contains(&check.clear_after),
                "clear_after must be between 1 and 1000"
            );
        }
        for hook in &self.hooks {
            ensure!(!hook.events.is_empty(), "hook events must not be empty");
            validate_events(&hook.events)?;
            validate_command(&hook.command)?;
            validate_timeout(hook.timeout)?;
        }
        Ok(())
    }
}

fn validate_events(events: &[String]) -> Result<()> {
    ensure!(events.len() <= EVENTS.len(), "too many follow events");
    let mut seen = HashSet::new();
    for event in events {
        ensure!(
            EVENTS.contains(&event.as_str()),
            "unknown follow event: {event}"
        );
        ensure!(seen.insert(event), "duplicate follow event: {event}");
    }
    Ok(())
}

fn validate_command(command: &[String]) -> Result<()> {
    ensure!(
        !command.is_empty() && command.len() <= 64,
        "command must contain 1..64 argv entries"
    );
    ensure!(
        !command[0].is_empty(),
        "command executable must not be empty"
    );
    ensure!(
        command.iter().all(|arg| !arg.contains('\0')),
        "command arguments must not contain NUL"
    );
    ensure!(
        command.iter().map(String::len).sum::<usize>() <= 16 * 1024,
        "command argv exceeds 16 KiB"
    );
    Ok(())
}

fn validate_timeout(timeout: Duration) -> Result<()> {
    ensure!(
        !timeout.is_zero() && timeout <= Duration::from_secs(300),
        "command timeout must be positive and at most 5min"
    );
    Ok(())
}

#[derive(Debug, Clone, Serialize)]
pub struct Finding {
    pub check: String,
    pub key: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Observation {
    findings: Vec<ObservedFinding>,
    #[serde(default)]
    step: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObservedFinding {
    key: String,
    message: String,
    #[serde(default)]
    step: Option<u64>,
}

struct CheckState {
    config: CheckConfig,
    deadline: Instant,
    // An entry is active until enough successful, fresh observations omit it.
    active: HashMap<String, u32>,
    last_step: Option<u64>,
    failed: bool,
}

impl CheckState {
    fn observe(&mut self, observation: Observation) -> Result<Vec<Finding>> {
        ensure!(
            observation.findings.len() <= MAX_FINDINGS,
            "check returned more than {MAX_FINDINGS} findings"
        );
        let mut keys = HashSet::new();
        for finding in &observation.findings {
            ensure!(
                !finding.key.trim().is_empty()
                    && finding.key.len() <= 128
                    && finding.key != ERROR_KEY,
                "finding key must be nonempty, at most 128 bytes, and not reserved"
            );
            ensure!(
                keys.insert(finding.key.as_str()),
                "check returned duplicate finding keys"
            );
            ensure!(
                !finding.message.trim().is_empty() && finding.message.len() <= MAX_MESSAGE_BYTES,
                "finding message must be nonempty and at most {MAX_MESSAGE_BYTES} bytes"
            );
        }
        // A cached metric result is not another successful observation for clearance.
        if matches!((observation.step, self.last_step), (Some(step), Some(last)) if step <= last) {
            self.failed = false;
            return Ok(Vec::new());
        }
        let new_count = keys
            .iter()
            .filter(|key| !self.active.contains_key(**key))
            .count();
        ensure!(
            self.active.len() + new_count <= MAX_ACTIVE,
            "check exceeded {MAX_ACTIVE} active finding keys"
        );
        self.failed = false;
        if let Some(step) = observation.step {
            self.last_step = Some(step);
        }
        self.active.retain(|key, absent| {
            if keys.contains(key.as_str()) {
                *absent = 0;
                true
            } else {
                *absent += 1;
                *absent < self.config.clear_after
            }
        });
        let mut findings = Vec::new();
        for finding in observation.findings {
            if !self.active.contains_key(&finding.key) {
                self.active.insert(finding.key.clone(), 0);
                findings.push(Finding {
                    check: self.config.id.clone(),
                    key: finding.key,
                    message: finding.message,
                    step: finding.step.or(observation.step),
                });
            }
        }
        Ok(findings)
    }

    fn failure(&mut self, error: &anyhow::Error) -> Option<Finding> {
        if self.failed {
            return None;
        }
        self.failed = true;
        let mut message = format!("check failed: {error}");
        if message.len() > MAX_MESSAGE_BYTES {
            let mut end = MAX_MESSAGE_BYTES;
            while !message.is_char_boundary(end) {
                end -= 1;
            }
            message.truncate(end);
        }
        Some(Finding {
            check: self.config.id.clone(),
            key: ERROR_KEY.into(),
            message,
            step: None,
        })
    }
}

pub struct CheckRunner {
    checks: Vec<CheckState>,
}

impl CheckRunner {
    pub fn new(config: Vec<CheckConfig>) -> Self {
        let now = Instant::now();
        Self {
            checks: config
                .into_iter()
                .map(|config| CheckState {
                    config,
                    deadline: now,
                    active: HashMap::new(),
                    last_step: None,
                    failed: false,
                })
                .collect(),
        }
    }

    pub fn next_deadline(&self) -> Option<Instant> {
        self.checks.iter().map(|check| check.deadline).min()
    }

    pub fn reset_attempt(&mut self) {
        let now = Instant::now();
        for check in &mut self.checks {
            check.deadline = now;
            check.active.clear();
            check.last_step = None;
            check.failed = false;
        }
    }

    /// The caller schedules this only while the followed attempt is running.
    pub async fn run_due(&mut self, context: &Value) -> Vec<Finding> {
        // Commit only after the last await: a job transition can cancel a batch
        // without consuming warning keys whose returned findings were lost.
        let mut completed = Vec::new();
        for (index, check) in self.checks.iter().enumerate() {
            if check.deadline > Instant::now() {
                continue;
            }
            let result = match execute(&check.config.command, check.config.timeout, context).await {
                Ok(output) => {
                    serde_json::from_slice::<Observation>(&output).context("invalid check JSON")
                }
                Err(error) => Err(error),
            };
            completed.push((index, Instant::now() + check.config.every, result));
        }
        let mut findings = Vec::new();
        for (index, deadline, result) in completed {
            let check = &mut self.checks[index];
            check.deadline = deadline;
            match result.and_then(|observation| check.observe(observation)) {
                Ok(new_findings) => findings.extend(new_findings),
                Err(error) => findings.extend(check.failure(&error)),
            }
        }
        findings
    }
}

/// Only explicitly selected hooks are passed by the caller. Output never reaches the terminal.
pub async fn run_hooks(hooks: &[HookConfig], event: &str, payload: &Value) -> Vec<String> {
    let mut errors = Vec::new();
    for hook in hooks {
        if hook.events.iter().any(|selected| selected == event)
            && let Err(error) = execute(&hook.command, hook.timeout, payload).await
        {
            errors.push(format!("{event} hook failed: {error}"));
        }
    }
    errors
}

struct BoundedInput(Vec<u8>);

impl Write for BoundedInput {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > MAX_BYTES - self.0.len() {
            return Err(std::io::Error::other("command JSON input exceeds 64 KiB"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// The group guard survives cancellation of the async execution future. Killing the
/// entire group also closes pipes inherited by forked descendants. Tokio's child
/// drop handler kills/reaps the direct child when cancellation prevents async wait.
struct ProcessGroup {
    child: Child,
    pgid: i32,
}

impl ProcessGroup {
    fn kill(&mut self) {
        // The child starts its own process group before exec; never signal ours.
        if self.pgid > 0 {
            unsafe {
                libc::killpg(self.pgid, libc::SIGKILL);
            }
        }
        let _ = self.child.start_kill();
    }
}

impl Drop for ProcessGroup {
    fn drop(&mut self) {
        self.kill();
    }
}

async fn execute(argv: &[String], timeout: Duration, payload: &Value) -> Result<Vec<u8>> {
    validate_command(argv)?;
    validate_timeout(timeout)?;
    let mut input = BoundedInput(Vec::new());
    serde_json::to_writer(&mut input, payload).context("serializing command JSON input")?;
    input.write_all(b"\n")?;
    let mut command = Command::new(&argv[0]);
    command
        .args(&argv[1..])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .process_group(0);
    let child = command.spawn().context("spawning follow command")?;
    let pgid = child.id().context("follow command has no process ID")? as i32;
    let mut group = ProcessGroup { child, pgid };
    let mut stdin = group
        .child
        .stdin
        .take()
        .context("follow command stdin unavailable")?;
    let mut stdout = group
        .child
        .stdout
        .take()
        .context("follow command stdout unavailable")?;
    let result = tokio::time::timeout(timeout, async {
        let write_input = async {
            // Early stdin closure is fine: a check may not need the context.
            if let Err(error) = stdin.write_all(&input.0).await
                && error.kind() != std::io::ErrorKind::BrokenPipe
            {
                return Err(anyhow::Error::from(error));
            }
            drop(stdin);
            Ok::<(), anyhow::Error>(())
        };
        let read_output = async {
            let mut output = Vec::new();
            let mut buffer = [0u8; 4096];
            loop {
                let count = stdout.read(&mut buffer).await?;
                if count == 0 {
                    break;
                }
                ensure!(
                    count <= MAX_BYTES - output.len(),
                    "command stdout exceeds 64 KiB"
                );
                output.extend_from_slice(&buffer[..count]);
            }
            Ok::<Vec<u8>, anyhow::Error>(output)
        };
        let wait_child = async {
            let status = group.child.wait().await?;
            // Stop inherited-pipe holders as soon as the direct child exits.
            group.kill();
            group.pgid = 0;
            ensure!(status.success(), "command exited with {status}");
            Ok::<(), anyhow::Error>(())
        };
        let (_, output, ()) = tokio::try_join!(write_input, read_output, wait_child)?;
        Ok::<Vec<u8>, anyhow::Error>(output)
    })
    .await;
    // Even successful commands must not leave background descendants behind.
    group.kill();
    let _ = group.child.wait().await;
    // Cleanup already ran; do not signal a subsequently reused process-group ID.
    group.pgid = 0;
    match result {
        Ok(result) => result,
        Err(_) => bail!(
            "command timed out after {}",
            humantime::format_duration(timeout)
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn state() -> CheckState {
        CheckState {
            config: CheckConfig {
                id: "loss".into(),
                command: vec!["check".into()],
                every: default_every(),
                timeout: default_timeout(),
                clear_after: 2,
            },
            deadline: Instant::now(),
            active: HashMap::new(),
            last_step: None,
            failed: false,
        }
    }

    fn observation(step: Option<u64>, findings: Value) -> Observation {
        serde_json::from_value(json!({ "step": step, "findings": findings })).unwrap()
    }

    #[test]
    fn active_keys_deduplicate_changed_text_and_rearm_after_clearance() {
        let mut check = state();
        let present = json!([{ "key": "stalled", "message": "loss stalled" }]);
        assert_eq!(
            check
                .observe(observation(None, present.clone()))
                .unwrap()
                .len(),
            1
        );
        assert!(
            check
                .observe(observation(
                    None,
                    json!([{ "key": "stalled", "message": "different text", "step": 42 }])
                ))
                .unwrap()
                .is_empty()
        );
        check.observe(observation(None, json!([]))).unwrap();
        assert!(
            check
                .observe(observation(None, present.clone()))
                .unwrap()
                .is_empty()
        );
        check.observe(observation(None, json!([]))).unwrap();
        check.observe(observation(None, json!([]))).unwrap();
        assert_eq!(check.observe(observation(None, present)).unwrap().len(), 1);
    }

    #[test]
    fn repeated_or_regressed_steps_do_not_clear_or_emit() {
        let mut check = state();
        let present = json!([{ "key": "stalled", "message": "loss stalled" }]);
        assert_eq!(
            check
                .observe(observation(Some(10), present.clone()))
                .unwrap()[0]
                .step,
            Some(10)
        );
        check.observe(observation(Some(11), json!([]))).unwrap();
        check.observe(observation(Some(11), json!([]))).unwrap();
        check.observe(observation(Some(9), json!([]))).unwrap();
        assert!(
            check
                .observe(observation(Some(12), present.clone()))
                .unwrap()
                .is_empty()
        );
        check.observe(observation(Some(13), json!([]))).unwrap();
        check.observe(observation(Some(14), json!([]))).unwrap();
        assert_eq!(
            check.observe(observation(Some(15), present)).unwrap().len(),
            1
        );
    }

    #[test]
    fn failed_checks_preserve_findings_and_deduplicate_until_recovery() {
        let mut check = state();
        let present = json!([{ "key": "stalled", "message": "loss stalled" }]);
        check.observe(observation(None, present.clone())).unwrap();
        check.observe(observation(None, json!([]))).unwrap();
        assert!(check.failure(&anyhow::anyhow!("timeout")).is_some());
        assert!(check.failure(&anyhow::anyhow!("other failure")).is_none());
        assert!(
            check
                .observe(observation(None, present))
                .unwrap()
                .is_empty()
        );
        assert!(check.failure(&anyhow::anyhow!("timeout")).is_some());
    }

    #[test]
    fn invalid_observation_does_not_partially_clear_active_keys() {
        let mut check = state();
        let present = json!([{ "key": "stalled", "message": "loss stalled" }]);
        check.observe(observation(None, present.clone())).unwrap();
        check.observe(observation(None, json!([]))).unwrap();
        assert!(
            check
                .observe(observation(None, json!([{ "key": "new", "message": "" }])))
                .is_err()
        );
        assert!(
            check
                .observe(observation(None, present))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn attempt_reset_rearms_keys_and_accepts_restarted_metric_steps() {
        let mut runner = CheckRunner {
            checks: vec![state()],
        };
        let present = json!([{ "key": "stalled", "message": "loss stalled" }]);
        runner.checks[0]
            .observe(observation(Some(100), present.clone()))
            .unwrap();
        runner.checks[0].failure(&anyhow::anyhow!("timeout"));
        runner.reset_attempt();
        assert_eq!(
            runner.checks[0]
                .observe(observation(Some(1), present))
                .unwrap()
                .len(),
            1
        );
        assert!(
            runner.checks[0]
                .failure(&anyhow::anyhow!("timeout"))
                .is_some()
        );
    }

    #[test]
    fn explicit_config_rejects_unknown_fields_and_invalid_intervals() {
        assert!(toml::from_str::<FollowConfig>("evnets = ['warning']").is_err());
        let config: FollowConfig =
            toml::from_str("[[checks]]\nid = 'loss'\ncommand = ['check']\nevery = '100ms'\n")
                .unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn only_executable_paths_are_relative_to_config_file() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("follow.toml");
        std::fs::write(
            &path,
            "[[checks]]\nid = 'local'\ncommand = ['./checker', './input']\n\
             [[checks]]\nid = 'path'\ncommand = ['checker']\n",
        )
        .unwrap();
        let config = load(&path).unwrap();
        assert_eq!(
            Path::new(&config.checks[0].command[0]),
            directory.path().join("./checker")
        );
        assert_eq!(config.checks[0].command[1], "./input");
        assert_eq!(config.checks[1].command[0], "checker");
    }

    #[tokio::test]
    async fn dropping_execution_kills_child_and_descendants() {
        let directory = tempfile::tempdir().unwrap();
        let pid_file = directory.path().join("pids");
        let argv = vec![
            "/bin/sh".into(),
            "-c".into(),
            "sleep 30 & echo \"$$ $!\" > \"$1\"; wait".into(),
            "follow-test".into(),
            pid_file.to_str().unwrap().into(),
        ];
        let execution =
            tokio::spawn(async move { execute(&argv, Duration::from_secs(30), &json!({})).await });
        let pids = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(text) = std::fs::read_to_string(&pid_file) {
                    let pids: Vec<u32> = text
                        .split_whitespace()
                        .filter_map(|pid| pid.parse().ok())
                        .collect();
                    if pids.len() == 2 {
                        break pids;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        execution.abort();
        assert!(execution.await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(5), async {
            for pid in pids {
                loop {
                    match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
                        Ok(stat)
                            if matches!(
                                stat.rsplit_once(')').unwrap().1.split_whitespace().next(),
                                Some("Z" | "X")
                            ) =>
                        {
                            break;
                        }
                        Err(error) => panic!("reading child process state: {error}"),
                        Ok(_) => tokio::time::sleep(Duration::from_millis(10)).await,
                    }
                }
            }
        })
        .await
        .expect("cancelled command left a live child or descendant");
    }

    #[tokio::test]
    async fn subprocess_timeout_and_output_limit_are_errors() {
        let hung = vec!["/bin/sh".into(), "-c".into(), "sleep 30".into()];
        let error = execute(&hung, Duration::from_millis(50), &json!({}))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("timed out"));
        let flood = vec![
            "/bin/sh".into(),
            "-c".into(),
            "while :; do printf '0123456789abcdef'; done".into(),
        ];
        let error = execute(&flood, Duration::from_secs(5), &json!({}))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("stdout exceeds"));
    }
}
