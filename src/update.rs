//! Staged daemon updates: stage a built `mlq`/`mlqd` pair, then replace the
//! installed pair only while no job is unfinished and no run lease is held.
//!
//! Staging runs in the operator's shell. Activation runs in a separate
//! systemd user service (`<daemon>-update.service`), started by a path unit
//! whenever an update is pending, because it has to stop the daemon. That
//! watcher blocks on a single queue subscription while it waits.
//!
//! The installed daemon may speak an older protocol than this binary, so the
//! watcher never talks to it directly. It runs a staged copy of the installed
//! `mlq` (the *observer*) and reads its JSON, keeping only fields every
//! subscription-capable release emits.
//!
//! Update root (`$XDG_STATE_HOME/mlqueue/updates/`):
//!
//! ```text
//! update.lock       serializes staging, cancellation, and activation
//! mlq               the updater the watcher unit executes
//! pending.json      manifest of the staged update
//! restart.json      written before stopping the daemon, removed once it is back
//! failed.json       an update whose daemon did not start, and its rollback
//! watcher-error.txt why the watcher's last attempt failed
//! staged-<id>/      mlqd, mlq (candidates), observer (installed mlq), and
//!                   original/ (the installed pair, backed up at activation)
//! previous/         what the last update replaced (its original/)
//! ```

use std::collections::BTreeMap;
use std::fs::{self, File, TryLockError};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;

use crate::paths::ensure_private_dir;
use crate::systemd;

const PENDING: &str = "pending.json";
const RESTART: &str = "restart.json";
const FAILED: &str = "failed.json";
/// The watcher's last error, while it retries.
const WATCHER_ERROR: &str = "watcher-error.txt";
const LOCK: &str = "update.lock";
const UPDATER: &str = "mlq";
const OBSERVER: &str = "observer";
const PREVIOUS: &str = "previous";
/// Inside a staging directory: the installed pair before this update.
const ORIGINAL: &str = "original";
const STAGED_PREFIX: &str = "staged-";
/// Prefix of every temporary file, including copies beside the installed
/// binaries that an interrupted activation may leave behind.
const TEMP_PREFIX: &str = ".mlqueue-update-";

/// How long a restarted daemon that has not exited may take to accept
/// connections, including any schema migration.
const READY_TIMEOUT: Duration = Duration::from_secs(120);

/// Environment the observer needs to reach the same daemon as the operator
/// who staged the update; the watcher service otherwise has systemd's.
const OBSERVER_ENV: &[&str] = &[
    "HOME",
    "PATH",
    "USER",
    "LOGNAME",
    "XDG_RUNTIME_DIR",
    "XDG_STATE_HOME",
    "XDG_CONFIG_HOME",
];

/// One of the two binaries of an installation, which are replaced together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Binary {
    Mlq,
    Mlqd,
}

impl Binary {
    /// Replacement order. The observer is the old `mlq`, so a new `mlq` with
    /// the old `mlqd` is a state a later activation can still finish; the
    /// reverse would leave the watcher unable to reach the daemon.
    const ALL: [Binary; 2] = [Binary::Mlq, Binary::Mlqd];

    fn name(self) -> &'static str {
        match self {
            Binary::Mlq => "mlq",
            Binary::Mlqd => "mlqd",
        }
    }
}

/// SHA-256 of each candidate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checksums {
    mlq: String,
    mlqd: String,
}

impl Checksums {
    fn get(&self, binary: Binary) -> &str {
        match binary {
            Binary::Mlq => &self.mlq,
            Binary::Mlqd => &self.mlqd,
        }
    }
}

/// A staged update, persisted as `pending.json` (and as `restart.json` while
/// the daemon is stopped for it). Fields added later need `#[serde(default)]`
/// so a newer `mlq` can still read, finish, or cancel an older staging.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Manifest {
    /// The daemon's systemd user unit.
    pub service: String,
    /// `mlqd --version` of the candidate.
    pub version: String,
    /// Directory holding the installed `mlqd` (the unit's ExecStart) and `mlq`.
    pub install_dir: PathBuf,
    /// Directory holding the candidates and the observer.
    pub staged_dir: PathBuf,
    pub sha256: Checksums,
    /// The daemon's socket, for the readiness check after a restart.
    pub socket: PathBuf,
    /// Environment for the observer.
    pub env: BTreeMap<String, String>,
}

impl Manifest {
    fn candidate(&self, binary: Binary) -> PathBuf {
        self.staged_dir.join(binary.name())
    }

    fn installed(&self, binary: Binary) -> PathBuf {
        self.install_dir.join(binary.name())
    }

    fn observer(&self) -> PathBuf {
        self.staged_dir.join(OBSERVER)
    }

    fn original(&self, binary: Binary) -> PathBuf {
        self.staged_dir.join(ORIGINAL).join(binary.name())
    }

    fn short_sha(&self) -> &str {
        let sha = self.sha256.get(Binary::Mlqd);
        &sha[..sha.len().min(12)]
    }

    fn verify_candidates(&self) -> Result<()> {
        for binary in Binary::ALL {
            ensure!(
                digest(&self.candidate(binary))? == self.sha256.get(binary),
                "staged {} checksum changed",
                binary.name()
            );
        }
        Ok(())
    }

    /// The installed binaries that do not yet match their candidates.
    fn outdated(&self) -> Result<Vec<Binary>> {
        let mut outdated = Vec::new();
        for binary in Binary::ALL {
            if digest(&self.installed(binary))? != self.sha256.get(binary) {
                outdated.push(binary);
            }
        }
        Ok(outdated)
    }
}

/// The fields of `mlq daemon status --json` the updater relies on.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DaemonProbe {
    pub pid: u32,
    pub db_path: PathBuf,
    pub socket_path: PathBuf,
}

/// The fields of an `mlq subscribe` snapshot that decide idleness. Every job
/// without `finishedAt` counts as unfinished, including states this binary
/// does not know.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IdleProbe {
    active_leases: u32,
    admission_blocked: bool,
    jobs: Vec<IdleJob>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IdleJob {
    finished_at: Option<i64>,
}

impl IdleProbe {
    fn is_idle(&self) -> bool {
        self.active_leases == 0
            && !self.admission_blocked
            && self.jobs.iter().all(|job| job.finished_at.is_some())
    }
}

/// Everything activation does outside the update root, so tests can stand in
/// for systemd and the running daemon.
pub trait Host {
    fn unit_dir(&self) -> Result<PathBuf>;
    fn systemctl(&self, args: &[&str]) -> Result<()>;
    fn show(&self, unit: &str, property: &str) -> Result<String>;
    fn daemon_status(&self, manifest: &Manifest) -> Result<DaemonProbe>;
    /// Check that the observer can subscribe, by reading one snapshot.
    fn check_subscription(&self, manifest: &Manifest) -> Result<()>;
    /// Block until the queue is idle, without polling.
    fn wait_idle(&self, manifest: &Manifest) -> Result<()>;
    /// Wait until the unit's main process runs the installed `mlqd` and
    /// accepts connections.
    fn wait_ready(&self, manifest: &Manifest) -> Result<()>;
}

/// The real host: `systemctl --user` and the staged observer.
pub struct Systemd;

impl Host for Systemd {
    fn unit_dir(&self) -> Result<PathBuf> {
        systemd::user_unit_dir()
    }

    fn systemctl(&self, args: &[&str]) -> Result<()> {
        systemd::systemctl(args)
    }

    fn show(&self, unit: &str, property: &str) -> Result<String> {
        systemd::show(unit, property)
    }

    fn daemon_status(&self, manifest: &Manifest) -> Result<DaemonProbe> {
        let output = observer(manifest)
            .args(["daemon", "status", "--json"])
            .stderr(Stdio::inherit())
            .output()
            .context("running the staged observer")?;
        ensure!(
            output.status.success(),
            "`mlq daemon status` failed against the installed daemon"
        );
        serde_json::from_slice(&output.stdout).context("parsing `mlq daemon status --json`")
    }

    fn check_subscription(&self, manifest: &Manifest) -> Result<()> {
        let supported = observer(manifest)
            .args(["subscribe", "--help"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .context("running the staged observer")?
            .success();
        ensure!(
            supported,
            "the installed mlq cannot subscribe to queue snapshots (added in v0.3.0); \
             upgrade by hand as the README describes"
        );
        ensure!(
            read_snapshots(manifest, |_| true)?,
            "the installed mlq could not subscribe to the installed daemon"
        );
        Ok(())
    }

    fn wait_idle(&self, manifest: &Manifest) -> Result<()> {
        // A daemon restart ends the subscription. Resubscribe while it comes
        // back, so systemd's retry backoff only counts real failures.
        let mut retry_until = Instant::now() + READY_TIMEOUT;
        loop {
            match read_snapshots(manifest, IdleProbe::is_idle) {
                Ok(true) => return Ok(()),
                Ok(false) => retry_until = Instant::now() + READY_TIMEOUT,
                Err(err) if Instant::now() >= retry_until => {
                    return Err(err.context("the installed daemon stayed unreachable"));
                }
                Err(_) => {}
            }
            std::thread::sleep(Duration::from_secs(1));
        }
    }

    fn wait_ready(&self, manifest: &Manifest) -> Result<()> {
        let service = &manifest.service;
        let deadline = Instant::now() + READY_TIMEOUT;
        let mut first_restarts = None;
        loop {
            let [pid, active, sub, restarts] =
                systemd::show_all(service, ["MainPID", "ActiveState", "SubState", "NRestarts"])?;
            let first_restarts = first_restarts.get_or_insert_with(|| restarts.clone());
            if is_ready(manifest, &pid) {
                return Ok(());
            }
            let exited = matches!(active.as_str(), "failed" | "inactive")
                || sub == "auto-restart"
                || restarts != *first_restarts;
            ensure!(!exited, "{service} exited while starting");
            ensure!(
                Instant::now() < deadline,
                "{service} did not accept connections within {}s",
                READY_TIMEOUT.as_secs()
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

/// Whether the unit's main process runs the installed `mlqd` and accepts
/// connections.
fn is_ready(manifest: &Manifest, main_pid: &str) -> bool {
    let Some(pid) = main_pid.parse::<u32>().ok().filter(|&pid| pid != 0) else {
        return false;
    };
    match (
        fs::metadata(format!("/proc/{pid}/exe")),
        fs::metadata(manifest.installed(Binary::Mlqd)),
    ) {
        (Ok(running), Ok(installed))
            if running.dev() == installed.dev() && running.ino() == installed.ino() =>
        {
            UnixStream::connect(&manifest.socket).is_ok()
        }
        _ => false,
    }
}

/// Follow the observer's subscription until a snapshot satisfies `done`
/// (`true`) or the stream ends (`false`).
fn read_snapshots(manifest: &Manifest, done: impl Fn(&IdleProbe) -> bool) -> Result<bool> {
    let mut subscriber = observer(manifest)
        .arg("subscribe")
        .stdout(Stdio::piped())
        .spawn()
        .context("starting the queue subscription")?;
    let stdout = subscriber.stdout.take().expect("stdout is piped");
    let found = first_snapshot(BufReader::new(stdout), done);
    if !matches!(found, Ok(false)) {
        // We stopped reading; the subscriber has nothing left to say.
        let _ = subscriber.kill();
    }
    let exit = subscriber
        .wait()
        .context("waiting for the queue subscription")?;
    if found? {
        return Ok(true);
    }
    ensure!(exit.success(), "`mlq subscribe` failed ({exit})");
    Ok(false)
}

fn first_snapshot(snapshots: impl BufRead, done: impl Fn(&IdleProbe) -> bool) -> Result<bool> {
    for line in snapshots.lines() {
        let line = line.context("reading the queue subscription")?;
        let probe: IdleProbe = serde_json::from_str(&line).context("parsing a queue snapshot")?;
        if done(&probe) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn observer(manifest: &Manifest) -> Command {
    let mut command = Command::new(manifest.observer());
    command.env_clear().envs(&manifest.env).stdin(Stdio::null());
    command
}

fn main_pid(host: &impl Host, service: &str) -> Result<u32> {
    let pid: u32 = host
        .show(service, "MainPID")?
        .parse()
        .context("parsing MainPID")?;
    ensure!(pid != 0, "{service} is not running");
    Ok(pid)
}

/// A plain (non-template) systemd service name, safe to derive unit file
/// names and unit file lines from.
fn is_service_name(service: &str) -> bool {
    service.strip_suffix(".service").is_some_and(|stem| {
        !stem.is_empty()
            && stem
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || ":_.-".contains(c))
    })
}

/// Names of the watcher units for a daemon unit: `mlqd.service` is watched by
/// `mlqd-update.path`, which starts `mlqd-update.service`.
pub fn watcher_units(service: &str) -> (String, String) {
    let stem = service.strip_suffix(".service").unwrap_or(service);
    (
        format!("{stem}-update.path"),
        format!("{stem}-update.service"),
    )
}

// ---------------------------------------------------------------------------
// Staging
// ---------------------------------------------------------------------------

pub struct StageRequest<'a> {
    /// Absolute: systemd units refer to it.
    pub root: &'a Path,
    /// Directory holding the candidate `mlq` and `mlqd`.
    pub source_dir: &'a Path,
    pub service: &'a str,
    /// The `mlq` the watcher will run; normally this executable.
    pub updater: &'a Path,
    pub env: BTreeMap<String, String>,
}

/// Copy the candidates into the update root, record them as pending, and
/// make sure the watcher units are installed and running.
pub fn stage(host: &impl Host, request: StageRequest<'_>) -> Result<Manifest> {
    let StageRequest {
        root,
        source_dir,
        service,
        updater,
        env,
    } = request;
    ensure!(root.is_absolute(), "update directory must be absolute");
    // The units name the root; keep systemd specifiers, quoting, and
    // environment expansion out of play.
    ensure!(
        !root
            .to_string_lossy()
            .contains(['\n', '\r', '%', '"', '\\', '$']),
        "unsupported character in the update directory {}",
        root.display()
    );
    ensure!(
        is_service_name(service),
        "{service:?} is not a systemd service name"
    );
    ensure_private_dir(root)?;
    let _lock = lock(root)?;
    ensure!(
        !root.join(RESTART).exists(),
        "an interrupted activation is still being recovered; check `systemctl --user status {}` \
         or abandon it with `mlq daemon update cancel --force`",
        watcher_units(service).1
    );
    let replaced = read_json::<Manifest>(&root.join(PENDING))?;
    if let Some(replaced) = &replaced {
        // The observer is copied from the installed mlq below.
        undo_partial_swap(replaced)?;
    }
    let version = candidate_version(source_dir)?;
    let install_dir = validate_service(host, service)?;

    // Removed on any error below; kept once the manifest refers to it.
    let staged = tempfile::Builder::new()
        .prefix(STAGED_PREFIX)
        .rand_bytes(16)
        .tempdir_in(root)
        .context("creating a staging directory")?;
    for binary in Binary::ALL {
        replace_file(
            &source_dir.join(binary.name()),
            &staged.path().join(binary.name()),
        )?;
    }
    // A pinned copy of the installed CLI, which speaks the running daemon's
    // protocol even if the installation changes meanwhile.
    replace_file(
        &install_dir.join(Binary::Mlq.name()),
        &staged.path().join(OBSERVER),
    )?;
    let mut manifest = Manifest {
        service: service.to_owned(),
        version,
        install_dir,
        staged_dir: staged.path().to_owned(),
        sha256: Checksums {
            mlq: digest(&staged.path().join(Binary::Mlq.name()))?,
            mlqd: digest(&staged.path().join(Binary::Mlqd.name()))?,
        },
        socket: PathBuf::new(),
        env,
    };
    ensure!(
        !manifest.outdated()?.is_empty(),
        "{} is already installed in {}",
        manifest.version,
        manifest.install_dir.display()
    );
    let status = host.daemon_status(&manifest)?;
    ensure!(
        main_pid(host, service)? == status.pid,
        "the daemon `mlq` reaches (pid {}) is not {service}'s main process; \
         stage with the same MLQUEUE_* and XDG environment as the service",
        status.pid
    );
    manifest.socket = status.socket_path;
    host.check_subscription(&manifest)?;
    // Activation renames into this directory; find out now, not in the
    // watcher's retries.
    NamedTempFile::with_prefix_in(TEMP_PREFIX, &manifest.install_dir).with_context(|| {
        format!(
            "the installed binaries in {} cannot be replaced",
            manifest.install_dir.display()
        )
    })?;

    replace_file(updater, &root.join(UPDATER))?;
    if let Some(replaced) = &replaced {
        release_staging(root, replaced)?;
    }
    // From here the directory may be referenced, so only sweeping removes it.
    let staged_dir = staged.keep();
    write_json(&root.join(PENDING), &manifest)?;
    remove_if_exists(&root.join(FAILED))?;
    remove_if_exists(&root.join(WATCHER_ERROR))?;
    remove_unreferenced_staging(root, &staged_dir)?;
    if let Some(replaced) = replaced
        && replaced.service != service
    {
        remove_watcher_units(host, &replaced.service)?;
    }
    if let Err(err) = install_watcher(host, root, service) {
        // Nothing would activate it.
        discard(root, &manifest)?;
        return Err(err);
    }
    Ok(manifest)
}

/// `mlqd --version` of the candidate, after checking `mlq` reports the same
/// release: the pair is replaced together and must agree on the protocol.
fn candidate_version(source_dir: &Path) -> Result<String> {
    let version = |binary: Binary| -> Result<String> {
        let path = source_dir.join(binary.name());
        let output = Command::new(&path)
            .arg("--version")
            .stdin(Stdio::null())
            .output()
            .with_context(|| format!("running {} --version", path.display()))?;
        ensure!(
            output.status.success(),
            "{} --version failed",
            path.display()
        );
        let text = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        match text
            .strip_prefix(binary.name())
            .and_then(|v| v.strip_prefix(' '))
        {
            Some(version) => Ok(version.to_owned()),
            None => bail!(
                "{} is not {} (it reports {text:?})",
                path.display(),
                binary.name()
            ),
        }
    };
    let daemon = version(Binary::Mlqd)?;
    let cli = version(Binary::Mlq)?;
    ensure!(
        daemon == cli,
        "mlqd {daemon} and mlq {cli} in {} report different versions",
        source_dir.display()
    );
    Ok(daemon)
}

/// Check that `service` can be stopped and restarted without touching its
/// workers, and return the directory of the `mlqd` it runs.
fn validate_service(host: &impl Host, service: &str) -> Result<PathBuf> {
    ensure!(
        host.show(service, "LoadState")? == "loaded",
        "{service} is not installed; run `mlq daemon install` first"
    );
    ensure!(
        host.show(service, "KillMode")? == "process",
        "refusing to restart {service} without KillMode=process: stopping it would kill running jobs"
    );
    ensure!(
        host.show(service, "Transient")? == "no",
        "updating requires a persistent systemd unit, not a transient one"
    );
    let daemon = systemd::exec_start_path(&host.show(service, "ExecStart")?)
        .with_context(|| format!("reading {service} ExecStart="))?;
    ensure!(
        daemon.is_absolute() && daemon.file_name() == Some(Binary::Mlqd.name().as_ref()),
        "{service} runs {}, not an absolute path to mlqd",
        daemon.display()
    );
    let install_dir = daemon.parent().expect("absolute file path").to_owned();
    for binary in Binary::ALL {
        let path = install_dir.join(binary.name());
        let meta = fs::symlink_metadata(&path)
            .with_context(|| format!("inspecting installed {}", path.display()))?;
        ensure!(
            meta.is_file(),
            "{} must be a regular file, not a symlink",
            path.display()
        );
    }
    Ok(install_dir)
}

/// Write the path and service units that run `watch` whenever an update is
/// pending or an activation was interrupted, and (re)start the watcher so it
/// runs the updater just staged.
fn install_watcher(host: &impl Host, root: &Path, service: &str) -> Result<()> {
    let (path_unit, watch_unit) = watcher_units(service);
    let updater = root.join(UPDATER);
    let units = host.unit_dir()?;
    fs::create_dir_all(&units).with_context(|| format!("creating {}", units.display()))?;
    let root = root.display();
    let path_changed = write_if_changed(
        &units.join(&path_unit),
        &format!(
            "[Unit]\n\
             Description=Watch for staged updates of {service}\n\
             \n\
             [Path]\n\
             PathExists={root}/{PENDING}\n\
             PathExists={root}/{RESTART}\n\
             Unit={watch_unit}\n\
             \n\
             [Install]\n\
             WantedBy=default.target\n"
        ),
    )?;
    // Retries back off from 30s to 30min: each one that fails while the
    // daemon is stopped restarts the daemon.
    write_if_changed(
        &units.join(&watch_unit),
        &format!(
            "[Unit]\n\
             Description=Activate the staged update of {service} once the queue is idle\n\
             StartLimitIntervalSec=0\n\
             \n\
             [Service]\n\
             Type=exec\n\
             ExecStart=\"{}\" daemon update watch --root \"{root}\"\n\
             Restart=on-failure\n\
             RestartSec=30\n\
             # systemd < 254 ignores these and retries every 30s.\n\
             RestartSteps=6\n\
             RestartMaxDelaySec=30min\n\
             TimeoutStopSec=60\n",
            updater.display()
        ),
    )?;
    host.systemctl(&["daemon-reload"])?;
    // Under update.lock the watcher is waiting, not activating, so a restart
    // only swaps in the new updater.
    host.systemctl(&["try-restart", "--quiet", &watch_unit])?;
    host.systemctl(&["enable", "--now", &path_unit])?;
    if path_changed {
        // An already active path unit keeps watching what it was started with.
        host.systemctl(&["restart", &path_unit])?;
    }
    Ok(())
}

/// Discard any staged update for `service` and remove its watcher units.
/// Returns whether the units were installed.
pub fn uninstall_watcher(host: &impl Host, root: &Path, service: &str) -> Result<bool> {
    let _lock = if root.exists() {
        let lock = lock(root)?;
        ensure!(
            !root.join(RESTART).exists(),
            "an update activation was interrupted; wait for the watcher to restart the daemon \
             or abandon it with `mlq daemon update cancel --force`"
        );
        if let Some(manifest) = read_json::<Manifest>(&root.join(PENDING))?
            && manifest.service == service
        {
            discard(root, &manifest)?;
            remove_if_exists(&root.join(WATCHER_ERROR))?;
        }
        Some(lock)
    } else {
        None
    };
    remove_watcher_units(host, service)
}

/// Stop, disable, and delete `service`'s watcher units. Returns whether they
/// were installed.
fn remove_watcher_units(host: &impl Host, service: &str) -> Result<bool> {
    let units = host.unit_dir()?;
    let (path_unit, watch_unit) = watcher_units(service);
    if !units.join(&path_unit).exists() && !units.join(&watch_unit).exists() {
        return Ok(false);
    }
    host.systemctl(&["disable", "--now", "--quiet", &path_unit])?;
    host.systemctl(&["stop", "--quiet", &watch_unit])?;
    for unit in [&path_unit, &watch_unit] {
        let path = units.join(unit);
        if let Err(err) = fs::remove_file(&path)
            && err.kind() != std::io::ErrorKind::NotFound
        {
            return Err(err).with_context(|| format!("removing {}", path.display()));
        }
    }
    host.systemctl(&["daemon-reload"])?;
    Ok(true)
}

// ---------------------------------------------------------------------------
// Status and cancellation
// ---------------------------------------------------------------------------

/// `restart.json`: written before the daemon is stopped for an activation and
/// removed once it is running again.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Restart {
    pub manifest: Manifest,
    /// The stopped daemon's database.
    pub db_path: PathBuf,
    /// Schema version before the first candidate was renamed into place. Set
    /// once renaming may have begun, so recovery knows a rollback applies.
    #[serde(default)]
    pub schema: Option<i64>,
}

/// An update whose daemon did not start, persisted as `failed.json` so it is
/// reported and never retried.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FailedUpdate {
    pub manifest: Manifest,
    pub reason: String,
    pub rollback: Rollback,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "camelCase")]
pub enum Rollback {
    /// Decided; the watcher finishes it, even after being interrupted.
    InProgress,
    /// The original pair is installed and running again.
    Restored,
    /// The new daemon migrated the schema, which the original daemon cannot
    /// open, so the new pair stays installed and systemd keeps restarting it.
    Refused { from: i64, to: i64 },
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateStatus {
    pub pending: Option<Manifest>,
    /// Set while the daemon may be stopped for an activation that has not
    /// finished; the watcher restarts it.
    pub interrupted: Option<Restart>,
    pub failed: Option<FailedUpdate>,
    /// Why the watcher's last attempt failed; it retries with a backoff.
    pub watcher_error: Option<String>,
}

pub fn status(root: &Path) -> Result<UpdateStatus> {
    let watcher_error = match fs::read_to_string(root.join(WATCHER_ERROR)) {
        Ok(error) => Some(error),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
        Err(err) => return Err(err).context("reading the watcher's last error"),
    };
    Ok(UpdateStatus {
        pending: read_json(&root.join(PENDING))?,
        interrupted: read_json(&root.join(RESTART))?,
        failed: read_json(&root.join(FAILED))?,
        watcher_error,
    })
}

/// What `cancel` removed.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Cancelled {
    pub pending: Option<Manifest>,
    /// The pending pair was already installed, so only its staging was left.
    pub pending_installed: bool,
    pub interrupted: Option<Restart>,
    pub failed: Option<FailedUpdate>,
    /// Where the binaries the abandoned update replaced were kept, if it
    /// replaced any.
    pub kept_originals: Option<PathBuf>,
}

/// Discard the pending update and any failure report, and stop the watcher.
/// An interrupted activation is only abandoned with `force`, leaving the
/// daemon in whatever state it is in.
pub fn cancel(host: &impl Host, root: &Path, force: bool) -> Result<Cancelled> {
    if !root.exists() {
        return Ok(Cancelled::default());
    }
    let _lock = lock(root)?;
    let mut kept = false;
    let interrupted = read_json::<Restart>(&root.join(RESTART))?;
    if let Some(restart) = &interrupted {
        ensure!(
            force,
            "an activation was interrupted and the watcher is restarting the daemon; \
             pass --force to abandon it and leave the daemon as it is"
        );
        kept |= release_staging(root, &restart.manifest)?;
        remove_file(&root.join(RESTART))?;
    }
    let failed = read_json::<FailedUpdate>(&root.join(FAILED))?;
    remove_if_exists(&root.join(FAILED))?;
    remove_if_exists(&root.join(WATCHER_ERROR))?;
    let pending = read_json::<Manifest>(&root.join(PENDING))?;
    let mut pending_installed = false;
    if let Some(manifest) = &pending {
        pending_installed = manifest.outdated()?.is_empty();
        kept |= discard(root, manifest)?;
    }
    sync_dir(root)?;
    // The triggers are gone, so the path unit will not start it again. Under
    // update.lock the watcher is waiting, never activating: safe to stop.
    let service = pending
        .as_ref()
        .or(interrupted.as_ref().map(|restart| &restart.manifest))
        .map(|manifest| &manifest.service);
    if let Some(service) = service {
        host.systemctl(&["stop", "--quiet", &watcher_units(service).1])?;
    }
    Ok(Cancelled {
        pending,
        pending_installed,
        interrupted,
        failed,
        kept_originals: kept.then(|| root.join(PREVIOUS)),
    })
}

// ---------------------------------------------------------------------------
// Activation
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq)]
pub enum Activation {
    NothingPending,
    /// Work appeared between the idle snapshot and the stop.
    Deferred,
    Activated(Manifest),
    /// The new daemon did not start. `failed.json` holds the same record.
    Failed(FailedUpdate),
}

/// The watcher: finish any interrupted activation, then wait for an idle
/// queue and activate until nothing is pending. Its last error is kept for
/// `status`, since systemd retries it out of sight.
pub fn watch(host: &impl Host, root: &Path) -> Result<()> {
    let result = watch_until_done(host, root);
    let recorded = match &result {
        Ok(()) => remove_if_exists(&root.join(WATCHER_ERROR)),
        Err(err) => write_atomically(
            &root.join(WATCHER_ERROR),
            format!("{err:#}").as_bytes(),
            0o600,
        ),
    };
    result.and(recorded)
}

fn watch_until_done(host: &impl Host, root: &Path) -> Result<()> {
    recover(host, root)?;
    while let Some(manifest) = read_json::<Manifest>(&root.join(PENDING))? {
        // An installed update only needs finishing, and its daemon may not
        // speak the observer's protocol.
        if !manifest.outdated()?.is_empty() {
            println!(
                "waiting to activate mlqd {} until no job is unfinished and no lease is held",
                manifest.version
            );
            host.wait_idle(&manifest)?;
        }
        match activate(host, root)? {
            Activation::NothingPending => {}
            Activation::Deferred => println!("queue changed before shutdown; update deferred"),
            Activation::Activated(manifest) => println!(
                "activated mlqd {} ({})",
                manifest.version,
                manifest.short_sha()
            ),
            Activation::Failed(failed) => eprintln!("{}", failed.describe()),
        }
    }
    Ok(())
}

impl FailedUpdate {
    pub fn describe(&self) -> String {
        let version = &self.manifest.version;
        let reason = &self.reason;
        match self.rollback {
            Rollback::InProgress => {
                format!("mlqd {version} did not start ({reason}); rolling back")
            }
            Rollback::Restored => {
                format!("mlqd {version} did not start ({reason}); restored the original pair")
            }
            Rollback::Refused { from, to } => format!(
                "mlqd {version} did not start ({reason}), but it migrated the database from \
                 schema {from} to {to}, which the original mlqd cannot open; the new pair stays \
                 installed and needs an operator, and the original pair is in {}",
                self.manifest.staged_dir.with_file_name(PREVIOUS).display()
            ),
        }
    }
}

/// Finish an activation the previous watcher did not: restart the daemon,
/// or complete a rollback that was decided or is now needed.
fn recover(host: &impl Host, root: &Path) -> Result<()> {
    if !root.join(RESTART).exists() {
        return Ok(());
    }
    let _lock = lock(root)?;
    let Some(restart) = read_json::<Restart>(&root.join(RESTART))? else {
        return Ok(());
    };
    let manifest = &restart.manifest;
    if let Some(failed) =
        read_json::<FailedUpdate>(&root.join(FAILED))?.filter(|failed| failed.manifest == *manifest)
    {
        let failed = match failed.rollback {
            Rollback::InProgress => roll_back(host, root, &restart, failed.reason)?,
            // Decided and carried out; only the cleanup was interrupted.
            Rollback::Restored | Rollback::Refused { .. } => {
                conclude_rollback(root, &failed)?;
                failed
            }
        };
        eprintln!("{}", failed.describe());
        return Ok(());
    }
    // Renaming stopped before mlqd: the original daemon starts again, and
    // with the original mlq so the two can talk while the update waits.
    let candidate_installed = !manifest.outdated()?.contains(&Binary::Mlqd);
    if !candidate_installed {
        undo_partial_swap(manifest)?;
    }
    host.systemctl(&["start", &manifest.service])?;
    match host.wait_ready(manifest) {
        Ok(()) => {}
        // The new mlqd may have been installed and was never judged.
        Err(not_ready) if restart.schema.is_some() && candidate_installed => {
            let failed = roll_back(host, root, &restart, format!("{not_ready:#}"))?;
            eprintln!("{}", failed.describe());
            return Ok(());
        }
        Err(not_ready) => return Err(not_ready),
    }
    remove_file(&root.join(RESTART))?;
    sync_dir(root)?;
    let pending = read_json::<Manifest>(&root.join(PENDING))?;
    if pending.as_ref() == Some(manifest) && manifest.outdated()?.is_empty() {
        discard(root, manifest)?;
        println!(
            "recovered the activation of mlqd {} ({})",
            manifest.version,
            manifest.short_sha()
        );
    } else {
        println!(
            "restarted {} after an interrupted activation",
            manifest.service
        );
    }
    Ok(())
}

/// Stop the daemon, confirm from its database that it was idle, replace the
/// installed pair, and start it again, rolling back if it does not come up.
pub fn activate(host: &impl Host, root: &Path) -> Result<Activation> {
    let _lock = lock(root)?;
    let Some(manifest) = read_json::<Manifest>(&root.join(PENDING))? else {
        return Ok(Activation::NothingPending);
    };
    if manifest.outdated()?.is_empty() {
        // An earlier activation confirmed the daemon was up, then stopped
        // before finishing.
        discard(root, &manifest)?;
        return Ok(Activation::Activated(manifest));
    }
    manifest.verify_candidates()?;
    ensure!(
        validate_service(host, &manifest.service)? == manifest.install_dir,
        "{} no longer runs the mlqd in {}",
        manifest.service,
        manifest.install_dir.display()
    );
    let status = host.daemon_status(&manifest)?;
    ensure!(
        main_pid(host, &manifest.service)? == status.pid,
        "`mlq` and systemd refer to different daemons"
    );
    // All copying and syncing happens now; only renames remain while the
    // daemon is stopped.
    let swap = Swap::prepare(&manifest)?;

    // Recovery intent outlives this process: if it dies with the daemon
    // stopped, the path unit starts a watcher that restarts it.
    let mut restart = Restart {
        manifest,
        db_path: status.db_path,
        schema: None,
    };
    write_json(&root.join(RESTART), &restart)?;
    let committed = host
        .systemctl(&["stop", &restart.manifest.service])
        .and_then(|()| swap.commit_if_idle(root, &mut restart));
    let started = host
        .systemctl(&["start", &restart.manifest.service])
        .and_then(|()| host.wait_ready(&restart.manifest));
    match (committed, started) {
        (Ok(Commit::Replaced), Err(not_ready)) => {
            roll_back(host, root, &restart, format!("{not_ready:#}")).map(Activation::Failed)
        }
        (Ok(_), Err(not_ready)) => Err(not_ready),
        (Err(not_applied), Err(not_ready)) => Err(not_ready.context(format!(
            "after failing to apply the update: {not_applied:#}"
        ))),
        (Err(not_applied), Ok(())) => {
            // Renaming may have stopped after mlq; the original daemon runs.
            undo_partial_swap(&restart.manifest)?;
            remove_file(&root.join(RESTART))?;
            sync_dir(root)?;
            Err(not_applied)
        }
        (Ok(committed), Ok(())) => {
            remove_file(&root.join(RESTART))?;
            sync_dir(root)?;
            match committed {
                Commit::Deferred => Ok(Activation::Deferred),
                Commit::Replaced => {
                    discard(root, &restart.manifest)?;
                    Ok(Activation::Activated(restart.manifest))
                }
            }
        }
    }
}

/// Record that the update failed, then restore the original pair. The
/// record comes first, so nothing activates this build again even if the
/// rollback is interrupted; recovery finishes it from the record.
fn roll_back(
    host: &impl Host,
    root: &Path,
    restart: &Restart,
    reason: String,
) -> Result<FailedUpdate> {
    let manifest = &restart.manifest;
    let schema = restart
        .schema
        .context("no schema version was recorded before the swap")?;
    let mut failed = FailedUpdate {
        manifest: manifest.clone(),
        reason,
        rollback: Rollback::InProgress,
    };
    write_json(&root.join(FAILED), &failed)?;
    if read_json::<Manifest>(&root.join(PENDING))?.as_ref() == Some(manifest) {
        remove_file(&root.join(PENDING))?;
    }
    sync_dir(root)?;

    failed.rollback = restore_original(host, manifest, &restart.db_path, schema)
        .with_context(|| format!("rolling back after: {}", failed.reason))?;
    write_json(&root.join(FAILED), &failed)?;
    conclude_rollback(root, &failed)?;
    Ok(failed)
}

/// Clean up after a rollback whose outcome `failed.json` already records.
/// A refused rollback keeps the original pair in `previous/` for the
/// operator.
fn conclude_rollback(root: &Path, failed: &FailedUpdate) -> Result<()> {
    release_staging(root, &failed.manifest)?;
    remove_if_exists(&root.join(RESTART))?;
    sync_dir(root)
}

/// Put back `original/`, unless the new daemon already migrated the
/// database past what the original daemon can open.
fn restore_original(
    host: &impl Host,
    manifest: &Manifest,
    db: &Path,
    schema: i64,
) -> Result<Rollback> {
    host.systemctl(&["stop", &manifest.service])?;
    let outcome = put_back_originals(manifest, db, schema);
    // Whatever happened, the daemon should run again.
    host.systemctl(&["start", &manifest.service])?;
    let outcome = outcome?;
    if outcome == Rollback::Restored {
        host.wait_ready(manifest)?;
    }
    Ok(outcome)
}

fn put_back_originals(manifest: &Manifest, db: &Path, schema: i64) -> Result<Rollback> {
    let current = schema_version(db)?;
    if current != schema {
        return Ok(Rollback::Refused {
            from: schema,
            to: current,
        });
    }
    for binary in Binary::ALL {
        let original = manifest.original(binary);
        if original.exists() && digest(&original)? != digest(&manifest.installed(binary))? {
            replace_file(&original, &manifest.installed(binary))?;
        }
    }
    Ok(Rollback::Restored)
}

/// Outcome of `Swap::commit_if_idle`.
enum Commit {
    /// Work appeared; nothing was replaced.
    Deferred,
    Replaced,
}

/// Synced copies of the outdated candidates beside the installed binaries,
/// made while the daemon still runs. Dropping it removes whatever was not
/// committed.
struct Swap {
    steps: Vec<SwapStep>,
}

struct SwapStep {
    target: PathBuf,
    /// Digest of the installed binary when the copies were made.
    installed_sha: String,
    candidate: NamedTempFile,
}

impl Swap {
    /// Back up each installed binary this staging has not replaced yet into
    /// `staged_dir/original/`, unless an earlier attempt already did, and
    /// copy its candidate next to it.
    fn prepare(manifest: &Manifest) -> Result<Self> {
        remove_install_temps(&manifest.install_dir)?;
        ensure_private_dir(&manifest.staged_dir.join(ORIGINAL))?;
        let mut steps = Vec::new();
        for binary in Binary::ALL {
            let target = manifest.installed(binary);
            let installed_sha = digest(&target)?;
            if installed_sha == manifest.sha256.get(binary) {
                continue;
            }
            let original = manifest.original(binary);
            if digest(&original).ok().as_ref() != Some(&installed_sha) {
                replace_file(&target, &original)?;
            }
            steps.push(SwapStep {
                candidate: copy_to_temp(&manifest.candidate(binary), &manifest.install_dir)?,
                target,
                installed_sha,
            });
        }
        Ok(Self { steps })
    }

    /// With the daemon stopped, take its singleton lock so no other daemon
    /// can admit work, check the database, and rename the prepared copies
    /// into place in `Binary::ALL` order, recording the schema version in
    /// `restart.json` first. The lock is released on return, before the
    /// daemon is started again.
    fn commit_if_idle(self, root: &Path, restart: &mut Restart) -> Result<Commit> {
        let db = &restart.db_path;
        let state_dir = db.parent().context("database path has no parent")?;
        let lock_path = state_dir.join("daemon.lock");
        let daemon_lock = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&lock_path)
            .with_context(|| format!("opening {}", lock_path.display()))?;
        match daemon_lock.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                bail!("another mlqd holds {}", lock_path.display())
            }
            Err(TryLockError::Error(err)) => {
                return Err(err).with_context(|| format!("locking {}", lock_path.display()));
            }
        }
        if !database_is_idle(db)? {
            return Ok(Commit::Deferred);
        }
        for step in &self.steps {
            ensure!(
                digest(&step.target)? == step.installed_sha,
                "{} changed while the update was being applied",
                step.target.display()
            );
        }
        restart.schema = Some(schema_version(db)?);
        write_json(&root.join(RESTART), restart)?;
        for step in self.steps {
            persist(step.candidate, &step.target)?;
        }
        Ok(Commit::Replaced)
    }
}

fn open_database(db: &Path) -> Result<rusqlite::Connection> {
    rusqlite::Connection::open_with_flags(db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("opening {}", db.display()))
}

/// Read the stopped daemon's database directly. Unknown or nonterminal job
/// states and unreleased leases count as busy.
fn database_is_idle(db: &Path) -> Result<bool> {
    let busy: bool = open_database(db)?
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM jobs WHERE finished_at IS NULL) \
             OR EXISTS(SELECT 1 FROM run_leases WHERE released_at IS NULL)",
            [],
            |row| row.get(0),
        )
        .context("checking the queue for unfinished work")?;
    Ok(!busy)
}

fn schema_version(db: &Path) -> Result<i64> {
    open_database(db)?
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .context("reading the database schema version")
}

/// Complete an activation: keep what it replaced in `previous/` and discard
/// the staging.
/// Remove a staging directory without losing the only copy of what it
/// replaced: a half-replaced pair is put back first, and originals that are
/// no longer installed become `previous/`. Returns whether they did.
fn release_staging(root: &Path, manifest: &Manifest) -> Result<bool> {
    undo_partial_swap(manifest)?;
    let mut superseded = false;
    for binary in Binary::ALL {
        let original = manifest.original(binary);
        superseded |=
            original.exists() && digest(&original)? != digest(&manifest.installed(binary))?;
    }
    if superseded {
        keep_originals(root, manifest)?;
    }
    remove_staged(root, &manifest.staged_dir)?;
    Ok(superseded)
}

/// Make the staging's `original/` the new `previous/`. Replacing the whole
/// directory drops copies from older updates and can be repeated after an
/// interruption.
fn keep_originals(root: &Path, manifest: &Manifest) -> Result<()> {
    let original = manifest.staged_dir.join(ORIGINAL);
    let previous = root.join(PREVIOUS);
    match fs::remove_dir_all(&previous) {
        Err(err) if err.kind() != std::io::ErrorKind::NotFound => {
            return Err(err).with_context(|| format!("removing {}", previous.display()));
        }
        _ => {}
    }
    fs::rename(&original, &previous).with_context(|| format!("keeping {}", original.display()))?;
    sync_dir(root)
}

/// Put back what this staging replaced if only part of the pair was
/// (renaming stopped after `mlq`), so discarding it never leaves an `mlq`
/// that cannot talk to the installed `mlqd`. The daemon still runs the
/// original `mlqd`, so nothing needs restarting.
fn undo_partial_swap(manifest: &Manifest) -> Result<()> {
    let outdated = manifest.outdated()?;
    if outdated.is_empty() || outdated.len() == Binary::ALL.len() {
        return Ok(());
    }
    for binary in Binary::ALL {
        let original = manifest.original(binary);
        if !outdated.contains(&binary) && original.exists() {
            replace_file(&original, &manifest.installed(binary))?;
        }
    }
    Ok(())
}

/// Release the staging directory of the pending update, then remove
/// `pending.json`. This finishes an installed update and abandons one that
/// is not. Returns whether replaced binaries were kept in `previous/`.
fn discard(root: &Path, manifest: &Manifest) -> Result<bool> {
    let kept = release_staging(root, manifest)?;
    remove_file(&root.join(PENDING))?;
    sync_dir(root)?;
    Ok(kept)
}

// ---------------------------------------------------------------------------
// Files
// ---------------------------------------------------------------------------

fn lock(root: &Path) -> Result<File> {
    let path = root.join(LOCK);
    let file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(&path)
        .with_context(|| format!("opening {}", path.display()))?;
    file.lock()
        .with_context(|| format!("locking {}", path.display()))?;
    Ok(file)
}

/// Remove a staging directory, refusing anything else.
fn remove_staged(root: &Path, dir: &Path) -> Result<()> {
    let owned = dir.parent() == Some(root)
        && dir
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with(STAGED_PREFIX));
    ensure!(owned, "refusing to remove {}", dir.display());
    match fs::remove_dir_all(dir) {
        Err(err) if err.kind() != std::io::ErrorKind::NotFound => {
            Err(err).with_context(|| format!("removing {}", dir.display()))
        }
        _ => Ok(()),
    }
}

/// Remove staging directories left by replaced, cancelled, or interrupted
/// stagings. Callers hold `update.lock`.
fn remove_unreferenced_staging(root: &Path, current: &Path) -> Result<()> {
    for entry in fs::read_dir(root).with_context(|| format!("listing {}", root.display()))? {
        let path = entry?.path();
        let is_staging = path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with(STAGED_PREFIX));
        if is_staging && path != current && path.is_dir() {
            remove_staged(root, &path)?;
        }
    }
    Ok(())
}

/// Remove copies an interrupted activation left beside the installed
/// binaries. Callers hold `update.lock`.
fn remove_install_temps(install_dir: &Path) -> Result<()> {
    for entry in
        fs::read_dir(install_dir).with_context(|| format!("listing {}", install_dir.display()))?
    {
        let entry = entry?;
        if entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.starts_with(TEMP_PREFIX))
        {
            remove_file(&entry.path())?;
        }
    }
    Ok(())
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .with_context(|| format!("parsing {}", path.display())),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err).with_context(|| format!("reading {}", path.display())),
    }
}

/// Durably replace `path` with `value` as JSON.
fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value)?;
    write_atomically(path, &bytes, 0o600)
}

/// Replace `path` with `contents` unless it already holds them, returning
/// whether it changed.
fn write_if_changed(path: &Path, contents: &str) -> Result<bool> {
    if fs::read(path).is_ok_and(|current| current == contents.as_bytes()) {
        return Ok(false);
    }
    write_atomically(path, contents.as_bytes(), 0o644)?;
    Ok(true)
}

fn write_atomically(path: &Path, contents: &[u8], mode: u32) -> Result<()> {
    let dir = path.parent().context("path has no parent directory")?;
    let mut file = NamedTempFile::with_prefix_in(TEMP_PREFIX, dir)
        .with_context(|| format!("creating a file in {}", dir.display()))?;
    file.write_all(contents)?;
    file.as_file()
        .set_permissions(fs::Permissions::from_mode(mode))?;
    file.as_file().sync_all()?;
    persist(file, path)
}

/// Durably replace `target` with a copy of `source`. The rename never
/// truncates an executable a running process may still be using.
fn replace_file(source: &Path, target: &Path) -> Result<()> {
    let dir = target.parent().context("target has no parent directory")?;
    persist(copy_to_temp(source, dir)?, target)
}

/// A synced executable copy of `source` in `dir`, removed when dropped.
fn copy_to_temp(source: &Path, dir: &Path) -> Result<NamedTempFile> {
    let mut input = File::open(source).with_context(|| format!("opening {}", source.display()))?;
    let mut file = NamedTempFile::with_prefix_in(TEMP_PREFIX, dir)
        .with_context(|| format!("creating a file in {}", dir.display()))?;
    std::io::copy(&mut input, &mut file)
        .with_context(|| format!("copying {} to {}", source.display(), dir.display()))?;
    file.as_file()
        .set_permissions(fs::Permissions::from_mode(0o755))?;
    file.as_file().sync_all()?;
    Ok(file)
}

/// Rename a synced temporary file to `target` and sync the directory.
fn persist(file: NamedTempFile, target: &Path) -> Result<()> {
    file.persist(target)
        .with_context(|| format!("replacing {}", target.display()))?;
    sync_dir(target.parent().context("target has no parent directory")?)
}

fn remove_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Err(err) if err.kind() != std::io::ErrorKind::NotFound => {
            Err(err).with_context(|| format!("removing {}", path.display()))
        }
        _ => Ok(()),
    }
}

fn remove_file(path: &Path) -> Result<()> {
    fs::remove_file(path).with_context(|| format!("removing {}", path.display()))
}

fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)
        .and_then(|dir| dir.sync_all())
        .with_context(|| format!("syncing {}", dir.display()))
}

fn digest(path: &Path) -> Result<String> {
    let mut file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut hasher = Sha256::new();
    std::io::copy(&mut file, &mut hasher).with_context(|| format!("hashing {}", path.display()))?;
    Ok(hex::encode(hasher.finalize()))
}

/// The variables the observer needs, taken from the staging environment.
pub fn observer_env() -> BTreeMap<String, String> {
    std::env::vars()
        .filter(|(key, _)| OBSERVER_ENV.contains(&key.as_str()) || key.starts_with("MLQUEUE_"))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;

    const SERVICE: &str = "test.service";

    /// Stands in for systemd and the running daemon. The daemon's "process"
    /// is just its singleton lock: `start` asserts the updater released it.
    struct FakeHost {
        dir: PathBuf,
        kill_mode: std::cell::Cell<&'static str>,
        calls: RefCell<Vec<String>>,
        /// Another daemon holding the singleton lock.
        competitor: RefCell<Option<File>>,
        /// `wait_ready` fails while the installed mlqd reports this.
        broken: std::cell::Cell<Option<&'static str>>,
        /// `stop` overwrites the installed mlqd, like a concurrent install.
        reinstall_on_stop: std::cell::Cell<bool>,
        /// `start` of mlqd 2.0.0 migrates the database schema.
        migrate_on_start: std::cell::Cell<bool>,
        /// The observer predates `mlq subscribe`.
        no_subscribe: std::cell::Cell<bool>,
        /// The running daemon rejects the observer's protocol.
        observer_rejected: std::cell::Cell<bool>,
        /// `systemctl` commands starting with this fail.
        failing_systemctl: std::cell::Cell<Option<&'static str>>,
    }

    impl FakeHost {
        fn install_dir(&self) -> PathBuf {
            self.dir.join("bin")
        }

        fn state_dir(&self) -> PathBuf {
            self.dir.join("state")
        }

        fn calls(&self) -> Vec<String> {
            self.calls.borrow().clone()
        }
    }

    impl Host for FakeHost {
        fn unit_dir(&self) -> Result<PathBuf> {
            Ok(self.dir.join("units"))
        }

        fn systemctl(&self, args: &[&str]) -> Result<()> {
            if let Some(command) = self.failing_systemctl.get() {
                ensure!(args[0] != command, "systemctl {command} failed");
            }
            if args == ["start", SERVICE] && self.competitor.borrow().is_none() {
                File::create(self.state_dir().join("daemon.lock"))?
                    .try_lock()
                    .expect("the updater released daemon.lock before starting the daemon");
            }
            if args == ["start", SERVICE]
                && self.migrate_on_start.get()
                && fs::read_to_string(self.install_dir().join("mlqd"))?.contains("mlqd 2.0.0")
            {
                rusqlite::Connection::open(self.state_dir().join("mlqueue.db"))?
                    .execute_batch("PRAGMA user_version = 99")?;
            }
            if args == ["stop", SERVICE] && self.reinstall_on_stop.get() {
                script(&self.install_dir().join("mlqd"), "mlqd 1.5.0");
            }
            self.calls.borrow_mut().push(args.join(" "));
            Ok(())
        }

        fn show(&self, unit: &str, property: &str) -> Result<String> {
            assert_eq!(unit, SERVICE);
            let mlqd = self.install_dir().join("mlqd");
            Ok(match property {
                "LoadState" => "loaded".into(),
                "KillMode" => self.kill_mode.get().into(),
                "Transient" => "no".into(),
                "MainPID" => "42".into(),
                "ExecStart" => format!("{{ path={} ; argv[]={} }}", mlqd.display(), mlqd.display()),
                other => panic!("unexpected property {other}"),
            })
        }

        fn daemon_status(&self, _: &Manifest) -> Result<DaemonProbe> {
            Ok(DaemonProbe {
                pid: 42,
                db_path: self.state_dir().join("mlqueue.db"),
                socket_path: self.dir.join("mlqd.sock"),
            })
        }

        fn check_subscription(&self, _: &Manifest) -> Result<()> {
            ensure!(
                !self.no_subscribe.get(),
                "the installed mlq cannot subscribe"
            );
            Ok(())
        }

        fn wait_idle(&self, _: &Manifest) -> Result<()> {
            ensure!(!self.observer_rejected.get(), "`mlq subscribe` failed");
            Ok(())
        }

        fn wait_ready(&self, _: &Manifest) -> Result<()> {
            let installed = fs::read_to_string(self.install_dir().join("mlqd"))?;
            match self.broken.get() {
                Some(version) if installed.contains(version) => bail!("{version} crashed"),
                _ => Ok(()),
            }
        }
    }

    struct Fixture {
        _temp: tempfile::TempDir,
        host: FakeHost,
        root: PathBuf,
    }

    fn script(path: &Path, version_line: &str) {
        fs::write(path, format!("#!/bin/sh\necho '{version_line}'\n")).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
    }

    fn fixture() -> Fixture {
        let temp = tempfile::tempdir().unwrap();
        let host = FakeHost {
            dir: temp.path().to_owned(),
            kill_mode: "process".into(),
            calls: RefCell::default(),
            competitor: RefCell::default(),
            broken: Default::default(),
            reinstall_on_stop: Default::default(),
            migrate_on_start: Default::default(),
            no_subscribe: Default::default(),
            observer_rejected: Default::default(),
            failing_systemctl: Default::default(),
        };
        let root = temp.path().join("state/updates");
        fs::create_dir_all(&root).unwrap();
        let conn = rusqlite::Connection::open(host.state_dir().join("mlqueue.db")).unwrap();
        conn.execute_batch(
            "CREATE TABLE jobs (finished_at INTEGER);
             CREATE TABLE run_leases (released_at INTEGER);
             INSERT INTO jobs VALUES (123);",
        )
        .unwrap();
        fs::create_dir(host.install_dir()).unwrap();
        script(&host.install_dir().join("mlqd"), "mlqd 1.0.0");
        script(&host.install_dir().join("mlq"), "mlq 1.0.0");
        let source = temp.path().join("build");
        fs::create_dir(&source).unwrap();
        script(&source.join("mlqd"), "mlqd 2.0.0");
        script(&source.join("mlq"), "mlq 2.0.0");
        script(&temp.path().join("updater"), "mlq 2.0.0");
        Fixture {
            _temp: temp,
            host,
            root,
        }
    }

    impl Fixture {
        fn source(&self) -> PathBuf {
            self.host.dir.join("build")
        }

        fn stage(&self) -> Result<Manifest> {
            stage(
                &self.host,
                StageRequest {
                    root: &self.root,
                    source_dir: &self.source(),
                    service: SERVICE,
                    updater: &self.host.dir.join("updater"),
                    env: BTreeMap::from([("HOME".into(), "/home/test".into())]),
                },
            )
        }

        fn staged(&self) -> Manifest {
            let manifest = self.stage().unwrap();
            self.host.calls.borrow_mut().clear();
            manifest
        }

        fn installed(&self, binary: &str) -> String {
            fs::read_to_string(self.host.install_dir().join(binary)).unwrap()
        }

        fn sql(&self, statement: &str) {
            rusqlite::Connection::open(self.host.state_dir().join("mlqueue.db"))
                .unwrap()
                .execute_batch(statement)
                .unwrap();
        }

        fn write_restart(&self, manifest: &Manifest, schema: Option<i64>) -> Restart {
            let restart = Restart {
                manifest: manifest.clone(),
                db_path: self.host.state_dir().join("mlqueue.db"),
                schema,
            };
            write_json(&self.root.join(RESTART), &restart).unwrap();
            restart
        }

        /// Interrupted between the renames: `mlq` is new, `mlqd` still old,
        /// and `original/` holds what was replaced.
        fn half_replace(&self, manifest: &Manifest) {
            ensure_private_dir(&manifest.staged_dir.join(ORIGINAL)).unwrap();
            for binary in Binary::ALL {
                replace_file(&manifest.installed(binary), &manifest.original(binary)).unwrap();
            }
            replace_file(
                &manifest.candidate(Binary::Mlq),
                &manifest.installed(Binary::Mlq),
            )
            .unwrap();
        }

        fn previous(&self, binary: &str) -> String {
            fs::read_to_string(self.root.join(PREVIOUS).join(binary)).unwrap()
        }

        fn assert_untouched(&self) {
            assert!(self.installed("mlqd").contains("mlqd 1.0.0"));
            assert!(self.installed("mlq").contains("mlq 1.0.0"));
            assert!(self.root.join(PENDING).exists());
            assert!(!self.root.join(RESTART).exists());
        }
    }

    #[test]
    fn terminal_history_is_idle_but_unknown_or_leased_work_is_not() {
        let probe = |json: &str| serde_json::from_str::<IdleProbe>(json).unwrap().is_idle();
        assert!(probe(
            r#"{"activeLeases":0,"admissionBlocked":false,"newField":1,
                "jobs":[{"finishedAt":123,"state":"succeeded"}]}"#
        ));
        assert!(!probe(
            r#"{"activeLeases":0,"admissionBlocked":false,
                "jobs":[{"finishedAt":123},{"state":"new_future_state"}]}"#
        ));
        assert!(!probe(
            r#"{"activeLeases":1,"admissionBlocked":false,"jobs":[]}"#
        ));
        assert!(!probe(
            r#"{"activeLeases":0,"admissionBlocked":true,"jobs":[]}"#
        ));
    }

    #[test]
    fn staging_copies_the_pair_and_starts_the_watcher() {
        let fixture = fixture();
        let manifest = fixture.stage().unwrap();
        assert_eq!(manifest.version, "2.0.0");
        assert_eq!(manifest.install_dir, fixture.host.install_dir());
        assert_eq!(manifest.socket, fixture.host.dir.join("mlqd.sock"));
        assert_eq!(
            read_json::<Manifest>(&fixture.root.join(PENDING)).unwrap(),
            Some(manifest.clone())
        );
        manifest.verify_candidates().unwrap();
        assert!(
            fs::read_to_string(manifest.observer())
                .unwrap()
                .contains("mlq 1.0.0")
        );
        assert!(fixture.root.join(UPDATER).exists());
        fixture.assert_untouched();

        let units = fixture.host.dir.join("units");
        let watch_unit = fs::read_to_string(units.join("test-update.service")).unwrap();
        assert!(watch_unit.contains(&format!(
            "ExecStart=\"{}/mlq\" daemon update watch --root \"{}\"",
            fixture.root.display(),
            fixture.root.display()
        )));
        let path_unit = fs::read_to_string(units.join("test-update.path")).unwrap();
        assert!(path_unit.contains(&format!(
            "PathExists={}/pending.json",
            fixture.root.display()
        )));
        assert_eq!(
            fixture.host.calls(),
            [
                "daemon-reload",
                "try-restart --quiet test-update.service",
                "enable --now test-update.path",
                "restart test-update.path",
            ]
        );
    }

    #[test]
    fn restaging_replaces_the_previous_candidates() {
        let fixture = fixture();
        let first = fixture.staged();
        script(&fixture.source().join("mlqd"), "mlqd 3.0.0");
        script(&fixture.source().join("mlq"), "mlq 3.0.0");
        let second = fixture.stage().unwrap();
        assert_eq!(second.version, "3.0.0");
        assert!(!first.staged_dir.exists());
        assert!(second.staged_dir.exists());
    }

    #[test]
    fn staging_rejects_mismatched_installed_or_unsafe_binaries() {
        let fixture = fixture();
        script(&fixture.source().join("mlq"), "mlq 2.0.1");
        let err = fixture.stage().unwrap_err();
        assert!(err.to_string().contains("different versions"), "{err:#}");

        let err = stage(
            &fixture.host,
            StageRequest {
                root: &fixture.root,
                source_dir: &fixture.host.install_dir(),
                service: SERVICE,
                updater: &fixture.host.dir.join("updater"),
                env: BTreeMap::new(),
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("already installed"), "{err:#}");

        script(&fixture.source().join("mlq"), "mlq 2.0.0");
        fixture.host.kill_mode.set("control-group");
        let err = fixture.stage().unwrap_err();
        assert!(err.to_string().contains("KillMode=process"), "{err:#}");

        // Nothing staged and nothing left behind.
        assert!(!fixture.root.join(PENDING).exists());
        let leftovers: Vec<_> = fs::read_dir(&fixture.root)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .filter(|name| name.to_string_lossy().starts_with(STAGED_PREFIX))
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");
    }

    #[test]
    fn idle_activation_replaces_the_pair_and_keeps_the_previous_one() {
        let fixture = fixture();
        let manifest = fixture.staged();
        assert_eq!(
            activate(&fixture.host, &fixture.root).unwrap(),
            Activation::Activated(manifest.clone())
        );
        assert!(fixture.installed("mlqd").contains("mlqd 2.0.0"));
        assert!(fixture.installed("mlq").contains("mlq 2.0.0"));
        let previous = fixture.root.join(PREVIOUS);
        assert!(
            fs::read_to_string(previous.join("mlqd"))
                .unwrap()
                .contains("mlqd 1.0.0")
        );
        assert!(
            fs::read_to_string(previous.join("mlq"))
                .unwrap()
                .contains("mlq 1.0.0")
        );
        assert!(!fixture.root.join(PENDING).exists());
        assert!(!fixture.root.join(RESTART).exists());
        assert!(!manifest.staged_dir.exists());
        assert_eq!(
            fixture.host.calls(),
            ["stop test.service", "start test.service"]
        );
        assert_eq!(
            activate(&fixture.host, &fixture.root).unwrap(),
            Activation::NothingPending
        );
    }

    #[test]
    fn job_arriving_after_the_idle_snapshot_defers_the_update() {
        let fixture = fixture();
        fixture.staged();
        fixture.sql("INSERT INTO jobs VALUES (NULL)");
        assert_eq!(
            activate(&fixture.host, &fixture.root).unwrap(),
            Activation::Deferred
        );
        fixture.assert_untouched();
        assert_eq!(
            fixture.host.calls(),
            ["stop test.service", "start test.service"]
        );
    }

    #[test]
    fn unreleased_lease_defers_the_update_without_unfinished_jobs() {
        let fixture = fixture();
        fixture.staged();
        fixture.sql("INSERT INTO run_leases VALUES (NULL)");
        assert_eq!(
            activate(&fixture.host, &fixture.root).unwrap(),
            Activation::Deferred
        );
        fixture.assert_untouched();
    }

    #[test]
    fn competing_daemon_lock_prevents_replacement_and_restarts_the_daemon() {
        let fixture = fixture();
        fixture.staged();
        let competitor = File::create(fixture.host.state_dir().join("daemon.lock")).unwrap();
        competitor.lock().unwrap();
        fixture.host.competitor.replace(Some(competitor));
        let err = activate(&fixture.host, &fixture.root).unwrap_err();
        assert!(format!("{err:#}").contains("another mlqd holds"), "{err:#}");
        fixture.assert_untouched();
        assert_eq!(
            fixture.host.calls(),
            ["stop test.service", "start test.service"]
        );
    }

    #[test]
    fn changed_candidate_or_service_aborts_before_stopping() {
        let fixture = fixture();
        let manifest = fixture.staged();
        fs::write(manifest.candidate(Binary::Mlqd), "tampered").unwrap();
        assert!(activate(&fixture.host, &fixture.root).is_err());
        fixture.assert_untouched();
        assert!(fixture.host.calls().is_empty());
    }

    #[test]
    fn watcher_recovers_an_activation_interrupted_after_replacement() {
        let fixture = fixture();
        let manifest = fixture.staged();
        fixture.write_restart(&manifest, Some(0));
        for binary in Binary::ALL {
            replace_file(&manifest.candidate(binary), &manifest.installed(binary)).unwrap();
        }
        watch(&fixture.host, &fixture.root).unwrap();
        assert_eq!(fixture.host.calls(), ["start test.service"]);
        assert!(!fixture.root.join(RESTART).exists());
        assert!(!fixture.root.join(PENDING).exists());
        assert!(!manifest.staged_dir.exists());
    }

    #[test]
    fn watcher_resumes_an_activation_interrupted_before_replacement() {
        let fixture = fixture();
        let manifest = fixture.staged();
        fixture.write_restart(&manifest, None);
        watch(&fixture.host, &fixture.root).unwrap();
        assert_eq!(
            fixture.host.calls(),
            [
                "start test.service",
                "stop test.service",
                "start test.service"
            ]
        );
        assert!(fixture.installed("mlqd").contains("mlqd 2.0.0"));
        assert!(!fixture.root.join(PENDING).exists());
    }

    #[test]
    fn cancel_discards_the_staged_update_and_stops_the_watcher() {
        let fixture = fixture();
        let manifest = fixture.staged();
        assert_eq!(
            cancel(&fixture.host, &fixture.root, false).unwrap(),
            Cancelled {
                pending: Some(manifest.clone()),
                ..Cancelled::default()
            }
        );
        assert!(!fixture.root.join(PENDING).exists());
        assert!(!manifest.staged_dir.exists());
        assert_eq!(fixture.host.calls(), ["stop --quiet test-update.service"]);
        assert_eq!(
            cancel(&fixture.host, &fixture.root, false).unwrap(),
            Cancelled::default()
        );
    }

    #[test]
    fn new_daemon_that_never_comes_up_is_rolled_back_and_not_retried() {
        let fixture = fixture();
        let manifest = fixture.staged();
        fixture.host.broken.set(Some("mlqd 2.0.0"));
        let Activation::Failed(failed) = activate(&fixture.host, &fixture.root).unwrap() else {
            panic!("expected a failed activation");
        };
        assert_eq!(failed.rollback, Rollback::Restored);
        assert!(failed.reason.contains("crashed"), "{}", failed.reason);
        assert!(fixture.installed("mlqd").contains("mlqd 1.0.0"));
        assert!(fixture.installed("mlq").contains("mlq 1.0.0"));
        assert!(!fixture.root.join(PENDING).exists());
        assert!(!fixture.root.join(RESTART).exists());
        assert!(!manifest.staged_dir.exists());
        assert_eq!(status(&fixture.root).unwrap().failed, Some(failed));
        assert_eq!(
            fixture.host.calls(),
            [
                "stop test.service",
                "start test.service",
                "stop test.service",
                "start test.service"
            ]
        );
        // The watcher finds nothing left to activate.
        watch(&fixture.host, &fixture.root).unwrap();
        assert_eq!(fixture.host.calls().len(), 4);
        // Staging again clears the report.
        fixture.host.broken.set(None);
        fixture.stage().unwrap();
        assert_eq!(status(&fixture.root).unwrap().failed, None);
    }

    #[test]
    fn rollback_is_refused_once_the_new_daemon_migrated_the_database() {
        let fixture = fixture();
        fixture.staged();
        fixture.host.broken.set(Some("mlqd 2.0.0"));
        fixture.host.migrate_on_start.set(true);
        let Activation::Failed(failed) = activate(&fixture.host, &fixture.root).unwrap() else {
            panic!("expected a failed activation");
        };
        assert_eq!(failed.rollback, Rollback::Refused { from: 0, to: 99 });
        let described = failed.describe();
        assert!(described.contains("needs an operator"), "{described}");
        assert!(
            described.contains(&fixture.root.join(PREVIOUS).display().to_string()),
            "{described}"
        );
        assert!(fixture.installed("mlqd").contains("mlqd 2.0.0"));
        assert!(fixture.installed("mlq").contains("mlq 2.0.0"));
        // The originals are kept for the operator.
        assert!(fixture.previous("mlqd").contains("mlqd 1.0.0"));
        assert!(fixture.previous("mlq").contains("mlq 1.0.0"));
        // Terminal: nothing is retried or left for recovery.
        assert!(!fixture.root.join(RESTART).exists());
        assert!(!fixture.root.join(PENDING).exists());
        assert_eq!(status(&fixture.root).unwrap().failed, Some(failed.clone()));

        // Killed after recording the outcome but before cleaning up: the next
        // watcher only cleans up.
        fixture.write_restart(&failed.manifest, Some(0));
        fixture.host.calls.borrow_mut().clear();
        watch(&fixture.host, &fixture.root).unwrap();
        assert!(!fixture.root.join(RESTART).exists());
        assert!(
            fixture.host.calls().is_empty(),
            "{:?}",
            fixture.host.calls()
        );
        assert!(fixture.previous("mlqd").contains("mlqd 1.0.0"));
    }

    #[test]
    fn interrupted_rollback_is_finished_by_the_next_watcher() {
        let fixture = fixture();
        fixture.staged();
        // Even the original daemon fails, so the rollback cannot finish.
        fixture.host.broken.set(Some("mlqd"));
        assert!(activate(&fixture.host, &fixture.root).is_err());
        assert!(fixture.installed("mlqd").contains("mlqd 1.0.0"));
        assert!(fixture.root.join(RESTART).exists());
        assert!(!fixture.root.join(PENDING).exists());
        let failed = status(&fixture.root).unwrap().failed.unwrap();
        assert_eq!(failed.rollback, Rollback::InProgress);

        assert!(watch(&fixture.host, &fixture.root).is_err());
        let report = status(&fixture.root).unwrap();
        assert!(report.watcher_error.unwrap().contains("crashed"));
        assert!(cancel(&fixture.host, &fixture.root, false).is_err());
        assert!(uninstall_watcher(&fixture.host, &fixture.root, SERVICE).is_err());

        fixture.host.broken.set(None);
        watch(&fixture.host, &fixture.root).unwrap();
        let report = status(&fixture.root).unwrap();
        assert_eq!(report.failed.unwrap().rollback, Rollback::Restored);
        assert!(report.interrupted.is_none() && report.watcher_error.is_none());
    }

    #[test]
    fn watcher_killed_after_the_swap_rolls_back_a_broken_daemon() {
        let fixture = fixture();
        let manifest = fixture.staged();
        let mut restart = fixture.write_restart(&manifest, None);
        Swap::prepare(&manifest)
            .unwrap()
            .commit_if_idle(&fixture.root, &mut restart)
            .unwrap();
        assert!(fixture.installed("mlqd").contains("mlqd 2.0.0"));
        // The watcher died here; its replacement finds a broken daemon.
        fixture.host.broken.set(Some("mlqd 2.0.0"));
        watch(&fixture.host, &fixture.root).unwrap();
        assert!(fixture.installed("mlqd").contains("mlqd 1.0.0"));
        assert!(fixture.installed("mlq").contains("mlq 1.0.0"));
        let report = status(&fixture.root).unwrap();
        assert_eq!(report.failed.unwrap().rollback, Rollback::Restored);
        assert!(report.pending.is_none() && report.interrupted.is_none());
    }

    #[test]
    fn forced_cancel_abandons_an_interrupted_activation() {
        let fixture = fixture();
        let manifest = fixture.staged();
        let restart = fixture.write_restart(&manifest, None);
        assert!(cancel(&fixture.host, &fixture.root, false).is_err());
        assert_eq!(
            cancel(&fixture.host, &fixture.root, true).unwrap(),
            Cancelled {
                pending: Some(manifest),
                interrupted: Some(restart),
                ..Cancelled::default()
            }
        );
        let report = status(&fixture.root).unwrap();
        assert!(report.pending.is_none() && report.interrupted.is_none());
    }

    #[test]
    fn forced_cancel_after_the_swap_keeps_the_replaced_pair() {
        let fixture = fixture();
        let manifest = fixture.staged();
        let mut restart = fixture.write_restart(&manifest, None);
        Swap::prepare(&manifest)
            .unwrap()
            .commit_if_idle(&fixture.root, &mut restart)
            .unwrap();
        let cancelled = cancel(&fixture.host, &fixture.root, true).unwrap();
        assert!(cancelled.pending_installed);
        assert_eq!(cancelled.kept_originals, Some(fixture.root.join(PREVIOUS)));
        assert!(fixture.installed("mlqd").contains("mlqd 2.0.0"));
        assert!(fixture.previous("mlqd").contains("mlqd 1.0.0"));
        assert!(fixture.previous("mlq").contains("mlq 1.0.0"));
    }

    #[test]
    fn forced_cancel_mid_rollback_keeps_a_consistent_pair() {
        let fixture = fixture();
        fixture.staged();
        // Even the original daemon fails, so the rollback cannot finish.
        fixture.host.broken.set(Some("mlqd"));
        assert!(activate(&fixture.host, &fixture.root).is_err());
        let cancelled = cancel(&fixture.host, &fixture.root, true).unwrap();
        assert!(cancelled.pending.is_none() && cancelled.failed.is_some());
        assert!(fixture.installed("mlqd").contains("mlqd 1.0.0"));
        assert!(fixture.installed("mlq").contains("mlq 1.0.0"));
        let leftovers: Vec<_> = fs::read_dir(&fixture.root)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .filter(|name| name.to_string_lossy().starts_with(STAGED_PREFIX))
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");
    }

    #[test]
    fn recovery_from_a_crash_between_the_renames_does_not_blame_the_candidate() {
        let fixture = fixture();
        let manifest = fixture.staged();
        fixture.half_replace(&manifest);
        fixture.write_restart(&manifest, Some(0));
        // The original daemon fails to come back for its own reasons.
        fixture.host.broken.set(Some("mlqd 1.0.0"));
        assert!(watch(&fixture.host, &fixture.root).is_err());
        let report = status(&fixture.root).unwrap();
        assert!(report.failed.is_none());
        assert_eq!(report.pending, Some(manifest));
        // The original mlq is back, matching the running mlqd.
        assert!(fixture.installed("mlq").contains("mlq 1.0.0"));

        fixture.host.broken.set(None);
        watch(&fixture.host, &fixture.root).unwrap();
        assert!(fixture.installed("mlqd").contains("mlqd 2.0.0"));
        assert!(fixture.previous("mlq").contains("mlq 1.0.0"));
    }

    #[test]
    fn staging_refuses_what_activation_could_never_do() {
        let fixture = fixture();
        fixture.host.no_subscribe.set(true);
        let err = fixture.stage().unwrap_err();
        assert!(err.to_string().contains("cannot subscribe"), "{err:#}");
        fixture.host.no_subscribe.set(false);

        let install_dir = fixture.host.install_dir();
        fs::set_permissions(&install_dir, fs::Permissions::from_mode(0o555)).unwrap();
        let err = fixture.stage().unwrap_err();
        fs::set_permissions(&install_dir, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(err.to_string().contains("cannot be replaced"), "{err:#}");

        let err = stage(
            &fixture.host,
            StageRequest {
                root: &fixture.root,
                source_dir: &fixture.source(),
                service: "x.service\nExecStart=/bin/false",
                updater: &fixture.host.dir.join("updater"),
                env: BTreeMap::new(),
            },
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("not a systemd service name"),
            "{err:#}"
        );

        let err = stage(
            &fixture.host,
            StageRequest {
                root: &fixture.host.dir.join("100%"),
                source_dir: &fixture.source(),
                service: SERVICE,
                updater: &fixture.host.dir.join("updater"),
                env: BTreeMap::new(),
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("unsupported character"), "{err:#}");
        assert!(!fixture.host.dir.join("100%").exists());

        // Without a watcher nothing would activate it.
        fixture.host.failing_systemctl.set(Some("daemon-reload"));
        let err = fixture.stage().unwrap_err();
        assert!(err.to_string().contains("daemon-reload"), "{err:#}");
        assert!(!fixture.root.join(PENDING).exists());
    }

    #[test]
    fn installation_changed_during_shutdown_is_not_overwritten() {
        let fixture = fixture();
        fixture.staged();
        fixture.host.reinstall_on_stop.set(true);
        let err = activate(&fixture.host, &fixture.root).unwrap_err();
        assert!(err.to_string().contains("changed"), "{err:#}");
        assert!(fixture.installed("mlqd").contains("mlqd 1.5.0"));
        assert!(fixture.installed("mlq").contains("mlq 1.0.0"));
        assert!(fixture.root.join(PENDING).exists());
        assert!(!fixture.root.join(RESTART).exists());
        assert_eq!(
            fixture.host.calls(),
            ["stop test.service", "start test.service"]
        );
        // The prepared copies are gone.
        let names: Vec<_> = fs::read_dir(fixture.host.install_dir())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(names.len(), 2, "{names:?}");
    }

    #[test]
    fn activation_finishes_a_pair_left_half_replaced() {
        let fixture = fixture();
        let manifest = fixture.staged();
        fixture.half_replace(&manifest);
        fs::write(
            fixture.host.install_dir().join(format!("{TEMP_PREFIX}abc")),
            "leftover",
        )
        .unwrap();
        assert_eq!(
            activate(&fixture.host, &fixture.root).unwrap(),
            Activation::Activated(manifest)
        );
        assert!(fixture.installed("mlqd").contains("mlqd 2.0.0"));
        assert_eq!(fs::read_dir(fixture.host.install_dir()).unwrap().count(), 2);
        assert!(fixture.previous("mlq").contains("mlq 1.0.0"));
        assert!(fixture.previous("mlqd").contains("mlqd 1.0.0"));
    }

    #[test]
    fn discarding_a_half_replaced_pair_restores_the_original_mlq() {
        let fixture = fixture();
        let manifest = fixture.staged();
        fixture.half_replace(&manifest);
        cancel(&fixture.host, &fixture.root, false).unwrap();
        assert!(fixture.installed("mlq").contains("mlq 1.0.0"));
        assert!(fixture.installed("mlqd").contains("mlqd 1.0.0"));

        // Restaging likewise backs up and observes the original mlq.
        let manifest = fixture.staged();
        fixture.half_replace(&manifest);
        let restaged = fixture.stage().unwrap();
        assert!(fixture.installed("mlq").contains("mlq 1.0.0"));
        assert!(
            fs::read_to_string(restaged.observer())
                .unwrap()
                .contains("mlq 1.0.0")
        );
    }

    #[test]
    fn installed_update_left_pending_is_finished_without_the_observer() {
        let fixture = fixture();
        let manifest = fixture.staged();
        // The watcher died after confirming the new daemon, before finishing;
        // the new daemon no longer speaks the observer's protocol.
        fixture.half_replace(&manifest);
        replace_file(
            &manifest.candidate(Binary::Mlqd),
            &manifest.installed(Binary::Mlqd),
        )
        .unwrap();
        fixture.host.observer_rejected.set(true);
        watch(&fixture.host, &fixture.root).unwrap();
        let report = status(&fixture.root).unwrap();
        assert!(report.pending.is_none() && report.watcher_error.is_none());
        assert!(!manifest.staged_dir.exists());
        assert!(fixture.previous("mlqd").contains("mlqd 1.0.0"));
        assert!(
            fixture.host.calls().is_empty(),
            "{:?}",
            fixture.host.calls()
        );
    }

    #[test]
    fn staging_sweeps_abandoned_staging_directories() {
        let fixture = fixture();
        let abandoned = fixture.root.join(format!("{STAGED_PREFIX}abandoned"));
        fs::create_dir(&abandoned).unwrap();
        let manifest = fixture.stage().unwrap();
        assert!(!abandoned.exists());
        assert!(manifest.staged_dir.exists());
    }

    #[test]
    fn uninstall_discards_the_update_and_removes_the_units() {
        let fixture = fixture();
        let manifest = fixture.staged();
        fs::write(fixture.root.join(WATCHER_ERROR), "subscription failed").unwrap();
        assert!(uninstall_watcher(&fixture.host, &fixture.root, SERVICE).unwrap());
        assert!(!fixture.root.join(PENDING).exists());
        assert!(status(&fixture.root).unwrap().watcher_error.is_none());
        assert!(!manifest.staged_dir.exists());
        assert!(!fixture.host.dir.join("units/test-update.path").exists());
        assert!(!fixture.host.dir.join("units/test-update.service").exists());
        assert!(!uninstall_watcher(&fixture.host, &fixture.root, SERVICE).unwrap());

        write_json(&fixture.root.join(RESTART), &manifest).unwrap();
        assert!(uninstall_watcher(&fixture.host, &fixture.root, SERVICE).is_err());
    }

    #[test]
    fn watcher_units_follow_the_service_name() {
        assert_eq!(
            watcher_units("mlqd.service"),
            ("mlqd-update.path".into(), "mlqd-update.service".into())
        );
    }
}
