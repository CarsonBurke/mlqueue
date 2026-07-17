//! XDG base-directory resolution with environment overrides for tests.

use std::env;
use std::fs;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

use crate::domain::AttemptId;

#[derive(Debug, Clone)]
pub struct Paths {
    pub runtime_dir: PathBuf,
    pub state_dir: PathBuf,
    pub config_file: PathBuf,
}

impl Paths {
    /// Resolve directories from `MLQUEUE_*` overrides, then XDG variables,
    /// then home-relative defaults.
    pub fn resolve() -> Result<Self> {
        let home = || -> Result<PathBuf> {
            env::var_os("HOME")
                .map(PathBuf::from)
                .context("HOME is not set and no explicit mlqueue directory overrides were given")
        };

        let state_dir = match env::var_os("MLQUEUE_STATE_DIR") {
            Some(dir) => PathBuf::from(dir),
            None => match env::var_os("XDG_STATE_HOME") {
                Some(dir) if !dir.is_empty() => PathBuf::from(dir).join("mlqueue"),
                _ => home()?.join(".local/state/mlqueue"),
            },
        };

        let runtime_dir = match env::var_os("MLQUEUE_RUNTIME_DIR") {
            Some(dir) => PathBuf::from(dir),
            None => match env::var_os("XDG_RUNTIME_DIR") {
                Some(dir) if !dir.is_empty() => PathBuf::from(dir).join("mlqueue"),
                // No per-user runtime dir: fall back to a subdirectory of the
                // state dir so a socket path always exists.
                _ => state_dir.join("runtime"),
            },
        };

        let config_file = match env::var_os("MLQUEUE_CONFIG_FILE") {
            Some(file) => PathBuf::from(file),
            None => match env::var_os("XDG_CONFIG_HOME") {
                Some(dir) if !dir.is_empty() => PathBuf::from(dir).join("mlqueue/config.toml"),
                _ => home()?.join(".config/mlqueue/config.toml"),
            },
        };

        Ok(Self { runtime_dir, state_dir, config_file })
    }

    pub fn socket(&self) -> PathBuf {
        self.runtime_dir.join("mlqd.sock")
    }

    pub fn db(&self) -> PathBuf {
        self.state_dir.join("mlqueue.db")
    }

    /// The singleton lock lives in the stable state directory, not the
    /// volatile runtime directory, so recreation of the runtime directory
    /// cannot yield two daemon locks.
    pub fn daemon_lock(&self) -> PathBuf {
        self.state_dir.join("daemon.lock")
    }

    pub fn follow_tts_lock(&self) -> PathBuf {
        self.state_dir.join("follow-tts.lock")
    }

    pub fn attempts_dir(&self) -> PathBuf {
        self.state_dir.join("attempts")
    }

    pub fn attempt_dir(&self, attempt: AttemptId) -> PathBuf {
        self.attempts_dir().join(attempt.to_string())
    }

    /// Create the runtime, state, and attempts directories with mode 0700 and
    /// verify pre-existing ones are private.
    pub fn ensure_dirs(&self) -> Result<()> {
        for dir in [&self.runtime_dir, &self.state_dir, &self.attempts_dir()] {
            ensure_private_dir(dir)?;
        }
        Ok(())
    }
}

pub fn ensure_private_dir(dir: &Path) -> Result<()> {
    if !dir.exists() {
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir)
            .with_context(|| format!("creating {}", dir.display()))?;
    }
    let meta = fs::metadata(dir).with_context(|| format!("inspecting {}", dir.display()))?;
    if !meta.is_dir() {
        bail!("{} exists but is not a directory", dir.display());
    }
    let mode = meta.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        fs::set_permissions(dir, fs::Permissions::from_mode(0o700))
            .with_context(|| format!("tightening permissions on {}", dir.display()))?;
    }
    Ok(())
}
