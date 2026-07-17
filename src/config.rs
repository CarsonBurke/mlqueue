//! Versioned TOML configuration with conservative defaults.

use std::fs;
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

pub const CONFIG_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub version: u32,
    /// SIGTERM-to-SIGKILL grace period for forced cancellation.
    pub cancel_grace_ms: u64,
    /// How long a spawned runner waits for launch authorization before it
    /// gives up and records an authorization-timeout failure.
    pub runner_start_wait_ms: u64,
    /// How long the daemon waits for a spawned runner to publish its identity
    /// before declaring a never-authorized attempt dead.
    pub runner_identity_grace_ms: u64,
    /// Runner supervision poll interval.
    pub runner_poll_ms: u64,
    /// Daemon reconcile/schedule tick interval.
    pub tick_interval_ms: u64,
    /// Maximum concurrent client connections.
    pub max_connections: usize,
    /// Hard frame limit checked before allocation.
    pub max_frame_bytes: u32,
    /// Idle client connections are closed after this long.
    pub idle_timeout_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            version: CONFIG_VERSION,
            cancel_grace_ms: 30_000,
            runner_start_wait_ms: 600_000,
            runner_identity_grace_ms: 15_000,
            runner_poll_ms: 100,
            tick_interval_ms: 150,
            max_connections: 64,
            max_frame_bytes: 1 << 20,
            idle_timeout_ms: 60_000,
        }
    }
}

impl Config {
    /// A missing file yields defaults; a present file must parse exactly
    /// (unknown fields are rejected) and match the supported version.
    pub fn load(path: &Path) -> Result<Self> {
        let config = match fs::read_to_string(path) {
            Ok(text) => toml::from_str::<Config>(&text)
                .with_context(|| format!("parsing {}", path.display()))?,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Config::default(),
            Err(err) => return Err(err).with_context(|| format!("reading {}", path.display())),
        };
        if config.version != CONFIG_VERSION {
            bail!(
                "unsupported config version {} in {} (supported: {})",
                config.version,
                path.display(),
                CONFIG_VERSION
            );
        }
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_file_is_default() {
        let config = Config::load(Path::new("/nonexistent/mlqueue/config.toml")).unwrap();
        assert_eq!(config.version, CONFIG_VERSION);
        assert_eq!(config.cancel_grace_ms, 30_000);
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        fs::write(&path, "version = 1\nnot_a_real_field = true\n").unwrap();
        assert!(Config::load(&path).is_err());
    }

    #[test]
    fn wrong_version_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        fs::write(&path, "version = 999\n").unwrap();
        assert!(Config::load(&path).is_err());
    }
}
