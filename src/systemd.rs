//! `systemctl --user` helpers shared by service installation and the staged
//! daemon updater.

use std::path::PathBuf;
use std::process::Command;

use anyhow::{Context, Result, bail};

/// The user unit `mlq daemon install` writes and the updater manages.
pub const DAEMON_SERVICE: &str = "mlqd.service";

/// `$XDG_CONFIG_HOME/systemd/user`, falling back to `~/.config/systemd/user`.
pub fn user_unit_dir() -> Result<PathBuf> {
    let config_home = match std::env::var_os("XDG_CONFIG_HOME").filter(|v| !v.is_empty()) {
        Some(xdg) => PathBuf::from(xdg),
        None => {
            let home = std::env::var_os("HOME")
                .filter(|v| !v.is_empty())
                .context("HOME is not set; cannot locate the systemd user unit directory")?;
            PathBuf::from(home).join(".config")
        }
    };
    Ok(config_home.join("systemd/user"))
}

/// Run `systemctl --user ARGS`, letting its output reach the terminal or
/// journal.
pub fn systemctl(args: &[&str]) -> Result<()> {
    let status = Command::new("systemctl")
        .arg("--user")
        .args(args)
        .status()
        .context("running systemctl (is this a systemd system?)")?;
    if !status.success() {
        bail!("`systemctl --user {}` failed", args.join(" "));
    }
    Ok(())
}

/// One unit property, as `systemctl --user show UNIT -p NAME --value` prints
/// it.
pub fn show(unit: &str, property: &str) -> Result<String> {
    let output = Command::new("systemctl")
        .args(["--user", "show", unit, "-p", property, "--value"])
        .output()
        .context("running systemctl (is this a systemd system?)")?;
    if !output.status.success() {
        bail!(
            "`systemctl --user show {unit} -p {property}` failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let value = String::from_utf8(output.stdout)
        .with_context(|| format!("{unit} {property} is not UTF-8"))?;
    Ok(value.trim_end_matches('\n').to_owned())
}

/// The executable of a unit's only `ExecStart=` line, from its `show` value,
/// e.g. `{ path=/usr/bin/mlqd ; argv[]=/usr/bin/mlqd ; ... }`.
pub fn exec_start_path(value: &str) -> Result<PathBuf> {
    let mut paths = value
        .match_indices("path=")
        .filter(|(at, _)| *at == 0 || value[..*at].ends_with(' '))
        .map(|(at, prefix)| {
            let rest = &value[at + prefix.len()..];
            rest.split_once(" ;").map_or(rest, |(path, _)| path)
        });
    match (paths.next(), paths.next()) {
        (Some(path), None) if !path.is_empty() => Ok(PathBuf::from(path)),
        (None, _) => bail!("unit has no ExecStart= command"),
        _ => bail!("unit must have exactly one ExecStart= command"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exec_start_path_reads_the_single_command() {
        let value = "{ path=/home/u/.cargo/bin/mlqd ; argv[]=/home/u/.cargo/bin/mlqd ; \
                     ignore_errors=no ; start_time=[n/a] ; stop_time=[n/a] ; pid=0 ; \
                     code=(null) ; status=0/0 }";
        assert_eq!(
            exec_start_path(value).unwrap(),
            PathBuf::from("/home/u/.cargo/bin/mlqd")
        );
    }

    #[test]
    fn exec_start_path_rejects_missing_or_multiple_commands() {
        assert!(exec_start_path("").is_err());
        let two = "{ path=/a ; argv[]=/a } { path=/b ; argv[]=/b }";
        assert!(exec_start_path(two).is_err());
    }
}
