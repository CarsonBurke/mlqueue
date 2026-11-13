//! Process identity beyond raw PIDs: boot ID plus `/proc` start times, and
//! process-group membership scans used to prove a command group empty.

use std::fs;
use std::io;
use std::sync::OnceLock;

/// Stable identifier for the current boot; PIDs and start times are only
/// meaningful within one boot. A running process cannot outlive its boot, so
/// the first successful read is cached: liveness checks run every poll.
pub fn boot_id() -> io::Result<&'static str> {
    static BOOT_ID: OnceLock<String> = OnceLock::new();
    if let Some(id) = BOOT_ID.get() {
        return Ok(id);
    }
    let id = fs::read_to_string("/proc/sys/kernel/random/boot_id")?
        .trim()
        .to_string();
    Ok(BOOT_ID.get_or_init(|| id))
}

#[derive(Debug, Clone, Copy)]
pub struct ProcStat {
    pub pid: i32,
    pub pgrp: i32,
    /// Kernel `starttime` in clock ticks since boot (field 22).
    pub start_time: i64,
    /// Exited but not yet reaped (state `Z`, or `X` while being torn down).
    pub zombie: bool,
}

/// Parse `/proc/<pid>/stat`, robust to whitespace and parentheses in comm.
pub fn proc_stat(pid: i32) -> Option<ProcStat> {
    let raw = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    parse_stat(pid, &raw)
}

fn parse_stat(pid: i32, raw: &str) -> Option<ProcStat> {
    // comm may contain spaces and parentheses; everything after the last ')'
    // is fixed-format. Field numbering: state=3, ppid=4, pgrp=5, ...,
    // starttime=22; after the comm the k-th field is index k-3.
    let rest = &raw[raw.rfind(')')? + 1..];
    let fields: Vec<&str> = rest.split_whitespace().collect();
    Some(ProcStat {
        pid,
        zombie: matches!(*fields.first()?, "Z" | "X"),
        pgrp: fields.get(2)?.parse().ok()?,
        start_time: fields.get(19)?.parse().ok()?,
    })
}

/// Whether a recorded (pid, start_time, boot_id) triple still names a live
/// process. A recycled PID fails the start-time or boot comparison, and an
/// exited one awaiting reaping (a zombie, whose reaper may itself be hung) is
/// not alive.
pub fn identity_alive(pid: i32, start_time: i64, recorded_boot_id: &str) -> bool {
    match boot_id() {
        Ok(current) if current == recorded_boot_id => {}
        _ => return false,
    }
    matches!(proc_stat(pid), Some(stat) if stat.start_time == start_time && !stat.zombie)
}

/// PIDs of the live processes in the given process group, from a full
/// `/proc` scan. Zombies are left out: an exited member runs no code and holds
/// no memory, files or devices, so a group of zombies has drained. Their
/// reaping is up to their parent, which after an orphaning is the subreaper or
/// init; waiting on it would let a hung init hold a lease forever. Leaving them
/// out keeps the recycling guarantee below, since the kernel keeps a pgid
/// allocated while any member, zombie or not, still belongs to the group.
pub fn group_members(pgid: i32) -> Vec<i32> {
    let Ok(entries) = fs::read_dir("/proc") else {
        return Vec::new();
    };
    entries
        .filter_map(|entry| entry.ok()?.file_name().to_str()?.parse::<i32>().ok())
        .filter_map(proc_stat)
        .filter(|stat| stat.pgrp == pgid && !stat.zombie)
        .map(|stat| stat.pid)
        .collect()
}

pub fn group_empty(pgid: i32) -> bool {
    group_members(pgid).is_empty()
}

/// Whether the group has no members besides `excluded_pid`. Used by the
/// runner while it deliberately keeps the reapable group leader as a zombie:
/// the zombie pins the PID (so the numeric pgid cannot be recycled by an
/// unrelated process) but still shows up in the scan and must be ignored.
pub fn group_empty_except(pgid: i32, excluded_pid: i32) -> bool {
    group_members(pgid).iter().all(|&pid| pid == excluded_pid)
}

/// Whether the recorded command group can still contain live processes. A
/// different boot ID proves it empty without a scan.
///
/// `leader_start_time` is the recorded start time of the group leader (whose
/// pid equals the pgid). The kernel keeps a PID allocated while any process
/// still belongs to its group, so the number can only be recycled after the
/// original group has fully drained: finding a process under that pid with a
/// *different* start time therefore proves the recorded group empty, and
/// stops the daemon from signalling (or waiting on) an unrelated group that
/// reused the number. A foreign group whose recycled leader has itself since
/// exited is indistinguishable from an orphaned original group; that residual
/// case errs on the safe side (treated as possibly alive, lease retained).
pub fn group_possibly_alive(
    pgid: i32,
    recorded_boot_id: &str,
    leader_start_time: Option<i64>,
) -> bool {
    match boot_id() {
        Ok(current) if current != recorded_boot_id => return false,
        _ => {}
    }
    if let (Some(recorded), Some(stat)) = (leader_start_time, proc_stat(pgid))
        && stat.start_time != recorded
    {
        return false;
    }
    !group_empty(pgid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_stat_with_hostile_comm() {
        let raw = "1234 (a b) c) R 1 1234 1234 0 -1 4194304 100 0 0 0 0 0 0 0 20 0 1 0 5555 0 0";
        let stat = parse_stat(1234, raw).unwrap();
        assert_eq!(stat.pgrp, 1234);
        assert_eq!(stat.start_time, 5555);
        assert!(!stat.zombie);
        let raw = "1234 (a Z b) Z 1 1234 1234 0 -1 4194304 100 0 0 0 0 0 0 0 20 0 1 0 5555 0 0";
        assert!(parse_stat(1234, raw).unwrap().zombie);
    }

    #[test]
    fn an_unreaped_process_is_not_alive_and_does_not_keep_its_group_alive() {
        use std::os::unix::process::CommandExt;
        let mut child = std::process::Command::new("true")
            .process_group(0)
            .spawn()
            .unwrap();
        let pid = child.id() as i32;
        // Unreaped until `wait` below: it turns into a zombie in its own group.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !proc_stat(pid).unwrap().zombie {
            assert!(std::time::Instant::now() < deadline, "child never exited");
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert_eq!(proc_stat(pid).unwrap().pgrp, pid);
        assert!(group_empty(pid));
        let boot = boot_id().unwrap();
        assert!(!identity_alive(
            pid,
            proc_stat(pid).unwrap().start_time,
            boot
        ));
        child.wait().unwrap();
    }

    #[test]
    fn self_is_visible_and_alive() {
        let pid = std::process::id() as i32;
        let stat = proc_stat(pid).unwrap();
        assert_eq!(stat.pid, pid);
        let boot = boot_id().unwrap();
        assert!(identity_alive(pid, stat.start_time, boot));
        assert!(!identity_alive(pid, stat.start_time + 1, boot));
        assert!(!identity_alive(pid, stat.start_time, "not-a-boot-id"));
    }

    #[test]
    fn own_group_is_not_empty() {
        let pid = std::process::id() as i32;
        let pgid = proc_stat(pid).unwrap().pgrp;
        assert!(group_members(pgid).contains(&pid));
        assert!(!group_empty(pgid));
    }
}
