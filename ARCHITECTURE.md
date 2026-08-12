# Architecture

mlqueue is a **coordination service** for local ML commands on a single Linux
user account. One durable daemon (`mlqd`) is the only admission authority;
clients (`mlq`) talk to it over a Unix socket. It is not a cluster scheduler,
GPU allocator, or security boundary.

Operator-facing usage lives in [README.md](README.md). This document describes
how the system is built and the invariants it maintains.

## Goals

- Independently acting agents and shells share one machine-wide queue.
- Concurrency safety is expressed by the submitter as a single integer per
  job, not inferred from hardware.
- Daemon restart and upgrade never kill workers.
- Mutations are durable and safely retryable.

## Non-goals

The daemon does **not**:

- Discover GPUs or parse NVIDIA/ROCm telemetry
- Meter or reserve VRAM, RAM, CPU, or wall time
- Infer workload type from commands or repositories
- Preempt, suspend, or checkpoint healthy work
- Detect unmanaged processes or foreign GPU use
- Accept remote submission or multi-user isolation

Bypassing `mlq` (`python train.py`, `nohup`, detached terminals, repo-local
workers) is invisible to the model. Submitted workloads and their descendants
must stay in the runner’s process group (no `setsid` / daemonizing).

## Components

One Rust package, two public binaries, one internal runner role:

| Binary / role | Responsibility |
|---|---|
| `mlq` | CLI, socket client, local log tailing |
| `mlqd` | Singleton daemon, protocol, scheduling, recovery |
| `mlqd __runner` | Detached per-attempt process supervisor |

### Source layout

```text
src/bin/mlq.rs          CLI entry
src/bin/mlqd.rs         daemon / runner entry
src/lib.rs
src/cli.rs              clap surface and human/JSON formatting
src/client.rs           framed client
src/config.rs           TOML config
src/paths.rs            XDG paths (overridable in tests)
src/domain.rs           IDs, states, scheduler semantics version
src/protocol.rs         wire ops, public camelCase views, framing
src/db.rs               migrations, repositories, operations, events
src/scheduler.rs        pure admission + backfill policy (no I/O)
src/daemon/
  mod.rs                listen loop, singleton lock, runner adopt
  coordinator.rs        serialized mutations and schedule passes
  views.rs              status/job eligibility explanations
src/process/
  runner.rs             spawn, handshake, cancel, result
  identity.rs           PID/pgid + boot/start-time identity
  artifacts.rs          exclusive/fsync artifact publication
```

Dependencies of note: Tokio (socket/orchestration), Clap, Serde/JSON,
`rusqlite` (bundled SQLite), `libc` for `setsid`/`setpgid`/`killpg`/`flock`,
`tracing`.

**Serialization rule:** all scheduling and DB writes go through one
coordinator path. Handlers never launch work themselves. Transactions and
mutexes are never held across socket I/O, filesystem waits, or process spawn.

## Admission model

Each job declares:

```text
maxParallelRuns  positive integer, default 1   CLI: --max-parallel-runs
priority         signed i64, default 0         CLI: --priority
timeLimitMs      optional positive integer     CLI: --time-limit DURATION
```

JSON uses camelCase (`maxParallelRuns`). Rust/SQLite use snake_case.

### Compatibility formula

Let `R` be attempts that currently hold run leases. Candidate `j` may acquire
a lease iff:

```text
|R| + 1  <=  min(j.maxParallelRuns,  every r.maxParallelRuns for r in R)
```

The limit is **global and symmetric**: every running declaration must remain
true after admission. It does not count “only copies of the same command.”
Default `1` serializes unknown work.

One integer cannot express pair-specific compatibility. Agents must pick the
value safe next to *arbitrary* other managed jobs (`1` when type-dependent).

### Queue rank

Eligible jobs (not held, retry delay elapsed) are ordered by:

1. **priority** descending  
2. **submission sequence** ascending (job id / FIFO)

Priority never preempts an active lease. Strict priority can starve lower
priorities under sustained higher-priority arrivals.

### Protection and backfill (semantics v3)

When the highest-ranked eligible job cannot start only because of active
limits, it becomes the **protected** job. One global reservation is persisted.

1. **Open window** — while any *initial blocker* attempt still advances
   (`prepared` / `authorized` / `running`), equal-priority eligible jobs may
   backfill open slots **once each**. Lower priorities cannot.
2. **Freeze** — when the last initial blocker stops advancing, pin
   `max(job sequence)` as the backfill cutoff exactly once.
3. **Frozen frontier** — only equal-priority, unconsumed jobs at or before the
   cutoff may still bypass; later submissions wait.
4. **Protected first** — whenever the formula allows the protected job, it
   starts before further backfill.
5. **Supersede** — a newly eligible higher-priority job invalidates a lower-
   priority reservation (running leases untouched).

Orphaned/quarantined blockers keep their leases (containment) but count as
drained for the window so they cannot hold it open forever.

### Live mutations

| Mutation | Allowed when | Notes |
|---|---|---|
| `set-max-parallel-runs` | queued, held, starting, running, needs_attention | Live path updates job + attempt + lease atomically; lowering may leave the set temporarily oversubscribed until excess leases drain |
| `set-priority` | queued, held only | No preemption; invalidates reservation if the protected job’s priority changes |
| `hold` | queued | Invalidates reservation if protected |
| `cancel` | most states | Before start → terminal; after start → signal process group |

## Scheduling pass (outline)

On each pass the coordinator:

1. Reconciles leases and terminal attempts.
2. Supersedes a protected job if a higher-priority eligible head exists.
3. Starts the protected job if now compatible.
4. If still reserved and blocked, considers only equal-priority unconsumed
   candidates (window or frozen cutoff rules).
5. Else walks eligible jobs in priority/FIFO; protects the first incompatible
   head; admits greedily against a **shadow** running set (never evaluate the
   whole batch against the same initial count).
6. Commits attempts, leases, reservation rows, and events in one transaction
   after revalidation.

Unknown reservation semantics versions block ordinary admission until an
operator migrates or invalidates the reservation (`admission_blocked`).

## Protocol

- Unix stream socket, big-endian `u32` length-prefixed JSON
- Hard frame limit checked before allocation
- Peer UID must match the daemon
- Protocol version is exact-match (currently **6**)
- Mutations require a durable **idempotency key**; identical retry returns the
  original result; same key + different payload → conflict

Log bytes are not streamed through the daemon. `mlq logs` asks for paths and
reads files directly (same user, same machine).

## Filesystem layout

XDG conventions (overridable in tests):

```text
$XDG_RUNTIME_DIR/mlqueue/mlqd.sock
$XDG_STATE_HOME/mlqueue/mlqueue.db
$XDG_STATE_HOME/mlqueue/daemon.lock          # stable singleton (not under runtime)
$XDG_STATE_HOME/mlqueue/attempts/<id>/
  stdout.log  stderr.log
  identity.json
  start                 # launch authorization marker
  exec.json             # command identity / running
  cancel.json
  result.json
$XDG_CONFIG_HOME/mlqueue/config.toml
```

Directories `0700`, artifacts `0600`. Singleton lock lives in **state**, not
runtime, so a recreated `$XDG_RUNTIME_DIR` cannot create a second lock.

## Persistence

SQLite: foreign keys, WAL, `synchronous=FULL`, busy timeout, embedded
migrations, one serialized writer.

The v3 migration removes dependency edges. A queued or held job whose legacy
prerequisites were not yet satisfied is cancelled rather than allowed to run
out of order, with a migration-time terminal timestamp and audit event;
already-terminal `skipped` records remain readable as historical state, but
new versions never create that state.

| Table | Role |
|---|---|
| `jobs` | Command, cwd, env, state, concurrency/priority, retries, optional time limit, timestamps |
| `attempts` | Launch token, state, copied limit, runner/command identity, exit |
| `run_leases` | One active lease per attempt; source of truth for the formula |
| `scheduler_reservation` | Protected job, cutoff, initial blockers, semantics version |
| `scheduler_reservation_backfills` | One bypass consumption per job per reservation |
| `operations` | Idempotency keys, request hash, stored response / tombstone |
| `events` | Append-only audit log |

Job IDs are `AUTOINCREMENT` rowids and double as submission sequence. Cutoff
reads `sqlite_sequence` so retention deletes cannot lower the frontier.

## Job and attempt states

```text
Job:
  held ⇄ queued → starting → running → succeeded | failed | cancelled | lost
  starting | running → needs_attention → lost | cancelled

Attempt (persisted):
  prepared → authorized → running → finalized (success/fail/cancel/lost)
  authorized | running → orphaned | quarantined → resolved when group empty
```

`prepared` holds a lease before authorization. Eligibility strings
(`protected_drain`, `waiting_for_higher_priority`, …) are **derived** for
status, not stored states.

## Crash-safe attempt lifecycle

Process spawn and SQLite are not atomic. Flow:

1. Transaction: create `prepared` attempt + lease, job → `starting`.
2. Spawn detached runner (new session); exclusive per-attempt flock.
3. Publish runner identity; coordinator attaches it.
4. Revalidate formula; commit `authorized`; publish `start`.
5. Runner forks command into its own process group; publishes `exec.json`;
   job/attempt → `running`.
6. Runner waits for process-group emptiness (not just leader exit), then
   publishes `result.json`.
7. Daemon finalizes job/attempt, releases lease, appends events.

After authorization, a missing ack means the command **may** have run—never
launch a replacement until the group is proven empty.

On daemon restart: adopt live runners, finalize complete results, respawn
prepared runners under the same flock, republish `start` only when still
valid, quarantine corrupt identity, retain leases for orphans.

### Cancellation

- Before authorization: never publish `start`; finalize cancelled.
- After: durable intent + `cancel.json`; runner signals the **command** group
  (SIGTERM, optional SIGKILL after grace).
- Lease released only when the group is empty.
- Natural exit before signal delivery can win (`cancellation_too_late`).

### Per-attempt wall-clock limits

- An optional limit starts at successful command spawn, not submission or
  authorization. Queue wait is excluded; everything executed by the command,
  including compilation and descendants, is included.
- Expiry is a failed attempt and therefore follows the ordinary retry policy.
- The runner sends SIGTERM to the command process group and always schedules
  SIGKILL after the cancellation grace period. It publishes the timed-out
  result only after the group is empty, so the run lease remains truthful.
- The detached runner owns the monotonic deadline, so daemon restart does not
  reset it. The command spawn time is also persisted; if the runner dies, the
  daemon takes over SIGTERM/SIGKILL enforcement while retaining the lease.

### Group emptiness

- Runner: `waitid(WNOWAIT)` keeps the zombie leader pinning the pgid while
  scanning `/proc`; reaps after consecutive empty scans.
- Daemon (no zombie): conservative—same-boot pgid + start-time checks; false
  release is impossible; stuck D-state holds the lease visibly.

## Command and environment model

- Exact argv after `--` (UTF-8, no shell reconstruction).
- Explicit cwd; revalidated at launch.
- Env = small baseline + `--env` / `--inherit-env` only (not full parent env).
- Secrets in SQLite are plaintext; prefer credential files read by the job.
- No GPU env interpretation by the daemon.

## Configuration

Small versioned TOML under `$XDG_CONFIG_HOME/mlqueue/`: grace periods,
protocol limits, retention knobs. Unknown fields rejected. Reload requires
restart. Scheduling semantics are **code-versioned**, not config-toggled.

## Agent skill

[`skills/queue-ml-jobs/`](skills/queue-ml-jobs/) is versioned with the repo.
It requires agents to use `mlq`, keep work foregrounded, choose
`maxParallelRuns` explicitly (`1` for exclusive/unknown, higher only from
evidence), and leave priority at `0` unless ordering is intentional.

## Invariants

1. One daemon is authoritative for managed admission.
2. Every starting/running attempt owns exactly one durable run lease.
3. Leases are acquired transactionally before launch authorization.
4. Admission never creates an active lease set that violates any member’s
   `maxParallelRuns`. Operator lowers of a live limit may leave the set
   temporarily oversubscribed; excess leases drain without preemption and no
   new starts are admitted until the set is compatible again.
5. Leases release only when the attempt’s process group is empty.
6. A protected job is considered before ordinary admissions each pass.
7. At most one bypass per job per reservation; post-cutoff jobs cannot pass a
   frozen frontier; lower priority cannot backfill a higher protected job.
8. Commands are argv + cwd + explicit env—no shell strings.
9. Every mutation is idempotent under its key.
10. The daemon does not kill foreign work or preempt healthy managed work.
11. Events, results, and logs remain inspectable without a live daemon.

## Known limitations

- Pair-specific compatibility needs `1` or future lanes/groups (not built).
- Descendants that call `setsid` escape the lease model (unsupported).
- Uninterruptible D-state after SIGKILL can hold a lease indefinitely
  (visible; no silent release).
- Strict priority can starve lower priorities.
- No remote clients; no multi-tenant isolation.
