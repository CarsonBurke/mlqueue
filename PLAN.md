# mlqueue implementation plan

## Objective

Build a small, reliable, machine-wide queue for arbitrary local ML commands.
Every repository should submit work through the same service so independently
acting agents do not start an unsafe number of jobs at once.

The first target is a single OS user on one Linux workstation. The queue is a
coordination service, not a cluster scheduler or a hardware resource manager.

## Deliberately simple admission model

Each job declares one concurrency value:

```text
maxParallelRuns: positive integer, default 1
```

The CLI spells this `--max-parallel-runs`, Rust and SQLite use
`max_parallel_runs`, and stable JSON output uses `maxParallelRuns`.

The value means:

> This job is safe to run only when the total number of concurrent managed
> jobs, including itself, is no greater than `maxParallelRuns`.

It is global and symmetric. It does not count copies of only the same command.
Let `R` be the set of attempts that currently hold run leases. A candidate job
`j` may acquire a lease exactly when:

```text
|R| + 1 <= min(j.maxParallelRuns, each r.maxParallelRuns for r in R)
```

For an empty running set, the minimum is the candidate's value. This produces
the intended behavior:

| Running jobs | Candidate | Result |
|---|---|---|
| none | LLM with `1` | starts alone |
| two CleanRL jobs with `3` | CleanRL with `3` | starts as the third job |
| two CleanRL jobs with `3` | LLM with `1` | waits for all jobs to finish |
| LLM with `1` | CleanRL with `3` | waits |
| one job with `2` | one job with `4` | starts; the lower limit of `2` wins |

An attempt consumes one run lease from launch preparation until its complete
process group is gone. Child-process count does not affect the formula.

### Why the limit is symmetric

Checking only the candidate's value would allow a permissive job to start next
to an already-running exclusive job. Checking the minimum across the resulting
set ensures every running job's declaration remains satisfied.

### Conservative default

Default to `1`. Missing information therefore serializes work instead of
silently increasing concurrency. Agents should explicitly pass a higher value
only when the workload is known to share safely. For example, characterized
small CleanRL runs normally use `3`; large-model training, benchmark-sensitive
work, and unfamiliar jobs use `1`.

### Expressiveness limit

One integer cannot express pair-specific compatibility. If workload A is safe
three-wide only with other A jobs and workload B is safe three-wide only with
other B jobs, giving both `3` permits an unsafe mixture. Agents must use the
most conservative value that is safe with arbitrary other managed jobs. Use
`1` when compatibility depends on job type.

Named concurrency lanes or compatibility groups are a possible future
extension, but are intentionally absent from the MVP.

## Explicit non-goals

The daemon does not:

- Discover GPUs or parse NVIDIA/ROCm telemetry.
- Estimate, reserve, or enforce VRAM, RAM, CPU, compute share, or wall time.
- Infer workload type from a command or repository.
- Change concurrency from observed utilization.
- Detect unmanaged processes and foreign GPU work.
- Preempt, suspend, checkpoint, or kill healthy work to improve queue order.

These omissions are deliberate. Agents and users express their safety
knowledge through `maxParallelRuns`, and all cooperating work goes through the
queue. A direct `python train.py`, `nohup`, detached terminal, or repository-
local worker bypasses the model and is invisible to `mlqueue`.

Jobs run as the same trusted OS user as the daemon. The queue coordinates that
user's work and is not a security boundary against a malicious submitted
command.

The process-group MVP requires submitted workloads and their descendants to
remain in the runner-created command process group. A command that daemonizes
or calls `setsid` violates the admission contract because the queue can no
longer know when its lease is safe to release. Such a workflow must be changed
to stay foregrounded or wait for the cgroup extension.

## Scheduling and backfill

### Queue rank

Order eligible jobs by monotonic submission sequence (FIFO). Dependencies,
holds, and retry delays determine eligibility before ranking. Priority is not
part of the MVP; add it only if concrete workflows demonstrate that FIFO plus
protected backfill is insufficient.

### Greedy admission

On each scheduling pass:

1. Reconcile active run leases and terminal attempts.
2. Start an existing protected job immediately if it is now compatible.
3. If a reservation exists and its owner is still blocked, restrict candidates
   to eligible jobs whose one bypass is not consumed: every such job while the
   backfill window is open, only those at or before the frozen cutoff once the
   window has closed. Later jobs are not part of that scheduling pass.
4. Without a reservation, consider every eligible job in FIFO order. When the
   first job is incompatible with the cumulative shadow set, protect it before
   considering lower jobs. Seed the consumed set with attempts already active
   and jobs selected earlier in that same shadow batch, then switch immediately
   to the reservation candidate rules in step 3.
5. Admit the first compatible candidate in the permitted FIFO set.
6. Add its lease to a shadow running set and repeat until no candidate can be
   admitted.
7. Commit attempts, leases, reservation creation/satisfaction/invalidation, and
   seeded or newly consumed backfill rows in one transaction after
   revalidation.

Build multi-start decisions cumulatively against the shadow set. Evaluating
every candidate against the same initial count could over-admit a batch.

### Protected jobs and bounded backfill

A restrictive job can reach the head while permissive jobs are already
running. Stopping all admissions immediately wastes concurrency those running
jobs explicitly allow; letting every later permissive arrival pass can starve
the restrictive job.

Use a backfill window scoped to the protected job's original blockers, then a
frozen frontier:

1. When the highest-ranked job cannot start only because of the active
   concurrency limits, persist it as the protected job together with its
   initial blockers: the attempts holding (shadow) leases at that moment.
2. While any initial blocker still holds its lease in a live-advancing state
   (prepared, authorized, or running), the backfill window is open: every
   eligible job whose bypass is unconsumed may pass the protected job,
   regardless of submission order. The machine was already busy with work
   that predates the protection; open slots are not idled on its account.
3. On the first pass after the last initial blocker stops advancing, freeze
   the frontier: pin the current maximum submission sequence as the backfill
   cutoff, exactly once. The stored cutoff is an explicit unfrozen sentinel
   until then, so a crash between drain and pinning merely re-freezes at a
   slightly later, still finite, sequence — and a pinned cutoff can never be
   advanced again.
4. After the freeze, derive the backfill set as all currently eligible,
   unconsumed jobs submitted at or before the cutoff, excluding the protected
   job. This includes a previously held or dependency-delayed job that later
   becomes eligible; membership is not snapshotted separately.
5. Admit compatible candidates in FIFO order. Record each job that starts as
   a backfill; the same job cannot bypass this reservation again through a
   retry or another attempt, in the open window or after the freeze.
6. Jobs submitted after the frozen cutoff may not pass the protected job.
7. When nothing fits, launch nothing. Re-run the same checks after an active
   attempt finishes; do not mistake “currently full” for an exhausted frontier.
8. Start the protected job as soon as the compatibility formula permits,
   before reconsidering any backfill.

The window plus one-bypass-per-job rule bounds scheduler-created starvation
without requiring duration estimates or assuming retries are finite: the
window closes when the finite initial blocker set terminates, and the frozen
frontier is finite thereafter. Orphaned or quarantined blockers retain their
leases (the protected job still waits for containment) but count as drained
for the window, so an attempt that stopped advancing can never hold the
window open for an unbounded stream of newcomers. Seed the consumed set with
the jobs already active when protection is created so their retries cannot
become fresh backfills.

This deliberately inverts the tradeoff of the earlier freeze-at-creation
design. There, a reservation was created on the restrictive job's own
submission tick with an empty frontier behind it, so in incremental use no
later submission could ever backfill: slots idled, and the protected job
started at the earliest drain. Now slots stay busy through the window and the
protected job may wait longer — for the window's backfills and the frozen
frontier to drain too. A job declared `maxParallelRuns 1` asked to run alone,
not to run soon; jobs present before its original blockers cleared may delay
it once each. The simpler alternative — closing the window with no frozen
frontier at all, letting the machine drain immediately — was rejected because
it re-idles slots for every job that arrived during a possibly hours-long
window without ever getting a slot, which is the same waste this design
removes, one step later.

Consume a job's bypass when its prepared attempt and run lease are acquired,
even if launch later fails or is cancelled. Reservation creation and seeding,
a backfill lease and consumption row, a cutoff freeze, protected-job lease
and reservation satisfaction, or an invalidating hold/cancel/limit mutation
are each atomic transactions. Recovery therefore never reconstructs fairness
from partial events; the window itself is re-derived from durable leases and
attempt states, never from history.

In the motivating example, suppose two CleanRL runs with limit `3` are active
and an LLM job with limit `1` is submitted. The LLM becomes protected with
the two CleanRL attempts as its initial blockers. CleanRL jobs submitted
afterwards keep taking the empty third position, once each, because the
resulting set is valid at three-wide. When both original CleanRL runs finish,
the frontier freezes: jobs submitted from then on wait behind the LLM, the
already-admitted backfills drain, and the LLM starts alone.

### Reservation lifecycle

Persist one global protected-job reservation in the MVP because there is one
global concurrency domain. It contains:

- Protected job ID.
- Backfill cutoff submission sequence: an unfrozen sentinel while the window
  is open, pinned exactly once when the initial blockers stop advancing.
- Creation time and the attempt IDs that initially blocked it, which also
  scope the backfill window.
- Initially active and subsequently admitted job IDs whose one bypass has been
  consumed.
- Scheduler-semantics version.
- Satisfaction or invalidation reason and timestamps.

Normal new arrivals do not displace it. Invalidate it when the job is cancelled,
held, becomes dependency-ineligible, or an operator explicitly changes its
concurrency limit.

If a software upgrade cannot interpret an active reservation's scheduler-
semantics version exactly, restore no ordinary admission. Surface an operator-
attention state until an explicit audited migration preserves or invalidates
the reservation; never silently clear it and let post-cutoff jobs pass.
Cancelling or holding the protected job remains possible while blocked and is
itself an audited resolution: once the offending reservation is resolved,
ordinary admission resumes without a daemon restart. An active version-1
reservation is interpretable exactly: its cutoff was frozen at creation, so
it schedules as a permanently frozen frontier and never gains an open window
retroactively.

If an admitted backfill never terminates, the non-preemptive queue cannot
guarantee that the protected job starts. It stops admitting further work and
reports the blocker; it does not pretend the concurrency parameter is a time
limit.

### Unsatisfiable jobs

Represent `maxParallelRuns` as a nonzero unsigned 32-bit integer and reject
zero or values outside that wire/storage type. Any accepted value is
intrinsically satisfiable once the running set empties. A limit of `1` is not a
special scheduler mode; it falls naturally out of the same formula.

## Principles and invariants

- A single daemon is authoritative for managed admission.
- Every starting or running attempt owns exactly one durable run lease.
- A lease is acquired transactionally before launch authorization.
- The resulting active set always satisfies every member's
  `maxParallelRuns` declaration.
- Run leases are not released until the attempt's process group is empty.
- A protected job is considered before ordinary admissions on every pass.
- Jobs submitted after a protected job's frontier froze cannot bypass it, and
  no job bypasses it more than once.
- Submission and state transitions are durable under concurrent agents.
- Commands are argument vectors with explicit working directories and
  environments; no shell string is reconstructed.
- Every mutation is idempotent so clients can retry after losing a response.
- The daemon never kills foreign work or preempts managed work implicitly.
- Events, attempt results, and logs remain inspectable without the daemon.

## Components and Rust structure

Use one Rust package with two public binaries and an internal execution role:

- `mlq`: CLI and Unix-socket client.
- `mlqd`: long-running user daemon.
- Internal `mlqd` attempt-runner mode for durable process supervision.

Suggested modules:

```text
src/bin/mlq.rs        CLI entry point
src/bin/mlqd.rs       daemon and internal runner entry point
src/lib.rs
src/config.rs
src/paths.rs
src/domain/               IDs, states, jobs, leases, decisions
src/protocol/             framed messages and public JSON views
src/db/                   migrations and transactional repositories
src/scheduler/            pure concurrency and backfill policy
src/process/              runner, identity, recovery, cancellation, logs
```

Use Tokio for daemon/socket orchestration, Clap for the CLI, Serde/JSON for
wire and artifact formats, `rusqlite` with a controlled bundled SQLite version
for persistence, `libc` for the handful of Linux process/lock syscalls that
std does not cover (`setsid`, `setpgid`, `killpg`, `flock`), and `tracing`
for diagnostics. Rust's standard library opens every descriptor close-on-exec
and `std::process::Command::spawn` already reports `execve` failure through an
internal pipe, so no extra descriptor hygiene or exec-handshake machinery is
required beyond `pre_exec` hooks.

Keep scheduler types independent of Tokio, SQLite, and process code. Run all
scheduling mutations through one coordinator task. Request handlers validate
input and send it commands; they never launch work independently. Serialize
database writes and never hold a transaction or mutex across socket I/O,
filesystem waits, or process spawning.

## Filesystem layout

Follow XDG base-directory conventions with test overrides:

```text
$XDG_RUNTIME_DIR/mlqueue/mlqd.sock       Unix socket
$XDG_STATE_HOME/mlqueue/mlqueue.db           SQLite database
$XDG_STATE_HOME/mlqueue/daemon.lock          stable singleton lock
$XDG_STATE_HOME/mlqueue/attempts/<id>/
  stdout.log
  stderr.log
  identity.json
  start                                      launch authorization
  exec.json                                  exec handshake
  cancel.json                                cancellation intent
  result.json                                terminal result
$XDG_CONFIG_HOME/mlqueue/config.toml
```

Runtime and state directories are owned by the current user with mode `0700`.
Files and sockets are not world-readable. Keep the authoritative lock in the
stable state directory so recreation of the volatile runtime directory cannot
produce two daemon locks.

## Protocol and daemon ownership

Use a Unix-domain stream socket with big-endian `u32` length-prefixed JSON
messages and a hard frame limit checked before allocation. Each request has:

- Protocol version.
- Request ID.
- Operation and typed payload.
- Required durable idempotency key for mutations.

Require Linux peer credentials and reject a UID different from the daemon's
before parsing. Apply connection-count, idle, and write limits. Reject
unsupported protocol versions, malformed payloads, duplicate keys, and
oversized frames with stable error codes.

Log access is deliberately not a daemon protocol feature in the MVP. The CLI
asks the daemon for the attempt's log paths and then reads or follows the
files directly: client and daemon are the same user on the same machine, and
the log files are already the durable artifact. This removes a framed
streaming protocol, offset resumption, and slow-follower handling from the
daemon. Framed log streaming becomes necessary only with remote submission,
which is an explicit non-goal for now.

Acquire an advisory singleton lock before binding. Probe or remove only a
confirmed-stale socket owned by the user. Never treat a socket path alone as
proof of a live daemon.

## SQLite model

Enable foreign keys, WAL, `synchronous = FULL`, a busy timeout, and embedded
monotonic migrations. Use one serialized writer. Every state transition and
its event are committed together.

### `jobs`

- Numeric user-facing job ID. The rowid is allocated monotonically
  (`AUTOINCREMENT`) and doubles as the submission sequence; a separate
  sequence column would duplicate it without adding information.
- Name.
- Exact UTF-8 argument vector and canonical working directory.
- Explicit persisted environment.
- State, retry policy, and dependency metadata.
- Positive `max_parallel_runs`, default `1`.
- Created, updated, and terminal timestamps.

The value is immutable after a job reaches `starting`. A queued job may change
it through an explicit idempotent mutation that also invalidates and reruns the
current scheduling decision.

### `dependencies`

- Parent and child job IDs.
- Required outcome, initially `success` or `completion`.
- Unique edges and cycle rejection in the submission transaction.

### `attempts`

- Attempt ID, job ID, and monotonically increasing attempt number.
- Launch token and detailed attempt state.
- A copied immutable `max_parallel_runs` used by its lease.
- Runner and command PID/process-group identity: boot ID and `/proc` start
  times, not PID alone.
- Start, observation, cancellation, and finish timestamps.
- Exit code or terminating signal.
- Paths derived from the trusted attempt ID.

### `run_leases`

- Attempt ID as the unique owner.
- Copied `max_parallel_runs`.
- Acquisition and release timestamps.

The active lease rows are the source of truth for the scheduler formula.
Create an attempt and lease atomically. A unique lease per attempt and the
serialized coordinator prevent double starts.

### `scheduler_reservation`

- Protected job ID.
- Backfill cutoff sequence (unfrozen sentinel until the window closes).
- Initial blocker attempt IDs, which also scope the backfill window.
- Creation, invalidation, and satisfaction records.
- Scheduler-semantics version.

There is at most one active row in the global-domain MVP. Restore it before
ordinary admission after restart.

### `scheduler_reservation_backfills`

- Reservation ID and job ID.
- Consumption reason: active at reservation creation or admitted afterward.
- Timestamp and optional attempt ID.

A unique reservation/job constraint enforces one bypass per job across retries
and daemon restarts.

### `operations`

- Idempotency key, operation type, and canonical request hash.
- Completion state and stored response or stable object reference.
- Creation and retention timestamps.

Insert the operation and mutation in one transaction. An identical retry
returns the original result; a reused key with a different payload conflicts.
Retain tombstones after cleanup so an old key cannot become executable again.

### `events`

- Monotonic event ID and timestamp.
- Job and optional attempt ID.
- Event type, actor, and versioned structured details.

Events are append-only audit records. Current state remains normalized so
status does not replay the event log.

## Job and attempt states

Keep aggregate job state separate from launch detail:

```text
Job:
held <-------> queued ------> starting ------> running ------> succeeded
  |               |              |               |  |  |
  |               |              |               |  |  +----> failed
  |               |              |               |  +-------> cancelled
  |               |              |               +----------> lost
  |               |              +--------------------------> cancelled/failed
  |               +-- failed prerequisite -----------------> skipped
  +---------------------------------------------------------> cancelled
starting/running -------------> needs_attention -----------> lost/cancelled

Attempt:
prepared -> authorized -> exec_pending -> running -> exited -> finalized
    |           |              |             |          |
    +-----------+--------------+-------------+----------+-> cancelled/failed
                                             +-----------> orphaned
orphaned/quarantined -- process group proven empty -------> lost/cancelled
```

`prepared` owns a run lease but has no launch authorization. `authorized` is
the committed database decision; its marker may still need publication.
`exec_pending` has a durable command identity and a fork/exec handshake in
progress. `running` requires exec acknowledgment. `orphaned` and `quarantined`
retain their leases until the process group is proven empty or an operator
resolves them safely.

Eligibility reasons such as `waiting_for_dependency`, `waiting_for_slot`,
`protected_drain`, `backfill_window_open`, and `behind_backfill_cutoff` are
derived status fields, not persisted job states.

Attempts never return from a terminal state. An idempotent retry moves an
eligible failed/lost job to `queued`; the scheduler later creates the next
attempt and lease. A quarantined attempt cannot retry while it may still own
live processes.

## Command and environment model

Accept commands after `--` and persist the exact argument vector. The JSON MVP
accepts valid UTF-8 arguments, paths, keys, and values and rejects NUL. Validate
the executable and working directory at submission for fast feedback, then
revalidate at launch because queued filesystem state can change. `chdir` and
`execve` failures become durable attempt failures.

Do not snapshot the complete submitter environment. Capture a documented
baseline plus explicitly supplied `--env` or client-resolved `--inherit-env`
values. Secure secret injection is not in the MVP: explicit secrets are
plaintext in SQLite/WAL and backups. Warn on sensitive-looking keys, redact
ordinary output, and recommend an external credential file or tool read by the
workload at launch.

The daemon does not set or interpret GPU visibility variables. Hardware access
is outside its model; the agent's declared concurrency is the only admission
contract.

## Crash-safe attempt lifecycle

Process spawn and SQLite commit are not atomic. Use a runner and durable
handshake:

1. Transactionally choose the job, create a `prepared` attempt and run lease,
   move the job to `starting`, and append events.
2. Create the private attempt directory and spawn the runner in a new session.
   A per-attempt exclusive runner lock prevents duplicate recovery spawns.
3. Atomically write and verify runner identity, then attach it to the attempt.
4. Serialize cancellation against authorization. Revalidate job state and the
   global lease formula, commit `authorized`, then publish and `fsync` `start`.
5. The runner validates the token and forks a child into a separate command
   process group. The child blocks before exec while the runner records its
   identity. A close-on-exec error pipe distinguishes successful exec from
   setup or `execve` failure.
6. The runner publishes `exec.json`; the daemon mirrors `exec_pending` and
   `running` into SQLite without moving state backward when an artifact leads.
7. The runner records direct-command exit but retains the lease until the
   verified command process group is empty. It atomically publishes and
   `fsync`s `result.json` and its directory.
8. The daemon transactionally finalizes job/attempt state, releases the lease,
   and appends events.

After authorization, a missing acknowledgment means the command may have run.
Never launch a replacement until process-group emptiness is proven. On daemon
restart:

- Adopt a verified live runner.
- Finalize a complete valid result.
- Resume an unlaunched prepared attempt after a bounded spawn grace period.
- Republish a missing authorization marker only when the committed state still
  allows it.
- Treat a dead runner after authorization as orphaned while its command group
  remains live.
- Quarantine identity mismatches or corrupt artifacts and retain the lease.

Process groups do not contain a descendant that calls `setsid`; submission of
such a workload is unsupported in the MVP rather than silently considered
safe. Per-attempt cgroups/systemd scopes are the planned extension if real
foreground workloads cannot satisfy this contract.

Set `umask 077`, make daemon descriptors close-on-exec, and use `close_range`
or a descriptor whitelist so workers do not inherit the lock, socket, database,
or client connections. Do not set a parent-death signal on the runner.

Create artifacts with `O_EXCL` and mode `0600`, write and `fsync` content,
rename into place, and `fsync` the containing directory. Reject reused
nonempty attempt directories, oversized artifacts, and token/schema
mismatches. The state directory is `0700` and owned by the single trusted
user, so per-file symlink and ownership audits add little on top of the
directory boundary; they are not part of the MVP checklist.

### Cancellation

Holding is valid only in `queued`; use cancellation after preparation begins.
If cancellation commits before authorization, never publish `start` and stop
the waiting runner. After authorization, durably record intent, publish
`cancel.json`, and wake the verified runner. The runner stays outside the
command process group and signals that group.

Use `SIGTERM`, then optional explicit/policy-approved `SIGKILL` after a grace
period. Finalize cancellation and release the run lease only after the process
group is empty. Recovery resumes committed cancellation intent.

The runner linearizes cancellation against natural completion. If it observes
natural exit and group emptiness before delivering a signal, retain the natural
result and record `cancellation_too_late`. If it delivers a signal first, the
terminal result is `cancelled`, even if a signal handler returns zero.

## CLI

Initial commands:

```text
mlq [--idempotency-key KEY] submit --max-parallel-runs N --name NAME
        --cwd PATH [--after-success JOB]... [--after-completion JOB]...
        [--max-attempts N] [--retry-delay DURATION] -- COMMAND [ARGS...]
mlq status [--watch] [--json]
mlq show JOB [--json]
mlq logs JOB [--attempt N] [--stderr] [--follow]
mlq wait JOB [--timeout DUR] [--json]
mlq [--idempotency-key KEY] cancel JOB [--force]
mlq daemon status [--json]
```

Workflow commands:

```text
mlq [--idempotency-key KEY] hold JOB
mlq [--idempotency-key KEY] release JOB
mlq [--idempotency-key KEY] retry JOB
mlq [--idempotency-key KEY] set-max-parallel-runs JOB N
mlq recover list
mlq [--idempotency-key KEY] recover resolve JOB --attempt N
        --as lost|cancelled
```

The protocol always requires a mutation key. The CLI generates one before a
mutation and retains it for automatic transport retries; an explicit global
flag lets an agent safely rerun a whole CLI invocation after losing its output.
`submit` defaults to `--max-parallel-runs 1`. Dependency flags are repeatable
and inserted atomically with the job. Human and JSON status explain:

- The declared limit for every job and active attempt.
- Current run-lease count and effective minimum running limit.
- Why a candidate is incompatible.
- The protected job, and its open window or frozen cutoff.
- Whether a queued job is eligible to backfill or arrived too late.
- The attempts that currently prevent the protected job from starting.

Every mutation supports stable structured errors and idempotency. Recovery
resolution refuses to release a lease until known containment is absent; any
future unsafe force-release must be explicit and audited.

## Agent skill: `queue-ml-jobs`

Ship a repository-owned skill at `skills/queue-ml-jobs/` and install or link it
into agent environments during deployment. Keeping it beside the CLI makes its
examples versioned with the implementation.

Trigger the skill whenever an agent intends to start any cooperating local ML
command, including short smokes and CPU preprocessing as well as GPU workloads,
benchmarks, experiment batches, and dependent chains. It requires the agent to:

1. Use `mlq`; never launch managed work directly, with `nohup`, in a
   detached terminal, or through a repository-local scheduler that bypasses
   the daemon.
2. Identify the exact command, working directory, environment, dependencies,
   and desired failure behavior.
3. Verify that the command stays in the foreground and does not daemonize or
   escape its process group.
4. Deliberately choose `maxParallelRuns` for each submission.
5. Use `1` for large-model training, near-full-device workloads, multi-GPU
   training, benchmark-sensitive work, unfamiliar commands, or uncertainty.
6. Use a value above `1` only from known workload behavior. Small characterized
   CleanRL runs normally use `3`.
7. Interpret the value globally: `3` asserts safety beside any other managed
   jobs whose limits also permit the resulting total, not merely other copies
   of the same script.
8. Never increase the value merely because the machine looks idle at the
   moment; it is a compatibility declaration, not a utilization target.
9. Report the job ID and declared limit, and use queue status/log/cancel
   commands afterward (the CLI generates idempotency keys automatically).
10. Lower future declarations if observed concurrency harms correctness,
   stability, throughput, or benchmark validity.

Omitting the flag remains safe because the CLI defaults to `1`, but the skill
should make the choice explicit in commands it proposes. It must not invent
VRAM, compute-share, or duration flags that the queue does not support.

Forward-test the skill on:

- One large LLM fine-tune that must run alone.
- Six known-small CleanRL runs intended to run three-wide.
- An unfamiliar training script with no measurements.
- A benchmark affected by concurrent work.
- A smoke/control/treatment dependency chain.
- A request to run work directly despite an active queue.

Tests must inspect proposed commands and limits, not merely whether the agent
mentions `mlq`.

## Repository integrations

### CleanRL

Keep experiment-specific configuration, completion detection, and result
tools. Replace direct launch with `mlq submit --max-parallel-runs 3` for the
small characterized configurations that are known to share safely. Use `1`
for larger or uncharacterized configurations. The number is an explicit
integration choice, not framework detection inside the daemon.

### Large-model and benchmark-sensitive training

Submit with `--max-parallel-runs 1`. Dependency chains remain ordinary queue
metadata, for example:

```text
production smoke -> same-layer control -> cross-layer treatment
```

A failed prerequisite explicitly skips expensive descendants.

### Other repositories

Use the CLI first. Thin language wrappers may speak the daemon protocol but do
not implement their own admission policy.

## Configuration

Use a small versioned TOML configuration with conservative defaults:

- Cancellation grace period.
- Protocol connection/frame limits.
- Operation tombstone, log, and completed-job retention.

Configuration reload initially requires daemon restart. Reject unknown fields.
Scheduling semantics are versioned in code and with protected reservations;
ordinary configuration does not alter the concurrency formula or cutoff rules.

## Implementation sequence

### Phase 0: domain contract and scheduler model

- Split into shared library and two binaries.
- Define job/attempt states, positive concurrency limits, leases, reservation,
  decisions, protocol envelopes, and stable error types.
- Implement the compatibility formula as a pure function.
- Implement windowed-then-frozen backfill scheduling with deterministic job
  fixtures.
- Implement XDG paths, config validation, and embedded migrations.

Exit criteria:

- Empty and mixed-limit examples match the documented formula.
- Sequential shadow admission never creates an invalid active set.
- A protected job starts before arrivals its frontier froze out.
- A temporary database migrates from empty to current.

### Phase 1: durable CPU-only vertical slice

- Singleton daemon, socket framing, concurrent clients, and idempotent
  operations.
- Submit, status, show, logs, cancel, and daemon status.
- Serialized scheduling, run leases, protected reservations, and restart
  restoration.
- Runner authorization, fork/exec handshake, process groups, durable result,
  logs, cancellation, and reconciliation.
- Fault injection at every database/artifact/process boundary.

Exit criteria:

- Concurrent submitters cannot duplicate jobs or exceed the formula.
- Response loss and retries do not repeat mutations.
- Daemon crashes before and after authorization neither duplicate nor kill
  work.
- Logs and exit results survive restart.
- Cancellation waits for process-group emptiness before releasing a lease.

### Phase 2: workflow features and agent skill

- Dependencies with cycle detection and failure propagation.
- Hold, release, retry, and queued-limit changes.
- Complete structured JSON and decision explanations.
- Build, validate, and forward-test `queue-ml-jobs`.

Exit criteria:

- Failed dependencies skip descendants.
- Queue rank and frozen cutoffs are deterministic.
- Agents choose `1` for uncertain/exclusive work and `3` for characterized
  CleanRL examples.

### Phase 3: operational hardening and adoption

- systemd user installation. Prefer per-attempt transient scopes so daemon
  restart/upgrade does not kill workers.
- Stress concurrent clients, PID reuse, corrupt artifacts, filesystem errors,
  and reservation recovery.
- Log retention and database maintenance.
- Incrementally migrate CleanRL and large-model workflows.

Exit criteria:

- Real repository workflows use one daemon and no longer make competing local
  admission decisions.
- systemd stop/restart/logout semantics are tested rather than assumed.

### Optional extensions

Add only after a concrete limitation appears:

- Named concurrency lanes or compatibility groups.
- Per-attempt cgroup containment.
- Cooperative checkpoint-aware preemption.
- Local TUI/web status view.
- Remote submission.

GPU telemetry and resource-aware bin-packing are not presumed future phases;
they require a demonstrated need that `maxParallelRuns` cannot solve.

## Test strategy

### Scheduler unit and property tests

- Limits `1`, `2`, `3`, and mixed sets use the minimum declaration.
- The active set always satisfies `count <= every active limit`.
- Batch decisions are evaluated cumulatively.
- A restrictive candidate never starts beside an incompatible permissive job.
- A permissive candidate never starts beside a restrictive running job.
- Any eligible job may pass once while the window is open; post-cutoff
  arrivals never pass a frozen frontier.
- Cancelling, holding, or changing the limit of the protected job invalidates its
  reservation deterministically.
- Reservations restore identically after daemon restart; the window is
  re-derived from durable leases and attempt states.
- Continuous new arrivals cannot extend a frozen frontier, and a pinned
  cutoff never advances.
- Randomized job streams never violate the formula or reservation precedence.

### Database, protocol, and concurrency tests

- Concurrent identical and distinct idempotency keys.
- Same key with different payload conflicts.
- Client disconnect after commit but before response.
- Oversized, malformed, slow, truncated, and duplicate-key frames.
- Slow log followers and arbitrary binary output.
- Multiple clients racing submit, cancel, hold, retry, and queued-limit change.
- Operation tombstones prevent key reuse after job cleanup.

### Process and recovery tests

- Crash before/after attempt transaction, runner spawn, authorization, child
  identity, exec acknowledgment, result publication, and terminal commit.
- Cancellation at every launch boundary and racing natural exit.
- Runner death while the command group remains alive.
- Command leader exit while same-group descendants remain.
- PID/process-group reuse and identity mismatch.
- `ENOSPC`, `EIO`, missing/truncated artifacts, symlink substitution, and failed
  `fsync`/rename.
- No daemon lock, database, listener, or client descriptors inherited by work.
- Executable or working directory removed while queued.

## Acceptance criteria

- Every active set satisfies every member's `maxParallelRuns` declaration.
- The default value of `1` makes an unspecified job exclusive among managed
  work.
- Three `maxParallelRuns=3` jobs may run together; a fourth may not.
- A `maxParallelRuns=1` job starts only with no other active lease.
- Compatible jobs may backfill a protected restrictive job once each while
  its original blockers run; submissions after the frontier froze cannot pass.
- Concurrent clients cannot corrupt state, duplicate work, or over-admit.
- Every mutation is safely retryable through its idempotency key.
- Daemon restart does not kill workers, lose completed runner results, or forget
  a protected job.
- Cancellation targets the verified command process group and retains the
  lease until the group is empty.
- MVP workload commands remain foregrounded in their assigned process group;
  daemonizing/`setsid` workloads are explicitly unsupported by the contract.
- Dependency chains terminate explicitly after failed prerequisites.
- Status explains concurrency declarations, active limits, protection, and
  backfill decisions.
- The agent skill consistently routes ML work through the queue and chooses
  conservative explicit limits.

## Decisions deferred until implementation evidence

- Whether the backfill window should also have a configurable maximum number
  of backfill starts.
- Whether real workloads require cgroups rather than process groups.
- Retention periods for logs, operations, and completed jobs.
- Whether concrete mixed-workload failures justify compatibility groups or
  lanes.

Record resolved decisions here or in a focused architecture decision record
with the evidence that resolved them.

## Implementation status and resolved decisions

Phases 0–2 are implemented: the shared library with pure scheduler, SQLite
model, framed Unix-socket protocol, singleton daemon with serialized
coordinator, detached attempt runners, recovery, dependencies with skip
propagation, retry policy, hold/release/retry/queued-limit mutations, the
full CLI, and the `queue-ml-jobs` agent skill. `tests/e2e.rs` exercises real
daemon/runner/CLI binaries against the acceptance criteria (formula limits,
windowed-then-frozen backfill, cancellation, idempotency replay/conflict,
dependency skips, retries, daemon-crash adoption, singleton locking).

Decisions resolved during implementation:

- `exec_pending` is not a persisted attempt state. `std::process::Command`
  already synchronizes fork/exec through an internal close-on-exec pipe, so a
  successful spawn implies a successful `execve`; attempts move directly from
  `authorized` to `running` when the runner publishes `exec.json`.
- Recovery "resume" of an unlaunched prepared attempt is implemented as a
  respawn of the runner for the same attempt directory. The per-attempt
  exclusive `flock` makes a duplicate-spawn race benign: exactly one runner
  proceeds. A prepared attempt whose runner never publishes identity within
  the grace period is failed safely — without a published `start` marker the
  command provably never ran, so releasing the lease is sound.
- The daemon sets `SIGCHLD` to `SIG_IGN` to auto-reap detached runners;
  runners restore `SIG_DFL` before supervising their command (ignored
  dispositions survive `execve`).
- A dead runner after a *published* authorization but before the exec
  handshake quarantines the attempt (command existence unknowable); a dead
  runner whose authorization was never published fails the attempt safely.
- systemd integration ships as `contrib/systemd/mlqd.service` with
  `KillMode=process` so daemon restarts never signal workers. Per-attempt
  transient scopes remain a Phase 3 extension.
- The runner's group-emptiness proof is PID-reuse safe: the leader's exit is
  observed with `waitid(WNOWAIT)` so the zombie keeps pinning the leader PID
  (and therefore the numeric pgid) while `/proc` is scanned for surviving
  members (excluding the zombie itself); the leader is reaped only after two
  consecutive empty scans, which also closes the fork-then-exit window
  between `/proc` directory reads. Because the runner starts a fresh session,
  no foreign process can join the group, so zero non-leader members proves
  the group empty.
- Daemon-side emptiness verdicts (runner already dead, so no zombie can pin
  the PID) are deliberately conservative: a same-boot pgid match keeps the
  attempt orphaned with its lease held and the job flagged for attention;
  false release is impossible, and a PID-reuse false positive resolves via
  `recover resolve` after human inspection.
- Accepted limitation: a descendant stuck in uninterruptible D-state after
  SIGKILL keeps the group non-empty and the lease held indefinitely; the
  attempt is visible as running/orphaned rather than silently released.
  Escalating this to an automatic `needs_attention` timer is future work.
- Daemon-side group checks additionally compare the group leader's recorded
  start time: because the kernel keeps a PID allocated while its group has
  members, a process found under the leader's pid with a different start
  time proves the recorded group drained — so the daemon never signals (or
  waits forever on) an unrelated group that recycled the number.
- `identity.json` is written non-exclusively (the per-attempt flock already
  serializes runners), so a recovery runner replacing a dead predecessor can
  re-announce itself; the coordinator refuses to authorize an identity whose
  runner is no longer alive and safely fails the attempt after the grace
  period (still `prepared` ⇒ no `start` was ever published).
- Submission argument/filesystem validation runs on the coordinator before
  the write transaction opens; only the dependency-parent existence check
  runs inside it (a hung NFS/FUSE `cwd` must never stall the write path).
- Exclusive artifact publication uses `renameat2(RENAME_NOREPLACE)` (with a
  plain-rename fallback on filesystems lacking support), the backfill cutoff
  reads `sqlite_sequence` so future retention deletes can never lower it, and
  declaring the same dependency parent under both flags keeps the stricter
  `success` requirement.
- Scheduler semantics version 2 replaced freeze-at-creation with the
  blocker-scoped backfill window. Version 1 froze the cutoff on the protected
  job's own submission tick, so in incremental use the frontier behind it was
  always empty and every later submission idled open slots until full drain.
  The window reuses the stored cutoff column (`0` as the unfrozen sentinel,
  pinned exactly once), so no schema migration was needed; active version-1
  reservations are still interpreted exactly, as permanently frozen
  frontiers.
