# mlqueue

**mlqueue** is a machine-wide queue for local ML work: one durable daemon
(`mlqd`) and one CLI (`mlq`). Agents and shells across repositories submit
commands through the same service so they cannot accidentally start an unsafe
number of runs at once.

It is a **coordination** tool for a single Linux user on one workstation—not a
cluster scheduler, not a GPU allocator, and not a security boundary.

Design rationale, components, and invariants live in
[ARCHITECTURE.md](ARCHITECTURE.md).

## Install

```bash
cargo install --path .    # installs mlq + mlqd into ~/.cargo/bin
mlq daemon install        # systemd user unit: enable + start
# without systemd:
mlq daemon run            # foreground daemon
```

Confirm:

```bash
mlq --version
mlq daemon status
```

## Quick start

```bash
# exclusive smoke job (default maxParallelRuns = 1, priority = 0)
mlq submit --name smoke -- python train.py --smoke

# allow sharing with other cooperative jobs (only when known-safe)
mlq submit --name cleanrl --max-parallel-runs 3 -- python train.py

# prefer this job over default-priority queue work (does not preempt runners)
mlq submit --name urgent --priority 1 --max-parallel-runs 1 -- python eval.py

# durable smoke gate: if smoke submission returns job 12
mlq submit --name train --after-success 12 -- python train.py

# bound each attempt to two hours (queue wait is not counted)
mlq submit --name bounded --time-limit 2h -- cargo run --release --bin train

mlq status                # live queue: running, queued, held
mlq status -f             # recent finished runs only
mlq show 1
mlq logs 1 --follow       # exit code matches the attempt
mlq wait 1                # block until terminal; exit 0 / exit-code / 128+sig
mlq cancel 1              # SIGTERM; add --force for SIGKILL after grace
```

## Admission model

Every job declares one concurrency number, `--max-parallel-runs N` (default
**1**):

> This job is safe to run only while the total number of concurrent managed
> jobs, including itself, is at most **N**.

Admission is **symmetric**. A candidate starts only when the resulting set of
run leases still satisfies every running job’s limit *and* the candidate’s:

```text
|running| + 1  <=  min(candidate N, every running N)
```

| Situation | Result |
|---|---|
| Machine idle, job with `1` | Starts alone |
| Two jobs with `3`, candidate with `3` | Starts as the third |
| Two jobs with `3`, candidate with `1` | Waits until the machine is empty |
| Job with `1` running, candidate with `3` | Waits |
| Job with `2` + candidate with `4` | Candidate may start; effective cap is `2` |

Default `1` means “unknown work is exclusive.” Raise `N` only when the
workload is safe next to *arbitrary* other managed jobs, not only next to
copies of itself.

The queue does **not** discover GPUs, meter VRAM/CPU, infer job types, or
preempt healthy work. Cooperation is the contract: submit through `mlq`, and
keep the command (and descendants) foregrounded in the runner’s process group
(no `setsid` / daemonizing).

### Protection and backfill

When a restrictive job reaches the head of the queue, it is **protected**.
While the attempts that originally blocked it are still advancing, equal-
priority eligible jobs may backfill open slots once each. When those blockers
drain, the frontier freezes so later arrivals cannot starve the protected job
indefinitely.

### Priority

Each job has a signed `--priority P` (default **0**). Eligible jobs are ordered
by **higher priority first**, then FIFO within the same priority.

- Priority never preempts a job that already holds a run lease.
- A protected job only allows equal-priority backfill.
- A newly eligible higher-priority job can replace a lower-priority
  reservation.
- Sustained high-priority submissions can starve lower-priority work—use
  nonzero values deliberately.

```bash
mlq submit --priority 1  --max-parallel-runs 1 -- COMMAND...   # ahead of default
mlq submit --priority -1 --max-parallel-runs 3 -- COMMAND...   # yields to default
mlq set-priority 12 2     # retune queued or held job only
```

## CLI

| Command | Purpose |
|---|---|
| `submit` | Enqueue a command (`--` then argv) |
| `status` | Live queue (leases, protection, reasons) |
| `status -f` | Most recent finished runs only |
| `subscribe` | Stream queue snapshots as NDJSON (no polling) |
| `show JOB` | Full job detail and attempts |
| `logs JOB` | Stdout/stderr; `--follow` exits with attempt outcome |
| `wait JOB` | Block until terminal (`--timeout`, exit codes below) |
| `cancel JOB` | Cancel; `--force` escalates to SIGKILL after grace |
| `hold` / `release` | Park and restore a queued job |
| `retry JOB` | Requeue a failed or lost job |
| `set-max-parallel-runs JOB N` | Change limit on queued **or live** jobs |
| `set-priority JOB P` | Change priority on **queued/held** jobs only |
| `follow-tts` | Speak completions and newly running work via local `tts` |
| `daemon …` | `status` / `run` / `install` / `uninstall` |
| `recover …` | List or resolve orphaned/quarantined attempts |

Common flags:

- `--json` on most commands for machine-readable output
- `--idempotency-key KEY` on mutations (auto-generated if omitted; safe to
  retry an identical request)
- Submit: `--name`, `--cwd`, `--env KEY=VAL`, `--inherit-env VAR`,
  `--max-attempts`, `--retry-delay`, `--time-limit`, `--after-success JOB`,
  `--after-terminal JOB`

`submit --time-limit DURATION` is a per-attempt wall-clock limit. It starts
when the runner spawns the command, so queue wait is excluded, while command
startup, compilation, and all descendants are included. Expiry sends SIGTERM
to the command process group, escalates to SIGKILL after the configured grace
period, and records a failed attempt; normal retry policy still applies.
The detached runner enforces the limit across daemon restarts; if that runner
dies, the daemon resumes enforcement from the persisted command start time.
As with any failed attempt, a separate user cancellation suppresses retries.

### `wait` and `logs --follow` exit codes

| Outcome | Exit |
|---|---|
| Success | `0` |
| Command failed | command’s exit code |
| Killed by signal | `128 + signal` |
| Skipped, lost, cancelled, or failed before launch | `1` |
| `--timeout` expired | `124` |

### `subscribe` for programmatic consumers

```bash
mlq subscribe | while IFS= read -r snapshot; do ...; done
```

`subscribe` keeps one connection to `mlqd` open and prints **one compact JSON
queue snapshot per line** (NDJSON, flushed immediately): the current state right
away, then a new line whenever the queue changes — submission, state transition,
lease change, priority or limit change. Each line is the daemon's raw `status`
payload, which (unlike `mlq status --json`) still carries recently finished jobs.
Consumers never replay events; bursts are coalesced into the newest snapshot
instead of a backlog, and a refresh identical to the previous line is skipped.

No rendering, no colours, no polling. It exits `0` when the daemon shuts down
and non-zero with a message on stderr if the daemon cannot be reached, so a
supervisor can restart it. Streams are budgeted at a quarter of
`max_connections`, so an excess subscriber is refused with
`too_many_subscriptions` rather than starving `submit`/`status`.
`mlq status --watch` remains the human, redrawing view.

### Changing a live limit

`set-max-parallel-runs` updates the job, its live attempt, and its run lease
atomically. Lowering a limit below the current number of active leases is
allowed: existing work keeps running (no preemption), and the scheduler
refuses new admissions until the active set drains to a compatible size.

## Submit recipe for agents

```bash
mlq submit \
  --name NAME \
  --max-parallel-runs N \
  --cwd /absolute/repo/path \
  -- COMMAND...
```

- Always set `--max-parallel-runs` explicitly (`1` for exclusive / unknown /
  multi-GPU / benchmarks).
- Omit `--priority` unless queue precedence is intentional.
- Use `--after-success JOB` for durable prerequisite gates and
  `--after-terminal JOB` for cleanup after any terminal outcome. Repeated
  dependency flags are ANDed. Metric-gated or output-derived pipelines still
  belong in the submitter.
- Secrets: prefer files over `--env` (env is stored in SQLite as plaintext).

A ready-made agent skill ships in [`skills/queue-ml-jobs/`](skills/queue-ml-jobs/).
Install or link it next to the CLI in agent environments.

## Durability

- **SQLite** (WAL, `synchronous=FULL`) under `$XDG_STATE_HOME/mlqueue`
- **Unix socket** under `$XDG_RUNTIME_DIR/mlqueue` with peer-UID checks
- Each attempt is supervised by a small detached **runner**; daemon restarts
  and upgrades do not kill workers—the new daemon adopts live runners and
  finalizes results exactly once
- A run lease is held until the attempt’s **entire process group** is gone
- Cancel is SIGTERM, then optional SIGKILL after a configured grace period
- A time-limited attempt always escalates to SIGKILL after the same grace
  period if its process group has not drained

## Development

```bash
cargo test        # unit + property tests and e2e (real daemons/runners/CLI)
cargo clippy
cargo install --path . --force && mlq daemon install   # rebuild + restart unit
```

The e2e suite (`tests/e2e.rs`) covers the concurrency formula, signed priority
ordering, windowed-then-frozen backfill, dependency gates and skip cascades,
live limit updates, cancellation, wall-clock limits, retries, idempotency
replay/conflict, and daemon-crash recovery.
