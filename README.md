# mlqueue

mlqueue is a machine-wide queue for coordinating local ML jobs across
repositories: one durable daemon (`mlqd`) plus a CLI (`mlq`). The daemon
admits arbitrary commands through a single per-job `maxParallelRuns`
declaration, so independently acting agents cannot start an unsafe number of
runs at once.

The design rationale, invariants, and full model live in [PLAN.md](PLAN.md).

## The admission model

Each job declares one number, `--max-parallel-runs N` (default `1`):

> This job is safe to run only when the total number of concurrent managed
> jobs, including itself, is no greater than N.

A candidate starts exactly when `|running| + 1 <= min(candidate limit, every
running limit)`. The default of `1` makes unspecified work exclusive; three
`--max-parallel-runs 3` jobs may share; a `1` job waits for the machine and
makes everything else wait for it. When a restrictive job reaches the head of
the queue, it is *protected*: jobs already submitted may backfill open slots
once each, but later submissions cannot pass it (a frozen backfill frontier
that bounds starvation without duration estimates).

The daemon deliberately does **not** discover GPUs, meter VRAM/CPU, infer
workload types, or preempt work. Cooperation is the contract: all managed
work goes through the queue, and commands must stay foregrounded in their
process group (no daemonizing/`setsid`).

## Quick start

```bash
cargo install --path .    # installs mlq + mlqd
mlq daemon install        # systemd user service (enable + start)
# or, without systemd:
mlq daemon run            # foreground daemon

mlq submit --name smoke --max-parallel-runs 1 -- python train.py --smoke
mlq status                # limits, protected job, admission reasons
mlq logs 1 --follow       # exits with the attempt's outcome
mlq wait 1 [--timeout 2h] # block until terminal; exit 0 / exit-code / 128+sig
mlq cancel 1 [--force]
```

Workflow commands: `hold`, `release`, `retry`, `set-max-parallel-runs`,
`recover list`, `recover resolve`, plus `--after-success`/`--after-completion`
dependency chains, `--max-attempts`/`--retry-delay` retry policy, and
`--json` everywhere. Every mutation takes an idempotency key
(`--idempotency-key`) and is safely retryable.

## Durability

- SQLite (WAL, `synchronous=FULL`) under `$XDG_STATE_HOME/mlqueue`; Unix
  socket under `$XDG_RUNTIME_DIR/mlqueue` with peer-UID checks.
- Every running attempt is supervised by a small detached runner process, so
  daemon crashes/restarts/upgrades never kill workers — the restarted daemon
  adopts live runners and finalizes durable results exactly once.
- Run leases are released only when an attempt's entire process group is
  provably empty; cancellation is SIGTERM, then opt-in SIGKILL after a grace
  period.

## Agent integration

`skills/queue-ml-jobs/` ships an agent skill that routes ML work through the
queue and picks conservative explicit limits (`1` for exclusive/unknown work,
`3` for small characterized CleanRL runs). Install or link it into agent
environments alongside the CLI.

## Development

```bash
cargo test        # scheduler/property, db, protocol unit tests + e2e suite
cargo clippy
```

The end-to-end suite (`tests/e2e.rs`) runs real daemons, runners, and CLI
binaries in isolated temp directories, covering the concurrency formula,
frozen-frontier backfill, cancellation, retries, dependency skips,
idempotency replay/conflict, and daemon-crash recovery.
