# mlqueue

A shared queue for expensive work on one Linux machine: training runs,
evaluations, preprocessing, and benchmarks. Submit a command from any shell or
agent running as the same user. `mlqd` decides when it can start; `mlq` lets you
inspect it, follow its outcome, or change the queue.

Jobs run alone by default. Running commands survive daemon restarts, and the
queue keeps job state in SQLite and captures stdout and stderr in files.

## Install

You need Linux and a Rust toolchain with support for the 2024 edition. For a
first installation:

```bash
git clone https://github.com/CarsonBurke/mlqueue.git
cd mlqueue
git checkout v0.3.0
cargo install --path . --locked       # installs mlq and mlqd into ~/.cargo/bin
mlq daemon install                   # enables and starts a systemd user service
```

Make sure `~/.cargo/bin` is on your `PATH`. Without systemd, keep
`mlq daemon run` running in a terminal. For an existing installation, use the
[upgrade instructions](#upgrade).

## Submit and follow a job

```bash
mlq submit --name smoke -- python train.py --smoke
```

Submission prints a job ID. For job 42:

```bash
mlq status                           # live queue and reasons jobs are waiting
mlq see 42                           # job details and latest stdout
mlq logs 42 --follow                  # stream stdout; add --stderr for stderr
mlq follow 42                        # wait for the outcome; exit with its code
mlq status --finished                # recent finished jobs
```

`mlq follow` prints only the final outcome by default. It exits 124 after 55
minutes of waiting; the job keeps running. Set a different deadline with
`--timeout 4h`, or select progress events with `--events started,retry,yielded`.
For scripts, `mlq show 42 --json` returns job details and attempt history;
`mlq subscribe` streams queue snapshots as newline-delimited JSON.

Everything after `--` is an argument to the command, passed without a shell.
The working directory defaults to where you submit; use `--cwd /path/to/repo`
to choose another. The queue records the repository and worktree when available.

The environment is captured at submission: `PATH`, `HOME`, `USER`, `LOGNAME`,
`SHELL`, `LANG`, and `TMPDIR`. Pass other values explicitly:

```bash
mlq submit --env CUDA_VISIBLE_DEVICES=0 --inherit-env VIRTUAL_ENV \
  -- .venv/bin/python train.py
```

Environment values are stored in plaintext. Have jobs read secrets from
credential files.

## Choose what can run together

`--max-parallel-runs N` declares that a job is safe with at most N managed jobs
active at once, including itself:

```bash
mlq submit --max-parallel-runs 3 -- python evaluate.py
```

A job starts only when the new total fits its own cap and every active job's
cap. Three cap-3 jobs can share the machine. A default cap-1 job waits for them
to finish, then runs alone. Starting jobs and process groups still shutting
down count toward the total; queued and held jobs do not.

mlqueue does not measure GPU memory or CPU load, and work started outside the
queue does not count. Leave the cap at 1 unless the job is safe alongside any
other admitted workload. Keep commands and their descendants in the foreground;
processes that daemonize or call `setsid` escape queue supervision.

Higher `--priority` values take precedence; the default is 0. Equal priorities
use FIFO order with bounded backfill so compatible jobs can use spare capacity
while a restrictive job waits. Running jobs are never preempted by priority.

## Dependencies, retries, and time limits

```bash
mlq submit --after-success 42 -- python evaluate.py
mlq submit --after-terminal 42 -- python cleanup.py
mlq submit --max-attempts 3 --retry-delay 30s -- python train.py
mlq submit --time-limit 2h -- python benchmark.py
```

Repeat dependency flags to wait for several jobs. A success dependency waits
through retries and yields; if its prerequisite finishes unsuccessfully, the
dependent job is skipped. Dependencies survive daemon restarts.

Time limits apply to each attempt from command spawn, including startup and
compilation, and exclude time in the queue. An expired attempt receives SIGTERM,
then SIGKILL after the grace period if needed. Change a queued or live job's
limit with `mlq set-time-limit 42 3h`, or remove it with `--clear`. The clock
stays anchored at command spawn; shortening a limit below elapsed time stops
the attempt.

## Control the queue

```bash
mlq pause                            # stop new starts; running jobs continue
mlq unpause
mlq hold 42                          # hold a queued job
mlq release 42
mlq set-priority 42 10                # change a queued or held job's priority
mlq set-max-parallel-runs 42 2        # change a queued or live job's cap
mlq cancel 42                        # send SIGTERM to running work
mlq cancel 42 --force                # escalate to SIGKILL after the grace period
mlq retry 42                         # give a failed or lost job one more attempt
```

Lowering a live concurrency cap lets existing work drain; it does not kill jobs.
The pause state is durable across daemon restarts.

## Yield at a checkpoint

A command that can checkpoint and resume can opt into an operator-requested
yield:

```bash
mlq submit --yield-signal SIGUSR1 -- python train.py --resume auto
mlq yield 42 --reason "let the eval queue drain"
```

The command must handle SIGUSR1, save a checkpoint when it can, and exit with
code 75 (or its configured `--yield-exit-code`). The signal is sent once to the
command's leader process and is never escalated. A wrapper must `exec` the
trainer or forward the signal. If no checkpoint is ready, the command should
remember the request and yield after its next checkpoint.

After yielding, the job keeps its ID and returns to the back of its priority
class without consuming a retry attempt. Add `--hold` to the yield request to
keep it held until `mlq release`. Each attempt receives `MLQ_JOB_ID` and
`MLQ_ATTEMPT` in its environment.

Unless `--hold` is set, another eligible job at the same or a higher priority
must be waiting; otherwise the yield request is refused.

`mlq yield` requires confirmation in a terminal. Noninteractive requests are
refused unless the daemon config sets `allow_noninteractive_yield = true`.

## Upgrade

Upgrade `mlq` and `mlqd` together: v0.3.0 uses protocol 8 and database schema 10.
The daemon migrates the database on startup and refuses schemas newer than it
supports. Back up the state directory before upgrading if you need a rollback
path; there is no schema downgrade command.

**From v0.2.0:** wait until no queued, held, running, or recovery-blocked jobs
remain. From a checkout of v0.3.0, stop the systemd service, install both
binaries, and start it again:

```bash
systemctl --user stop mlqd
cargo install --path . --locked --force
systemctl --user start mlqd
mlq --version
mlq daemon status
```

**With the staged updater:** if your current installation already supports
`mlq subscribe` and `mlq daemon status --json`, you can stage a daemon while
jobs are busy. The updater needs Python 3.11 or later and systemd; it waits for
no unfinished jobs or active leases before replacing and restarting `mlqd`.
From the new release checkout:

```bash
cargo build --release --locked
python3 contrib/mlq-update install
~/.local/bin/mlq-update stage --mlqd target/release/mlqd
~/.local/bin/mlq-update status
```

Staging copies the current CLI as an observer for the old daemon. Held, queued,
and recovery-blocked jobs all keep the update pending. Once `status` reports
no pending update, install the matching CLI:

```bash
cargo install --path . --locked --force --bin mlq
mlq --version
mlq daemon status
```

Without systemd, wait for the queue to finish, stop the foreground daemon,
install both binaries with `cargo install --path . --locked --force`, and start
`mlq daemon run` again.

## Configuration and further reading

Configuration lives at `~/.config/mlqueue/config.toml`; queue state and logs
live at `~/.local/state/mlqueue/`. Both follow the XDG directory overrides.
Configuration changes take effect after a daemon restart.

- [Architecture](ARCHITECTURE.md): admission rules, recovery, storage, protocol,
  configuration, and limitations.
- [Agent skill](skills/queue-ml-jobs/SKILL.md): how agents should submit and
  monitor shared workloads.
- `mlq --help` and `mlq <command> --help`: all commands and options.
