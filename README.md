# mlqueue

A shared queue for expensive local work: training, evaluation, preprocessing,
and benchmarks. Submit commands with `mlq`; `mlqd` schedules them for the same
Linux user. Jobs run alone by default and survive daemon restarts.

## Install

Requires Linux and Rust with 2024 edition support.

```bash
git clone https://github.com/CarsonBurke/mlqueue.git
cd mlqueue
git checkout v0.3.0
cargo install --path . --locked
mlq daemon install
```

Keep `~/.cargo/bin` on your `PATH`. Without systemd, run `mlq daemon run` in a
terminal. For an existing installation, see [Upgrade](#upgrade).

## Use

```bash
mlq submit --name train -- python train.py
mlq status                       # queue and reasons jobs are waiting
mlq see 42                       # job details and recent stdout
mlq logs 42 --follow              # stream stdout; --stderr for stderr
mlq follow 42                    # wait and exit with the job's outcome
mlq cancel 42
```

Submission prints the job ID. Commands run in the submission directory, with
arguments after `--` passed directly, without a shell. Use `--cwd` to choose
another directory and `--env KEY=VALUE` or `--inherit-env VAR` for environment
variables beyond the small baseline. Environment values are stored in plaintext.

`follow` exits 124 after 55 minutes without stopping the job. Change the wait
with `--timeout 4h`. Use `show 42 --json` for attempt history or `subscribe` for
streaming queue snapshots.

## Scheduling

`--max-parallel-runs N` declares that a job is safe with at most N managed jobs
active, including itself. A start must fit every active job's cap. Three cap-3
jobs can share the machine; a default cap-1 job runs alone.

The queue does not measure GPU memory or CPU load, or count external work.
Keep commands and descendants in the foreground so they stay supervised.

```bash
mlq submit --max-parallel-runs 3 -- python evaluate.py
mlq submit --after-success 42 -- python report.py
mlq submit --max-attempts 3 --retry-delay 30s -- python train.py
mlq submit --time-limit 2h -- python benchmark.py
mlq pause                        # stop new starts; running jobs continue
mlq unpause
```

Higher `--priority` values run first; the default is 0. Running jobs are not
preempted. Time limits apply per attempt, excluding queue wait. Use `hold`,
`release`, `retry`, `set-priority`, `set-max-parallel-runs`, and `set-time-limit`
to adjust jobs. See `mlq <command> --help` for options.

Commands that checkpoint and resume can opt into `--yield-signal SIGUSR1`.
`mlq yield JOB --reason TEXT` asks them to checkpoint and exit 75, then requeues
them under the same ID without consuming a retry attempt. See the
[yield protocol](ARCHITECTURE.md#cooperative-yield) before opting in.

## Upgrade

From v0.2.0, wait until all queued, held, running, and recovery-blocked jobs
have finished. From the new release checkout:

```bash
systemctl --user stop mlqd
cargo install --path . --locked --force
systemctl --user start mlqd
```

Without systemd, stop and restart the foreground daemon around installation.
Update both binaries. v0.3.0 uses protocol 8 and schema 10; back up the state
directory before upgrading if you need to roll back. For installations that
already support subscriptions, [the staged updater](contrib/mlq-update) can
wait for the queue to empty before activating a new daemon.

Configuration: `~/.config/mlqueue/config.toml`. State and logs:
`~/.local/state/mlqueue/`. Both follow XDG overrides.

[Architecture and recovery](ARCHITECTURE.md) ·
[Agent skill](skills/queue-ml-jobs/SKILL.md) · `mlq --help`
