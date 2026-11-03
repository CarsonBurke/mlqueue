# mlqueue

mlqueue is one queue for expensive local work, such as training runs,
evaluations, and benchmarks, submitted from any shell or agent running as the
same Linux user. You submit commands with `mlq`. The `mlqd` daemon
starts each one when it is safe to, and running jobs survive daemon restarts.

## Install

```bash
cargo install --path . --locked   # installs mlq and mlqd into ~/.cargo/bin
mlq daemon install                # runs mlqd as a systemd user service
```

Without systemd, keep `mlq daemon run` going in a spare terminal.

## Use

```bash
mlq submit --name smoke -- python train.py --smoke
```

The command runs in the current directory, with everything after `--` passed
as-is (no shell). It gets a small environment captured at submission (`PATH`,
`HOME`, `LANG`, and a few others); pass anything else with `--env KEY=VALUE` or
`--inherit-env VAR`. `mlq submit` prints the job's ID; with ID 42:

```bash
mlq status              # the queue, and why waiting jobs are waiting
mlq see 42              # job detail and latest stdout
mlq logs 42 --follow    # stream stdout; --stderr for stderr
mlq follow 42           # wait for the outcome, exit with the command's code
mlq cancel 42
```

`mlq follow` stops waiting after 55 minutes by default and exits 124, but the
job keeps running. Retries, dependencies, time limits, priorities, and holds
are covered by `mlq <command> --help`.

## Running jobs side by side

By default a job runs alone. `--max-parallel-runs N` declares that a job is
safe while at most N managed jobs, counting itself, run at once:

```bash
mlq submit --max-parallel-runs 3 -- python evaluate.py
```

The count covers every currently active managed job (starting, running, or
still shutting down), not just copies of the same command; queued and held jobs
don't count. A job starts only if the new total fits its own cap and the cap of
every active job. So two cap-3 jobs can be joined by a third cap-3 job, but
a default job waits until both finish, and while it runs nothing else starts.

mlqueue does not look at GPU memory or CPU load, and it doesn't count work
started outside it. Choose N for what is safe next to any other job, and leave
it at 1 when that depends on what else is running.

## Yielding a long job

A job that can checkpoint and resume can opt in to being stopped and requeued:

```bash
mlq submit --yield-signal SIGUSR1 -- python train.py --resume auto
mlq yield 42 --reason "let the eval queue drain"    # from a terminal
```

On SIGUSR1 the command should save a checkpoint and exit with code 75. mlq
sends the signal once, to the command's own process, and never forces it, so a
command with nothing to save yet should note the request and exit 75 after its
next checkpoint. `mlq yield` asks for confirmation and refuses to run without
a terminal unless the daemon config sets `allow_noninteractive_yield = true`. On exit 75 the job goes to the
back of the queue with the same ID, so jobs waiting on it with
`--after-success` still wait, and it doesn't use up a `--max-attempts`
attempt. Every attempt gets `MLQ_JOB_ID` and `MLQ_ATTEMPT` in its environment.

Agents can use the [queue-ml-jobs skill](skills/queue-ml-jobs/SKILL.md).
[ARCHITECTURE.md](ARCHITECTURE.md) covers admission and priority, crash
recovery, storage, and configuration.
