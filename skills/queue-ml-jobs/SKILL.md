---
name: queue-ml-jobs
description: Submit and manage local training, evaluation, preprocessing, and benchmarks through mlq, or inspect its queue and daemon.
---

# Queue local ML work

Use `mlq` for expensive local workloads so they share the machine's queue.

```bash
mlq submit --name NAME --cwd /path/to/repo -- COMMAND...
mlq follow JOB
```

Submission returns a job ID. Report it so the work can be found later.
`follow` waits for the outcome and exits with its status. Its default 55-minute
timeout exits 124 without stopping the job; reattach rather than resubmit.

Jobs run alone by default. Set `--max-parallel-runs N` when a job is safe with
N total managed jobs active, including itself. Every active job's cap must fit;
the queue does not measure resource usage or count work started outside it.
Keep workloads in the foreground so their descendants stay supervised.

Useful submission options:

- `--env KEY=VALUE` or `--inherit-env VAR`: pass required environment explicitly.
  Values are stored in plaintext; use credential files for secrets.
- `--after-success JOB` or `--after-terminal JOB`: declare dependencies.
- `--max-attempts N --retry-delay 30s`: retry failed attempts.
- `--time-limit 2h`: bound each attempt from command spawn, excluding queue wait.
- `--priority P`: change ordering; higher values run first, default 0.

Use `status` for the queue, `see JOB` for details and recent output, and
`logs JOB --follow` to stream stdout (`--stderr` for stderr). `show JOB --json`
returns attempt history; `subscribe` streams queue snapshots.

Use `cancel`, `retry`, `hold`, `release`, and the `set-*` commands as needed.
`pause` stops new starts across the queue; running work continues.

For checkpoint-capable commands, `--yield-signal SIGUSR1` opts into yielding.
The command must handle the signal, checkpoint, and exit 75 so the same command
can resume later. Read the [yield protocol](../../ARCHITECTURE.md#cooperative-yield)
when implementing this. See the [README](../../README.md#upgrade) for upgrades
and `mlq <command> --help` for other options.
