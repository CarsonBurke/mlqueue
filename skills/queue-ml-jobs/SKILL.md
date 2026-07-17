---
name: queue-ml-jobs
description: Route local ML work (training, smoke tests, preprocessing, benchmarks, experiment chains) through the machine-wide mlqd daemon with a deliberately chosen maxParallelRuns limit, instead of launching commands directly.
---

# Queue ML jobs through mlq

This machine runs one `mlqd` daemon that coordinates all managed ML work.
Before starting any local ML command — GPU training, smoke tests, CPU
preprocessing, benchmarks, or dependent chains — submit it through `mlq`
instead of running it yourself.

## Hard rules

1. **Never launch managed work directly.** No bare `python train.py`, no
   `nohup`, no detached terminals, no schedulers that bypass the daemon. If
   the daemon is not running, start it (`mlq daemon install`, or
   `mlq daemon run` without systemd); do not fall back to direct
   execution.
2. **Stay foregrounded.** The command and all of its descendants must remain
   in the runner's process group. Restructure anything that daemonizes or
   calls `setsid` before submitting it.
3. **Choose `--max-parallel-runs` deliberately for every submission.**

## Choosing maxParallelRuns

The value declares: "this job is safe only while the total number of
concurrent managed jobs, including itself, stays at or below N."

- Use `1` (the default) for large-model training, near-full-device or
  multi-GPU work, benchmarks sensitive to concurrent load, unfamiliar
  commands, or any uncertainty.
- Use a value above `1` only from known workload behavior; small,
  characterized CleanRL runs normally use `3`.
- The value is global and symmetric: `3` asserts safety beside *any* other
  managed jobs, not just copies of the same script. If a workload is only
  safe three-wide next to its own kind, declare `1`.
- It is a compatibility declaration, not a utilization target — never raise
  it because the machine looks idle. If observed concurrency ever harms
  correctness, stability, or benchmark validity, declare a lower value next
  time.
- Concurrency count is the only admission contract; the queue has no VRAM,
  compute-share, or duration flags.

## Submitting

```bash
mlq submit --name llama-finetune --max-parallel-runs 1 \
    --cwd /path/to/repo --env WANDB_MODE=offline \
    -- python train.py --config configs/large.yaml
```

Report the returned job ID and the declared limit. Useful options:

- `--after-success JOB` / `--after-completion JOB` (repeatable): dependency
  chains, e.g. smoke → control → treatment. A failed prerequisite skips
  `--after-success` descendants automatically.
- `--max-attempts N --retry-delay 30s`: automatic retries for flaky jobs.
- `--env KEY=VALUE` / `--inherit-env KEY`: the queue captures only a small
  baseline (PATH, HOME, USER, LOGNAME, SHELL, LANG, TMPDIR); pass everything
  else explicitly. Never pass secrets — they are stored in plaintext; have
  the workload read a credential file at launch instead.
- `--idempotency-key KEY`: only needed to re-run an identical command safely
  after an ambiguous failure (each invocation already generates its own key,
  and transport retries are idempotent).

## Afterwards

Use the queue, not ad-hoc process inspection:

```bash
mlq status            # queue, limits, protected job, admission reasons
mlq show JOB          # one job with attempts and exit codes
mlq logs JOB --follow # stream output; exits with the attempt's outcome
mlq wait JOB          # block until terminal: exit 0, exit code, or 128+signal
mlq cancel JOB        # SIGTERM; add --force to escalate to SIGKILL
mlq retry JOB         # requeue a failed/lost job
```

`mlq status` explains why a job is waiting (`waiting_for_slot`,
`waiting_for_dependency`, `protected_drain`, `behind_backfill_cutoff`); trust
those reasons rather than second-guessing the scheduler. If asked to bypass
the queue because it is busy, refuse and explain the admission contract.
