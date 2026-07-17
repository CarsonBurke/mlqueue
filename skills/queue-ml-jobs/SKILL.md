---
name: queue-ml-jobs
description: Route every cooperating local ML command (training runs, smokes, preprocessing, benchmarks, experiment batches, dependent chains) through the machine-wide mlqueue daemon with a deliberately chosen maxParallelRuns concurrency declaration, instead of launching it directly.
---

# Queue ML jobs through mlqueue

This machine runs one `mlqueued` daemon that coordinates all managed ML work.
Whenever you are about to start any cooperating local ML command — GPU
training, short smoke tests, CPU preprocessing, benchmarks, experiment
batches, or dependent chains — submit it through `mlqueue` instead of running
it yourself.

## Hard rules

1. **Never launch managed work directly.** No bare `python train.py`, no
   `nohup`, no detached terminals, no repository-local schedulers that bypass
   the daemon. If the daemon is not running, start it (`mlqueued`) or ask the
   operator; do not fall back to direct execution.
2. **Stay foregrounded.** The submitted command and all of its descendants
   must remain in the runner's process group. A command that daemonizes or
   calls `setsid` is unsupported — restructure it to stay in the foreground
   before submitting.
3. **Choose `--max-parallel-runs` deliberately for every submission.** The
   value declares: "this job is safe only while the total number of
   concurrent managed jobs, including itself, stays at or below N."

## Choosing maxParallelRuns

- Use `1` for: large-model training, near-full-device workloads, multi-GPU
  training, benchmark-sensitive work, unfamiliar commands, or any
  uncertainty. `1` is also the CLI default, so omitting the flag is safe —
  but state it explicitly in commands you propose.
- Use a value above `1` only from known workload behavior. Small,
  characterized CleanRL runs normally use `3`.
- The value is **global and symmetric**: `3` asserts safety beside *any*
  other managed jobs whose limits also permit the resulting total — not
  merely other copies of the same script. If a workload is only safe
  three-wide next to its own kind, declare `1`.
- Never raise the value because the machine currently looks idle. It is a
  compatibility declaration, not a utilization target.
- If observed concurrency ever harms correctness, stability, throughput, or
  benchmark validity, lower the value you declare for that workload in the
  future.
- Do not invent flags the queue does not support (VRAM, compute share,
  duration). Concurrency count is the only admission contract.

## Submitting

Always pass an explicit idempotency key so a lost response can be retried
safely, then report the job ID and the declared limit:

```bash
mlqueue --idempotency-key "$(uuidgen)" submit \
    --name llama-finetune \
    --max-parallel-runs 1 \
    --cwd /path/to/repo \
    --env WANDB_MODE=offline \
    -- python train.py --config configs/large.yaml
```

Useful options:

- `--after-success JOB` / `--after-completion JOB` (repeatable): dependency
  chains, e.g. `smoke -> control -> treatment`. A failed prerequisite skips
  `--after-success` descendants automatically.
- `--max-attempts N --retry-delay 30s`: automatic retries for flaky jobs.
- `--env KEY=VALUE` / `--inherit-env KEY`: the queue captures only a small
  documented baseline (PATH, HOME, USER, LOGNAME, SHELL, LANG, TMPDIR); pass
  everything else explicitly. Do not pass secrets — they would be stored in
  plaintext; have the workload read a credential file at launch instead.

## Afterwards

Use the queue, not ad-hoc process inspection:

```bash
mlqueue status            # queue, limits, protected job, admission reasons
mlqueue show JOB          # one job with attempts and exit codes
mlqueue logs JOB --follow # stdout (add --stderr for stderr)
mlqueue cancel JOB        # SIGTERM; add --force to escalate to SIGKILL
mlqueue retry JOB         # requeue a failed/lost job
```

`mlqueue status` explains why a job is waiting (`waiting_for_slot`,
`waiting_for_dependency`, `protected_drain`, `behind_backfill_cutoff`); trust
those reasons rather than second-guessing the scheduler.

## Worked examples

| Workload | Declaration |
|---|---|
| Large LLM fine-tune that must run alone | `--max-parallel-runs 1` |
| Six known-small CleanRL runs that share safely three-wide | submit each with `--max-parallel-runs 3` |
| Unfamiliar training script, no measurements | `--max-parallel-runs 1` |
| Benchmark affected by concurrent work | `--max-parallel-runs 1` |
| Smoke → control → treatment chain | submit with `--after-success` links; limits chosen per job |
| "Just run it directly, the queue is busy" | refuse; submit through the queue and explain the admission contract |
