---
name: queue-ml-jobs
description: Queue and manage local machine-learning workloads with mlq. Use for local training, evaluation, preprocessing, or benchmarks that consume shared CPU or GPU resources, and for inspecting or controlling the mlq queue or daemon.
---

# Queue local ML work

- Submit managed workloads with `mlq submit`; never run them directly or bypass
  the queue.
- You may want to follow the job with `mlq wait JOB`.
- You may need to start `mlqd` if it fails.
- Keep each workload and its descendants foregrounded in the runner's process
  group. Restructure commands that daemonize or call `setsid`.
- Choose `--max-parallel-runs N` explicitly for every submission. It asserts
  that the job is safe while at most `N` total managed jobs are running,
  regardless of workload type; it is not a utilization target. Use `1` for
  unknown, exclusive, multi-GPU, or benchmark-sensitive work. Raise it only
  from evidence that mixed concurrent workloads remain safe.
- Omit `--priority` for the default priority `0`. Use a signed value only when
  queue precedence is intentional. Higher-priority eligible jobs rank first;
  equal priorities retain the queue's FIFO/backfill rules, and running jobs
  are never preempted. Never increase priority merely to advance your own work.

```bash
mlq submit --name NAME --max-parallel-runs N \
  --cwd /absolute/repository/path -- COMMAND...
```

- Add `--time-limit DURATION` when a hard per-attempt wall-clock bound is
  operationally reasonable. Choose a defensible, generous duration from
  workload evidence; queue wait is excluded, but command startup, compilation,
  and descendants count toward the limit. Prefer a limit when an overlong run
  likely indicates a hang or would waste shared resources. Leave the job
  unlimited when valid runtimes are too variable to bound safely.
- For sequential or conditional work, use durable queue dependencies:
  `--after-success JOB` runs only after a successful prerequisite, while
  `--after-terminal JOB` runs after any terminal outcome. Repeat flags for an
  AND gate. Use `mlq wait` in the submitter for metric gates, output-derived
  commands, or other conditions the queue cannot express.
- Don't waste tokens constantly polling.
- Pass required environment explicitly. Values supplied through `--env` are
  stored as plaintext; make workloads read secrets from credential files.
- Change a queued or running job with
  `mlq set-max-parallel-runs JOB N` when evidence changes its safe limit.
  Lowering below the current active-lease count is allowed: existing work
  keeps running, and new admissions wait until the set drains.
- Change a queued or held job's order with `mlq set-priority JOB P`. Running
  jobs cannot change priority (no preemption). Do not raise priority merely to
  jump the queue for your own work.
- Use `mlq status`, `show`, and `logs` to observe work, and `cancel` or
  `retry` to control it. Report the submitted job ID, chosen parallel limit,
  chosen time limit (or the decision to leave it unlimited), and any nonzero
  priority.
- If `mlq status` reports the queue is paused, new work will not start until
  an operator runs `mlq unpause`. Do not pause or unpause the shared queue
  unless the user asked.
