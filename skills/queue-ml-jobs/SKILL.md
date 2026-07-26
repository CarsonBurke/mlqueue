---
name: queue-ml-jobs
description: Queue and manage local machine-learning workloads with mlq. Use for local training, evaluation, preprocessing, or benchmarks that consume shared CPU or GPU resources, and for inspecting or controlling the mlq queue or daemon.
---

# Queue local ML work

- Submit managed workloads with `mlq submit`; never run them directly or bypass
  the queue. Leave work queued when busy; restore `mlqd` when unavailable.
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

- Encode sequencing with repeatable `--after-success JOB` or
  `--after-completion JOB` dependencies instead of shell backgrounding.
- Enqueue all immediately known independent jobs or dependency-DAG nodes before
  waiting. Then run `mlq wait JOB` for each independent job or terminal leaf
  and continue from their results. Treat submission alone as incomplete unless
  the user explicitly requests detached or fire-and-forget execution.
- Pass required environment explicitly. Values supplied through `--env` are
  stored as plaintext; make workloads read secrets from credential files.
- Change a queued or running job with
  `mlq set-max-parallel-runs JOB N` when evidence changes its safe limit. A
  decrease below the active-lease count is rejected; wait for enough jobs to
  finish and retry.
- Change a queued or held job's order with `mlq set-priority JOB P`. Running
  jobs cannot change priority (no preemption). Do not raise priority merely to
  jump the queue for your own work.
- Use `mlq status`, `show`, and `logs` to observe work, and `cancel` or
  `retry` to control it. Report the submitted job ID, chosen parallel limit,
  and any nonzero priority.
