---
name: queue-ml-jobs
description: Queue and manage local machine-learning workloads with mlq. Use for local training, evaluation, preprocessing, or benchmarks that consume shared CPU or GPU resources, and for inspecting or controlling the mlq queue or daemon.
---

# Queue local ML work

- Submit managed workloads with `mlq submit`; never run them directly or bypass
  the queue.
- Use `mlq follow JOB` instead of the removed `wait` command. It subscribes until
  the final outcome or a 55-minute observation timeout, and exits with the
  command's status (or `128 + signal`) on completion. Plain `follow` prints only
  final output: no polling, heartbeats, log tails, hooks, checks, or LLM calls.
  On timeout it prints `follow timed out after 55 minutes` and exits `124`;
  the job is untouched. `--timeout DURATION` overrides the observation duration.
- The 55-minute default leaves headroom for a 60-minute agent KV-cache TTL.
  After timeout, reassess: reattach with `mlq follow JOB` only if waiting is still
  useful; otherwise record the job ID and stop following. Never resubmit or retry
  the workload solely because following timed out. Avoid an unattended shell
  loop around `follow`: return control to the agent between observation windows.
- Opt into intermediate events only when actionable: `--events started,retry`
  or an explicit `--config follow.toml`. Checks require `warning` selection.
  `--only-finish` overrides intermediate events/checks; `--no-hooks` also disables
  explicitly configured finish notifications. `--json` emits compact NDJSON,
  not full job history; use `show JOB --json` for detail. Never poll an LLM to
  interpret metrics.
- You may need to start `mlqd` if it fails.
- When authorized to update this installation, build with `cargo build --release
  --locked`, then `mlq-update stage --mlqd target/release/mlqd` before upgrading
  the CLI with `cargo install --path . --locked --force --bin mlq`. The configured
  updater activates only when no unfinished jobs or active leases remain.
  Do not directly replace/restart a busy daemon or install both binaries.
  `mlq-update status` shows the pending candidate; staging never fetches Git.
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
  AND gate. Use `mlq follow` in the submitter for output-derived commands, or
  explicitly configure cheap local warning checks for metric conditions. Checks
  notify; they never cancel/retry the job. Keep keys stable and advance metric
  steps only for new observations so persistent findings do not waste tokens.
- Pass required environment explicitly. Values supplied through `--env` are
  stored as plaintext; make workloads read secrets from credential files.
- Change a queued or running job with
  `mlq set-max-parallel-runs JOB N` when evidence changes its safe limit.
  Lowering below the current active-lease count is allowed: existing work
  keeps running, and new admissions wait until the set drains.
- Change a queued or held job's order with `mlq set-priority JOB P`. Running
  jobs cannot change priority (no preemption). Do not raise priority merely to
  jump the queue for your own work.
- Change a queued or running job's wall-clock bound with
  `mlq set-time-limit JOB DURATION`, or remove it with `--clear`. The clock
  stays anchored at command spawn. Shortening below the time already elapsed
  stops that attempt; once termination has started, extending it does not
  revive the attempt. An earlier cancellation stays cancelled. Later retries
  use the updated limit.
- Use `mlq status`, `show`, `see`, and `logs` to observe work, and `cancel` or
  `retry` to control it. Report the submitted job ID, chosen parallel limit,
  chosen time limit (or the decision to leave it unlimited), and any nonzero
  priority.
- If `mlq status` reports the queue is paused, new work will not start until
  an operator runs `mlq unpause`. Do not pause or unpause the shared queue
  unless the user asked.

