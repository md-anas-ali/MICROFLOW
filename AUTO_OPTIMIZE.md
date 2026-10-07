# Auto Optimize

> Auto Optimize attempts to release MacroFlow-owned resources and provides a
> 60-second stabilization window before the next Service.

It is an **additive, best-effort** layer. It does not promise that RAM or CPU
drop to zero, that OS caches are cleared, that HTTPS connections are fully
reset, or that the system is "fresh" after the cooldown. The Go runtime, the
allocator and the OS decide how much memory is really returned.

## Lifecycle

Every workflow run already passes through the single global queue
(`internal/scheduler/queue.go`, max concurrency 1). Auto Optimize hooks into it:

```
Service run (runner.runOnce)
  └─ deferred Release of THIS execution's resources   <- runs on success / error /
                                                          timeout / cancel / panic
Queue worker, after the job:
  PostService   global steps in phase order, then verification
  (lastFinish = now)
Queue worker, before the next job:
  Cooldown      remainder of the 60 s since the previous Service finished
  PreServiceCheck  health check + safe recovery of stale resources (never blocks the job)
  Next Service
```

The cooldown applies to every path through the queue (schedules, manual Run,
webhooks, Run Service / Run All steps, recovery). It is measured from the end of
the previous Service's cleanup, so a Service that arrives later than 60 s after
the previous one starts without extra waiting; the very first job never waits.

## What is tracked and cleaned (per execution = `autoopt.ServiceContext`)

| Resource | How |
|---|---|
| Child processes (`executeCommand`) | `ServiceContext.Run` replaces `cmd.Run()` (same semantics). On unix the child gets its own process group: a timeout kills the whole group, and any member still alive after the command ended is terminated `SIGTERM -> grace -> SIGKILL` and verified. Only the group MacroFlow created is ever signalled. |
| HTTP response bodies (`httpRequest`, Code-node helper) | Body is wrapped; reads/close behave identically. An unclosed body is closed at release. The shared `http.Client` / connection pool is **never** closed (the next Service needs it); only idle keep-alive connections are dropped, as before. |
| Background tasks / timers / listeners / sockets / streams / temp paths / large refs | Generic registry on the context (`TrackCancel`, `TrackStop`, `TrackCloser`, `TrackTemp`, `TrackRelease`). Temp paths are only removed strictly inside the configured temp roots. |
| Scratch dirs, stray `/tmp` leftovers, idle keep-alive conns, `FreeOSMemory` | The **existing** between-run cleanup, reused as global steps (not duplicated). |

Cleanup order (phases, each time-boxed, panic-isolated; a failure logs a warning
and the next phase still runs): stop new work → cancel background tasks → wait
briefly → terminate leftover processes → close HTTP bodies → release
sessions/sockets → stop timers/listeners → remove temp resources → drop large
refs → runtime cleanup → verify. Phases with nothing registered are skipped; the
dominant resource kind of a Service (process- or HTTP-heavy) gets a doubled time box.

## Configuration (all optional)

| Variable | Default | Meaning |
|---|---|---|
| `MICROFLOW_AUTO_OPTIMIZE` | on | `0`/`off`/`false` disables the whole layer and restores the previous behaviour (10 s cooldown, original cleanup hook). |
| `SERVICE_COOLDOWN_SECONDS` | `60` (`10` when Auto Optimize is off) | An explicit value always wins. |
| `MICROFLOW_AUTO_OPTIMIZE_PHASE_TIMEOUT_SECONDS` | `10` | Time box per cleanup phase / step. |
| `MICROFLOW_AUTO_OPTIMIZE_TERM_GRACE_SECONDS` | `3` | SIGTERM grace before SIGKILL. |

## Logging

Lines are prefixed `[AutoOptimize]`: `Service completed`, `Starting cleanup`,
`Child processes checked`, `HTTP resources released`, `Network resources
released`, `Timers/background tasks checked`, `Temporary resources cleaned`,
`Cleanup verification completed`, `Starting N-second cooldown`, `Cooldown
completed`, `Pre-service health check passed`, `Next service allowed`, and
`Cleanup warning: ...` on problems. Only binary base names are logged for
processes — never arguments, environment, headers or bodies.

## Known limits

- Best-effort only; see the disclaimer at the top.
- A pre-service health problem is logged but never blocks a Service (consistent
  with MacroFlow not refusing scheduled work).
- Cooldown delays the *start* of a queued/scheduled Service by up to 60 s after
  the previous one finished.
- Windows: no process-group handling (default kill behaviour is unchanged).
- A node whose child keeps the stdout pipe open can still keep `Wait` blocked
  until that child exits (pre-existing behaviour, intentionally not changed).
- Running MacroFlow in a terminal on unix and pressing Ctrl-C no longer reaches
  `executeCommand` children (they are in their own process group); they end at
  their own timeout.
