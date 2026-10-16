---
name: sync-branching-chain
description: Work safely as the initial coordinator or an alternating chain actor in the Sync Branching Chain flow. Use for three-chain planning, isolated chain work between barriers, the adoption decision in the first session after a barrier, durable reports and artifacts, checkpoints, and resume.
---

# Sync Branching Chain

Treat the runtime prompt as the authoritative role, objective, epoch, session count, and turn
identity. Never infer ownership or barrier state from the current directory alone.

Read [chains.md](references/chains.md) before acting. It defines isolation, the barrier, the
adoption protocol, reports, artifacts, and alternating fresh sessions.

Then route by role:

- Initial coordinator: plan only. Give the three chains materially different, falsifiable
  directions. The chains are equal. Lane 1 works in the original source but has no integration
  duty.
- Chain actor, ordinary session: work only inside your chain and return an evidence-backed
  `LaneReport`.
- Chain actor, first session after a barrier (the prompt says `BARRIER k JUST COMPLETED`):
  follow the adoption protocol in chains.md before continuing your own direction.
- A resumed run, changed TASK.md, or checkpoint recovery: also read
  [state-events.md](references/state-events.md).

This flow owns local coordination, including task-provided local evaluator submissions. Those
evaluator calls do not authorize uploading, deploying, publishing, remotely submitting to a
competition or service, purchasing, messaging, or any other remote action.
