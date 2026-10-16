# Chains, barriers, and adoption

## Fixed topology

The coordinator plans once and does not return. Three chains run concurrently. Each chain is one
lane whose actors A and B alternate in fresh sessions, and each session inherits the chain's
retained worktree and its previous report.

- Lane 1's worktree is the original source. Lanes 2 and 3 work in private snapshots.
- The chains are equal. None integrates another and none is replaced.
- All three chains may run task-provided local evaluators and submit reconstructable candidates.

Read the repository at the start of every session. Conversational memory does not cross actors or
sessions. Leave the next actor a coherent worktree, tests, and a precise report.

## Isolation between barriers

Between barriers a chain sees only its own work: its cwd, its artifact root, its checkpoint, its
own report log, and the latest frozen epoch. Do not read another chain's live worktree, report
log, artifact root, or checkpoint, the planning workspace or planning revisions, the live
leaderboard, the manifest, the state mirror, or the workspace map. The prompt lists the exact
paths. The filesystem does not enforce this boundary. Keeping it is what keeps
the chains independent, and independence is the point of the flow.

## The barrier

The prompt names the interval N and your session number within the current epoch. After your
chain's Nth session of an epoch the runtime holds it until every live chain has finished its N
sessions. A blocked chain does not hold the barrier, and its last state is still exchanged.
The runtime then freezes every chain's worktree, artifact root, and report log, plus the
leaderboard, into one epoch directory. Frozen copies show each chain at barrier time. They do not
follow later edits.

## Adoption in the first session after a barrier

The prompt lists each frozen chain's code, artifacts, and reports, plus your own frozen worktree
as a rollback point. The peer reports appear inline, capped per chain. Any that do not fit
arrive in your next sessions, and the complete logs are in the epoch.

1. Diff each peer's frozen worktree against yours before copying anything.
2. Adopt whole files, hunks, parameters, or ideas, or nothing. No chain is replaced.
3. Rerun your evaluator after any adoption.
4. In `evidence`, add one line per peer chain:
   `barrier-k lane-x: adopted|partial|declined - why`.
5. In `changes`, state exactly what you adopted and from which chain.

Your worktree may already hold a partial adoption from an earlier failed attempt at the same
session. Inspect it before porting again. Never write inside the epoch directory. Copy out of it.

## Reports and evidence

Return facts, not forecasts. Select:

- `progress` when material local work landed and another session can answer a concrete question;
- `deliverable_ready` only when the explicit package exists and can be reconstructed;
- `no_result` when a direction was tested and exhausted with useful negative evidence;
- `blocked` only for a concrete blocker beyond ordinary unfinished work. A blocked chain stops.

The runtime alone records `turn_failed`. A peer batch is acknowledged only after your own valid
report lands, so a failed session sees the same batch again.

## Artifacts and candidates

A deliverable declares regular files relative to your artifact root. Do not use absolute paths,
parent traversal, or symlinks. Every chain may attach one evaluator-accepted candidate to a
`deliverable_ready` report through the `submission` field. Give each candidate new artifact paths
and never alter a published candidate file. The leaderboard your prompt shows is the one frozen at
the latest barrier. Your own newer submissions appear in it only after the next barrier. Frozen
candidate artifact paths resolve under the epoch's `lanes/<lane>/artifacts/`.

## Checkpoints

The checkpoint path is optional recovery evidence for a session likely to be interrupted. Write the
exact identity supplied in the prompt and a truthful partial or terminal report. A checkpoint does
not trigger a barrier, replace the final structured report, or carry across a different generation.
