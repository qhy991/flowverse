# Sync Branching Chain

`sync_branching_chain` runs the SEIS Sync Branching Chain
([arXiv:2610.04646](https://arxiv.org/abs/2610.04646)) on the shared Parallel Flame Chase
runtime. Three chains work in parallel, each isolated from the other two. Every `N` completed
sessions per chain, a barrier hands each chain frozen copies of the other chains' worktrees,
artifacts, reports, and leaderboard. Each chain then decides what to adopt. No chain is replaced.

## Table of contents

- [Background](#background)
- [Install](#install)
- [Usage](#usage)
- [Limitations](#limitations)
- [License](#license)

## Background

A chain is one lane. Its A and B actors alternate in fresh sessions, and each session inherits
the chain's retained worktree and the previous session's report. Lane 1's worktree is the
original source. Lanes 2 and 3 work in private snapshots. The chains are equal, and none of them
owns integration.

Between barriers, the runtime gives a chain nothing from the other chains. The prompt contains
no peer reports, no peer workspace paths, and only the leaderboard frozen at the last barrier.
After a chain completes its `N` sessions of an epoch, the runtime holds it until every live chain
has completed its `N` sessions. A blocked chain does not hold the barrier. The runtime then copies
every chain's worktree, artifact root, and report log, plus the leaderboard, into
`shared/epochs/NNNN/`, and appends one barrier receipt to the run state. The next session of each
chain gets the frozen paths of every other chain, the peer reports, and an adoption protocol.
Blocked chains are frozen and offered like the others, and the prompt marks them as blocked.

The run state stores one new fact, an append-only list of barrier receipts. Each receipt records
every chain's turn count, whether it was blocked, and the size of its frozen report log. The
runtime derives which chains are held, and whether a session is the first after a barrier, from
the receipts and the chains' turn counts. A barrier is committed only when its receipt is
persisted. On resume, the runtime deletes an epoch directory that has no receipt, logs
`barrier_epoch_discarded`, and freezes that barrier again from the chains' current content.

## Install

The flow lives in this repository at `flows/sync_branching_chain` and imports the hidden sibling
package `flows/_parallel_flame_chase`. It needs the `hmz` command and no other setup.

## Usage

Pass seven agents in this order: the planning coordinator, then the A and B actors for lanes
1, 2, and 3. The person at the prompt is injected automatically.

```console
hmz exec -f ./flows/sync_branching_chain \
  -a codex/gpt-5.6-sol:max \
  -a claude/claude-opus-5:max -a codex/gpt-5.6-sol:max \
  -a claude/claude-opus-5:max -a codex/gpt-5.6-sol:max \
  -a claude/claude-opus-5:max -a codex/gpt-5.6-sol:max \
  -c ./flows/sync_branching_chain/examples/n3.yaml \
  "$(cat TASK.md)"
```

The two example configurations match the paper's settings:

| File | `barrier_interval` | Migration rate |
| --- | --- | --- |
| `examples/n3.yaml` | 3 | A barrier after every 3 sessions per chain |
| `examples/n9.yaml` | 9 | A barrier after every 9 sessions per chain |

`barrier_interval` accepts 1 to 100 and defaults to 3. The other fields are the shared
Parallel Flame Chase settings for pacing, resume, and the large-workspace warning.

The flow is resumable. Repeat the same task or enter `continue` to resume. A run keeps the
`barrier_interval` it was created with, and a resume with a different value fails. To change the
interval, set `resume_mode: fresh` to start a new run.

Run data lives under `~/.humanize/parallel_flame_chase/<workspace-key>/<run-id>/`. Only the latest
epoch is kept. During a barrier, two epochs are on disk until the new receipt is committed.

## Limitations

- Isolation is advisory. The runtime never delivers or advertises another chain's live state
  between barriers, and every prompt lists the paths a chain must not read. The filesystem does
  not stop an agent from reading them. Each prompt's artifact root path shows where `shared/` is,
  and `shared/state.json` mirrors every chain's latest report.
- There is no barrier timeout. A session that hangs holds every other chain at the barrier until
  it ends. This matches the paper's synchronous barrier.
- The shared lane and planning prompts still mention Lane 1 integration ownership and a live
  leaderboard. The flow's own instructions override that text in each prompt.
- A barrier freezes each chain's current worktree, not its best evaluator-accepted package.
- A frozen copy is writable. An agent that writes inside `shared/epochs/` can change what the
  other chains see. The runtime checks only the frozen report-log sizes against the receipt.

## License

This repository declares no license file. Ask the repository owner before reuse.
