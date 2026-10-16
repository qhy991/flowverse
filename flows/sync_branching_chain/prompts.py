"""Isolation and barrier-exchange instructions for Sync Branching Chain lanes."""

from __future__ import annotations

from typing import NamedTuple

from _parallel_flame_chase.core.models import LaneName


class FrozenChain(NamedTuple):
    """One chain as a barrier froze it, with the paths a recipient may read."""

    lane: LaneName
    turns: int
    blocked: bool
    worktree: str
    artifacts: str
    reports: str


def _isolation(lane: LaneName, run_root: str, own_reports: str) -> str:
    source = (
        "" if lane == "lane-1" else "the original source, which is lane-1's cwd, and "
    )
    return f"""Isolation: between barriers your chain works alone. Use only your cwd, your artifact
root, your checkpoint, your own report log `{own_reports}`, and the latest frozen epoch. Do not
read:
- other chains' live worktrees: {source}everything under `{run_root}/private/` except your cwd;
- any other report log under `{run_root}/shared/reports/`;
- other chains' artifact roots under `{run_root}/shared/artifacts/`;
- `{run_root}/shared/leaderboard.json`, `{run_root}/shared/state.json`, or
  `{run_root}/shared/workspace-map.json`.
The runtime hands you other chains' code, reports, and candidates only at a barrier. The
filesystem does not enforce this boundary; keeping it is what keeps the chains independent."""


def _chain_line(chain: FrozenChain, lane: LaneName) -> str:
    where = (
        f"code `{chain.worktree}`, artifacts `{chain.artifacts}`, "
        f"reports `{chain.reports}`"
    )
    if chain.lane == lane:
        return (
            f"- your own frozen state after session {chain.turns} (rollback point): "
            f"code `{chain.worktree}`"
        )
    if chain.blocked:
        return (
            f"- {chain.lane}: BLOCKED, frozen as its final state after session "
            f"{chain.turns}. {where}"
        )
    return f"- {chain.lane}: frozen after its session {chain.turns}. {where}"


def _exchange(
    lane: LaneName, barrier: int, epoch: str, chains: list[FrozenChain]
) -> str:
    listing = "\n".join(_chain_line(chain, lane) for chain in chains)
    return f"""BARRIER {barrier} JUST COMPLETED. This is your chain's first session since the
exchange. Frozen epoch: `{epoch}`. Never write there; copy out of it.
{listing}
The peer reports above are capped at 12 records or 128 KB per chain; any not shown arrive in
your next sessions, and the complete logs are the files listed here. Candidate artifact paths in
the frozen leaderboard resolve under `{epoch}/lanes/<lane>/artifacts/`.
Decide what to adopt. No chain is replaced: you may adopt whole files, hunks, parameters, or
ideas, or nothing. Diff each peer worktree against yours before you copy anything, and rerun
your evaluator after you adopt. Your worktree may already contain a partial adoption from an
earlier attempt at this session; inspect it before porting again.
In `evidence`, add one line per peer chain:
`barrier-{barrier} <lane>: adopted|partial|declined - why`. In `changes`, state exactly what
you adopted and from which chain."""


def barrier_protocol(
    *,
    lane: LaneName,
    interval: int,
    committed: int,
    session: int,
    run_root: str,
    own_reports: str,
    epoch: str | None,
    exchange: list[FrozenChain] | None,
) -> str:
    """Describe the chain's epoch, its isolation, and, right after a barrier, the exchange.

    ``exchange`` is set only for the first session after a barrier, and it repeats on a
    retry of that session because the peer batch is redelivered until a report lands.
    """
    since = (
        "before the first barrier" if committed == 0 else f"after barrier {committed}"
    )
    header = f"""Sync Branching Chain: {lane}, epoch {committed + 1} ({since}), session {session}
of {interval} in this epoch. After session {interval} the runtime holds your chain until every
live chain arrives, then runs barrier {committed + 1}, where every chain receives frozen copies
of every other chain's worktree, artifacts, and reports."""
    sections = [header, _isolation(lane, run_root, own_reports)]
    if exchange is not None and epoch is not None:
        sections.append(_exchange(lane, committed, epoch, exchange))
    elif epoch is not None:
        sections.append(
            f"Barrier {committed}'s frozen copies remain at `{epoch}` until barrier "
            f"{committed + 1}. You may still adopt from them."
        )
    return "\n\n".join(sections)


__all__ = ["FrozenChain", "barrier_protocol"]
