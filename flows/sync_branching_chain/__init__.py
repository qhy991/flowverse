"""Sync Branching Chain -- three isolated chains that exchange everything at barriers.

    hmz exec -f ./flows/sync_branching_chain \
      -c ./flows/sync_branching_chain/examples/n3.yaml \
      -a coordinator -a lane1-a -a lane1-b -a lane2-a -a lane2-b \
      -a lane3-a -a lane3-b "$(cat TASK.md)"

The coordinator plans once and does not return. Each lane is a chain of fresh sessions that
inherits its own retained worktree and reports. Between barriers the chains are isolated.
After every live chain completes ``barrier_interval`` sessions, the runtime freezes all three
worktrees, artifact roots, report logs, and the leaderboard into one epoch, and each chain's
next session decides what to adopt from the others. No chain is replaced.

Runs are resumable. Repeating the same task or entering ``continue`` resumes compatible state.
A run keeps the ``barrier_interval`` it was created with; set ``resume_mode: fresh`` to change
it or to force a new run.
"""

from __future__ import annotations

from typing import Any

from _parallel_flame_chase.core.api import Agents, BaseConfig
from hmz.flows import flow
from pydantic import Field

from sync_branching_chain.runtime import execute


class Config(BaseConfig):
    """Pacing, resume policy, and the SEIS migration interval."""

    barrier_interval: int = Field(
        default=3,
        ge=1,
        le=100,
        description=(
            "Completed sessions each live chain finishes between all-to-all barriers "
            "(SEIS N). Fixed when the run is created; resuming with another value "
            "fails."
        ),
    )


@flow(resumable=True)
def run(
    agents: Agents,
    task: str,
    config: Config | None = None,
    state: dict[str, Any] | None = None,
) -> None:
    """Run three isolated chains with an all-to-all exchange every N sessions."""
    execute(agents, task, config or Config(), state)


__all__ = ["Agents", "Config", "run"]
