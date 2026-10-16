"""Sync Branching Chain: isolated chains that exchange everything at barriers."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, cast

from _parallel_flame_chase.core.models import LaneName
from _parallel_flame_chase.core.utils import now
from _parallel_flame_chase.report_share import ReportShareRuntime

from .barrier import barrier_ready, first_after_barrier, frozen_turns, quota_end
from .epochs import EpochStore
from .prompts import FrozenChain, barrier_protocol

if TYPE_CHECKING:
    import datetime as dt
    from collections.abc import Callable

CONTROL_KEY = "sync_branching_chain"
CONTROL_VERSION = 1


def _count(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"resumable {name} must be a non-negative integer")
    return value


class SyncBranchingChainRuntime(ReportShareRuntime):
    """Three isolated chains with a derived per-lane quota and an all-to-all barrier.

    The only new durable state is an append-only list of immutable barrier receipts;
    every other barrier fact is derived from it and the lanes' turns. The hold lives in
    ``_lane_context``, the exchange in ``_control_cycle``, and isolation in
    ``_unread_reports``, ``_candidate_view``, and ``_workspace_map``, which read only
    the latest frozen epoch. Same-lane inheritance comes from ReportShareRuntime.
    """

    mode_name = "sync-branching-chain"
    skill_name = "sync-branching-chain"
    planning_cadence = (
        "This is the only coordinator turn. In this Sync Branching Chain mode the "
        "three lanes are equal, isolated chains: they exchange code and reports only "
        "at runtime barriers every few completed sessions, and each chain decides "
        "independently what to adopt. Lane 1 works in the original source but has no "
        "integration duty, which overrides the integration-owner wording above. Plan "
        "three materially different directions."
    )

    @property
    def epochs(self) -> EpochStore:
        # Derived from paths, not set in a hook: _resume_run validates the mode layout
        # before _initialize_mode_paths runs.
        return EpochStore(self.paths, self.lane_names)

    @property
    def _barrier_state(self) -> dict[str, Any]:
        return cast("dict[str, Any]", self.control[CONTROL_KEY])

    @property
    def _receipts(self) -> list[dict[str, Any]]:
        return cast("list[dict[str, Any]]", self._barrier_state["epochs"])

    @property
    def _interval(self) -> int:
        return int(self._barrier_state["barrier_interval"])

    def _turns(self, lane: LaneName) -> int:
        return int(self.control["lanes"][lane].get("turns", 0))

    def _new_mode_control(self) -> dict[str, object]:
        return {
            CONTROL_KEY: {
                "version": CONTROL_VERSION,
                "barrier_interval": int(self.config.barrier_interval),
                "epochs": [],
            }
        }

    def _validate_mode_control(self) -> None:
        """Validate receipt shape and the quota invariant without touching the disk."""
        held = self.control.get(CONTROL_KEY)
        if not isinstance(held, dict) or held.get("version") != CONTROL_VERSION:
            raise ValueError("resumable Sync Branching Chain state is malformed")
        interval = _count(held.get("barrier_interval"), "barrier_interval")
        if interval < 1:
            raise ValueError("resumable barrier_interval must be at least 1")
        if interval != self.config.barrier_interval:
            raise ValueError(
                f"this run was created with barrier_interval {interval}, not "
                f"{self.config.barrier_interval}; resume with the run's value or set "
                "resume_mode: fresh"
            )
        receipts = held.get("epochs")
        if not isinstance(receipts, list):
            raise TypeError("resumable barrier receipts are malformed")
        previous: dict[LaneName, tuple[int, int]] = dict.fromkeys(
            self.lane_names, (0, 0)
        )
        for receipt in receipts:
            lanes = receipt.get("lanes") if isinstance(receipt, dict) else None
            if (
                not isinstance(lanes, dict)
                or set(lanes) != set(self.lane_names)
                or not isinstance(receipt.get("committed_at"), str)
            ):
                raise ValueError("resumable barrier receipt is malformed")
            for lane in self.lane_names:
                entry = lanes[lane]
                if not isinstance(entry, dict) or not isinstance(
                    entry.get("blocked"), bool
                ):
                    raise ValueError(f"{lane} barrier receipt is malformed")
                turns = _count(entry.get("turns"), f"{lane} frozen turns")
                size = _count(entry.get("report_bytes"), f"{lane} frozen report bytes")
                if turns < previous[lane][0] or size < previous[lane][1]:
                    raise ValueError(f"{lane} barrier receipts move backwards")
                previous[lane] = (turns, size)
        for lane in self.lane_names:
            turns = self._turns(lane)
            if (
                not frozen_turns(receipts, lane)
                <= turns
                <= quota_end(receipts, lane, interval)
            ):
                raise ValueError(f"{lane} turns fall outside the current barrier epoch")

    def _initialize_mode_paths(self) -> None:
        epochs = self.epochs
        epochs.initialize(cast("str", self.control["run_id"]))
        self._log_discarded(epochs.sweep(len(self._receipts)))

    def _validate_mode_layout(self) -> None:
        # Also runs before _initialize_mode_paths on resume, when a crash may have left
        # an uncommitted epoch beside the committed one; only the committed one counts.
        if self._receipts:
            self.epochs.validate(
                len(self._receipts),
                {
                    lane: int(entry["report_bytes"])
                    for lane, entry in self._receipts[-1]["lanes"].items()
                },
            )

    def _log_discarded(self, names: list[str]) -> None:
        for name in names:
            self.control["events"].append(
                {"at": now(), "kind": "barrier_epoch_discarded", "epoch": name}
            )

    def _manifest_fields(self) -> dict[str, object]:
        live: list[LaneName] = [
            lane
            for lane in self.lane_names
            if not self.control["lanes"][lane].get("blocked")
        ]
        held: list[LaneName] = [
            lane
            for lane in live
            if self._turns(lane) >= quota_end(self._receipts, lane, self._interval)
        ]
        committed = len(self._receipts)
        return {
            CONTROL_KEY: {
                "barrier_interval": self._interval,
                "barriers_committed": committed,
                "latest_epoch": str(self.epochs.dir(committed)) if committed else None,
                "held": held,
                "waiting_on": [lane for lane in live if lane not in held],
            }
        }

    def _workspace_copy_plan(self, *, resume: bool, revised: bool) -> tuple[int, str]:
        copies, description = super()._workspace_copy_plan(
            resume=resume, revised=revised
        )
        if resume:
            return copies, description
        frozen = 2 * len(self.lane_names)
        return (
            copies + frozen,
            f"{description}, plus {len(self.lane_names)} frozen worktree copies at "
            f"every barrier, with at most {frozen} on disk while one barrier replaces "
            "the previous",
        )

    def _lane_context(self, lane: LaneName) -> tuple[bool, dict[str, object] | None]:
        """Hold a lane once it has finished this epoch's sessions; a per-pass skip."""
        return self._turns(lane) < quota_end(self._receipts, lane, self._interval), None

    def _control_cycle(self) -> None:
        lanes: dict[LaneName, tuple[int, bool]] = {
            lane: (self._turns(lane), bool(self.control["lanes"][lane].get("blocked")))
            for lane in self.lane_names
        }
        in_flight = any(runtime.future is not None for runtime in self.lanes.values())
        if barrier_ready(lanes, self._receipts, self._interval, in_flight=in_flight):
            self._commit_barrier()

    def _commit_barrier(self) -> None:
        """Freeze every chain into the next epoch and commit its receipt.

        Runs only while every lane is quiescent. The receipt append and its persist are
        the commit point. It never touches lane turns, bus cursors, or latest reports,
        so the staleness gate and the base ack path are unaffected.
        """
        committed = len(self._receipts)
        barrier = committed + 1
        epochs = self.epochs
        self._log_discarded(epochs.sweep(committed))
        report_bytes = epochs.freeze(barrier, self.control["candidate_board"])
        lanes = {
            lane: {
                "turns": self._turns(lane),
                "blocked": bool(self.control["lanes"][lane].get("blocked")),
                "report_bytes": report_bytes[lane],
            }
            for lane in self.lane_names
        }
        self._receipts.append({"committed_at": now(), "lanes": lanes})
        self.control["events"].append(
            {
                "at": now(),
                "kind": "barrier_committed",
                "barrier": barrier,
                "blocked": [lane for lane, entry in lanes.items() if entry["blocked"]],
            }
        )
        self._persist()
        epochs.sweep(barrier)

    def _unread_reports(
        self, lane: LaneName, cursors: dict[str, Any]
    ) -> tuple[list[dict[str, object]], dict[str, int]]:
        """Deliver peer reports only from the latest frozen epoch."""
        if not self._receipts:
            return [], {}
        return self.epochs.bus(len(self._receipts)).unread(lane, cursors)

    def _candidate_view(self, lane: LaneName) -> tuple[dict[str, object], str]:
        committed = len(self._receipts)
        return self.epochs.board(committed), str(self.epochs.leaderboard(committed))

    def _workspace_map(self) -> dict[str, object]:
        committed = len(self._receipts)
        return {
            "version": 1,
            "run_id": self.control["run_id"],
            "mode": self.mode_name,
            "isolation": (
                "Each chain works alone between barriers. Other chains' worktrees, "
                "artifacts, and reports arrive only as frozen copies at a barrier."
            ),
            "barrier": {
                "interval": self._interval,
                "committed": committed,
                "latest_epoch": str(self.epochs.dir(committed)) if committed else None,
            },
            "candidate_submissions": {
                "all_lanes_may_submit": True,
                "local_evaluator_only": True,
                "report_field": "submission",
                "requires_reconstructable_deliverable": True,
                "leaderboard": "frozen at each barrier",
            },
            "remote_actions": "not-authorized-by-this-flow",
        }

    def _lane_ownership(self, lane: LaneName) -> str | None:
        where = (
            "Your retained worktree is the original source"
            if lane == "lane-1"
            else "Your retained worktree is a private snapshot"
        )
        return (
            f"You are {lane}, one of three equal, isolated chains in a Sync Branching "
            f"Chain. {where} and is your cwd; edit only there. No chain integrates "
            "another automatically and there is no integration owner, which "
            "overrides any Lane 1 integration wording below. Publish candidates as "
            "explicit files under your artifact root."
        )

    def _lane_instructions(self, lane: LaneName) -> str:
        committed = len(self._receipts)
        turns = self._turns(lane)
        epochs = self.epochs
        exchange = None
        if first_after_barrier(self._receipts, lane, turns):
            frozen = self._receipts[-1]["lanes"]
            exchange = [
                FrozenChain(
                    lane=peer,
                    turns=int(frozen[peer]["turns"]),
                    blocked=bool(frozen[peer]["blocked"]),
                    worktree=str(epochs.frozen(committed, peer).worktree),
                    artifacts=str(epochs.frozen(committed, peer).artifacts),
                    reports=str(epochs.frozen(committed, peer).reports),
                )
                for peer in self.lane_names
            ]
        return barrier_protocol(
            lane=lane,
            interval=self._interval,
            committed=committed,
            session=turns - frozen_turns(self._receipts, lane) + 1,
            run_root=str(self.paths.root),
            own_reports=str(self.paths.reports / f"{lane}.jsonl"),
            epoch=str(epochs.dir(committed)) if committed else None,
            exchange=exchange,
        )


def execute(
    agents: Any,
    task: str,
    config: Any,
    state: dict[str, Any] | None,
    *,
    _clock: Callable[[], dt.datetime] | None = None,
    _sleep: Callable[[float], None] = time.sleep,
    _max_turns: int | None = None,
) -> None:
    """Run one resumable Sync Branching Chain; underscored controls support tests."""
    SyncBranchingChainRuntime(
        agents,
        task,
        config,
        state,
        clock=_clock,
        sleeper=_sleep,
        max_turns=_max_turns,
    ).run()


__all__ = ["SyncBranchingChainRuntime", "execute"]
