"""Immutable per-barrier epochs: frozen worktrees, artifacts, report logs, and board."""

from __future__ import annotations

import json
import os
import shutil
import stat
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple, cast

from _parallel_flame_chase.core.models import LaneName
from _parallel_flame_chase.core.utils import atomic_json
from _parallel_flame_chase.persistence.events import ReportBus
from _parallel_flame_chase.persistence.leaderboard import empty_leaderboard
from _parallel_flame_chase.persistence.workspace import RunPaths, snapshot

_STAGING = ".staging-"


class FrozenLane(NamedTuple):
    """The frozen copies of one lane inside one epoch."""

    worktree: Path
    artifacts: Path
    reports: Path


def _layout(epoch: Path, source: Path, lane: LaneName) -> FrozenLane:
    return FrozenLane(
        worktree=epoch / "lanes" / lane / "worktree",
        artifacts=epoch / "lanes" / lane / "artifacts",
        reports=RunPaths(epoch, source).reports / f"{lane}.jsonl",
    )


def _numbered(name: str) -> int | None:
    return int(name) if len(name) == 4 and name.isdigit() else None


def _remove(path: Path) -> None:
    if path.is_symlink() or not path.is_dir():
        path.unlink()
        return
    retried: set[str] = set()

    def writable(_: object, failed: str, error: BaseException) -> None:
        # Frozen copies keep a lane's read-only dirs, such as a Go module cache.
        if not isinstance(error, PermissionError) or failed in retried:
            raise error
        retried.add(failed)
        directory = os.path.isdir(failed) and not os.path.islink(failed)
        for target in (os.path.dirname(failed), *([failed] if directory else [])):
            os.chmod(target, stat.S_IMODE(os.lstat(target).st_mode) | stat.S_IRWXU)
        if directory:
            shutil.rmtree(failed, onexc=writable)
        else:
            os.unlink(failed)

    shutil.rmtree(path, onexc=writable)


@dataclass(frozen=True, slots=True)
class EpochStore:
    """Own every byte under ``shared/epochs``.

    The runtime holds the receipts; this store holds the bytes. A numbered epoch
    directory is trustworthy only when a committed receipt names it, so every other
    numbered or staging directory is garbage that ``sweep`` removes. Each epoch nests
    its frozen report logs at ``shared/reports`` so a plain ``RunPaths`` reads them.
    """

    paths: RunPaths
    lanes: tuple[LaneName, ...]

    @property
    def root(self) -> Path:
        return self.paths.shared / "epochs"

    def dir(self, barrier: int) -> Path:
        return self.root / f"{barrier:04d}"

    def frozen(self, barrier: int, lane: LaneName) -> FrozenLane:
        return _layout(self.dir(barrier), self.paths.source, lane)

    def leaderboard(self, barrier: int) -> Path:
        """Return the board lanes cite: empty before barrier 1, then the frozen one."""
        if barrier == 0:
            return self.root / "empty-leaderboard.json"
        return self.dir(barrier) / "leaderboard.json"

    def initialize(self, run_id: str) -> None:
        """Create the epoch root and the pre-barrier empty board. Idempotent."""
        if self.root.is_symlink():
            raise RuntimeError(f"runtime directory was replaced or linked: {self.root}")
        self.root.mkdir(parents=True, exist_ok=True)
        if not self.leaderboard(0).exists():
            atomic_json(self.leaderboard(0), empty_leaderboard(run_id))

    def sweep(self, committed: int) -> list[str]:
        """Delete staging dirs and every numbered dir except ``committed``.

        Returns the deleted numbered names above ``committed``: freezes a crash left
        without a receipt. Safe whenever no freeze is running, which the single-threaded
        control loop guarantees.
        """
        discarded: list[str] = []
        for entry in sorted(self.root.iterdir()):
            number = _numbered(entry.name)
            if entry.name.startswith(_STAGING):
                _remove(entry)
            elif number is not None and number != committed:
                _remove(entry)
                if number > committed:
                    discarded.append(entry.name)
        return discarded

    def freeze(self, barrier: int, board: Mapping[str, object]) -> dict[LaneName, int]:
        """Materialize epoch ``barrier`` atomically and return each lane's report bytes.

        Requires ``dir(barrier)`` to be absent and every lane to be quiescent. All
        copies land in a staging dir that one rename publishes, so a crash at any step
        leaves only garbage for ``sweep``.
        """
        staging = self.root / f"{_STAGING}{barrier:04d}-{uuid.uuid4().hex}"
        report_bytes: dict[LaneName, int] = {}
        for lane in self.lanes:
            target = _layout(staging, self.paths.source, lane)
            snapshot(self.paths.workspace(lane), target.worktree)
            snapshot(self.paths.artifact_root(lane), target.artifacts)
            live = (self.paths.reports / f"{lane}.jsonl").read_bytes()
            # The bus delivers only whole lines, so freeze up to the last newline.
            prefix = live[: live.rfind(b"\n") + 1]
            target.reports.parent.mkdir(parents=True, exist_ok=True)
            target.reports.write_bytes(prefix)
            report_bytes[lane] = len(prefix)
        atomic_json(staging / "leaderboard.json", dict(board))
        os.rename(staging, self.dir(barrier))
        return report_bytes

    def board(self, barrier: int) -> dict[str, object]:
        return cast(
            "dict[str, object]",
            json.loads(self.leaderboard(barrier).read_text(encoding="utf-8")),
        )

    def bus(self, barrier: int) -> ReportBus:
        """Read only the frozen report prefixes of epoch ``barrier``.

        Each frozen log is a byte prefix of its append-only live log, so a cursor
        acknowledged from one epoch's bus stays valid on the next one.
        """
        return ReportBus(RunPaths(self.dir(barrier), self.paths.source), self.lanes)

    def validate(self, barrier: int, report_bytes: Mapping[LaneName, int]) -> None:
        """Fail closed unless epoch ``barrier`` matches its receipt; stats only."""
        epoch = self.dir(barrier)
        directories = [epoch]
        for lane in self.lanes:
            frozen = self.frozen(barrier, lane)
            directories += [frozen.worktree, frozen.artifacts]
        for path in directories:
            try:
                info = path.lstat()
            except OSError as why:
                raise RuntimeError(f"committed epoch path is missing: {path}") from why
            if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
                raise RuntimeError(f"committed epoch path was replaced: {path}")
        for lane in self.lanes:
            log = self.frozen(barrier, lane).reports
            try:
                info = log.lstat()
            except OSError as why:
                raise RuntimeError(f"committed epoch log is missing: {log}") from why
            if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
                raise RuntimeError(f"committed epoch log was replaced: {log}")
            if info.st_size != report_bytes[lane]:
                raise RuntimeError(
                    f"committed epoch log {log} has {info.st_size} bytes; "
                    f"its receipt froze {report_bytes[lane]}"
                )


__all__ = ["EpochStore", "FrozenLane"]
