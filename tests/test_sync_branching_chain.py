from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

import pytest
from _parallel_flame_chase.core.models import LANES
from _parallel_flame_chase.persistence.events import ReportBus
from _parallel_flame_chase.persistence.workspace import (
    RunPaths,
    initialize_paths,
)
from _parallel_flame_chase.persistence.workspace import snapshot as real_snapshot
from sync_branching_chain import epochs as epochs_module
from sync_branching_chain.barrier import (
    barrier_ready,
    first_after_barrier,
    held,
    quota_end,
)
from sync_branching_chain.epochs import EpochStore


def receipt(**turns: int) -> dict[str, Any]:
    return {
        "committed_at": "2026-10-16T00:00:00.000Z",
        "lanes": {
            lane.replace("_", "-"): {
                "turns": count,
                "blocked": False,
                "report_bytes": 0,
            }
            for lane, count in turns.items()
        },
    }


ONE = [receipt(lane_1=3, lane_2=3, lane_3=1)]


@pytest.mark.parametrize(
    ("receipts", "lane", "interval", "expected"),
    [
        ([], "lane-1", 3, 3),
        (ONE, "lane-1", 3, 6),
        (ONE, "lane-3", 3, 4),
        ([*ONE, receipt(lane_1=6, lane_2=6, lane_3=4)], "lane-2", 9, 15),
    ],
)
def test_quota_end_counts_from_the_lane_frozen_turns(
    receipts: list[dict[str, Any]], lane: str, interval: int, expected: int
) -> None:
    assert quota_end(receipts, lane, interval) == expected  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("turns", "quota", "expected"),
    [(0, 3, False), (2, 3, False), (3, 3, True), (4, 3, True)],
)
def test_held(turns: int, quota: int, expected: bool) -> None:
    assert held(turns, quota) is expected


@pytest.mark.parametrize(
    ("lanes", "receipts", "in_flight", "expected"),
    [
        (
            {"lane-1": (3, False), "lane-2": (3, False), "lane-3": (3, False)},
            [],
            False,
            True,
        ),
        (
            {"lane-1": (3, False), "lane-2": (2, False), "lane-3": (3, False)},
            [],
            False,
            False,
        ),
        (
            {"lane-1": (3, False), "lane-2": (3, False), "lane-3": (3, False)},
            [],
            True,
            False,
        ),
        (
            {"lane-1": (3, False), "lane-2": (0, True), "lane-3": (3, False)},
            [],
            False,
            True,
        ),
        (
            {"lane-1": (0, True), "lane-2": (0, True), "lane-3": (1, True)},
            [],
            False,
            False,
        ),
        (
            {"lane-1": (3, False), "lane-2": (3, False), "lane-3": (3, False)},
            ONE,
            False,
            False,
        ),
        (
            {"lane-1": (6, False), "lane-2": (6, False), "lane-3": (4, False)},
            ONE,
            False,
            True,
        ),
    ],
    ids=[
        "all-at-quota",
        "one-short",
        "session-in-flight",
        "blocked-lane-exempt",
        "all-blocked-never-fires",
        "just-committed",
        "next-quota-from-frozen-turns",
    ],
)
def test_barrier_ready(
    lanes: dict[str, tuple[int, bool]],
    receipts: list[dict[str, Any]],
    in_flight: bool,
    expected: bool,
) -> None:
    assert (
        barrier_ready(lanes, receipts, 3, in_flight=in_flight)  # type: ignore[arg-type]
        is expected
    )


@pytest.mark.parametrize(
    ("receipts", "lane", "turns", "expected"),
    [
        ([], "lane-1", 0, False),
        (ONE, "lane-1", 3, True),
        (ONE, "lane-1", 4, False),
        (ONE, "lane-3", 1, True),
    ],
)
def test_first_after_barrier(
    receipts: list[dict[str, Any]], lane: str, turns: int, expected: bool
) -> None:
    assert first_after_barrier(receipts, lane, turns) is expected  # type: ignore[arg-type]


def run_paths(tmp_path: Path) -> RunPaths:
    source = tmp_path / "source"
    source.mkdir()
    (source / "marker.txt").write_text("lane-1 v0\n", encoding="utf-8")
    paths = RunPaths(tmp_path / "run", source)
    initialize_paths(paths, make_snapshots=True, lanes=LANES)
    return paths


def store(tmp_path: Path) -> EpochStore:
    epochs = EpochStore(run_paths(tmp_path), LANES)
    epochs.initialize("run-1")
    return epochs


def test_interrupted_freeze_leaves_only_sweepable_garbage(
    tmp_path: Path, monkeypatch: Any
) -> None:
    epochs = store(tmp_path)
    copies = 0

    def crash_on_fourth_copy(source: Path, destination: Path) -> None:
        nonlocal copies
        copies += 1
        if copies == 4:
            raise OSError("simulated disk full")
        real_snapshot(source, destination)

    monkeypatch.setattr(epochs_module, "snapshot", crash_on_fourth_copy)
    with pytest.raises(OSError, match="simulated disk full"):
        epochs.freeze(1, {"best": None})
    assert not epochs.dir(1).exists()
    assert [p.name.startswith(".staging-0001-") for p in epochs.root.iterdir()].count(
        True
    ) == 1

    assert epochs.sweep(0) == []
    assert sorted(p.name for p in epochs.root.iterdir()) == ["empty-leaderboard.json"]


def test_sweep_is_idempotent_and_reports_uncommitted_dirs(tmp_path: Path) -> None:
    epochs = store(tmp_path)
    epochs.freeze(1, {"best": None})
    epochs.freeze(2, {"best": None})
    (epochs.root / ".staging-0003-abc").mkdir()

    assert epochs.sweep(1) == ["0002"]
    assert epochs.sweep(1) == []
    assert sorted(p.name for p in epochs.root.iterdir()) == [
        "0001",
        "empty-leaderboard.json",
    ]
    assert (epochs.frozen(1, "lane-1").worktree / "marker.txt").is_file()

    epochs.freeze(2, {"best": None})
    assert epochs.sweep(2) == []
    assert sorted(p.name for p in epochs.root.iterdir()) == [
        "0002",
        "empty-leaderboard.json",
    ]


def test_freeze_copies_trees_board_and_whole_report_lines(tmp_path: Path) -> None:
    epochs = store(tmp_path)
    paths = epochs.paths
    ReportBus(paths, LANES).publish("lane-2", {"summary": "lane-2 wrote v1"})
    with (paths.reports / "lane-2.jsonl").open("ab") as handle:
        handle.write(b'{"partial":')
    (paths.artifact_root("lane-3") / "candidate.py").write_text("v1\n")

    report_bytes = epochs.freeze(1, {"best": {"lane": "lane-3"}})
    (paths.source / "marker.txt").write_text("lane-1 v1\n", encoding="utf-8")

    whole = len(b'{"summary": "lane-2 wrote v1"}\n')
    assert report_bytes == {"lane-1": 0, "lane-2": whole, "lane-3": 0}
    assert (epochs.frozen(1, "lane-1").worktree / "marker.txt").read_text() == (
        "lane-1 v0\n"
    )
    assert (epochs.frozen(1, "lane-3").artifacts / "candidate.py").read_text() == "v1\n"
    assert epochs.board(1) == {"best": {"lane": "lane-3"}}
    epochs.validate(1, report_bytes)
    with pytest.raises(RuntimeError, match="its receipt froze"):
        epochs.validate(1, {**report_bytes, "lane-2": whole + 1})


def test_validate_tolerates_an_orphan_next_epoch(tmp_path: Path) -> None:
    epochs = store(tmp_path)
    committed = epochs.freeze(1, {"best": None})
    epochs.freeze(2, {"best": None})
    epochs.validate(1, committed)
    shutil.rmtree(epochs.frozen(1, "lane-2").artifacts)
    with pytest.raises(RuntimeError, match="committed epoch path is missing"):
        epochs.validate(1, committed)


def test_acks_from_one_epoch_bus_stay_valid_on_the_next(tmp_path: Path) -> None:
    epochs = store(tmp_path)
    live = ReportBus(epochs.paths, LANES)
    live.publish("lane-1", {"summary": "lane-1 wrote v1"})
    live.publish("lane-3", {"summary": "lane-3 wrote v1"})
    epochs.freeze(1, {"best": None})
    live.publish("lane-1", {"summary": "lane-1 wrote v2"})

    cursors: dict[str, Any] = {}
    first, acks = epochs.bus(1).unread("lane-2", cursors)
    assert [d["report"]["summary"] for d in first] == [
        "lane-1 wrote v1",
        "lane-3 wrote v1",
    ]
    ReportBus.acknowledge("lane-2", cursors, acks)
    assert epochs.bus(1).unread("lane-2", cursors)[0] == []

    live.publish("lane-3", {"summary": "lane-3 wrote v2"})
    epochs.freeze(2, {"best": None})
    live.publish("lane-1", {"summary": "lane-1 wrote v3"})
    second, _ = epochs.bus(2).unread("lane-2", cursors)
    assert [d["report"]["summary"] for d in second] == [
        "lane-1 wrote v2",
        "lane-3 wrote v2",
    ]
