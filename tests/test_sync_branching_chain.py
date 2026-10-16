from __future__ import annotations

from typing import Any

import pytest
from sync_branching_chain.barrier import (
    barrier_ready,
    first_after_barrier,
    held,
    quota_end,
)


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
