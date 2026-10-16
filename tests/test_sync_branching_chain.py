from __future__ import annotations

import json
import os
import re
import shutil
import socket
import stat
import tempfile
import time
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from _parallel_flame_chase.core.models import (
    LANES,
    ArtifactRef,
    CandidateSubmission,
    Deliverable,
    InitialPlan,
    LaneBrief,
    LaneReport,
    MissionSpec,
)
from _parallel_flame_chase.orchestration import state as runtime_state
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
from sync_branching_chain.prompts import barrier_protocol
from sync_branching_chain.runtime import SyncBranchingChainRuntime, execute


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


def test_sweep_removes_a_frozen_read_only_tree(tmp_path: Path) -> None:
    epochs = store(tmp_path)
    cache = epochs.paths.workspace("lane-2") / "gomodcache"
    (cache / "pkg").mkdir(parents=True)
    (cache / "pkg" / "mod.go").write_text("module m\n")
    for folder in (cache / "pkg", cache):
        folder.chmod(0o555)
    epochs.freeze(1, {"best": None})
    epochs.freeze(2, {"best": None})
    frozen = epochs.frozen(1, "lane-2").worktree / "gomodcache"
    assert stat.S_IMODE(frozen.stat().st_mode) == 0o555

    assert epochs.sweep(2) == []
    assert sorted(p.name for p in epochs.root.iterdir()) == [
        "0002",
        "empty-leaderboard.json",
    ]


@pytest.fixture
def short_tmp() -> Iterator[Path]:
    # A Unix socket path must fit in about 104 bytes on macOS.
    root = Path(tempfile.mkdtemp(dir="/tmp"))
    yield root
    shutil.rmtree(root)


def test_freeze_skips_sockets_and_keeps_files_dirs_and_symlinks(
    short_tmp: Path, monkeypatch: Any
) -> None:
    epochs = store(short_tmp)
    # GNU cp copies a socket as an inert node; without cp, snapshot takes the
    # inspect_workspace fallback that macOS's cp failure reaches.
    monkeypatch.setattr(shutil, "which", lambda _: None)
    git = epochs.paths.workspace("lane-1") / ".git"
    git.mkdir()
    (git / "HEAD").write_text("ref: refs/heads/main\n")
    (git / "head-link").symlink_to("HEAD")
    with socket.socket(socket.AF_UNIX) as daemon:
        daemon.bind(str(git / "d.ipc"))
        frozen = epochs.freeze(1, {"best": None})
        assert epochs.freeze(2, {"best": None}).skipped == frozen.skipped

    assert frozen.skipped == ["lanes/lane-1/worktree/.git/d.ipc"]
    copy = epochs.frozen(1, "lane-1").worktree
    assert (copy / ".git" / "HEAD").read_text() == "ref: refs/heads/main\n"
    assert os.readlink(copy / ".git" / "head-link") == "HEAD"
    assert (copy / "marker.txt").read_text() == "lane-1 v0\n"
    assert not (copy / ".git" / "d.ipc").exists()
    assert not (copy / ".git" / "d.ipc").is_symlink()
    epochs.validate(1, frozen.report_bytes)


def test_freeze_copies_trees_board_and_whole_report_lines(tmp_path: Path) -> None:
    epochs = store(tmp_path)
    paths = epochs.paths
    ReportBus(paths, LANES).publish("lane-2", {"summary": "lane-2 wrote v1"})
    with (paths.reports / "lane-2.jsonl").open("ab") as handle:
        handle.write(b'{"partial":')
    (paths.artifact_root("lane-3") / "candidate.py").write_text("v1\n")

    report_bytes = epochs.freeze(1, {"best": {"lane": "lane-3"}}).report_bytes
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
    committed = epochs.freeze(1, {"best": None}).report_bytes
    epochs.freeze(2, {"best": None})
    epochs.validate(1, committed)
    shutil.rmtree(epochs.frozen(1, "lane-2").artifacts)
    with pytest.raises(RuntimeError, match="committed epoch path is missing"):
        epochs.validate(1, committed)


@pytest.mark.parametrize(
    ("tamper", "message"),
    [
        (lambda board: board.unlink(), "epoch leaderboard is missing"),
        (
            lambda board: board.write_text("{not json"),
            "epoch leaderboard is unreadable",
        ),
        (
            lambda board: board.write_text("[]"),
            "epoch leaderboard is not a JSON object",
        ),
        (
            lambda board: (board.unlink(), board.mkdir()),
            "epoch leaderboard was replaced or linked",
        ),
        (
            lambda board: (
                board.rename(board.with_name("real.json")),
                board.symlink_to("real.json"),
            ),
            "epoch leaderboard was replaced or linked",
        ),
    ],
    ids=["missing", "unparseable", "not-an-object", "directory", "symlink"],
)
def test_validate_fails_closed_on_a_tampered_frozen_board(
    tmp_path: Path, tamper: Any, message: str
) -> None:
    epochs = store(tmp_path)
    committed = epochs.freeze(1, {"best": None}).report_bytes
    tamper(epochs.leaderboard(1))
    with pytest.raises(RuntimeError, match=message):
        epochs.validate(1, committed)


def test_initialize_rewrites_an_unreadable_empty_board(tmp_path: Path) -> None:
    epochs = store(tmp_path)
    epochs.leaderboard(0).write_text("{not json")
    epochs.initialize("run-1")
    assert epochs.board(0)["submission_count"] == 0
    assert epochs.board(0)["run_id"] == "run-1"


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


def spec(title: str, approach: str) -> MissionSpec:
    return MissionSpec(
        title=title,
        objective=f"Investigate {title}",
        success_criteria=["Land one evidence-backed increment"],
        approach_class=approach,
        change_scale="component",
        information_question=f"What does {title} reveal?",
    )


PLAN = InitialPlan(
    lanes=[
        LaneBrief(lane="lane-1", mission=spec("source", "baseline")),
        LaneBrief(lane="lane-2", mission=spec("algorithm", "algorithm")),
        LaneBrief(lane="lane-3", mission=spec("layout", "layout")),
    ]
)
TASK = "Optimize the kernel."
TURN = re.compile(r"taking turn (\d+) for (lane-\d)")
DELIVERY = re.compile(r'"report_id": "(lane-\d:\d+:[0-9a-f]{16})"')
ARTIFACT_ROOT = "Your artifact root is `"


class FakeSession:
    def __init__(self, agent: FakeAgent, cwd: Path) -> None:
        self.agent = agent
        self.cwd = cwd
        self.closed = False

    def __call__(self, prompt: str, *, suppress: bool, schema: type[Any]) -> Any:
        self.agent.prompts.append((self.cwd, prompt, schema))
        if schema is InitialPlan:
            return PLAN
        if schema is not LaneReport:
            raise AssertionError(schema)
        found = TURN.search(prompt)
        assert found is not None
        return self.report(int(found[1]), found[2], prompt)

    def report(self, turn: int, lane: str, prompt: str) -> LaneReport:
        """Leave a turn-stamped marker in the chain's worktree, then report it."""
        if (lane, turn) in self.agent.fail_turns:
            self.agent.fail_turns.discard((lane, turn))
            raise RuntimeError(f"{lane} turn {turn} backend dropped")
        (self.cwd / "marker.txt").write_text(f"{lane} v{turn}\n", encoding="utf-8")
        return LaneReport(
            status="progress",
            summary=f"{lane} wrote v{turn}",
            evidence=[f"cwd={self.cwd}"],
            next_step="Continue the chain.",
        )

    def close(self) -> None:
        self.closed = True


class FakeAgent:
    def __init__(self, name: str) -> None:
        self.name = name
        self.prompts: list[tuple[Path, str, type[Any]]] = []
        self.fail_turns: set[tuple[str, int]] = set()

    def new(self, cwd: str | Path | None = None) -> FakeSession:
        return FakeSession(self, Path(cwd or ".").resolve())


class FailingStartAgent(FakeAgent):
    def new(self, cwd: str | Path | None = None) -> FakeSession:
        raise RuntimeError(f"{self.name} backend is temporarily unavailable")


class CandidateSession(FakeSession):
    def report(self, turn: int, lane: str, prompt: str) -> LaneReport:
        """Submit a candidate that improves every turn, so each one becomes its best."""
        if (lane, turn) == ("lane-1", 3):
            time.sleep(0.3)
        artifact_root = Path(prompt.split(ARTIFACT_ROOT, 1)[1].split("`", 1)[0])
        (artifact_root / f"candidate-{turn}.py").write_text(f"# {lane} v{turn}\n")
        values = {"lane-1": 1100, "lane-2": 900, "lane-3": 1000}
        return LaneReport(
            status="deliverable_ready",
            summary=f"{lane} published candidate v{turn}.",
            evidence=["local evaluator exit 0"],
            deliverable=Deliverable(
                title=f"{lane} candidate v{turn}",
                approach_class="test candidate",
                artifacts=[
                    ArtifactRef(path=f"candidate-{turn}.py", description="candidate")
                ],
                integration_notes="Compare and reconstruct this candidate.",
            ),
            submission=CandidateSubmission(
                title=f"{lane} candidate v{turn}",
                metric="cycles",
                value=values[lane] - turn,
                direction="minimize",
                evaluator="task-provided local evaluator, exit 0",
            ),
        )


class CandidateAgent(FakeAgent):
    def new(self, cwd: str | Path | None = None) -> CandidateSession:
        return CandidateSession(self, Path(cwd or ".").resolve())


def agents(agent_type: type[FakeAgent] = FakeAgent) -> SimpleNamespace:
    return SimpleNamespace(
        coordinator=agent_type("coordinator"),
        lane_1_actor_a=agent_type("lane-1-a"),
        lane_1_actor_b=agent_type("lane-1-b"),
        lane_2_actor_a=agent_type("lane-2-a"),
        lane_2_actor_b=agent_type("lane-2-b"),
        lane_3_actor_a=agent_type("lane-3-a"),
        lane_3_actor_b=agent_type("lane-3-b"),
        human=None,
    )


def config(barrier_interval: int = 2) -> SimpleNamespace:
    return SimpleNamespace(
        resume_mode="auto",
        rest_seconds=0.001,
        confirm_large_workspace_copies=False,
        workspace_file_warning_threshold=5_000,
        workspace_copy_warning_threshold_bytes=1024**3,
        barrier_interval=barrier_interval,
    )


@pytest.fixture
def source(tmp_path: Path, monkeypatch: Any) -> Path:
    source = tmp_path / "source"
    source.mkdir()
    (source / "marker.txt").write_text("lane-1 v0\n", encoding="utf-8")
    monkeypatch.chdir(source)
    monkeypatch.setattr(runtime_state, "home", lambda: tmp_path / "humanize-home")
    return source


def run(
    chosen: SimpleNamespace,
    state: dict[str, Any],
    max_turns: int,
    *,
    task: str = TASK,
    barrier_interval: int = 2,
) -> None:
    runtime = SyncBranchingChainRuntime(
        chosen,
        task,
        config(barrier_interval),
        state,
        sleeper=lambda _: time.sleep(0.002),
        max_turns=max_turns,
    )
    try:
        runtime.run()
    finally:
        # run() stops without waiting, so a submitted session may still be recording
        # its prompt or writing its worktree when the test starts reading them.
        runtime.executor.shutdown(wait=True)


def lane_prompts(chosen: SimpleNamespace, lane: str) -> dict[int, list[str]]:
    """Every prompt one lane received, keyed by turn, in attempt order."""
    number = lane[-1]
    found: dict[int, list[str]] = {}
    for actor in ("a", "b"):
        for _, prompt, _ in getattr(chosen, f"lane_{number}_actor_{actor}").prompts:
            turn = TURN.search(prompt)
            assert turn is not None
            found.setdefault(int(turn[1]), []).append(prompt)
    return found


def test_chains_are_isolated_until_a_barrier_exchanges_frozen_copies(
    source: Path,
) -> None:
    chosen = agents()
    state: dict[str, Any] = {}
    run(chosen, state, 9)

    root = Path(state["run_root"])
    epoch = root / "shared" / "epochs" / "0001"
    [receipt] = state["sync_branching_chain"]["epochs"]
    assert {lane: entry["turns"] for lane, entry in receipt["lanes"].items()} == {
        "lane-1": 2,
        "lane-2": 2,
        "lane-3": 2,
    }
    prompts = lane_prompts(chosen, "lane-2")
    for turn in (1, 2):
        [before] = prompts[turn]
        assert "lane-1 wrote" not in before
        assert "lane-3 wrote" not in before
        assert str(source) not in before
        assert str(root / "private" / "lane-3") not in before
        assert str(root / "shared" / "artifacts" / "lane-3") not in before
        assert "BARRIER" not in before
    [after] = prompts[3]
    assert "BARRIER 1 JUST COMPLETED" in after
    assert "lane-1 wrote v2" in after
    assert "lane-3 wrote v1" in after
    assert f"code `{epoch / 'lanes' / 'lane-3' / 'worktree'}`" in after
    assert f"code `{epoch / 'lanes' / 'lane-1' / 'worktree'}`" in after
    [committed] = [e for e in state["events"] if e["kind"] == "barrier_committed"]
    assert committed["skipped_special_files"] == []
    assert (epoch / "lanes/lane-3/worktree/marker.txt").read_text() == "lane-3 v2\n"
    assert (epoch / "lanes/lane-1/worktree/marker.txt").read_text() == "lane-1 v2\n"
    assert (root / "private/lane-3/marker.txt").read_text() in {
        "lane-3 v3\n",
        "lane-3 v4\n",
    }


def test_blocked_lane_does_not_hold_the_barrier_but_is_exchanged(
    source: Path,
) -> None:
    chosen = agents()
    chosen.lane_2_actor_a = FailingStartAgent("lane-2-a")
    chosen.lane_2_actor_b = FailingStartAgent("lane-2-b")
    state: dict[str, Any] = {}
    run(chosen, state, 6)

    [receipt] = state["sync_branching_chain"]["epochs"]
    live_log = Path(state["run_root"]) / "shared/reports/lane-2.jsonl"
    assert receipt["lanes"]["lane-2"] == {
        "turns": 0,
        "blocked": True,
        "report_bytes": live_log.stat().st_size,
    }
    assert receipt["lanes"]["lane-2"]["report_bytes"] > 0
    assert receipt["lanes"]["lane-1"]["blocked"] is False
    [after] = lane_prompts(chosen, "lane-1")[3]
    assert "- lane-2: BLOCKED, frozen as its final state after session 0." in after
    assert "lane-2-a backend is temporarily unavailable" in after


def test_crash_between_epoch_rename_and_receipt_refreezes_on_resume(
    source: Path, monkeypatch: Any
) -> None:
    freeze = EpochStore.freeze

    def crash_after_rename(
        self: EpochStore, barrier: int, board: dict[str, object]
    ) -> epochs_module.Frozen:
        freeze(self, barrier, board)
        raise RuntimeError("simulated crash")

    state: dict[str, Any] = {}
    with monkeypatch.context() as patched:
        patched.setattr(EpochStore, "freeze", crash_after_rename)
        with pytest.raises(RuntimeError, match="simulated crash"):
            run(agents(), state, 10)

    root = Path(state["run_root"])
    epoch = root / "shared" / "epochs" / "0001"
    assert state["sync_branching_chain"]["epochs"] == []
    assert epoch.is_dir()
    assert [lane["turns"] for lane in state["lanes"].values()] == [2, 2, 2]
    (source / "marker.txt").write_text("edited after crash\n", encoding="utf-8")

    resumed = SyncBranchingChainRuntime(agents(), "continue", config(), state)
    try:
        resumed.prepare()
    finally:
        resumed.executor.shutdown(wait=False, cancel_futures=True)
    assert not epoch.exists()
    assert state["events"][-1]["kind"] == "barrier_epoch_discarded"

    run(agents(), state, 3, task="continue")

    discarded = [e for e in state["events"] if e["kind"] == "barrier_epoch_discarded"]
    assert [event["epoch"] for event in discarded] == ["0001"]
    [receipt] = state["sync_branching_chain"]["epochs"]
    assert all(entry["turns"] == 2 for entry in receipt["lanes"].values())
    assert (epoch / "lanes/lane-1/worktree/marker.txt").read_text() == (
        "edited after crash\n"
    )
    assert [lane["turns"] for lane in state["lanes"].values()] == [3, 3, 3]


def test_failed_post_barrier_turn_redelivers_the_same_peer_batch(
    source: Path,
) -> None:
    chosen = agents()
    chosen.lane_2_actor_a.fail_turns.add(("lane-2", 3))
    state: dict[str, Any] = {}
    run(chosen, state, 12)

    failed, retried = lane_prompts(chosen, "lane-2")[3]
    assert "BARRIER 1 JUST COMPLETED" in failed
    assert "BARRIER 1 JUST COMPLETED" in retried
    batch = DELIVERY.findall(failed)
    assert len(batch) == 4
    assert DELIVERY.findall(retried) == batch
    first = state["sync_branching_chain"]["epochs"][0]["lanes"]
    assert state["bus_cursors"]["lane-2"] == {
        "lane-1": first["lane-1"]["report_bytes"],
        "lane-3": first["lane-3"]["report_bytes"],
    }


def test_isolation_forbids_every_peer_channel_but_the_own_checkpoint() -> None:
    prompt = barrier_protocol(
        lane="lane-2",
        interval=3,
        committed=0,
        session=1,
        run_root="/run",
        own_reports="/run/shared/reports/lane-2.jsonl",
        epoch=None,
        exchange=None,
    )
    allowed, forbidden = prompt.split("Do not\nread:", 1)
    forbidden = forbidden.split("The runtime hands you", 1)[0]
    assert "your checkpoint" in allowed
    for channel in (
        "`/run/shared/manifest.json`",
        "other chains' checkpoints under `/run/shared/checkpoints/`",
        "`/run/shared/planning-workspace/`",
        "`/run/shared/planning-revisions/`",
        "`/run/shared/state.json`",
    ):
        assert channel in forbidden
    assert "only `lane-2.json` there\n  is yours" in forbidden


def test_resume_with_a_different_barrier_interval_is_rejected(
    source: Path,
) -> None:
    state: dict[str, Any] = {}
    run(agents(), state, 3)
    with pytest.raises(ValueError, match="created with barrier_interval 2, not 3"):
        execute(agents(), TASK, config(3), state, _max_turns=3)
    assert state["sync_branching_chain"]["barrier_interval"] == 2


def test_candidate_board_stays_frozen_until_the_next_barrier(source: Path) -> None:
    chosen = agents(CandidateAgent)
    state: dict[str, Any] = {}
    run(chosen, state, 12)

    root = Path(state["run_root"])
    epochs = root / "shared" / "epochs"
    [before] = lane_prompts(chosen, "lane-1")[2]
    assert f"leaderboard at `{epochs / 'empty-leaderboard.json'}`" in before
    assert '"submission_count": 0' in before
    [late] = lane_prompts(chosen, "lane-1")[4]
    assert f"leaderboard at `{epochs / '0001' / 'leaderboard.json'}`" in late
    assert '"submission_count": 6' in late
    assert '"value": 898.0' in late

    def recorded_at(lane: str, turn: int) -> str:
        log = (root / "shared" / "reports" / f"{lane}.jsonl").read_text()
        return next(
            record["at"]
            for record in map(json.loads, log.splitlines())
            if record["turn"] == turn
        )

    # Lane-2's post-barrier best was already on the live board when lane-1's turn 4,
    # scheduled right after lane-1's turn 3 landed, was built.
    assert recorded_at("lane-2", 3) < recorded_at("lane-1", 3)
    assert '"value": 897.0' not in late
    assert '"value": 896.0' not in late
    assert len(state["sync_branching_chain"]["epochs"]) == 2
    frozen = json.loads((epochs / "0002" / "leaderboard.json").read_text())
    assert frozen["best"]["value"] == 896.0
