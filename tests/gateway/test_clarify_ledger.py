"""Tests for the durable pending-clarify ledger (gateway/clarify_ledger.py).

shared/claude-plugins lane-chat, #1037/#1040 class C6 follow-up: a clarify entry in
``tools/clarify_gateway.py`` lives in pure process memory. This ledger records enough about a
pending clarify to recognize, at the NEXT boot, that a prompt was orphaned by a restart — so the
owner gets one explicit notice instead of a silent skip.
"""
import time

import pytest

from gateway import clarify_ledger as cl


@pytest.fixture(autouse=True)
def _fresh_db(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setattr(cl, "_db_path", lambda: home / "state.db")
    yield


def _record(cid="cl-1", session_key="agent:main:telegram:dm:921626919", **kw):
    cl.record_pending(
        cid, session_key,
        question=kw.get("question", "Which approach?"),
        choices=kw.get("choices", ["A", "B"]),
        multi_select=kw.get("multi_select", False),
        platform=kw.get("platform", "telegram"),
        chat_id=kw.get("chat_id", "921626919"),
        thread_id=kw.get("thread_id"),
        adapter_profile=kw.get("adapter_profile"),
    )


def _orphan(cid):
    """Make the row look like it belongs to a dead process (same technique as
    tests/gateway/test_delivery_ledger.py's _orphan helper)."""
    with cl._connect() as conn:
        conn.execute(
            "UPDATE pending_clarifications SET owner_pid=999999999, "
            "owner_started_at=1 WHERE clarify_id=?", (cid,))


def _row_count():
    with cl._connect() as conn:
        return conn.execute("SELECT COUNT(*) FROM pending_clarifications").fetchone()[0]


class TestRecordAndDone:
    def test_record_then_mark_done_leaves_no_row(self):
        _record()
        assert _row_count() == 1
        cl.mark_done("cl-1")
        assert _row_count() == 0

    def test_mark_done_on_unknown_id_is_a_noop(self):
        cl.mark_done("does-not-exist")  # must not raise

    def test_record_or_replace_is_idempotent_by_id(self):
        _record(question="first")
        _record(question="second")
        assert _row_count() == 1


class TestSweepOrphaned:
    def test_live_owner_row_never_claimed(self):
        _record()  # owner = this (live) test process
        assert cl.sweep_orphaned() == []
        assert _row_count() == 1  # untouched — still legitimately open

    def test_dead_owner_row_claimed_and_removed(self):
        _record(question="Ship tonight or wait for review?", choices=["Ship", "Wait"])
        _orphan("cl-1")
        claimed = cl.sweep_orphaned()
        assert len(claimed) == 1
        row = claimed[0]
        assert row["clarify_id"] == "cl-1"
        assert row["question"] == "Ship tonight or wait for review?"
        assert row["choices"] == ["Ship", "Wait"]
        assert row["platform"] == "telegram"
        assert row["chat_id"] == "921626919"
        # Claimed rows are removed — a second sweep must not double-notify.
        assert _row_count() == 0
        assert cl.sweep_orphaned() == []

    def test_stale_row_is_pruned_without_a_claim(self):
        _record()
        _orphan("cl-1")
        with cl._connect() as conn:
            conn.execute(
                "UPDATE pending_clarifications SET created_at=? WHERE clarify_id=?",
                (time.time() - cl._STALE_AFTER_SECONDS - 1, "cl-1"))
        assert cl.sweep_orphaned() == []
        # Pruned, not left behind for a future (even more stale) notice.
        assert _row_count() == 0

    def test_multiple_dead_rows_all_claimed(self):
        _record(cid="cl-1", session_key="agent:main:telegram:dm:1")
        _record(cid="cl-2", session_key="agent:main:slack:channel:C1", platform="slack", chat_id="C1")
        _orphan("cl-1")
        _orphan("cl-2")
        claimed = cl.sweep_orphaned()
        assert {row["clarify_id"] for row in claimed} == {"cl-1", "cl-2"}
        assert _row_count() == 0

    def test_choices_round_trip_none_when_open_ended(self):
        _record(choices=None)
        _orphan("cl-1")
        claimed = cl.sweep_orphaned()
        assert claimed[0]["choices"] is None
