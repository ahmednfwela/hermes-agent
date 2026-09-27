"""Tests for tools/clarify_gateway.py's wiring into the durable pending-clarify ledger
(gateway/clarify_ledger.py) — shared/claude-plugins lane-chat, #1037/#1040 class C6 follow-up.

register() must record a durable row (so a restart before the answer arrives can be recognized
and surfaced instead of silently dropped) and wait_for_response()'s cleanup must remove it once
the entry is no longer open, for ANY reason (resolved, or timed out). Ledger failures must never
break the live in-memory resolve path — clarify keeps working even with the ledger unavailable.
"""
from __future__ import annotations

import threading
import time

import pytest

from gateway import clarify_ledger as cl


def _clear_clarify_state():
    from tools import clarify_gateway as cm
    with cm._lock:
        cm._entries.clear()
        cm._session_index.clear()
        cm._notify_cbs.clear()


@pytest.fixture(autouse=True)
def _fresh_ledger_db(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setattr(cl, "_db_path", lambda: home / "state.db")
    _clear_clarify_state()
    yield
    _clear_clarify_state()


def _ledger_row(clarify_id):
    with cl._connect() as conn:
        r = conn.execute(
            "SELECT session_key, platform, chat_id, thread_id, question FROM pending_clarifications "
            "WHERE clarify_id=?", (clarify_id,)).fetchone()
    return None if r is None else {
        "session_key": r[0], "platform": r[1], "chat_id": r[2], "thread_id": r[3], "question": r[4]}


class TestRegisterRecordsDurably:
    def test_register_with_routing_writes_a_row(self):
        from tools import clarify_gateway as cm
        cm.register("id1", "agent:main:telegram:dm:1", "Pick one", ["A", "B"],
                    platform="telegram", chat_id="921626919", thread_id=None)
        row = _ledger_row("id1")
        assert row is not None
        assert row["session_key"] == "agent:main:telegram:dm:1"
        assert row["platform"] == "telegram"
        assert row["chat_id"] == "921626919"
        assert row["question"] == "Pick one"

    def test_register_without_routing_still_records_and_still_works(self):
        """Backward compat: an internal/synthetic caller that passes no routing hints must not
        break — the live entry still registers, the ledger row just has no chat to target."""
        from tools import clarify_gateway as cm
        entry = cm.register("id2", "ballot:task_1", "Approve?", ["Yes", "No"])
        assert entry.clarify_id == "id2"
        row = _ledger_row("id2")
        assert row is not None
        assert row["platform"] is None
        assert row["chat_id"] is None

    def test_ledger_failure_does_not_break_registration(self, monkeypatch):
        """A broken ledger (e.g. a locked/corrupt state.db) must never prevent the LIVE clarify
        registration — the in-memory path is what the current process actually resolves against."""
        from tools import clarify_gateway as cm

        def _boom(*a, **k):
            raise RuntimeError("simulated ledger failure")

        monkeypatch.setattr(cl, "record_pending", _boom)
        entry = cm.register("id3", "sk3", "Q?", None)
        assert entry.clarify_id == "id3"
        with cm._lock:
            assert "id3" in cm._entries  # live path unaffected


class TestWaitForResponseClearsLedgerRow:
    def test_resolved_via_button_clears_the_row(self):
        from tools import clarify_gateway as cm
        cm.register("id4", "sk4", "Pick one", ["A", "B"], platform="telegram", chat_id="1")
        assert _ledger_row("id4") is not None

        def resolver():
            time.sleep(0.05)
            cm.resolve_gateway_clarify("id4", "B")

        threading.Thread(target=resolver).start()
        result = cm.wait_for_response("id4", timeout=10.0)
        assert result == "B"
        assert _ledger_row("id4") is None

    def test_timeout_also_clears_the_row(self):
        """A timed-out clarify is no longer open — its durable row must not linger forever."""
        from tools import clarify_gateway as cm
        cm.register("id5", "sk5", "Pick one", ["A", "B"], platform="telegram", chat_id="1")
        assert _ledger_row("id5") is not None
        result = cm.wait_for_response("id5", timeout=0.05)
        assert result is None
        assert _ledger_row("id5") is None

    def test_ledger_failure_on_cleanup_does_not_break_the_return_value(self, monkeypatch):
        from tools import clarify_gateway as cm
        cm.register("id6", "sk6", "Pick one", ["A", "B"])

        def _boom(*a, **k):
            raise RuntimeError("simulated ledger failure")

        monkeypatch.setattr(cl, "mark_done", _boom)

        def resolver():
            time.sleep(0.05)
            cm.resolve_gateway_clarify("id6", "A")

        threading.Thread(target=resolver).start()
        result = cm.wait_for_response("id6", timeout=10.0)
        assert result == "A"  # live path unaffected by the ledger raising
