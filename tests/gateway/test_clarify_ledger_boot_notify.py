"""Tests for the boot-time orphaned-clarify sweep + notify (gateway/run_startup.py:
_claim_orphaned_clarifies / _notify_orphaned_clarifies) — shared/claude-plugins lane-chat,
#1037/#1040 class C6 follow-up.

A gateway that restarted while a clarify prompt was open cannot resume the blocked agent thread
(the call stack is gone), so the structural fix is a visible notice on the next boot naming the
interrupted question, instead of the owner's answer silently falling through as an unrelated new
turn. These tests exercise the claim + notify halves directly (mirrors the existing
delivery-ledger boot-recovery tests' shape).
"""
import pytest

from gateway import clarify_ledger as cl
from gateway.config import Platform
from tests.gateway.restart_test_helpers import make_restart_runner


@pytest.fixture(autouse=True)
def _fresh_ledger_db(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setattr(cl, "_db_path", lambda: home / "state.db")
    yield


def _orphan(cid):
    with cl._connect() as conn:
        conn.execute(
            "UPDATE pending_clarifications SET owner_pid=999999999, "
            "owner_started_at=1 WHERE clarify_id=?", (cid,))


class TestClaimOrphanedClarifies:
    @pytest.mark.asyncio
    async def test_claims_dead_owner_rows(self):
        runner, _adapter = make_restart_runner()
        cl.record_pending("cl-1", "agent:main:telegram:dm:1", question="Ship now?",
                          choices=["Yes", "No"], platform="telegram", chat_id="1")
        _orphan("cl-1")
        claimed = await runner._claim_orphaned_clarifies()
        assert len(claimed) == 1
        assert claimed[0]["clarify_id"] == "cl-1"

    @pytest.mark.asyncio
    async def test_no_rows_returns_empty_list(self):
        runner, _adapter = make_restart_runner()
        assert await runner._claim_orphaned_clarifies() == []

    @pytest.mark.asyncio
    async def test_broken_ledger_import_degrades_to_empty_list(self, monkeypatch):
        """A ledger failure must never break gateway boot — degrade to 'nothing to notify'."""
        runner, _adapter = make_restart_runner()

        def _boom(*a, **k):
            raise RuntimeError("simulated ledger failure")

        monkeypatch.setattr(cl, "sweep_orphaned", _boom)
        assert await runner._claim_orphaned_clarifies() == []


class TestNotifyOrphanedClarifies:
    @pytest.mark.asyncio
    async def test_sends_one_notice_naming_the_question(self):
        runner, adapter = make_restart_runner()
        claimed = [{
            "clarify_id": "cl-1", "session_key": "agent:main:telegram:dm:1",
            "platform": "telegram", "chat_id": "1", "thread_id": None,
            "question": "Ship tonight or wait for review?", "choices": ["Ship", "Wait"],
            "adapter_profile": None,
        }]
        notified = await runner._notify_orphaned_clarifies(claimed)
        assert notified == 1
        assert len(adapter.sent_calls) == 1
        chat_id, content, _metadata = adapter.sent_calls[0]
        assert chat_id == "1"
        assert "Ship tonight or wait for review?" in content
        assert "restarted" in content.lower()

    @pytest.mark.asyncio
    async def test_no_routing_info_is_skipped_not_raised(self):
        runner, adapter = make_restart_runner()
        claimed = [{
            "clarify_id": "cl-ballot", "session_key": "ballot:task_1", "platform": None,
            "chat_id": None, "thread_id": None, "question": "Approve?", "choices": None,
            "adapter_profile": None,
        }]
        notified = await runner._notify_orphaned_clarifies(claimed)
        assert notified == 0
        assert adapter.sent_calls == []

    @pytest.mark.asyncio
    async def test_unknown_platform_is_skipped_not_raised(self):
        runner, adapter = make_restart_runner()
        claimed = [{
            "clarify_id": "cl-weird", "session_key": "sk", "platform": "carrier-pigeon",
            "chat_id": "1", "thread_id": None, "question": "Q?", "choices": None,
            "adapter_profile": None,
        }]
        notified = await runner._notify_orphaned_clarifies(claimed)
        assert notified == 0
        assert adapter.sent_calls == []

    @pytest.mark.asyncio
    async def test_no_connected_adapter_for_platform_is_skipped(self):
        runner, _adapter = make_restart_runner()
        runner.adapters = {}  # nothing connected
        claimed = [{
            "clarify_id": "cl-1", "session_key": "sk", "platform": "telegram",
            "chat_id": "1", "thread_id": None, "question": "Q?", "choices": None,
            "adapter_profile": None,
        }]
        notified = await runner._notify_orphaned_clarifies(claimed)
        assert notified == 0

    @pytest.mark.asyncio
    async def test_multiple_rows_each_get_their_own_notice(self):
        runner, adapter = make_restart_runner()
        claimed = [
            {"clarify_id": "cl-1", "session_key": "sk1", "platform": "telegram", "chat_id": "1",
             "thread_id": None, "question": "Q1?", "choices": None, "adapter_profile": None},
            {"clarify_id": "cl-2", "session_key": "sk2", "platform": "telegram", "chat_id": "2",
             "thread_id": None, "question": "Q2?", "choices": None, "adapter_profile": None},
        ]
        notified = await runner._notify_orphaned_clarifies(claimed)
        assert notified == 2
        assert {c[0] for c in adapter.sent_calls} == {"1", "2"}

    @pytest.mark.asyncio
    async def test_empty_claim_list_is_a_noop(self):
        runner, adapter = make_restart_runner()
        assert await runner._notify_orphaned_clarifies([]) == 0
        assert adapter.sent_calls == []
