"""shared/claude-plugins lane-chat (#1037/#1040 class C6 follow-up): a cold boot
(``is_reconnect=False``) — which is what EVERY container restart looks like, not just a
genuine first-ever install — has always told Telegram ``drop_pending_updates=True``. That
evicts, permanently, any message sent while the gateway process was down (crash, OOM,
redeploy): the Bot API server-side queue is discarded and never redelivered. Upstream's own
contract, per NousResearch/hermes-agent#46621 (which fixed ONLY the watcher-reconnect leg),
explicitly leaves cold boot dropping the queue as "reasonable" — a fine default for a
dormant/first-run install, but wrong for an always-on hosted single-tenant deployment whose
"cold boot" is a routine pod restart indistinguishable, from the sender's perspective, from a
network blip.

These tests pin the new opt-in: ``extra.preserve_queue_on_cold_boot`` (default False, so
every other deployment's behavior is byte-identical to before) makes a cold boot behave like
a reconnect for the one purpose that matters here — never telling Telegram to evict queued
updates.
"""
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.config import PlatformConfig
from plugins.platforms.telegram.adapter import TelegramAdapter


def _adapter(*, preserve: bool = False) -> TelegramAdapter:
    extra = {"preserve_queue_on_cold_boot": True} if preserve else {}
    return TelegramAdapter(PlatformConfig(enabled=True, token="test-token", extra=extra))


# ---------------------------------------------------------------------------
# Unit level: the decision function itself, no PTB/asyncio machinery involved.
# ---------------------------------------------------------------------------

def test_cold_boot_drops_by_default():
    """Unchanged default: a cold boot with no opt-in still drops (today's behavior)."""
    a = _adapter(preserve=False)
    assert a._should_drop_pending_updates(is_reconnect=False) is True


def test_cold_boot_preserves_when_opted_in():
    """The fix: a cold boot on a deployment that opted in must NOT drop."""
    a = _adapter(preserve=True)
    assert a._should_drop_pending_updates(is_reconnect=False) is False


def test_reconnect_always_preserves_regardless_of_flag():
    """A watcher reconnect (#46621's already-fixed leg) must keep preserving the queue
    whether or not the new flag is set — the flag only changes the COLD-BOOT decision."""
    assert _adapter(preserve=False)._should_drop_pending_updates(is_reconnect=True) is False
    assert _adapter(preserve=True)._should_drop_pending_updates(is_reconnect=True) is False


def test_flag_accepts_string_forms():
    """Config values arrive as YAML-parsed strings in some paths; _coerce_bool_extra's
    existing string contract (\"true\"/\"1\"/\"yes\"/\"on\") must be honored here too."""
    a = TelegramAdapter(PlatformConfig(enabled=True, token="t", extra={"preserve_queue_on_cold_boot": "true"}))
    assert a._should_drop_pending_updates(is_reconnect=False) is False


# ---------------------------------------------------------------------------
# Wiring level: _start_polling_mode must actually pass the decision through to
# _start_polling_resilient's drop_pending_updates kwarg (catches a regression where
# someone bypasses the helper and hardcodes `not is_reconnect` again).
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_start_polling_mode_wires_preserved_flag_through(monkeypatch):
    a = _adapter(preserve=True)
    monkeypatch.setattr(a, "_delete_webhook_best_effort", AsyncMock(return_value=None))
    captured = {}

    async def _fake_start_polling_resilient(*, drop_pending_updates, error_callback, require_progress):
        captured["drop_pending_updates"] = drop_pending_updates
        captured["require_progress"] = require_progress
        return True

    monkeypatch.setattr(a, "_start_polling_resilient", _fake_start_polling_resilient)
    await a._start_polling_mode(is_reconnect=False)
    assert captured["drop_pending_updates"] is False
    # require_progress is a SEPARATE decision (cold-start readiness strictness) and must stay
    # tied to is_reconnect regardless of the new flag.
    assert captured["require_progress"] is True


@pytest.mark.asyncio
async def test_start_polling_mode_default_still_drops_on_cold_boot(monkeypatch):
    a = _adapter(preserve=False)
    monkeypatch.setattr(a, "_delete_webhook_best_effort", AsyncMock(return_value=None))
    captured = {}

    async def _fake_start_polling_resilient(*, drop_pending_updates, error_callback, require_progress):
        captured["drop_pending_updates"] = drop_pending_updates
        return True

    monkeypatch.setattr(a, "_start_polling_resilient", _fake_start_polling_resilient)
    await a._start_polling_mode(is_reconnect=False)
    assert captured["drop_pending_updates"] is True
