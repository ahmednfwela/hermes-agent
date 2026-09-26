"""Cron alert delivery must be able to give the owner's chat session a REAL turn, not only
append the output to its transcript for the next natural turn to discover.

Owner report (2026-09-26): "look at the cron jobs in hermes, they are flagging issues, and
they aren't giving my chat session a turn, so it never wakes up."

Root cause: `_maybe_mirror_cron_delivery` (cron/scheduler_delivery.py) only calls
`gateway.mirror.mirror_to_session`, which appends a USER-role message to the transcript at the
NEXT turn boundary — passive. Nothing in the cron delivery path calls `gateway.wake.deliver_wake`
(the mechanism `gateway/kanban_watchers_notifier.py`'s `notify+wake` delivery_mode already uses
for kanban task-completion events). A cron job that flags an alert therefore never opens a real
turn; the owner only sees it if/when they happen to send another message later.

Fix under test: an opt-in per-job `wake_on_alert: true` flag. When set AND the resolved target is
mirror-eligible (the same origin/home eligibility `_target_mirror_eligible` already computes — an
explicit non-opted-in target or a broadcast `all` expansion is never woken), the live-adapter
delivery lane calls `deliver_wake` instead of the passive mirror. Any failure to wake (adapter not
push-capable, no running loop, `WakeNotAccepted`, or any other exception) falls back to the
existing passive mirror — this feature can only ever be additive, never a new way to lose a cron
delivery.
"""

from __future__ import annotations

import asyncio
import threading

import pytest

from cron.scheduler_delivery import (
    _TargetDelivery,
    _cron_wake_requested,
    _maybe_wake_cron_target,
    _seed_live_delivery_sessions,
)


class TestCronWakeRequested:
    def test_absent_field_is_not_requested(self):
        assert _cron_wake_requested({}) is False

    def test_false_is_not_requested(self):
        assert _cron_wake_requested({"wake_on_alert": False}) is False

    def test_truthy_non_bool_is_not_requested(self):
        """Strict True only — a stray string/int in hand-edited jobs.json must not silently
        opt a job into waking a live human session."""
        assert _cron_wake_requested({"wake_on_alert": "yes"}) is False
        assert _cron_wake_requested({"wake_on_alert": 1}) is False

    def test_true_is_requested(self):
        assert _cron_wake_requested({"wake_on_alert": True}) is True


class _RunningLoop:
    """A real asyncio loop running on a background thread, the same shape
    `agent.async_utils.safe_schedule_threadsafe` expects (a loop whose `is_running()` is True from
    another thread)."""

    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def stop(self):
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(timeout=5)
        self.loop.close()


@pytest.fixture()
def running_loop():
    rl = _RunningLoop()
    try:
        yield rl.loop
    finally:
        rl.stop()


class PushAdapter:
    """Push-capable adapter shape (no supports_async_delivery attribute -> defaults True)."""

    def __init__(self):
        self.handled = []

    async def handle_message(self, event):
        self.handled.append(event)
        event._gateway_accepted = True


class NonPushAdapter:
    supports_async_delivery = False


class TestMaybeWakeCronTarget:
    def test_no_adapter_or_loop_returns_false(self):
        assert _maybe_wake_cron_target(
            {"id": "j1"}, "telegram", "chat-1", "alert text", adapter=None, loop=None,
        ) is False

    def test_empty_text_returns_false(self, running_loop):
        assert _maybe_wake_cron_target(
            {"id": "j1"}, "telegram", "chat-1", "   ", adapter=PushAdapter(), loop=running_loop,
        ) is False

    def test_non_push_adapter_returns_false(self, running_loop):
        """A non-push (stateless) adapter needs a raw session_id, which cron jobs never carry —
        out of scope here; caller falls back to the passive mirror."""
        assert _maybe_wake_cron_target(
            {"id": "j1"}, "telegram", "chat-1", "alert text",
            adapter=NonPushAdapter(), loop=running_loop,
        ) is False

    def test_push_adapter_delivers_a_real_turn(self, running_loop):
        adapter = PushAdapter()
        job = {"id": "j1", "name": "overnight-stopship-watchdog"}
        ok = _maybe_wake_cron_target(
            job, "telegram", "chat-1", "ALARM: build is broken", adapter=adapter, loop=running_loop,
            thread_id=None, user_id="921626919",
        )
        assert ok is True
        assert len(adapter.handled) == 1
        event = adapter.handled[0]
        assert "[Cron delivery: overnight-stopship-watchdog]" in event.text
        assert "ALARM: build is broken" in event.text

    def test_wake_not_accepted_falls_back(self, running_loop, monkeypatch):
        from gateway.wake import WakeNotAccepted
        import cron.scheduler_delivery as sd

        async def _raise(*a, **k):
            raise WakeNotAccepted("busy")

        monkeypatch.setattr(sd, "deliver_wake", _raise, raising=False)

        class _Adapter(PushAdapter):
            pass

        # Patch the module-level import site used inside _maybe_wake_cron_target (it imports
        # gateway.wake lazily, so patch the source module instead of the re-export).
        import gateway.wake as wake_mod
        monkeypatch.setattr(wake_mod, "deliver_wake", _raise)

        ok = _maybe_wake_cron_target(
            {"id": "j2"}, "telegram", "chat-1", "alert", adapter=_Adapter(), loop=running_loop,
        )
        assert ok is False


class TestSeedLiveDeliverySessionsSkipsMirrorOnWake:
    """`_seed_live_delivery_sessions` is the call site: on a successful wake it must skip the
    redundant passive mirror (never both — one real turn already saw the content)."""

    def _target(self, job, *, adapter, loop, mirror_this_target=True):
        return _TargetDelivery(
            job=job, platform=None, platform_name="telegram", chat_id="chat-1", thread_id=None,
            transport=None, pconfig=None, runtime_adapter=adapter, target_adapters=None,
            config=None, loop=loop, notify_delivery=True, origin={}, origin_target=True,
            origin_user_id="921626919", is_dm_target=True, mirror_text="ALARM: something broke",
            mirror_this_target=mirror_this_target, in_channel_surface=False,
            inchannel_continuable=False, opened_thread_id=None,
        )

    def test_wake_requested_and_delivered_skips_mirror(self, running_loop, monkeypatch):
        import cron.scheduler_delivery as sd

        mirror_calls = []
        monkeypatch.setattr(
            sd, "_maybe_mirror_cron_delivery",
            lambda *a, **k: mirror_calls.append(k.get("enabled")))

        job = {"id": "j3", "name": "fleet-death-ledger", "wake_on_alert": True}
        adapter = PushAdapter()
        t = self._target(job, adapter=adapter, loop=running_loop)
        _seed_live_delivery_sessions(t, delivered_message_id=None)

        assert len(adapter.handled) == 1, "wake_on_alert must actually wake a real turn"
        assert mirror_calls == [False], "a successful wake must suppress the redundant passive mirror"

    def test_wake_not_requested_still_mirrors_as_before(self, running_loop, monkeypatch):
        """Regression control: jobs that never opt in are byte-for-byte unaffected."""
        import cron.scheduler_delivery as sd

        mirror_calls = []
        monkeypatch.setattr(
            sd, "_maybe_mirror_cron_delivery",
            lambda *a, **k: mirror_calls.append(k.get("enabled")))

        job = {"id": "j4", "name": "routine-digest"}
        adapter = PushAdapter()
        t = self._target(job, adapter=adapter, loop=running_loop)
        _seed_live_delivery_sessions(t, delivered_message_id=None)

        assert adapter.handled == [], "no wake without opt-in"
        assert mirror_calls == [True]

    def test_wake_requested_but_delivery_fails_falls_back_to_mirror(self, running_loop, monkeypatch):
        """A non-push adapter can't be woken; the existing mirror must still fire so the alert is
        not silently lost."""
        import cron.scheduler_delivery as sd

        mirror_calls = []
        monkeypatch.setattr(
            sd, "_maybe_mirror_cron_delivery",
            lambda *a, **k: mirror_calls.append(k.get("enabled")))

        job = {"id": "j5", "name": "cure-watcher", "wake_on_alert": True}
        adapter = NonPushAdapter()
        t = self._target(job, adapter=adapter, loop=running_loop)
        _seed_live_delivery_sessions(t, delivered_message_id=None)

        assert mirror_calls == [True], "failed wake must degrade to the passive mirror, never drop the alert"

    def test_wake_fires_on_an_explicit_non_mirror_eligible_target(self, running_loop, monkeypatch):
        """The real production shape (shared/claude-plugins#894 routing ruling): the alert-worthy
        jobs address the owner's DM/group as an EXPLICIT `telegram:<chat_id>` target, which is
        never mirror-eligible without ALSO setting `attach_to_session: true`. wake_on_alert must
        not require that second flag — the job author's `wake_on_alert: true` is itself the
        explicit per-job consent."""
        import cron.scheduler_delivery as sd

        mirror_calls = []
        monkeypatch.setattr(
            sd, "_maybe_mirror_cron_delivery",
            lambda *a, **k: mirror_calls.append(k.get("enabled")))

        job = {"id": "j6", "name": "overnight-stopship-watchdog", "wake_on_alert": True,
               "deliver": "telegram:921626919"}
        adapter = PushAdapter()
        # mirror_this_target=False: exactly the explicit-target-without-attach_to_session shape.
        t = self._target(job, adapter=adapter, loop=running_loop, mirror_this_target=False)
        _seed_live_delivery_sessions(t, delivered_message_id=None)

        assert len(adapter.handled) == 1, "wake_on_alert must not require attach_to_session too"
        assert mirror_calls == [False]
