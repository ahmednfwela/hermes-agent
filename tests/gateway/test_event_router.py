"""Tests for gateway/event_router.py — the C2 (epic shared/claude-plugins#1040) typed event ->
jev severity -> lead wake router.

Owner ruling: "use jev for classification work" — the alarm-vs-routine decision must go through
jev (hermes_cluster.core.jev_client.classify), with a deterministic fallback ONLY on jev failure,
and every fail-open occurrence counted/reported (never silent).

Design (posted on #1040): ALARM/decision/failure severities call gateway.wake.deliver_wake into
the target session; ROUTINE severities are recorded (append-only ledger) and NEVER produce a chat
message. A jev classification failure (shim missing, node missing, timeout, import failure — any
`ok=False`) falls back to a small deterministic keyword rule and increments a counted, logged
fail-open marker so classifier-health is visible, never silently degraded.
"""

from __future__ import annotations

import asyncio
import json
import threading

import pytest

from gateway.event_router import (
    Event,
    classify_severity,
    fail_open_count,
    reset_fail_open_count,
    route_event,
)


@pytest.fixture(autouse=True)
def _reset_counter():
    reset_fail_open_count()
    yield
    reset_fail_open_count()


class _RunningLoop:
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
    def __init__(self):
        self.handled = []

    async def handle_message(self, event):
        self.handled.append(event)
        event._gateway_accepted = True


def _event(**overrides) -> Event:
    base = dict(
        kind="cron_alert", source="cron:overnight-stopship-watchdog",
        title="ALARM: build is broken", body="the stopship watchdog fired", metadata={},
    )
    base.update(overrides)
    return Event(**base)


class TestClassifySeverityJevPath:
    def test_jev_alarm_choice_is_alarm(self, monkeypatch):
        import gateway.event_router as er

        monkeypatch.setattr(
            er, "_jev_classify",
            lambda instruction, body, **k: {"ok": True, "choice": "ALARM", "probabilities": {"ALARM": 0.95}})
        severity, meta = classify_severity(_event())
        assert severity == "ALARM"
        assert meta["source"] == "jev"
        assert fail_open_count() == 0

    def test_jev_routine_choice_is_routine(self, monkeypatch):
        import gateway.event_router as er

        monkeypatch.setattr(
            er, "_jev_classify",
            lambda instruction, body, **k: {"ok": True, "choice": "ROUTINE", "probabilities": {"ROUTINE": 0.92}})
        severity, meta = classify_severity(_event(title="digest", body="nothing notable"))
        assert severity == "ROUTINE"
        assert meta["source"] == "jev"
        assert fail_open_count() == 0

    def test_always_serious_kind_bypasses_jev(self, monkeypatch):
        """node_offline is always-serious per the design note — never spends a jev call."""
        import gateway.event_router as er

        called = []
        monkeypatch.setattr(er, "_jev_classify", lambda *a, **k: called.append(1) or {"ok": True, "choice": "ROUTINE"})
        severity, meta = classify_severity(_event(kind="node_offline", title="node down", body="worker-3 unreachable"))
        assert severity == "ALARM"
        assert called == []
        assert meta["source"] == "always_serious_kind"


class TestClassifySeverityFailOpen:
    def test_jev_failure_falls_back_to_deterministic_and_counts(self, monkeypatch):
        import gateway.event_router as er

        monkeypatch.setattr(
            er, "_jev_classify", lambda instruction, body, **k: {"ok": False, "error": "shim_unavailable", "failOpen": True})
        severity, meta = classify_severity(_event(title="ALARM: build is broken", body="ci is red"))
        assert severity == "ALARM"  # deterministic keyword match ("ALARM")
        assert meta["source"] == "deterministic_fallback"
        assert fail_open_count() == 1

    def test_jev_failure_deterministic_routine_when_no_keyword_matches(self, monkeypatch):
        import gateway.event_router as er

        monkeypatch.setattr(
            er, "_jev_classify", lambda instruction, body, **k: {"ok": False, "error": "shim_unavailable", "failOpen": True})
        severity, meta = classify_severity(_event(kind="cron_alert", title="digest", body="everything is fine"))
        assert severity == "ROUTINE"
        assert meta["source"] == "deterministic_fallback"
        assert fail_open_count() == 1

    def test_jev_exception_also_counts_as_fail_open(self, monkeypatch):
        """An import failure (jev package not vendored into this pod) is a fail-open cause too —
        never a hard crash of the router."""
        import gateway.event_router as er

        def _raise(*a, **k):
            raise ImportError("no module named hermes_cluster")

        monkeypatch.setattr(er, "_jev_classify", _raise)
        severity, meta = classify_severity(_event(title="ALARM: x", body="y"))
        assert severity == "ALARM"
        assert meta["source"] == "deterministic_fallback"
        assert fail_open_count() == 1

    def test_fail_open_count_accumulates_across_calls(self, monkeypatch):
        import gateway.event_router as er

        monkeypatch.setattr(er, "_jev_classify", lambda *a, **k: {"ok": False, "error": "x", "failOpen": True})
        classify_severity(_event())
        classify_severity(_event())
        classify_severity(_event())
        assert fail_open_count() == 3


class TestRouteEvent:
    def test_alarm_wakes_the_target_session(self, running_loop, monkeypatch):
        import gateway.event_router as er

        monkeypatch.setattr(er, "classify_severity", lambda ev: ("ALARM", {"source": "test"}))
        recorded = []
        monkeypatch.setattr(er, "_record_routine_event", lambda ev, meta: recorded.append(ev))

        adapter = PushAdapter()
        result = route_event(
            _event(), adapter=adapter, loop=running_loop, platform_name="telegram",
            chat_id="921626919",
        )
        assert result.woke is True
        assert len(adapter.handled) == 1
        assert "ALARM: build is broken" in adapter.handled[0].text
        assert recorded == [], "an ALARM must never ALSO go through the routine ledger path"

    def test_routine_is_recorded_never_messaged(self, running_loop, monkeypatch):
        import gateway.event_router as er

        monkeypatch.setattr(er, "classify_severity", lambda ev: ("ROUTINE", {"source": "test"}))
        recorded = []
        monkeypatch.setattr(er, "_record_routine_event", lambda ev, meta: recorded.append((ev, meta)))

        adapter = PushAdapter()
        ev = _event(title="digest", body="nothing notable")
        result = route_event(
            ev, adapter=adapter, loop=running_loop, platform_name="telegram", chat_id="921626919",
        )
        assert result.woke is False
        assert adapter.handled == [], "a routine event must never produce a chat message"
        assert len(recorded) == 1
        assert recorded[0][0] is ev

    def test_wake_failure_still_records_so_the_event_is_not_lost(self, running_loop, monkeypatch):
        """An ALARM that fails to wake (adapter/loop unavailable) must still be durably recorded —
        never silently dropped."""
        import gateway.event_router as er

        monkeypatch.setattr(er, "classify_severity", lambda ev: ("ALARM", {"source": "test"}))
        recorded = []
        monkeypatch.setattr(er, "_record_routine_event", lambda ev, meta: recorded.append(ev))

        result = route_event(_event(), adapter=None, loop=None, platform_name="telegram", chat_id="921626919")
        assert result.woke is False
        assert len(recorded) == 1, "a failed wake must fall back to the durable record, not vanish"


class TestRecordRoutineEventLedger:
    def test_appends_a_json_line_with_event_and_severity_meta(self, tmp_path, monkeypatch):
        import gateway.event_router as er

        monkeypatch.setenv("HERMES_HOME", str(tmp_path))
        ev = _event(kind="lane_stalled", title="lane stalled", body="lane_key=abc idle 3h")
        er._record_routine_event(ev, {"source": "jev", "choice": "ROUTINE"})
        ledger = tmp_path / "events" / "ledger.jsonl"
        assert ledger.exists()
        row = json.loads(ledger.read_text(encoding="utf-8").strip().splitlines()[-1])
        assert row["kind"] == "lane_stalled"
        assert row["severity_meta"]["source"] == "jev"

    def test_multiple_records_append_not_overwrite(self, tmp_path, monkeypatch):
        import gateway.event_router as er

        monkeypatch.setenv("HERMES_HOME", str(tmp_path))
        er._record_routine_event(_event(), {"source": "jev"})
        er._record_routine_event(_event(), {"source": "jev"})
        ledger = tmp_path / "events" / "ledger.jsonl"
        assert len(ledger.read_text(encoding="utf-8").strip().splitlines()) == 2
