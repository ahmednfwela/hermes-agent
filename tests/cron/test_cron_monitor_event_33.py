"""gwi#33 (from hermes-agent-cluster#1018) — no_agent stdout → event_router sink.

Contract: whenever a no_agent cron job's script prints non-empty stdout, exactly ONE
``Event(kind='cron_monitor', source='cron:<job_id>', title=job_name, body=stdout)`` is
emitted through ``gateway.event_router.route_event`` — INDEPENDENT of the job's
``deliver`` value, so ``deliver: local`` keeps the owner DM silent while the lead
session's wake still fires via jev classification. Empty stdout emits nothing; the
wakeAgent=false gate does NOT suppress the event (the gate suppresses the AGENT turn /
owner DM, not a typed operational signal — hac#1018). A sink failure can never break or
delay the job's own delivery result, and with no lead target configured the sink skips
quietly.

No live gateway or adapter is needed (the acceptance names this harness shape): the
router is a captured stub installed in ``sys.modules`` and the script runner is
monkeypatched — the sink's wiring is what's under test, mirroring the ``route_event_fn``
injection seam ``gateway/lane_event_poller.py`` uses for producer #2.
"""

from __future__ import annotations

import sys
import types
from typing import Optional

import pytest

CRON_ENV = "HERMES_CLARIFY_NOTIFY_CHAT_ID"


class _FakeResult:
    severity = "ROUTINE"
    woke = False


class _FakeEvent:
    def __init__(self, kind: str, source: str, title: str, body: str,
                 metadata: Optional[dict] = None) -> None:
        self.kind, self.source, self.title, self.body = kind, source, title, body
        self.metadata = metadata or {}


class _StubRoute:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def __call__(self, event, **kwargs) -> _FakeResult:
        self.calls.append({"event": event, "kwargs": kwargs})
        return _FakeResult()


def _install_router(stub) -> None:
    """Replace gateway.event_router in sys.modules with a capturing stub (restored by
    the fixture's teardown via the saved originals)."""
    er = types.ModuleType("gateway.event_router")
    er.Event = _FakeEvent          # type: ignore[attr-defined]
    er.route_event = stub          # type: ignore[attr-defined]
    gw = sys.modules.get("gateway") or types.ModuleType("gateway")
    gw.event_router = er           # type: ignore[attr-defined]
    sys.modules.setdefault("gateway", gw)
    sys.modules["gateway.event_router"] = er


@pytest.fixture()
def sched(monkeypatch):
    """Real cron.scheduler with the script runner + workdir seam patched, lead chat
    pinned via the profile-env seam (cron_env_setting reads os.environ when no secret
    scope is active — every test runs without one)."""
    from cron import scheduler
    monkeypatch.setattr(scheduler, "_resolve_job_workdir", lambda job, job_id: None)
    monkeypatch.setattr(scheduler, "_run_job_script_with_claim_heartbeat",
                        lambda *a, **k: (True, "RAM 92% on host"))
    monkeypatch.setenv(CRON_ENV, "-100lead")
    return scheduler


def _job(**kw):
    j = {"id": "job-33", "name": "watchdog", "no_agent": True,
         "script": "w.sh", "deliver": "local"}
    j.update(kw)
    return j


def test_nonempty_stdout_routes_one_cron_monitor_event(sched, monkeypatch):
    stub = _StubRoute()
    _install_router(stub)
    ok, doc, final, err = sched._run_no_agent_job(_job(), "job-33", "watchdog", None)
    assert ok is True and err is None
    # delivered verbatim exactly as before — the sink never changes the job's own result
    assert final == "RAM 92% on host"
    assert len(stub.calls) == 1
    ev = stub.calls[0]["event"]
    assert (ev.kind, ev.source, ev.title, ev.body) == (
        "cron_monitor", "cron:job-33", "watchdog", "RAM 92% on host")
    kw = stub.calls[0]["kwargs"]
    assert kw["chat_id"] == "-100lead" and kw["platform_name"] == "telegram"


@pytest.mark.parametrize("deliver", ["local", "telegram", "origin", None])
def test_deliver_value_never_gates_the_event(sched, monkeypatch, deliver):
    """#33's whole point: the sink is independent of ``deliver``."""
    stub = _StubRoute()
    _install_router(stub)
    job = _job()
    if deliver is None:
        job.pop("deliver")
    else:
        job["deliver"] = deliver
    monkeypatch.setattr(sched, "_run_job_script_with_claim_heartbeat",
                        lambda *a, **k: (True, "metered"))
    ok, doc, final, err = sched._run_no_agent_job(job, "d", "n", None)
    assert ok is True
    assert [c["event"].source for c in stub.calls] == ["cron:d"]


def test_empty_stdout_emits_no_event(sched, monkeypatch):
    stub = _StubRoute()
    _install_router(stub)
    monkeypatch.setattr(sched, "_run_job_script_with_claim_heartbeat",
                        lambda *a, **k: (True, "   \n"))
    ok, doc, final, err = sched._run_no_agent_job(_job(), "job-33", "w", None)
    assert ok is True and final == "[SILENT]"
    assert stub.calls == []


def test_wake_agent_gate_does_not_suppress_the_event(sched, monkeypatch):
    stub = _StubRoute()
    _install_router(stub)
    monkeypatch.setattr(sched, "_parse_wake_gate", lambda out: False)
    ok, doc, final, err = sched._run_no_agent_job(_job(), "job-33", "w", None)
    assert ok is True and final == "[SILENT]"      # owner DM stays silent...
    assert len(stub.calls) == 1                      # ...the lead signal still routes
    assert stub.calls[0]["event"].body == "RAM 92% on host"


def test_sink_failure_never_breaks_delivery(sched, monkeypatch):
    class _Boom:
        def __call__(self, event, **kwargs):
            raise RuntimeError("wake lane exploded")
    _install_router(_Boom())
    ok, doc, final, err = sched._run_no_agent_job(_job(), "job-33", "w", None)
    assert ok is True and err is None and final == "RAM 92% on host"


def test_no_lead_target_skips_quietly(sched, monkeypatch):
    monkeypatch.delenv(CRON_ENV, raising=False)
    monkeypatch.delenv("HERMES_LEAD_CLARIFY_CHAT_IDS", raising=False)
    stub = _StubRoute()
    _install_router(stub)
    ok, doc, final, err = sched._run_no_agent_job(_job(), "job-33", "w", None)
    assert ok is True and final == "RAM 92% on host"
    assert stub.calls == []


def test_run_job_threads_adapters_and_loop_to_the_sink(sched, monkeypatch):
    """The no_agent sink is the ONLY reason run_job takes adapters/loop: without this
    threading the wake path stays dead (route_event records instead of waking)."""
    stub = _StubRoute()
    _install_router(stub)

    class _Loop:
        def is_running(self):
            return True

    marker_loop, marker_adapter = _Loop(), object()
    monkeypatch.setattr(sched, "_run_job_script_with_claim_heartbeat",
                        lambda *a, **k: (True, "wake me"))
    result = sched.run_job(_job(), adapters={"telegram": marker_adapter},
                           loop=marker_loop)
    assert len(stub.calls) == 1
    kw = stub.calls[0]["kwargs"]
    assert kw["adapter"] is marker_adapter and kw["loop"] is marker_loop
