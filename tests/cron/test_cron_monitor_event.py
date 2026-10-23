"""no_agent monitor stdout reaches the C2 event router as a cron_monitor Event regardless of
``deliver`` (hermes-agent-cluster#1018), with no live gateway/adapter."""
from unittest.mock import patch

from cron.scheduler_delivery import _emit_cron_monitor_event
from gateway.event_router import RouteResult


def _job(**kw):
    return {"id": "j1", "name": "fleet-selfheal-monitor", "no_agent": True, "deliver": "local", **kw}


def _routed(job, output):
    with patch("gateway.event_router.route_event",
               return_value=RouteResult(severity="ALARM", severity_meta={}, woke=True)) as m:
        woke = _emit_cron_monitor_event(job, output)
    return woke, m


def test_nonempty_stdout_emits_cron_monitor_event_even_with_deliver_local():
    woke, m = _routed(_job(event_target={"platform": "telegram", "chat_id": "42"}), "node X dead\n")
    assert woke is True
    ev = m.call_args.args[0]
    assert (ev.kind, ev.source, ev.title, ev.body) == (
        "cron_monitor", "cron:j1", "fleet-selfheal-monitor", "node X dead")
    assert m.call_args.kwargs["chat_id"] == "42"


def test_empty_or_silent_output_emits_nothing():
    for out in ("", "  \n", "[SILENT]"):
        _, m = _routed(_job(), out)
        m.assert_not_called()


def test_non_no_agent_job_emits_nothing():
    _, m = _routed(_job(no_agent=False), "text")
    m.assert_not_called()


def test_router_failure_never_raises():
    with patch("gateway.event_router.route_event", side_effect=RuntimeError("boom")):
        assert _emit_cron_monitor_event(_job(), "text") is False
