"""C2 producer #2 (shared/claude-plugins#1040): lane done/failed/stalled from the cluster main,
routed through gateway.event_router the same way the cron producer (#1045) is.

Pure decision logic only — classifying a lane row's status, building the Event, and deduping
already-seen terminal states — is what's under test here; the HTTP/peer-HMAC transport to the
cluster main's /api/v1/tasks is a thin, separately-reasoned adapter (mirrors the "pure functions /
thin transport" split ops/lead-monitor/remote.py already uses for the same API surface) and is not
covered by network-dependent tests.
"""

from __future__ import annotations

import pytest

from gateway.lane_event_poller import LaneRow, SeenLaneTracker, build_lane_event, classify_lane_kind, poll_once


class TestClassifyLaneKind:
    @pytest.mark.parametrize("status", ["failed", "gave_up", "crashed"])
    def test_terminal_failure_statuses_classify_as_lane_failed(self, status):
        assert classify_lane_kind(status) == "lane_failed"

    @pytest.mark.parametrize("status", ["blocked", "stalled"])
    def test_stall_statuses_classify_as_lane_stalled(self, status):
        assert classify_lane_kind(status) == "lane_stalled"

    @pytest.mark.parametrize("status", ["completed", "running", "ready", "pending"])
    def test_healthy_statuses_are_not_events(self, status):
        assert classify_lane_kind(status) is None

    def test_case_insensitive(self):
        assert classify_lane_kind("FAILED") == "lane_failed"


class TestBuildLaneEvent:
    def test_event_carries_lane_identity_and_reason(self):
        row = LaneRow(lane_key="invora-backend#680", status="failed",
                       fail_reason="HTTP 429: rate limit", node="windows_desktop")
        event = build_lane_event(row)
        assert event.kind == "lane_failed"
        assert event.source == "cluster:invora-backend#680"
        assert "invora-backend#680" in event.title
        assert "HTTP 429" in event.body
        assert event.metadata["node"] == "windows_desktop"

    def test_falls_back_to_status_when_no_fail_reason(self):
        row = LaneRow(lane_key="k2", status="blocked")
        event = build_lane_event(row)
        assert event.body == "blocked"


class TestSeenLaneTracker:
    def test_first_sighting_is_new(self):
        tracker = SeenLaneTracker()
        row = LaneRow(lane_key="k1", status="failed")
        assert tracker.is_new(row) is True

    def test_repeat_sighting_of_the_same_status_is_not_new(self):
        tracker = SeenLaneTracker()
        row = LaneRow(lane_key="k1", status="failed")
        tracker.is_new(row)
        assert tracker.is_new(row) is False

    def test_a_status_change_on_the_same_lane_is_new_again(self):
        """A lane that failed, was resumed, then failed again must alarm again — dedup keys on
        (lane_key, status), not lane_key alone."""
        tracker = SeenLaneTracker()
        tracker.is_new(LaneRow(lane_key="k1", status="failed"))
        assert tracker.is_new(LaneRow(lane_key="k1", status="running")) is True
        assert tracker.is_new(LaneRow(lane_key="k1", status="failed")) is True


class TestPollOnce:
    def test_routes_one_event_per_new_failure(self):
        routed = []
        tracker = SeenLaneTracker()
        rows = [
            LaneRow(lane_key="k1", status="failed", fail_reason="oops"),
            LaneRow(lane_key="k2", status="completed"),
        ]
        results = poll_once(
            rows, tracker, route_event_fn=lambda ev, **kw: routed.append(ev) or "ROUTED")
        assert len(routed) == 1
        assert routed[0].kind == "lane_failed"
        assert results == ["ROUTED"]

    def test_does_not_reroute_an_already_seen_row(self):
        routed = []
        tracker = SeenLaneTracker()
        row = LaneRow(lane_key="k1", status="failed", fail_reason="oops")
        poll_once([row], tracker, route_event_fn=lambda ev, **kw: routed.append(ev))
        poll_once([row], tracker, route_event_fn=lambda ev, **kw: routed.append(ev))
        assert len(routed) == 1

    def test_passes_adapter_and_loop_through_to_route_event(self):
        captured = {}

        def _fake_route(ev, **kwargs):
            captured.update(kwargs)
            return None

        poll_once(
            [LaneRow(lane_key="k1", status="failed")], SeenLaneTracker(),
            adapter="ADAPTER", loop="LOOP", platform_name="telegram", chat_id="921626919",
            route_event_fn=_fake_route,
        )
        assert captured["adapter"] == "ADAPTER"
        assert captured["loop"] == "LOOP"
        assert captured["chat_id"] == "921626919"

    def test_default_route_event_fn_is_the_real_router(self, monkeypatch):
        """No route_event_fn override -> poll_once must call the real gateway.event_router.route_event,
        not silently no-op."""
        import gateway.event_router as er

        called = []
        monkeypatch.setattr(er, "route_event", lambda ev, **kw: called.append(ev))
        poll_once([LaneRow(lane_key="k1", status="failed")], SeenLaneTracker())
        assert len(called) == 1
