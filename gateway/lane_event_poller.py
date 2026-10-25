"""C2 producer #2 (shared/claude-plugins#1040): lane done/failed/stalled from the cluster main,
routed through ``gateway.event_router`` the same way the cron producer (shared/claude-plugins#1045)
is — a second, independent producer feeding the same one router, per the epic's "one event bus"
design.

This module deliberately keeps the DECISION logic (what counts as an event, what kind, dedup
against re-firing on an unchanged terminal state) pure and separately testable from the HTTP
transport to the cluster main's task API. That split mirrors the one
``ops/lead-monitor/remote.py`` already uses for the same ``/api/v1/tasks`` surface — that module is
a different lane's in-flight work and is NOT imported here (avoiding an undeclared dependency on
unmerged code); ``fetch_lane_rows`` below is this module's own thin, independently-owned transport,
intentionally minimal and not unit-tested (network I/O), following the same "pure logic / thin
transport" split for the same reason: exercising real peer-HMAC auth against a live cluster main
belongs in an integration/live-verification pass, not a fast unit suite.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Optional

from gateway.event_router import Event

logger = logging.getLogger("gateway.lane_event_poller")

# Terminal statuses the cluster main's /api/v1/tasks rows use (see cron/scheduler_delivery.py's
# own kanban event-kind table for the sibling vocabulary this mirrors).
_TERMINAL_FAILURE_STATUSES = frozenset({"failed", "gave_up", "crashed"})
_STALL_STATUSES = frozenset({"blocked", "stalled", "timed_out"})


@dataclass
class LaneRow:
    """One row from the cluster main's task listing, trimmed to what this producer needs."""

    lane_key: str
    status: str
    fail_reason: Optional[str] = None
    title: Optional[str] = None
    node: Optional[str] = None


def classify_lane_kind(status: str) -> Optional[str]:
    """``"lane_failed"``, ``"lane_stalled"``, or ``None`` (a healthy/in-progress status — not an
    event at all)."""
    s = (status or "").strip().lower()
    if s in _TERMINAL_FAILURE_STATUSES:
        return "lane_failed"
    if s in _STALL_STATUSES:
        return "lane_stalled"
    return None


def build_lane_event(row: LaneRow) -> Event:
    kind = classify_lane_kind(row.status) or "lane_failed"
    title = f"Lane {row.lane_key} {row.status}"
    body = row.fail_reason or row.title or row.status
    return Event(
        kind=kind, source=f"cluster:{row.lane_key}", title=title, body=body,
        metadata={"node": row.node, "status": row.status, "lane_key": row.lane_key})


class SeenLaneTracker:
    """De-dupes repeated sightings of a lane's CURRENT status, in-process — tracks the last-seen
    status per lane_key rather than a lifetime set of (lane_key, status) pairs, so a lane that
    failed, was resumed, then failed again alarms again (a lifetime set would wrongly treat the
    second failure as already-seen because that exact pair fired once before). A restart losing
    this in-memory state and re-emitting one duplicate event on a still-terminal row is an
    acceptable rare cost — the alternative, a real re-failure silently swallowed, is the failure
    direction this class exists to avoid."""

    def __init__(self) -> None:
        self._last_status: dict[str, str] = {}

    def is_new(self, row: LaneRow) -> bool:
        if self._last_status.get(row.lane_key) == row.status:
            return False
        self._last_status[row.lane_key] = row.status
        return True


def poll_once(
    rows: list[LaneRow], tracker: SeenLaneTracker, *, adapter: Any = None, loop: Any = None,
    platform_name: str = "telegram", chat_id: str = "",
    route_event_fn: Optional[Callable[..., Any]] = None,
) -> list[Any]:
    """Route one ``Event`` per newly-observed failed/stalled row. Healthy rows and already-seen
    (lane_key, status) pairs are skipped. ``route_event_fn`` defaults to the real
    ``gateway.event_router.route_event`` — injectable purely for tests.

    ``tracker.is_new(row)`` is called for EVERY row, healthy included — never only for rows that
    already passed the ``classify_lane_kind`` filter. Filtering first was a real bug (independent
    review, PR #4 round 1): the tracker's last-seen status for a lane_key would then never advance
    past a stale "failed" across a resume (`running` sightings never reached the tracker), so a
    lane that fails, resumes, and fails again silently failed to re-alarm on the second failure —
    it read as "already seen" against the FIRST failure's still-current tracker entry. Observing
    every row keeps the tracker's state honest; routing decides separately whether THIS observed
    change is alert-worthy at all."""
    if route_event_fn is None:
        from gateway.event_router import route_event as route_event_fn  # noqa: PLC0414
    results = []
    for row in rows:
        is_new = tracker.is_new(row)
        if not is_new or classify_lane_kind(row.status) is None:
            continue
        event = build_lane_event(row)
        results.append(route_event_fn(
            event, adapter=adapter, loop=loop, platform_name=platform_name, chat_id=chat_id))
    return results


def fetch_lane_rows(base_url: str, *, timeout_seconds: float = 10.0) -> list[LaneRow]:
    """Thin transport: GET ``{base_url}/api/v1/tasks`` and trim to ``LaneRow``. Deliberately
    minimal — no unit coverage here by design (see module docstring); the cluster main's peer-HMAC
    auth headers must be supplied by the caller's own httpx client/transport (this function accepts
    a base URL only so it composes with an already-authenticated ``httpx.Client``; see
    ``poll_forever`` for the intended wiring). Any failure returns an empty list rather than
    raising — a poller tick that can't reach the cluster main must not crash the gateway process
    that also owns cron, and a raised exception here would do exactly that."""
    import httpx

    try:
        with httpx.Client(base_url=base_url, timeout=timeout_seconds) as client:
            resp = client.get("/api/v1/tasks")
            resp.raise_for_status()
            payload = resp.json()
    except Exception as e:
        logger.warning("lane_event_poller: fetch_lane_rows failed against %s: %s", base_url, e)
        return []
    rows_raw = payload.get("tasks") if isinstance(payload, dict) else payload
    rows: list[LaneRow] = []
    for r in rows_raw or []:
        if not isinstance(r, dict):
            continue
        lane_key = r.get("lane_key") or r.get("id")
        status = r.get("status")
        if not lane_key or not status:
            continue
        rows.append(LaneRow(
            lane_key=str(lane_key), status=str(status), fail_reason=r.get("fail_reason"),
            title=r.get("title"), node=r.get("node")))
    return rows
