"""C2 event router (shared/claude-plugins#1040, "signals that never reach the lead"): one typed
event -> a jev severity decision -> either a REAL agent turn (gateway.wake.deliver_wake) or a
durable, never-messaged record.

Design (posted on #1040): every signal a human should eventually hear about — a cron alert, a
lane failing/stalling, a pending decision ballot, CI going red, a new Sentry issue, a node going
offline, release drift — becomes one of these typed events. This module is the ONE place that
decides ALARM (wake the owner's lead session now) vs ROUTINE (record it; never send a message for
it). Producers (cron delivery, a cluster-lane poller, ...) call `route_event`; they never decide
severity themselves and never call `deliver_wake` directly.

Severity decision, owner ruling ("use jev for classification work"): jev
(`hermes_cluster.core.jev_client.classify`) is the PRIMARY classifier. On ANY jev failure (shim
missing, node missing, timeout, or the package simply isn't vendored into this pod — an
ImportError is just another fail-open cause, not a hard dependency this module imposes), the
fallback is UNCONDITIONAL ALARM (`_deterministic_fallback`) — never content-based. A missed alarm
is worse than one extra wake, and (round-2 review finding, PR#3) a content/keyword-gated fallback
was found to silently under-wake real alert text that doesn't happen to contain one of a small
keyword set — exactly the "never wakes up" bug this router exists to fix, reintroduced by the
fallback path itself. `hermes_cluster` is measured NOT importable even on the deployed gateway pod
as of 2026-09-27, so this fallback is not a rare edge case — it is currently the path every
`wake_on_alert` delivery takes in production. Every fail-open is counted AND logged at WARNING —
per the ruling, "fail-open paths must be reported, not silent." `fail_open_count()` is read by the
digest / the continuous regression oracle (shared/claude-plugins#1050) to surface classifier-health
so this interim state stays visible, not silently permanent.

`kind`s in `_ALWAYS_SERIOUS_KINDS` skip jev entirely (never spend a classification call on a
signal that is unambiguous by construction, e.g. a node going offline) and route straight to ALARM.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from dataclasses import dataclass, field
from typing import Any, Optional

logger = logging.getLogger("gateway.event_router")

# A wake self-post runs a full agent turn synchronously; mirror gateway.wake's own generous ceiling
# so a slow tool-using turn triggered by an alarm is never killed mid-flight.
_WAKE_TIMEOUT_FALLBACK_SECONDS = 600.0

_JEV_INSTRUCTION = (
    "You are triaging a single operational event for an on-call human. Read the title and body. "
    "Answer ALARM if this represents a failure, an active incident, something broken, a pending "
    "decision that needs the human NOW, or anything else that warrants interrupting them "
    "immediately. Answer ROUTINE if this is informational, expected, or a healthy/OK status update "
    "that can simply be recorded for later. Respond with exactly one of: ALARM, ROUTINE."
)

# Kinds that are unambiguous by construction — spending a jev call on them would only add latency
# and cost for a decision that is never in doubt.
_ALWAYS_SERIOUS_KINDS = frozenset({"node_offline", "lane_failed"})

_fail_open_lock = threading.Lock()
_fail_open_total = 0


def fail_open_count() -> int:
    """Process-local count of jev classification fail-opens since the last reset. Surfaced by the
    digest / continuous oracle (#1050) as classifier-health, never hidden."""
    with _fail_open_lock:
        return _fail_open_total


def reset_fail_open_count() -> None:
    """Test-only reset. Production never resets this — it is a lifetime-of-process counter."""
    global _fail_open_total
    with _fail_open_lock:
        _fail_open_total = 0


def _bump_fail_open(reason: str) -> None:
    global _fail_open_total
    with _fail_open_lock:
        _fail_open_total += 1
        total = _fail_open_total
    logger.warning(
        "event_router: jev classification fail-open (%s) — falling back to deterministic rule "
        "(lifetime fail-opens: %d)", reason, total)


@dataclass
class Event:
    """One typed operational signal. ``kind`` identifies the producer's event type (see module
    docstring); ``source`` is a free-form producer identity for logs/audit (e.g.
    ``cron:overnight-stopship-watchdog``, ``cluster:<lane_key>``); ``title``+``body`` are both the
    jev classification input and the text a wake turn (or the routine ledger) carries."""

    kind: str
    source: str
    title: str
    body: str
    metadata: dict = field(default_factory=dict)

    @property
    def text(self) -> str:
        title = (self.title or "").strip()
        body = (self.body or "").strip()
        return f"{title}\n\n{body}" if title and body else title or body


def _jev_classify(instruction: str, body: str, **kwargs) -> dict:
    """Thin, lazily-imported call into the shared jev client. A missing package/shim in THIS pod
    (the gateway may not vendor hermes_cluster) is itself just another fail-open cause — never a
    hard dependency of this module. Never raises; any failure returns the client's own
    ``{"ok": False, ...}`` shape or an equivalent one built here."""
    try:
        from hermes_cluster.core.jev_client import classify
    except Exception as e:
        return {"ok": False, "error": f"jev_client_unavailable: {e}", "failOpen": True}
    try:
        return classify(instruction, body, **kwargs)
    except Exception as e:
        return {"ok": False, "error": f"jev_classify_raised: {e}", "failOpen": True}


def _deterministic_fallback(event: Event) -> str:
    """Fail-open verdict when jev is unavailable. UNCONDITIONALLY "ALARM" — no content matching of
    any kind.

    Round-2 review finding (PR#3, ahmednfwela/hermes-agent): the first version of this function
    gated ALARM on an 11-word keyword allowlist and defaulted to ROUTINE for everything else — the
    OPPOSITE of the "errs toward ALARM (a missed alarm is worse than one extra wake)" contract this
    module's docstring already promised. Measured live (kubectl exec into the deployed gateway pod,
    2026-09-27): `hermes_cluster` is not importable there either, so this fallback is not a rare
    edge case — it is, right now, the path EVERY `wake_on_alert` delivery in production takes. Real
    alert text routinely carries no exact keyword match (e.g. lane-health-patrol's own
    "STALE-CHECKPOINT: ... running 60+min, last owner-note stale/none" line matches none of the old
    list), so the keyword gate silently re-broke the exact bug `wake_on_alert` exists to fix (the
    owner's report: "they aren't giving my chat session a turn"). `event` is intentionally unused —
    kept as a parameter so a future REAL fallback heuristic (if one is ever justified) has the same
    call shape as `classify_severity` already expects, without another signature change."""
    del event
    return "ALARM"


def classify_severity(event: Event) -> tuple[str, dict]:
    """Return ``(severity, meta)`` where severity is ``"ALARM"`` or ``"ROUTINE"``.

    ``meta["source"]`` is one of ``"always_serious_kind"``, ``"jev"``, or
    ``"deterministic_fallback"`` — never silently unattributed, so a caller (or a test) can tell
    which path decided."""
    if event.kind in _ALWAYS_SERIOUS_KINDS:
        return "ALARM", {"source": "always_serious_kind", "kind": event.kind}

    try:
        resp = _jev_classify(_JEV_INSTRUCTION, event.text, threshold=0.6)
    except Exception as e:
        resp = {"ok": False, "error": f"jev_classify_raised: {e}", "failOpen": True}
    if not isinstance(resp, dict):
        resp = {"ok": False, "error": "jev_client_bad_response", "failOpen": True}
    if resp.get("ok") and str(resp.get("choice", "")).strip().upper() in ("ALARM", "ROUTINE"):
        choice = str(resp["choice"]).strip().upper()
        return choice, {"source": "jev", "probabilities": resp.get("probabilities")}

    _bump_fail_open(resp.get("error") or "unknown")
    return _deterministic_fallback(event), {
        "source": "deterministic_fallback", "jev_error": resp.get("error")}


@dataclass
class RouteResult:
    severity: str
    severity_meta: dict
    woke: bool


def _record_routine_event(event: Event, severity_meta: dict) -> None:
    """Append-only durable record — NEVER a chat message. Best-effort: a ledger write failure must
    not be allowed to crash event routing (the event was already classified; losing the ledger
    entry is a lesser failure than raising out of route_event)."""
    try:
        from hermes_constants import get_hermes_home
        ledger_dir = get_hermes_home() / "events"
        ledger_dir.mkdir(parents=True, exist_ok=True)
        row = {
            "kind": event.kind, "source": event.source, "title": event.title, "body": event.body,
            "metadata": event.metadata, "severity_meta": severity_meta,
        }
        with open(ledger_dir / "ledger.jsonl", "a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.warning("event_router: failed to record routine event %s: %s", event.kind, e)


def _deliver_wake_for_event(
    event: Event, *, adapter: Any, loop: Any, platform_name: str, chat_id: str,
    thread_id: Optional[str], user_id: Optional[str],
) -> bool:
    """Best-effort wake delivery. Returns True only on a confirmed wake — every failure mode
    (no adapter/loop, non-push adapter, WakeNotAccepted, any exception) returns False so the caller
    falls back to the durable record instead of losing the event."""
    if adapter is None or loop is None or not getattr(loop, "is_running", lambda: False)():
        return False
    try:
        from gateway.wake import WAKE_TURN_TIMEOUT_SECONDS, WakeNotAccepted, adapter_supports_push, deliver_wake
    except Exception:
        return False
    if not adapter_supports_push(adapter):
        return False
    try:
        from agent.async_utils import safe_schedule_threadsafe
        from gateway.config import Platform
        from gateway.session import SessionSource
        platform_enum = Platform(platform_name.lower())
    except Exception:
        return False
    source = SessionSource(
        platform=platform_enum, chat_id=str(chat_id), thread_id=thread_id, user_id=user_id)
    try:
        coro = deliver_wake(adapter, text=event.text, source=source)
        future = safe_schedule_threadsafe(coro, loop)
        if future is None:
            return False
        future.result(timeout=WAKE_TURN_TIMEOUT_SECONDS or _WAKE_TIMEOUT_FALLBACK_SECONDS)
        return True
    except WakeNotAccepted as e:
        logger.warning("event_router: wake not accepted for %s: %s", event.kind, e)
        return False
    except Exception as e:
        logger.warning("event_router: wake delivery failed for %s: %s", event.kind, e)
        return False


def route_event(
    event: Event, *, adapter: Any = None, loop: Any = None, platform_name: str = "telegram",
    chat_id: str = "", thread_id: Optional[str] = None, user_id: Optional[str] = None,
) -> RouteResult:
    """Classify ``event`` and either wake the target session (ALARM) or durably record it
    (ROUTINE) — never both, and a failed wake still gets recorded so the event is never silently
    dropped."""
    severity, meta = classify_severity(event)
    woke = False
    if severity == "ALARM":
        woke = _deliver_wake_for_event(
            event, adapter=adapter, loop=loop, platform_name=platform_name, chat_id=chat_id,
            thread_id=thread_id, user_id=user_id)
        if not woke:
            _record_routine_event(event, meta)
    else:
        _record_routine_event(event, meta)
    return RouteResult(severity=severity, severity_meta=meta, woke=woke)
