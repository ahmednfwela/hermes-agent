"""Durable pending-clarify ledger (rows in the shared ``state.db``; owner pid + process-start
liveness — same convention as ``gateway/delivery_ledger.py``) so a gateway crash/restart while a
clarify prompt is outstanding is a VISIBLE, understood event instead of a silent drop.

THE BUG THIS CLOSES (shared/claude-plugins lane-chat, #1037/#1040 class C6 follow-up)
  ``tools/clarify_gateway.py``'s pending-clarify registry (``_entries`` / ``_session_index``) is
  PURE IN-PROCESS MEMORY: a module-level dict guarded by a ``threading.Lock``, with zero
  persistence. The registered entry blocks an agent-thread on a ``threading.Event`` inside
  ``wait_for_response()`` until a button tap or the gateway's text-intercept resolves it. If the
  gateway process restarts (crash, OOM-kill, or any of the routine redeploys this deployment does
  many times a day) between "the owner tapped Other / was asked a question" and "the owner typed
  their answer", the blocked Python thread AND the in-memory entry are both gone — unrecoverable,
  by construction (a call stack cannot survive a process restart). The owner's next message (their
  actual answer) then falls through ``gateway/run_inbound.py::_hm_clarify_reply`` (nothing pending
  for that session) as an ordinary new chat turn: from the owner's side, this reads as "I answered
  and Hermes just changed the subject" — the reported "clarify... sometimes skips the message".

THE FIX (record, don't resurrect)
  True resumption is impossible (the blocked thread cannot be revived), so this module does the
  next best structural thing: durably record ENOUGH about a pending clarify (question, routing) to
  recognize, on the NEXT boot, that one was orphaned by the restart — and hands that list back to
  the caller (``gateway/run_startup.py``) to send ONE explicit notice naming the interrupted
  question, so the owner understands what happened and can simply re-ask or resend their answer,
  instead of a silent, unexplained non-sequitur reply.

Checkpoints: ``record_pending()`` right after ``tools.clarify_gateway.register()`` creates the
in-memory entry | ``mark_done()`` once ``wait_for_response()`` returns for ANY reason (resolved,
cancelled, or timed out) — the row's only job was surviving a crash while the prompt was open, and
that job ends the moment the prompt stops being open, regardless of how it ended. Everything here
is best-effort: ledger failures must never block a clarify; callers wrap every call in try/except
(mirroring ``delivery_ledger.py``'s own contract).
"""

from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any, Dict, List, Optional

from hermes_constants import get_hermes_home

logger = logging.getLogger(__name__)
_DB_LOCK = threading.Lock()

# Rows older than this are dropped by the sweep without a notice: an owner who has been silent
# for a day is not still holding their phone waiting on this particular prompt, and a notice about
# a week-old interrupted question would be confusing noise, not a fix.
_STALE_AFTER_SECONDS = 24 * 60 * 60


def _db_path():
    return get_hermes_home() / "state.db"


def _connect():
    from hermes_cli.sqlite_util import open_db

    # Shared state.db: SessionDB owns the durable PRAGMA set; this opener keeps the plain-tuple
    # rows, matching gateway/delivery_ledger.py's own connection contract.
    return open_db(_db_path(), db_label="state.db (clarify_ledger)", busy_timeout_ms=10_000,
                   row_factory=None, initialize=_initialize_schema)


def _initialize_schema(conn) -> None:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS pending_clarifications (
            clarify_id TEXT PRIMARY KEY,
            session_key TEXT NOT NULL,
            platform TEXT,
            chat_id TEXT,
            thread_id TEXT,
            question TEXT NOT NULL,
            choices_json TEXT,
            multi_select INTEGER NOT NULL DEFAULT 0,
            adapter_profile TEXT,
            created_at REAL NOT NULL,
            owner_pid INTEGER,
            owner_started_at INTEGER
        )"""
    )


def _transaction():
    from hermes_cli.sqlite_util import transaction

    return transaction(_connect())


def _owner_stamp() -> tuple:
    # Reuse delivery_ledger's process-liveness plumbing rather than duplicate it: same package,
    # same convention (PID + process-start-time, portable across the Windows sig-0 footgun this
    # codebase already worked around once — see delivery_ledger._owner_alive's own comment).
    from gateway.delivery_ledger import _owner_stamp as _ledger_owner_stamp
    return _ledger_owner_stamp()


def _owner_alive(pid: Any, started_at: Any) -> bool:
    from gateway.delivery_ledger import _owner_alive as _ledger_owner_alive
    return _ledger_owner_alive(pid, started_at)


def record_pending(clarify_id: str, session_key: str, *, question: str,
                   choices: Optional[List[str]] = None, multi_select: bool = False,
                   platform: Optional[str] = None, chat_id: Optional[str] = None,
                   thread_id: Optional[str] = None, adapter_profile: Optional[str] = None) -> None:
    """Record a clarify as durably outstanding. Best-effort: caller must swallow exceptions —
    this is bookkeeping for the crash-recovery path, never load-bearing for the live answer."""
    now, (pid, started) = time.time(), _owner_stamp()
    with _DB_LOCK, _transaction() as conn:
        conn.execute(
            """INSERT OR REPLACE INTO pending_clarifications
               (clarify_id, session_key, platform, chat_id, thread_id, question, choices_json,
                multi_select, adapter_profile, created_at, owner_pid, owner_started_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (str(clarify_id), str(session_key), str(platform) if platform else None,
             str(chat_id) if chat_id else None, str(thread_id) if thread_id else None,
             str(question or ""), json.dumps(list(choices), ensure_ascii=False) if choices else None,
             1 if multi_select else 0, str(adapter_profile) if adapter_profile else None,
             now, pid, started))


def mark_done(clarify_id: str) -> None:
    """Drop the durable row: the clarify is no longer open (resolved, cancelled, or the blocking
    wait timed out) — there's nothing left to be orphaned by a future restart."""
    with _DB_LOCK, _transaction() as conn:
        conn.execute("DELETE FROM pending_clarifications WHERE clarify_id=?", (str(clarify_id),))


def sweep_orphaned(now: Optional[float] = None) -> List[Dict[str, Any]]:
    """Claim + remove every row owned by a DEAD process (i.e. every row, at boot, by
    construction — a fresh process's own pid/start-time cannot match a row it never wrote).
    Rows past the staleness window are dropped silently (too old for a notice to make sense).
    Returns the claimed rows for the caller to notify; never raises (best-effort ledger)."""
    now = now if now is not None else time.time()
    claimed: List[Dict[str, Any]] = []
    with _DB_LOCK, _transaction() as conn:
        rows = conn.execute(
            """SELECT clarify_id, session_key, platform, chat_id, thread_id, question,
                      choices_json, multi_select, adapter_profile, created_at,
                      owner_pid, owner_started_at
               FROM pending_clarifications"""
        ).fetchall()
        for (cid, session_key, platform, chat_id, thread_id, question, choices_json,
             multi_select, adapter_profile, created_at, owner_pid, owner_started_at) in rows:
            if _owner_alive(owner_pid, owner_started_at):
                continue  # a live process still legitimately owns this prompt
            conn.execute("DELETE FROM pending_clarifications WHERE clarify_id=?", (cid,))
            if (now - created_at) > _STALE_AFTER_SECONDS:
                continue  # too old to notify about — silently pruned, not surfaced
            try:
                choices = json.loads(choices_json) if choices_json else None
            except Exception:
                choices = None
            claimed.append({
                "clarify_id": cid, "session_key": session_key, "platform": platform,
                "chat_id": chat_id, "thread_id": thread_id, "question": question,
                "choices": choices, "multi_select": bool(multi_select),
                "adapter_profile": adapter_profile, "created_at": created_at,
            })
    return claimed


def debug_rows(limit: int = 20) -> str:
    """Human-readable dump for ad-hoc inspection (mirrors delivery_ledger.debug_rows)."""
    with _DB_LOCK, _transaction() as conn:
        rows = conn.execute(
            """SELECT clarify_id, session_key, platform, chat_id, question, created_at
               FROM pending_clarifications ORDER BY created_at DESC LIMIT ?""", (limit,)
        ).fetchall()
    return json.dumps(
        [{"clarify_id": r[0], "session": r[1], "platform": r[2], "chat_id": r[3],
          "question": r[4], "created_at": r[5]} for r in rows], indent=2)
