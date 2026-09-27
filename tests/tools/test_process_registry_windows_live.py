"""LIVE Windows E2E for background-executor spawn parity (#70716 / PR salvage).

Runs ONLY on a real Windows host (the on-demand ``windows-venv-e2e.yml``
lane). The systemd cgroup-isolation feature for local background executors
must be a strict no-op on Windows: jobs spawn exactly as before, output is
captured, exit codes are correct, and no systemd code path is ever reached
— even when the process claims gateway identity.

These tests drive the REAL ``ProcessRegistry.spawn_local`` pipe path on the
live Windows process table (real Popen, real Git Bash shell, real reader
thread) — no mocked spawn.
"""

from __future__ import annotations

import os
import sys
import time

import pytest

pytestmark = pytest.mark.skipif(
    sys.platform != "win32", reason="live Windows background-executor E2E"
)


@pytest.fixture()
def registry(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    import tools.process_registry as pr

    reg = pr.ProcessRegistry()
    yield reg
    for sid in list(reg._running):
        try:
            reg.kill_process(sid)
        except Exception:
            pass


def _wait_exit(reg, sid, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        sess = reg._finished.get(sid) or reg._running.get(sid)
        if sess is not None and sess.exited:
            return sess
        time.sleep(0.2)
    raise AssertionError(f"session {sid} did not exit within {timeout}s")


class TestWindowsSpawnParity:
    def test_background_job_runs_output_and_exit_code_unchanged(self, registry):
        """Plain background job: spawned, output captured, exit code correct."""
        session = registry.spawn_local("echo win-live-parity; exit 7")
        done = _wait_exit(registry, session.id)

        assert done.exit_code == 7
        assert "win-live-parity" in done.output_buffer
        # The systemd scope identity must never be recorded on Windows.
        assert done.systemd_unit == ""

    def test_gateway_identity_never_reaches_systemd_path_on_windows(
        self, registry, monkeypatch
    ):
        """Even with full (faked) gateway identity, the Windows spawn takes
        the legacy path: no scope argv is built, no probe runs, and the job
        behaves exactly as without the identity."""
        import tools.process_registry as pr

        monkeypatch.setenv("_HERMES_GATEWAY", "1")
        monkeypatch.setattr(
            "gateway.status.get_running_pid",
            lambda *, cleanup_stale=False: os.getpid(),
        )
        monkeypatch.setattr(
            "gateway.restart.is_gateway_supervisor_process", lambda: True
        )
        monkeypatch.setattr(pr, "_SYSTEMD_SCOPE_AVAILABLE", None)

        scope_builds = []
        monkeypatch.setattr(
            pr,
            "_build_systemd_scope_argv",
            lambda *a, **k: scope_builds.append(a) or a[0],
        )

        session = registry.spawn_local("echo win-live-gateway; exit 3")
        done = _wait_exit(registry, session.id)

        assert done.exit_code == 3
        assert "win-live-gateway" in done.output_buffer
        assert done.systemd_unit == ""
        assert scope_builds == [], "Windows must never build a systemd scope argv"
        # The availability probe must not have flipped to True on Windows.
        assert pr._SYSTEMD_SCOPE_AVAILABLE is not True

    def test_kill_process_windows_plain_path(self, registry):
        """kill_process on Windows works without any systemd unit cleanup."""
        session = registry.spawn_local("sleep 60")
        time.sleep(1.0)
        result = registry.kill_process(session.id)
        assert result.get("status") in {"killed", "already_exited"}
        assert session.systemd_unit == ""

    def test_kill_process_reaps_msys_background_descendant(self, registry):
        """shared/claude-plugins#1042 / #1052: an MSYS/Cygwin background (``&``)
        child is reparented to a Cygwin-internal process the moment it is
        spawned -- it never appears under the outer bash's PID in Windows' own
        ParentProcessId table. ``taskkill /PID <pid> /T /F`` (the pre-fix
        mechanism) walks that table and returns SUCCESS on the outer bash while
        the background child keeps running untouched -- this was OmniRoute's
        ``npm run dev`` tree surviving ``agent_close`` on live Hermes.

        The fix assigns the outer process to a Windows Job Object with
        ``JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`` at spawn time; job membership
        propagates through process creation / handle inheritance and is
        independent of the (broken) ParentProcessId link, so closing the job
        handle must reap the descendant even though it was never a "child" in
        the sense ``taskkill /T`` can see.

        Identification of the descendant is via Cygwin's own
        ``/proc/<msys-pid>/winpid`` (the real Windows PID), not by scanning
        the process table by name+recency -- this machine runs other
        concurrent sessions whose own ``sleep.exe``/similar processes would
        otherwise contaminate the match. It is tagged with a ``WINPID:``
        marker in the captured output rather than matched as a bare number --
        an early draft of this test matched the first digit run in the
        buffer and silently captured the ``[1] <job-number>`` bash prints
        when it announces the background job (bash runs ``-lic`` here with no
        tty, so job control is off and it prints "no job control in this
        shell" ahead of the real output) instead of the real winpid, which
        made the assertions below pass VACUOUSLY (pid 1 doesn't exist on
        Windows, so both "ppid mismatch" and "pid gone after kill" were
        trivially true for the wrong pid, not evidence of anything).
        """
        import re

        import psutil

        session = registry.spawn_local(
            'sleep 600 >/dev/null 2>&1 & p=$!; printf "WINPID:%s\\n" "$(cat /proc/$p/winpid)"; sleep 300'
        )

        deadline = time.time() + 10
        winpid = None
        while time.time() < deadline:
            m = re.search(r"WINPID:(\d+)", session.output_buffer)
            if m:
                winpid = int(m.group(1))
                break
            time.sleep(0.1)
        assert winpid is not None, (
            f"never saw the background child's winpid in output: {session.output_buffer!r}"
        )

        # Confirm this really reproduces the MSYS-reparenting escape (and isn't
        # accidentally testing a normal, PPID-visible child): the descendant's
        # real Windows PPID must NOT be the outer bash pid process_registry
        # recorded as session.pid.
        try:
            child_ppid = psutil.Process(winpid).ppid()
        except psutil.NoSuchProcess:
            child_ppid = None
        assert child_ppid != session.pid, (
            "test setup did not reproduce the MSYS reparenting escape -- the "
            f"background child's PPID unexpectedly IS the outer bash pid ({session.pid}); "
            "this test is meaningless without that escape shape"
        )

        result = registry.kill_process(session.id)
        assert result.get("status") in {"killed", "already_exited"}

        deadline = time.time() + 10
        while time.time() < deadline and psutil.pid_exists(winpid):
            time.sleep(0.2)
        assert not psutil.pid_exists(winpid), (
            f"MSYS background descendant (winpid={winpid}) survived kill_process() -- "
            "taskkill /T's ParentProcessId walk missed it (shared/claude-plugins#1042/#1052)"
        )
