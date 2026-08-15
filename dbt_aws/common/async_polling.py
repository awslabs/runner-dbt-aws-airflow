"""Polling primitives -- async for triggers, sync for non-deferrable ops.

Wraps sync ``boto3`` ``Get*`` / ``Describe*`` calls in
:func:`asyncio.to_thread` so the calling Trigger never blocks the
Triggerer event loop. This is the polling primitive every dbt-aws
custom trigger uses; it replaces the wedging pattern in the Amazon
Provider's triggers (which call ``boto3`` directly inside ``async
def`` methods).

Also exposes a sync counterpart :func:`poll_until_terminal_sync` used
by the non-deferrable code paths (``deferrable=False`` runners) which
block a worker slot until AWS reaches a terminal state instead of
handing off to the Triggerer. Useful for constrained metastores
(SQLite) and macOS Airflow standalone where the Triggerer can wedge.

Why this matters
----------------
The Triggerer process runs N triggers concurrently on a single
asyncio event loop. If one trigger blocks the loop (e.g. with a
sync ``boto3.client(...).get_job_run(...)`` call), every OTHER
deferred task on the same Triggerer is also stalled. Under high
fan-out (the canonical dbt-aws case: 8 bronze models all submitting
Glue Job runs at the same time), the loop wedges and tasks stay
``deferred`` long after the underlying AWS work has completed.

The fix is mechanical: take the sync call and hand it to a thread
pool via ``asyncio.to_thread``. The event loop stays free; other
triggers keep polling. This module exposes that as a small reusable
helper so every dbt-aws trigger looks the same.

Usage
-----
::

    from dbt_aws.common.async_polling import poll_until_terminal

    final_state = await poll_until_terminal(
        get_state=lambda: client.get_job_run(JobName=name, RunId=run_id)
                                 ["JobRun"]["JobRunState"],
        terminal_states=frozenset({"SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT"}),
        delay=15,
        max_attempts=240,
    )

A ``TimeoutError`` is raised if no terminal state is observed within
``max_attempts * delay`` seconds. The last observed state is included
in the error message for debuggability.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable


async def poll_until_terminal(
    *,
    get_state: Callable[[], str],
    terminal_states: frozenset[str],
    delay: int,
    max_attempts: int,
) -> str:
    """Poll ``get_state()`` every ``delay`` seconds until it returns a
    value in ``terminal_states`` or ``max_attempts`` is exhausted.

    Args:
        get_state: a zero-arg sync callable returning the current
            state string (e.g. ``lambda: client.get_job_run(...)
            ["JobRun"]["JobRunState"]``).
        terminal_states: the set of state strings considered terminal.
            The first time ``get_state()`` returns one of these, it is
            returned immediately.
        delay: seconds to ``await asyncio.sleep`` between polls.
        max_attempts: ceiling on the number of polls. ``TimeoutError``
            is raised after this many attempts return non-terminal
            states.

    Returns:
        the terminal state string observed.

    Raises:
        TimeoutError: when ``max_attempts`` polls all return non-terminal
            states. The last observed state is in the error message.

    Notes:
        ``get_state`` runs in a worker thread via
        :func:`asyncio.to_thread`, so the calling event loop is never
        blocked by the sync boto3 call. Any exception raised by
        ``get_state`` propagates out of ``poll_until_terminal``
        unchanged -- the caller decides whether to emit a failure
        ``TriggerEvent`` or retry.
    """
    state: str | None = None
    for _ in range(max_attempts):
        state = await asyncio.to_thread(get_state)
        if state in terminal_states:
            return state
        await asyncio.sleep(delay)
    raise TimeoutError(f"polling timed out after {max_attempts} attempts (state={state!r})")


def poll_until_terminal_sync(
    *,
    get_state: Callable[[], str],
    terminal_states: frozenset[str],
    delay: int,
    max_attempts: int,
) -> str:
    """Sync version of :func:`poll_until_terminal` for non-deferrable
    operators.

    Blocks the calling thread (an Airflow worker slot) with
    :func:`time.sleep` between polls. Use only when ``deferrable=False``
    is intentional -- e.g. on Airflow deployments where the Triggerer
    is unreliable (macOS Airflow standalone, SQLite metastore) or when
    the caller wants a simple sync path with no thread-pool trickery.

    Args:
        get_state: sync callable returning the current state string.
        terminal_states: set of state strings considered terminal.
        delay: seconds to :func:`time.sleep` between polls.
        max_attempts: ceiling on the number of polls.

    Returns:
        The terminal state string observed.

    Raises:
        TimeoutError: when ``max_attempts`` polls all return non-terminal
            states. The last observed state is in the error message.
    """
    state: str | None = None
    for _ in range(max_attempts):
        state = get_state()
        if state in terminal_states:
            return state
        time.sleep(delay)
    raise TimeoutError(f"polling timed out after {max_attempts} attempts (state={state!r})")


__all__ = ["poll_until_terminal", "poll_until_terminal_sync"]
