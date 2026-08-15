"""Glue-Job-level concurrency control for ``GlueSparkRunner`` /
``GluePythonShellRunner``.

When two Airflow DAGs (or two runs of the same DAG) submit a JobRun
for the same dbt node against the same Glue Job, the lib can:

* ``allow`` -- do nothing special; let Glue's own concurrency apply.
  Multiple JobRuns will execute in parallel (subject to the Job's
  ``MaxConcurrentRuns``).

* ``join`` -- detect the in-flight matching JobRun and ATTACH to it.
  Both Airflow tasks defer to the same JobRun, sharing its outcome.
  Zero duplicate compute; both tasks finish together.

* ``queue`` -- detect the in-flight match and WAIT for it to finish,
  then submit our own fresh JobRun. Two sequential JobRuns.

A JobRun matches when ALL of these ``Arguments`` are equal between
the candidate run and the new submission:

* ``--command``      -- same dbt verb
* ``--select``       -- same dbt node
* ``--target``       -- same dbt target
* ``--vars``         -- same dbt vars (or both unset)
* ``--full-refresh`` -- same full-refresh flag (or both unset)

Stricter than just matching ``(command, select)`` -- different targets
or vars mean different intent and must not share runs.

All deferrable: the wait + run lifecycle uses the Airflow Triggerer,
no worker pinning.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any, Literal

try:
    from botocore.exceptions import ClientError
except ImportError:  # pragma: no cover -- CI without boto3 installed
    ClientError = Exception  # noqa: F811

if TYPE_CHECKING:  # pragma: no cover
    pass

#: JobRun states the lib treats as "in-flight" for match purposes.
#: STOPPING is intentionally included for ``queue`` -- if a previous run
#: is being stopped, we still wait for the API to confirm terminal.
_IN_FLIGHT_STATES: frozenset[str] = frozenset({"STARTING", "RUNNING", "STOPPING"})

#: Script-args keys that participate in the match.
_MATCH_KEYS: tuple[str, ...] = (
    "--command",
    "--select",
    "--target",
    "--vars",
    "--full-refresh",
)

#: Default poll cadence for the ``queue``-mode trigger.
_DEFAULT_QUEUE_POLL_SECONDS = 30
#: Default maximum polls before the trigger times out (60 min at 30s).
_DEFAULT_QUEUE_MAX_ATTEMPTS = 120


ConcurrentRunsMode = Literal["allow", "join", "queue"]


_log = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# Helpers (sync; usable from both operator and trigger)
# ----------------------------------------------------------------------
def find_matching_in_flight_run(
    *,
    glue_client: Any,
    job_name: str,
    match_args: dict[str, str],
) -> str | None:
    """Return the ``JobRunId`` of an in-flight JobRun whose Arguments
    match all keys in :data:`_MATCH_KEYS`, or ``None`` if no match.

    ``match_args`` is the new submission's script_args -- we compare
    each key in :data:`_MATCH_KEYS`.
    """
    target = {k: match_args.get(k) for k in _MATCH_KEYS}
    paginator = glue_client.get_paginator("get_job_runs")
    for page in paginator.paginate(JobName=job_name):
        for run in page.get("JobRuns") or []:
            if run.get("JobRunState") not in _IN_FLIGHT_STATES:
                continue
            run_args = run.get("Arguments") or {}
            if all(run_args.get(k) == target[k] for k in _MATCH_KEYS):
                return run.get("Id")
    return None


# ----------------------------------------------------------------------
# Trigger for `queue` mode -- async polls until match clears
# ----------------------------------------------------------------------
def _build_no_match_trigger() -> type:
    """Lazily build the trigger so importing this module doesn't pull
    Airflow at the top level."""
    from airflow.triggers.base import BaseTrigger, TriggerEvent

    class _GlueJobNoMatchTrigger(BaseTrigger):
        """Polls ``glue:GetJobRuns`` until no in-flight JobRun matches
        the new submission's args. Then yields a ``no_match`` event and
        the operator can resume to submit its own run."""

        def __init__(
            self,
            job_name: str,
            match_args: dict[str, str],
            aws_conn_id: str = "aws_default",
            region_name: str | None = None,
            waiter_delay: int = _DEFAULT_QUEUE_POLL_SECONDS,
            waiter_max_attempts: int = _DEFAULT_QUEUE_MAX_ATTEMPTS,
        ) -> None:
            super().__init__()
            self.job_name = job_name
            self.match_args = match_args
            self.aws_conn_id = aws_conn_id
            self.region_name = region_name
            self.waiter_delay = waiter_delay
            self.waiter_max_attempts = waiter_max_attempts

        def serialize(self) -> tuple[str, dict[str, Any]]:
            return (
                "dbt_aws.common.airflow_extras.glue_concurrent.GlueJobNoMatchTrigger",
                {
                    "job_name": self.job_name,
                    "match_args": self.match_args,
                    "aws_conn_id": self.aws_conn_id,
                    "region_name": self.region_name,
                    "waiter_delay": self.waiter_delay,
                    "waiter_max_attempts": self.waiter_max_attempts,
                },
            )

        async def run(self) -> AsyncIterator[TriggerEvent]:
            client = _get_glue_client(self.aws_conn_id, self.region_name)
            for attempt in range(self.waiter_max_attempts):
                in_flight = await asyncio.to_thread(
                    find_matching_in_flight_run,
                    glue_client=client,
                    job_name=self.job_name,
                    match_args=self.match_args,
                )
                if in_flight is None:
                    yield TriggerEvent({"status": "no_match"})
                    return
                _log.info(
                    "queue: waiting for in-flight JobRun %s (attempt %d/%d)",
                    in_flight,
                    attempt + 1,
                    self.waiter_max_attempts,
                )
                await asyncio.sleep(self.waiter_delay)
            yield TriggerEvent(
                {
                    "status": "timeout",
                    "message": f"queue: timed out waiting for in-flight JobRun on "
                    f"{self.job_name!r} to terminate",
                }
            )

    return _GlueJobNoMatchTrigger


# Eagerly bind on first import so trigger serialization works.
GlueJobNoMatchTrigger = _build_no_match_trigger()


def _get_glue_client(aws_conn_id: str, region_name: str | None):  # noqa: ANN202
    from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

    hook = AwsBaseHook(aws_conn_id=aws_conn_id, client_type="glue", region_name=region_name)
    return hook.get_conn()


# ----------------------------------------------------------------------
# Operator subclass factories
# ----------------------------------------------------------------------
def build_concurrent_runs_operator_class(mode: ConcurrentRunsMode) -> type:
    """Build a :class:`GlueJobOperator` subclass that enforces
    ``concurrent_runs`` semantics.

    Returns a class ready to instantiate with the same kwargs as
    :class:`GlueJobOperator`. The class lazily imports the operator
    so this module is loadable without the Amazon provider.

    ``mode='allow'`` returns the unmodified ``GlueJobOperator`` -- the
    caller can use this factory uniformly and only get extra behaviour
    when needed.
    """
    if mode == "allow":
        from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

        return GlueJobOperator
    if mode == "join":
        return _build_join_aware_operator_class()
    if mode == "queue":
        return _build_queueing_operator_class()
    raise ValueError(f"concurrent_runs must be 'allow', 'join', or 'queue', got {mode!r}")


def _build_join_aware_operator_class() -> type:
    """``join`` mode: scan for in-flight match BEFORE start_job_run.
    If found, hijack ``self._job_run_id`` so ``GlueJobOperator``'s
    ``execute()`` skips the start and defers to the existing run."""
    from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

    class _JoinAwareGlueJobOperator(GlueJobOperator):
        def execute(self, context: Any) -> Any:
            try:
                client = self.hook.conn
                match = find_matching_in_flight_run(
                    glue_client=client,
                    job_name=self.job_name,
                    match_args=self.script_args or {},
                )
                if match is not None:
                    self._job_run_id = match
                    self.log.info(
                        "concurrent_runs=join: attaching to in-flight "
                        "JobRun %s on %s instead of starting a new run.",
                        match,
                        self.job_name,
                    )
            except ClientError:  # narrow (); permission/auth errors must not be swallowed
                self.log.error(
                    "concurrent_runs=join: scan failed; proceeding with a normal submission.",
                    exc_info=True,
                )
            return super().execute(context)

    return _JoinAwareGlueJobOperator


def _build_queueing_operator_class() -> type:
    """``queue`` mode: scan for in-flight match BEFORE start_job_run.
    If found, defer to ``GlueJobNoMatchTrigger`` until clear, then
    proceed with a normal ``start_job_run``."""
    from airflow.exceptions import AirflowException
    from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

    class _QueueingGlueJobOperator(GlueJobOperator):
        def execute(self, context: Any) -> Any:
            try:
                client = self.hook.conn
                match = find_matching_in_flight_run(
                    glue_client=client,
                    job_name=self.job_name,
                    match_args=self.script_args or {},
                )
            except ClientError:  # narrow (); permission/auth errors must not be swallowed
                self.log.error(
                    "concurrent_runs=queue: scan failed; proceeding with a normal submission.",
                    exc_info=True,
                )
                return super().execute(context)

            if match is None:
                # No queue needed, normal submit.
                return super().execute(context)

            self.log.info(
                "concurrent_runs=queue: in-flight JobRun %s detected; deferring until clear.",
                match,
            )
            self.defer(
                trigger=GlueJobNoMatchTrigger(
                    job_name=self.job_name,
                    match_args=self.script_args or {},
                    aws_conn_id=self.aws_conn_id,
                    region_name=self.region_name,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                ),
                method_name="execute_after_queue_wait",
            )

        def execute_after_queue_wait(self, context: Any, event: dict[str, Any]) -> Any:
            if event.get("status") == "timeout":
                raise AirflowException(event.get("message", "queue timeout"))
            # Clear -- submit normally. If a race made another run
            # appear in the meantime, GlueJobOperator's own
            # ConcurrentRunsExceededException handling will catch it.
            return super().execute(context)

    return _QueueingGlueJobOperator


__all__ = [
    "ConcurrentRunsMode",
    "GlueJobNoMatchTrigger",
    "build_concurrent_runs_operator_class",
    "find_matching_in_flight_run",
]
