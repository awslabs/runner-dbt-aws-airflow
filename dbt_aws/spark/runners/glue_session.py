"""Glue Interactive Session runner -- reusable warm Spark sessions.

Glue Interactive Sessions hold ONE Spark JVM warm across many
statements -- ~30-60s cold-start once, then sub-second between
statements. For dbt projects with many small Spark models this is the
fastest+cheapest mode: amortise the JVM startup over the whole DAG run.

Two configurations:

* ``reusable=True`` (default) -- ONE session per DAG run, shared by
  every dbt node. Setup task creates the session at DAG start;
  teardown deletes it at DAG end. Per-node tasks submit Statements to
  the warm session.
* ``reusable=False`` -- one session per dbt node. Each per-node task
  creates a fresh session, submits one Statement, deletes the session.
  Useful when models need fully-isolated Spark contexts.

All operators are DEFERRABLE: the wait for session-READY and the
wait for Statement-AVAILABLE both happen via a custom
:class:`BaseTrigger` that polls ``glue:GetSession`` /
``glue:GetStatement`` asynchronously, so worker slots aren't pinned.

apache-airflow-providers-amazon 9.x doesn't ship operators for Glue
Sessions, so the trigger + ops below are home-grown. They mirror the
shape of ``GlueJobCompleteTrigger`` and use ``asyncio.to_thread`` to
keep boto3 calls non-blocking inside the Triggerer's event loop.
"""

from __future__ import annotations

import asyncio
import re
import textwrap
import uuid
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Literal

from dbt_aws.common.lineage import OpenLineageConfig, validate_lineage_optin
from dbt_aws.common.runner import (
    Runner,
    RunnerOverride,
    resolve_override,
    validate_resource_tags,
)

if TYPE_CHECKING:  # pragma: no cover
    from airflow.models import BaseOperator
    from airflow.sdk import DAG

    from dbt_aws.common.graph.node import DbtNode


GlueSessionMode = Literal["attach", "create"]

# Glue session ID grammar: alphanumeric + hyphens, max 254 chars.
_SESSION_ID_SANITISE = re.compile(r"[^A-Za-z0-9-]")

_TERMINAL_STATEMENT_STATES = frozenset({"AVAILABLE", "ERROR", "CANCELLED"})
_TERMINAL_SESSION_STATES = frozenset({"READY", "FAILED", "TIMEOUT", "STOPPED"})


def _sanitise_session_id(raw: str) -> str:
    cleaned = _SESSION_ID_SANITISE.sub("-", raw)
    return cleaned[:254] or f"dbt-aws-{uuid.uuid4().hex[:12]}"


# ----------------------------------------------------------------------
# Per-model override
# ----------------------------------------------------------------------
@dataclass(frozen=True)
class GlueInteractiveSessionOverride(RunnerOverride):
    """Per-model overrides. All fields optional."""

    full_refresh: bool | None = None
    vars_json: str | None = None
    timeout_minutes: int | None = None
    #: Override the dbt ``--profile`` flag for this single node.
    profile_name: str | None = None
    #: Override the dbt ``--target`` flag for this single node.
    target: str | None = None
    #: Override the dbt CLI verb for this single node (e.g. ``"build"``
    #: instead of the default mapped verb). Forwarded verbatim to
    #: the worker.
    command: str | None = None


# ======================================================================
# Trigger + operators
# ======================================================================
def _get_glue_client(aws_conn_id: str, region_name: str | None):  # noqa: ANN202
    """Resolve a sync boto3 Glue client from the Airflow connection."""
    from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

    hook = AwsBaseHook(aws_conn_id=aws_conn_id, client_type="glue", region_name=region_name)
    return hook.get_conn()


class _GlueSessionTrigger:
    """Thin wrapper around :func:`dbt_aws.common.async_polling.poll_until_terminal`
    for backward-compat (existing imports of this class still work).
    New code should use ``poll_until_terminal`` directly."""

    @staticmethod
    async def _poll(
        get_state: Any,
        terminal_states: frozenset[str],
        delay: int,
        max_attempts: int,
    ) -> str:
        from dbt_aws.common.async_polling import poll_until_terminal

        return await poll_until_terminal(
            get_state=get_state,
            terminal_states=terminal_states,
            delay=delay,
            max_attempts=max_attempts,
        )


def _build_session_ready_trigger_cls() -> type:
    """Lazily build a BaseTrigger subclass that waits for a Glue
    session to enter ``READY``. Cached after first use."""
    from airflow.triggers.base import BaseTrigger, TriggerEvent

    class GlueSessionReadyTrigger(BaseTrigger):
        """Waits for ``glue:GetSession(Id).Session.Status`` to leave the
        provisioning state."""

        def __init__(
            self,
            session_id: str,
            aws_conn_id: str = "aws_default",
            region_name: str | None = None,
            waiter_delay: int = 15,
            waiter_max_attempts: int = 40,
        ) -> None:
            super().__init__()
            self.session_id = session_id
            self.aws_conn_id = aws_conn_id
            self.region_name = region_name
            self.waiter_delay = waiter_delay
            self.waiter_max_attempts = waiter_max_attempts

        def serialize(self) -> tuple[str, dict[str, Any]]:
            return (
                "dbt_aws.spark.runners.glue_session.GlueSessionReadyTrigger",
                {
                    "session_id": self.session_id,
                    "aws_conn_id": self.aws_conn_id,
                    "region_name": self.region_name,
                    "waiter_delay": self.waiter_delay,
                    "waiter_max_attempts": self.waiter_max_attempts,
                },
            )

        async def run(self) -> AsyncIterator[TriggerEvent]:
            client = _get_glue_client(self.aws_conn_id, self.region_name)
            try:
                final_state = await _GlueSessionTrigger._poll(
                    get_state=lambda: client.get_session(Id=self.session_id)["Session"]["Status"],
                    terminal_states=_TERMINAL_SESSION_STATES,
                    delay=self.waiter_delay,
                    max_attempts=self.waiter_max_attempts,
                )
            except TimeoutError as exc:
                yield TriggerEvent({"status": "timeout", "message": str(exc)})
                return
            yield TriggerEvent(
                {
                    "status": "success" if final_state == "READY" else "failure",
                    "state": final_state,
                    "session_id": self.session_id,
                }
            )

    return GlueSessionReadyTrigger


def _build_statement_trigger_cls() -> type:
    """Lazily build a BaseTrigger subclass that waits for a Glue
    Statement to terminate."""
    from airflow.triggers.base import BaseTrigger, TriggerEvent

    class GlueStatementTrigger(BaseTrigger):
        def __init__(
            self,
            session_id: str,
            statement_id: int,
            aws_conn_id: str = "aws_default",
            region_name: str | None = None,
            waiter_delay: int = 15,
            waiter_max_attempts: int = 240,  # ~1h ceiling at 15s delay
        ) -> None:
            super().__init__()
            self.session_id = session_id
            self.statement_id = statement_id
            self.aws_conn_id = aws_conn_id
            self.region_name = region_name
            self.waiter_delay = waiter_delay
            self.waiter_max_attempts = waiter_max_attempts

        def serialize(self) -> tuple[str, dict[str, Any]]:
            return (
                "dbt_aws.spark.runners.glue_session.GlueStatementTrigger",
                {
                    "session_id": self.session_id,
                    "statement_id": self.statement_id,
                    "aws_conn_id": self.aws_conn_id,
                    "region_name": self.region_name,
                    "waiter_delay": self.waiter_delay,
                    "waiter_max_attempts": self.waiter_max_attempts,
                },
            )

        async def run(self) -> AsyncIterator[TriggerEvent]:
            client = _get_glue_client(self.aws_conn_id, self.region_name)
            try:
                final_state = await _GlueSessionTrigger._poll(
                    get_state=lambda: client.get_statement(
                        SessionId=self.session_id, Id=self.statement_id
                    )["Statement"]["State"],
                    terminal_states=_TERMINAL_STATEMENT_STATES,
                    delay=self.waiter_delay,
                    max_attempts=self.waiter_max_attempts,
                )
            except TimeoutError as exc:
                yield TriggerEvent({"status": "timeout", "message": str(exc)})
                return
            # Fetch the final Output for the operator to inspect.
            final = await asyncio.to_thread(
                lambda: client.get_statement(SessionId=self.session_id, Id=self.statement_id)[
                    "Statement"
                ]
            )
            yield TriggerEvent(
                {
                    "status": "success" if final_state == "AVAILABLE" else "failure",
                    "state": final_state,
                    "output_status": final.get("Output", {}).get("Status"),
                    "error_name": final.get("Output", {}).get("ErrorName"),
                    "error_value": final.get("Output", {}).get("ErrorValue"),
                    "logs": final.get("Output", {}).get("Logs", ""),
                }
            )

    return GlueStatementTrigger


# Module-level publication for trigger serialization (Airflow loads
# triggers by fully-qualified class name).
GlueSessionReadyTrigger = _build_session_ready_trigger_cls()
GlueStatementTrigger = _build_statement_trigger_cls()


# ----------------------------------------------------------------------
# Operators
# ----------------------------------------------------------------------
def _create_session_operator() -> type:
    """Build the CreateSession operator lazily.

    The returned class supports BOTH deferrable and non-deferrable
    modes (controlled by the ``deferrable=`` kwarg passed to
    ``execute()`` via the constructor). Non-deferrable mode is useful
    on Airflow deployments where the Triggerer is unreliable
    (macOS Airflow standalone, SQLite metastore, high fan-out).
    """
    from airflow.exceptions import AirflowException
    from airflow.models import BaseOperator

    class GlueSessionCreateOperator(BaseOperator):
        """Calls ``glue:CreateSession`` then defers (or sync-polls when
        ``deferrable=False``) until the session enters ``READY``.
        Pushes ``session_id`` to XCom on success."""

        template_fields = ("session_id_template",)

        def __init__(
            self,
            *,
            session_id_template: str,
            iam_role_arn: str,
            glue_version: str = "5.0",
            worker_type: str = "G.1X",
            number_of_workers: int = 2,
            idle_timeout_minutes: int = 30,
            timeout_minutes: int = 60,
            additional_python_modules: str = "",
            default_arguments: dict[str, str] | None = None,
            resource_tags: dict[str, str] | None = None,
            aws_conn_id: str = "aws_default",
            region_name: str | None = None,
            waiter_delay: int = 15,
            waiter_max_attempts: int = 40,
            deferrable: bool = True,
            **kwargs: Any,
        ) -> None:
            super().__init__(**kwargs)
            self.session_id_template = session_id_template
            self.iam_role_arn = iam_role_arn
            self.glue_version = glue_version
            self.worker_type = worker_type
            self.number_of_workers = number_of_workers
            self.idle_timeout_minutes = idle_timeout_minutes
            self.timeout_minutes = timeout_minutes
            self.additional_python_modules = additional_python_modules
            self.default_arguments = dict(default_arguments or {})
            self.resource_tags = dict(resource_tags) if resource_tags else None
            self.aws_conn_id = aws_conn_id
            self.region_name = region_name
            self.waiter_delay = waiter_delay
            self.waiter_max_attempts = waiter_max_attempts
            self.deferrable = deferrable

        def execute(self, context: Any) -> str:
            session_id = _sanitise_session_id(self.session_id_template)
            client = _get_glue_client(self.aws_conn_id, self.region_name)

            default_args = dict(self.default_arguments)
            if self.additional_python_modules:
                default_args.setdefault(
                    "--additional-python-modules",
                    self.additional_python_modules,
                )

            create_kwargs: dict[str, Any] = dict(
                Id=session_id,
                Role=self.iam_role_arn,
                Command={"Name": "glueetl", "PythonVersion": "3"},
                GlueVersion=self.glue_version,
                WorkerType=self.worker_type,
                NumberOfWorkers=self.number_of_workers,
                IdleTimeout=self.idle_timeout_minutes,
                Timeout=self.timeout_minutes,
                DefaultArguments=default_args,
            )
            if self.resource_tags:
                create_kwargs["Tags"] = dict(self.resource_tags)
            client.create_session(**create_kwargs)

            if self.deferrable:
                self.log.info("Created Glue session %s; deferring until READY", session_id)
                self.defer(
                    trigger=GlueSessionReadyTrigger(
                        session_id=session_id,
                        aws_conn_id=self.aws_conn_id,
                        region_name=self.region_name,
                        waiter_delay=self.waiter_delay,
                        waiter_max_attempts=self.waiter_max_attempts,
                    ),
                    method_name="execute_complete",
                )
                return session_id  # unreachable but keeps mypy happy

            # ---- non-deferrable path: sync polling ----
            from dbt_aws.common.async_polling import poll_until_terminal_sync

            self.log.info(
                "Created Glue session %s; sync-polling until READY (deferrable=False)",
                session_id,
            )
            final_state = poll_until_terminal_sync(
                get_state=lambda: client.get_session(Id=session_id)["Session"]["Status"],
                terminal_states=_TERMINAL_SESSION_STATES,
                delay=self.waiter_delay,
                max_attempts=self.waiter_max_attempts,
            )
            if final_state != "READY":
                raise AirflowException(f"Glue session did not become READY: state={final_state}")
            self.log.info("Glue session %s is READY", session_id)
            return session_id  # auto-pushed to XCom

        def execute_complete(self, context: Any, event: dict[str, Any]) -> str:
            if event.get("status") != "success":
                raise AirflowException(f"Glue session did not become READY: {event}")
            self.log.info("Glue session %s is READY", event["session_id"])
            return event["session_id"]  # auto-pushed to XCom

    return GlueSessionCreateOperator


def _delete_session_operator() -> type:
    from airflow.models import BaseOperator

    class GlueSessionDeleteOperator(BaseOperator):
        """``glue:DeleteSession`` (also stops any running statements).
        Cheap, synchronous, idempotent."""

        def __init__(
            self,
            *,
            session_id_xcom_task_id: str,
            aws_conn_id: str = "aws_default",
            region_name: str | None = None,
            **kwargs: Any,
        ) -> None:
            super().__init__(**kwargs)
            self.session_id_xcom_task_id = session_id_xcom_task_id
            self.aws_conn_id = aws_conn_id
            self.region_name = region_name

        def execute(self, context: Any) -> None:
            ti = context["task_instance"]
            session_id = ti.xcom_pull(task_ids=self.session_id_xcom_task_id)
            if not session_id:
                self.log.warning(
                    "no session_id in XCom from %s; nothing to delete",
                    self.session_id_xcom_task_id,
                )
                return
            client = _get_glue_client(self.aws_conn_id, self.region_name)
            try:
                client.stop_session(Id=session_id)
            except Exception as exc:  # noqa: BLE001
                self.log.info("stop_session ignored: %s", exc)
            try:
                client.delete_session(Id=session_id)
                self.log.info("Deleted Glue session %s", session_id)
            except Exception as exc:  # noqa: BLE001
                self.log.warning("delete_session failed (continuing): %s", exc)

    return GlueSessionDeleteOperator


def _statement_operator() -> type:
    """Build the RunStatement operator lazily.

    Like :func:`_create_session_operator`, supports both deferrable
    and non-deferrable modes via the ``deferrable=`` kwarg.
    """
    from airflow.exceptions import AirflowException
    from airflow.models import BaseOperator

    class GlueRunStatementOperator(BaseOperator):
        """Submits ``glue:RunStatement(Code=...)`` then defers (or
        sync-polls when ``deferrable=False``) until the statement
        terminates."""

        template_fields = ("session_id", "session_id_xcom_task_id", "code")

        def __init__(
            self,
            *,
            code: str,
            session_id: str | None = None,
            session_id_xcom_task_id: str | None = None,
            aws_conn_id: str = "aws_default",
            region_name: str | None = None,
            waiter_delay: int = 15,
            waiter_max_attempts: int = 240,
            deferrable: bool = True,
            **kwargs: Any,
        ) -> None:
            super().__init__(**kwargs)
            if session_id is None and session_id_xcom_task_id is None:
                raise ValueError("Must set one of session_id= or session_id_xcom_task_id=")
            self.code = code
            self.session_id = session_id
            self.session_id_xcom_task_id = session_id_xcom_task_id
            self.aws_conn_id = aws_conn_id
            self.region_name = region_name
            self.waiter_delay = waiter_delay
            self.waiter_max_attempts = waiter_max_attempts
            self.deferrable = deferrable

        def _resolve_session_id(self, context: Any) -> str:
            if self.session_id:
                return self.session_id
            ti = context["task_instance"]
            sid = ti.xcom_pull(task_ids=self.session_id_xcom_task_id)
            if not sid:
                raise AirflowException(
                    f"No session_id in XCom from {self.session_id_xcom_task_id!r}"
                )
            return sid

        def execute(self, context: Any) -> Any:
            session_id = self._resolve_session_id(context)
            client = _get_glue_client(self.aws_conn_id, self.region_name)
            resp = client.run_statement(SessionId=session_id, Code=self.code)
            statement_id = resp["Id"]

            if self.deferrable:
                self.log.info(
                    "Submitted statement %s to session %s; deferring",
                    statement_id,
                    session_id,
                )
                self.defer(
                    trigger=GlueStatementTrigger(
                        session_id=session_id,
                        statement_id=statement_id,
                        aws_conn_id=self.aws_conn_id,
                        region_name=self.region_name,
                        waiter_delay=self.waiter_delay,
                        waiter_max_attempts=self.waiter_max_attempts,
                    ),
                    method_name="execute_complete",
                )
                return None  # unreachable

            # ---- non-deferrable path: sync polling ----
            from dbt_aws.common.async_polling import poll_until_terminal_sync

            self.log.info(
                "Submitted statement %s to session %s; sync-polling (deferrable=False)",
                statement_id,
                session_id,
            )
            final_state = poll_until_terminal_sync(
                get_state=lambda: client.get_statement(SessionId=session_id, Id=statement_id)[
                    "Statement"
                ]["State"],
                terminal_states=_TERMINAL_STATEMENT_STATES,
                delay=self.waiter_delay,
                max_attempts=self.waiter_max_attempts,
            )

            # Fetch full statement record for logs/output details, mirroring
            # the async trigger's TriggerEvent payload shape.
            statement = client.get_statement(SessionId=session_id, Id=statement_id)["Statement"]
            output = statement.get("Output") or {}
            logs_text = output.get("Data", {}).get("TextPlain", "")
            if logs_text:
                self.log.info("Statement logs:\n%s", logs_text)

            output_status = output.get("Status")
            if final_state != "AVAILABLE" or output_status == "error":
                error_name = output.get("ErrorName")
                error_value = output.get("ErrorValue")
                raise AirflowException(
                    f"Glue statement failed: state={final_state} "
                    f"output_status={output_status} "
                    f"error={error_name}: {error_value}"
                )
            self.log.info("Glue statement succeeded.")
            return {
                "status": "success",
                "state": final_state,
                "output_status": output_status,
                "logs": logs_text,
            }

        def execute_complete(self, context: Any, event: dict[str, Any]) -> Any:
            if event.get("logs"):
                self.log.info("Statement logs:\n%s", event["logs"])
            if event.get("status") != "success" or event.get("output_status") == "error":
                raise AirflowException(
                    f"Glue statement failed: state={event.get('state')} "
                    f"output_status={event.get('output_status')} "
                    f"error={event.get('error_name')}: {event.get('error_value')}"
                )
            self.log.info("Glue statement succeeded.")
            return event

    return GlueRunStatementOperator


# ----------------------------------------------------------------------
# Runner
# ----------------------------------------------------------------------
class GlueInteractiveSessionRunner(Runner):
    """Run dbt nodes inside Glue Interactive Sessions.

    Args:
        iam_role_arn: full ARN of the IAM role the session assumes.
        reusable: when True (default), ONE session is shared by every
            node in the DAG (cheapest+fastest). When False, each node
            gets its own session.
        session_id_prefix: name prefix for the session(s). Suffixed
            with the Airflow run_id at execute time.
        additional_python_modules: comma-separated pip requirements
            installed on the session at startup (e.g. dbt-core +
            dbt-spark + dbt-aws-common wheel S3 URI).
        glue_version / worker_type / number_of_workers / idle_timeout_minutes /
            timeout_minutes: session sizing.
        full_refresh / vars_json: dbt-side defaults applied to every
            node (overridable via meta.stratus or DbtDag(overrides=)).
        upload_artefacts_s3_prefix: when set, each node uploads its
            target/ to ``<prefix>/<unique_id>/``.
        aws_conn_id: AWS plumbing.
        region_name: AWS region for boto3 clients.
        waiter_delay / waiter_max_attempts: polling granularity.
        deferrable: when True (default) the CreateSession and
            RunStatement operators hand off to the Airflow Triggerer
            for async polling. Set to ``False`` on constrained
            deployments where the Triggerer wedges under load
            (macOS Airflow standalone, SQLite metastore) -- the
            operators then sync-poll on a worker slot until AWS
            reaches a terminal state. Prefer ``True`` on MWAA / any
            production deployment.
    """

    OVERRIDE_TYPE: ClassVar[type[RunnerOverride]] = GlueInteractiveSessionOverride

    def __init__(
        self,
        *,
        iam_role_arn: str,
        reusable: bool = True,
        with_deps: bool = True,
        session_id_prefix: str = "dbt-aws",
        additional_python_modules: str = "",
        # Extra ``DefaultArguments`` for the Glue session. Useful for
        # ``--python-modules-installer-option`` (pip extra-index-url
        # etc.) and any other ``--key value`` pair Glue sessions accept.
        # ``--additional-python-modules`` is auto-set from
        # ``additional_python_modules=`` unless the caller pre-populates
        # it here.
        default_arguments: dict[str, str] | None = None,
        # AWS resource tags applied to each Glue Session at
        # ``create_session`` time. See ``dbt_aws.common.runner.tags``
        # for validation rules.
        resource_tags: dict[str, str] | None = None,
        glue_version: str = "5.0",
        worker_type: str = "G.1X",
        number_of_workers: int = 2,
        idle_timeout_minutes: int = 30,
        timeout_minutes: int = 60,
        full_refresh: bool = False,
        vars_json: str | None = None,
        profile_name: str | None = None,
        target: str | None = None,
        upload_artefacts_s3_prefix: str | None = None,
        aws_conn_id: str = "aws_default",
        region_name: str | None = None,
        waiter_delay: int = 15,
        waiter_max_attempts_session: int = 40,
        waiter_max_attempts_statement: int = 240,
        deferrable: bool = True,
        # Per-task callbacks. The lib appends an audit-log callback
        # unless ``audit_log=False``. User callbacks always fire first.
        on_execute_callback: Callable[..., None] | list[Callable[..., None]] | None = None,
        on_success_callback: Callable[..., None] | list[Callable[..., None]] | None = None,
        on_failure_callback: Callable[..., None] | list[Callable[..., None]] | None = None,
        audit_log: bool = True,
        # OpenLineage / SMUS integration. ``None`` = feature off.
        openlineage: OpenLineageConfig | None = None,
    ) -> None:
        if upload_artefacts_s3_prefix is not None and not upload_artefacts_s3_prefix.startswith(
            "s3://"
        ):
            raise ValueError(
                f"upload_artefacts_s3_prefix must start with 's3://', "
                f"got {upload_artefacts_s3_prefix!r}"
            )

        self.iam_role_arn = iam_role_arn
        self.reusable = reusable
        self.with_deps = with_deps
        self.session_id_prefix = session_id_prefix
        self.additional_python_modules = additional_python_modules
        self.default_arguments = dict(default_arguments or {})
        validate_resource_tags(
            resource_tags, where="GlueInteractiveSessionRunner.resource_tags"
        )
        self.resource_tags: dict[str, str] | None = (
            dict(resource_tags) if resource_tags else None
        )
        self.glue_version = glue_version
        self.worker_type = worker_type
        self.number_of_workers = number_of_workers
        self.idle_timeout_minutes = idle_timeout_minutes
        self.timeout_minutes = timeout_minutes
        self.full_refresh = full_refresh
        self.vars_json = vars_json
        self.profile_name = profile_name
        self.target = target
        self.upload_artefacts_s3_prefix = upload_artefacts_s3_prefix
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts_session = waiter_max_attempts_session
        self.waiter_max_attempts_statement = waiter_max_attempts_statement
        self.deferrable = deferrable
        self.on_execute_callback = on_execute_callback
        self.on_success_callback = on_success_callback
        self.on_failure_callback = on_failure_callback
        self.audit_log = audit_log
        validate_lineage_optin(
            openlineage,
            region_fallback=region_name,
            runner_class_name="GlueInteractiveSessionRunner",
        )
        self.openlineage = openlineage

    # ------------------------------------------------------------------
    # Setup / teardown (reusable mode only)
    # ------------------------------------------------------------------
    def make_setup_task(
        self,
        *,
        dag: DAG | None = None,
        airflow_kwargs: dict[str, Any] | None = None,
    ) -> BaseOperator | None:
        if not self.reusable:
            return None
        cls = _create_session_operator()
        return cls(
            task_id="dbt_aws_glue_session__setup",
            dag=dag,
            session_id_template=f"{self.session_id_prefix}-{{{{ run_id }}}}",
            iam_role_arn=self.iam_role_arn,
            glue_version=self.glue_version,
            worker_type=self.worker_type,
            number_of_workers=self.number_of_workers,
            idle_timeout_minutes=self.idle_timeout_minutes,
            timeout_minutes=self.timeout_minutes,
            additional_python_modules=self.additional_python_modules,
            default_arguments=self.default_arguments,
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            waiter_delay=self.waiter_delay,
            waiter_max_attempts=self.waiter_max_attempts_session,
            deferrable=self.deferrable,
            **(airflow_kwargs or {}),
        )

    def make_teardown_task(
        self,
        *,
        dag: DAG | None = None,
        airflow_kwargs: dict[str, Any] | None = None,
    ) -> BaseOperator | None:
        if not self.reusable:
            return None
        cls = _delete_session_operator()
        kwargs = {"trigger_rule": "all_done"}
        kwargs.update(airflow_kwargs or {})
        return cls(
            task_id="dbt_aws_glue_session__teardown",
            dag=dag,
            session_id_xcom_task_id="dbt_aws_glue_session__setup",
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            **kwargs,
        )

    # ------------------------------------------------------------------
    # Per-node task
    # ------------------------------------------------------------------
    def make_task(
        self,
        *,
        task_id: str,
        node: DbtNode,
        dbt_command: str,
        select: str,
        target: str,
        dag: DAG | None = None,
        project_archive_s3: str,
        run_id_template: str = "{{ run_id }}",
        airflow_kwargs: dict[str, Any] | None = None,
        overrides: dict[str, dict[str, Any]] | None = None,
        tag_name: str | None = None,
    ) -> BaseOperator:
        ov = resolve_override(
            node=node,
            override_class=GlueInteractiveSessionOverride,
            explicit_overrides=overrides,
        )
        assert isinstance(ov, GlueInteractiveSessionOverride)
        if ov.command:
            dbt_command = ov.command

        code = self._build_statement_code(
            node=node,
            dbt_command=dbt_command,
            select=select,
            target=target,
            project_archive_s3=project_archive_s3,
            run_id_template=run_id_template,
            override=ov,
        )

        statement_cls = _statement_operator()

        # Build the callback kwargs once -- shared between reusable and
        # per-node-TaskGroup paths.
        statement_cb_kwargs = self._build_callback_kwargs(
            node=node,
            override=ov,
            dbt_command=dbt_command,
            select=select,
            target=target,
        )

        if self.reusable:
            return statement_cls(
                task_id=task_id,
                dag=dag,
                code=code,
                session_id_xcom_task_id="dbt_aws_glue_session__setup",
                aws_conn_id=self.aws_conn_id,
                region_name=self.region_name,
                waiter_delay=self.waiter_delay,
                waiter_max_attempts=self.waiter_max_attempts_statement,
                deferrable=self.deferrable,
                **statement_cb_kwargs,
                **(airflow_kwargs or {}),
            )

        # Non-reusable: 3-op chain in a TaskGroup so each node owns its
        # session lifecycle. The builder wires the dependencies between
        # nodes; we wire setup -> statement -> teardown within the group.
        # TaskGroup picks up the ambient DAG/TaskGroup context that
        # DbtDag / DbtTaskGroup establishes via ``with``; no need
        # to pass ``dag=`` explicitly.
        from dbt_aws.common._airflow_compat import TaskGroup

        create_cls = _create_session_operator()
        delete_cls = _delete_session_operator()

        with TaskGroup(group_id=task_id) as tg:
            setup = create_cls(
                task_id="setup",
                session_id_template=(f"{self.session_id_prefix}-{{{{ run_id }}}}-{node.name}"),
                iam_role_arn=self.iam_role_arn,
                glue_version=self.glue_version,
                worker_type=self.worker_type,
                number_of_workers=self.number_of_workers,
                idle_timeout_minutes=self.idle_timeout_minutes,
                timeout_minutes=self.timeout_minutes,
                additional_python_modules=self.additional_python_modules,
                default_arguments=self.default_arguments,
                aws_conn_id=self.aws_conn_id,
                region_name=self.region_name,
                waiter_delay=self.waiter_delay,
                waiter_max_attempts=self.waiter_max_attempts_session,
                deferrable=self.deferrable,
            )
            statement = statement_cls(
                task_id="statement",
                code=code,
                session_id_xcom_task_id=f"{task_id}.setup",
                aws_conn_id=self.aws_conn_id,
                region_name=self.region_name,
                waiter_delay=self.waiter_delay,
                waiter_max_attempts=self.waiter_max_attempts_statement,
                deferrable=self.deferrable,
                **statement_cb_kwargs,
            )
            teardown = delete_cls(
                task_id="teardown",
                session_id_xcom_task_id=f"{task_id}.setup",
                aws_conn_id=self.aws_conn_id,
                region_name=self.region_name,
                trigger_rule="all_done",
            )
            setup >> statement >> teardown
        return tg  # type: ignore[return-value]  # TaskGroup is chainable like a BaseOperator

    # ------------------------------------------------------------------
    def _build_callback_kwargs(
        self,
        *,
        node: DbtNode,
        override: GlueInteractiveSessionOverride,
        dbt_command: str,
        select: str,
        target: str,
    ) -> dict[str, Callable[..., None] | list[Callable[..., None]]]:
        """Build the ``on_execute_callback`` / ``on_success_callback`` /
        ``on_failure_callback`` kwargs for the statement operator.
        Merges user-supplied callbacks with the log-link audit callbacks
        (when ``audit_log=True``) which emit the session console URL +
        CloudWatch log link. See
        :mod:`dbt_aws.common.airflow_extras.log_link`.

        In reusable mode the statement operator pulls the session id
        from the upstream setup task's XCom; we pass that task id so
        the callback can resolve the URL pre-execute.
        """
        from dbt_aws.common.airflow_extras.log_link import (
            make_glue_session_audit_callback,
            merge_callbacks,
        )

        audit: dict[str, Callable[..., None]] = {}
        if self.audit_log and self.region_name:
            # In reusable mode the session id lives on the setup task;
            # in per-node mode each TaskGroup has its own setup so the
            # statement pulls from ``<task_id>.setup``. The runner's
            # statement-builder wires the right ``session_id_xcom_task_id``,
            # but for audit-pull we don't know it here -- fall back to
            # the default key search inside the callback.
            audit = make_glue_session_audit_callback(
                region=self.region_name,
            )

        out: dict[str, Callable[..., None] | list[Callable[..., None]]] = {}
        for key in (
            "on_execute_callback",
            "on_success_callback",
            "on_failure_callback",
        ):
            user_cb = getattr(self, key)
            audit_cb = audit.get(key)
            merged = merge_callbacks(user_cb, audit_cb)
            if merged is not None:
                out[key] = merged
        return out

    # ------------------------------------------------------------------
    # Statement-code generation
    # ------------------------------------------------------------------
    def _build_statement_code(
        self,
        *,
        node: DbtNode,
        dbt_command: str,
        select: str,
        target: str,
        project_archive_s3: str,
        run_id_template: str,
        override: GlueInteractiveSessionOverride,
    ) -> str:
        """Build the Python source we submit to the session.

        Calls :func:`dbt_aws.common.runtime.run_one_node` IN-PROCESS
        (no subprocess) so dbt-spark sees the session's Spark.
        """
        effective_full_refresh = (
            override.full_refresh if override.full_refresh is not None else self.full_refresh
        )
        effective_vars = override.vars_json if override.vars_json is not None else self.vars_json
        # ``profile_name`` and ``target`` follow the same override -> runner
        # -> caller precedence. See notes in glue_job.py / glue_python_shell.py.
        effective_profile_name = (
            override.profile_name if override.profile_name is not None else self.profile_name
        )
        effective_target = (
            override.target if override.target is not None else (self.target or target)
        )
        upload = None
        if self.upload_artefacts_s3_prefix:
            prefix = self.upload_artefacts_s3_prefix.rstrip("/")
            upload = f"{prefix}/{node.unique_id}/"

        # We pass run_id_template as a Jinja string -- Airflow renders
        # it before submitting, so the actual run_id ends up baked into
        # the Code we send to RunStatement. No XCom indirection needed.
        kwargs = {
            "command": dbt_command,
            "select": select,
            "target": effective_target,
            "project_archive_s3": project_archive_s3,
            "full_refresh": effective_full_refresh,
            "vars_json": effective_vars,
            "profile_name": effective_profile_name,
            "upload_artefacts_s3": upload,
            "run_id": run_id_template,
            # Worker-side dbt deps install -- the session's worker
            # runs ``dbt deps`` into the per-task /tmp/<run-id>/
            # project/dbt_packages/ before invoking dbt. Default True.
            "with_deps": self.with_deps,
        }
        # Attach OpenLineage args when configured. Empty kwargs when
        # ``openlineage`` is None -- keeps existing DAGs byte-identical.
        if self.openlineage is not None:
            ol = self.openlineage
            kwargs.update(
                {
                    "ol_namespace": ol.namespace,
                    "ol_s3_uri": ol.s3_uri,
                    "ol_smus_domain": ol.smus_domain_id,
                    "ol_smus_region": ol.smus_region or self.region_name,
                    "ol_parent_run_id": ol.parent_run_id_template,
                    "ol_parent_job_name": ol.parent_job_name_template,
                    "ol_parent_job_namespace": ol.parent_job_namespace,
                    "ol_node_unique_id": node.unique_id,
                    "ol_extra_env": dict(ol.extra_env) if ol.extra_env else None,
                }
            )
        # Use repr() so Python literals (True/False/None) round-trip
        # correctly. ``json.dumps`` would emit lowercase ``true``/
        # ``false``/``null`` which the Glue session's Python
        # interpreter would treat as undefined names.
        #
        # CRITICAL: dbt-core writes to stdout via its own structlog
        # logger, which is set up BEFORE we get a chance to redirect
        # ``sys.stdout`` -- so ``contextlib.redirect_stdout`` (Python-
        # level) doesn't catch the dbt log lines. Worse, Glue Interactive
        # Session's RunStatement captures ALL stdout then tries to parse
        # it as JSON to populate the statement's ``Output``. Any non-JSON
        # text -- ANSI escape codes, deprecation warnings (``Custom
        # config keys ...``), dbt-utils version notices -- surfaces in
        # Airflow as
        # ``com.fasterxml.jackson.core.JsonParseException: Illegal
        # character ((CTRL-CHAR, code 27))`` even though dbt ran cleanly.
        #
        # Fix: dup the underlying file descriptors (fd 1 / fd 2) to
        # ``os.devnull`` for the duration of the invoke. This is the
        # OS-level equivalent of ``2>&1 >/dev/null`` and catches any
        # output regardless of which logging library dbt-core uses or
        # whether ``sys.stdout`` was replaced earlier. dbt's own log
        # files inside the project (``logs/dbt.log``) and the artefacts
        # uploaded to S3 are unaffected.
        return textwrap.dedent(
            f"""
            import os, sys
            from dbt_aws.common.runtime import run_one_node
            _devnull = os.open(os.devnull, os.O_WRONLY)
            _saved_stdout = os.dup(1)
            _saved_stderr = os.dup(2)
            # Only redirect fd 1 (stdout) -- dbt-core's structlog
            # spams stdout with ANSI escapes that Glue's RunStatement
            # parser chokes on (JsonParseException). fd 2 (stderr)
            # stays connected so OL / boto3 exceptions surface in the
            # Session's CloudWatch stream for debugging.
            try:
                os.dup2(_devnull, 1)
                _rc = run_one_node(**{kwargs!r})
            finally:
                os.dup2(_saved_stdout, 1)
                os.close(_saved_stdout)
                os.close(_saved_stderr)
                os.close(_devnull)
            if _rc != 0:
                raise SystemExit(_rc)
            """
        ).strip()


__all__ = [
    "GlueInteractiveSessionMode",
    "GlueInteractiveSessionOverride",
    "GlueInteractiveSessionRunner",
    "GlueSessionReadyTrigger",
    "GlueStatementTrigger",
]
GlueInteractiveSessionMode = Literal["attach", "create"]  # reserved for future
