"""Shared validation for AWS resource tags on dbt-aws runners.

``resource_tags`` is a ``dict[str, str]`` optional field on every
concrete runner. Its value is passed to the corresponding AWS create
call (``glue:CreateJob``, ``glue:CreateSession``, EMR
``StartJobRun`` / ``RunJobFlow``, EMR Serverless
``CreateApplication``) so operators can attach ownership / cost-centre
/ compliance tags without wrapping every runner constructor.

AWS resource tag constraints (shared across services, taken from the
AWS Tag Editor documentation):

* Key: 1-128 Unicode characters. May not be empty. May not start with
  the reserved ``aws:`` prefix (AWS-internal tags).
* Value: 0-256 Unicode characters. Empty string is valid AWS-side; we
  reject it defensively because ``resource_tags`` is user-facing and
  an empty string is almost always a typo.
* Maximum 50 tags per resource. AWS enforces this at the API level;
  we do not re-check here so runners can layer tags without hard caps.

Per-model / per-tag precedence: ``resource_tags`` merges shallowly
across the runner default → per-tag override → per-model override
ladder (later layers win per key).
"""

from __future__ import annotations

from typing import Any

_MAX_KEY_LEN = 128
_MAX_VALUE_LEN = 256
_RESERVED_PREFIX = "aws:"


class ResourceTagsError(ValueError):
    """Raised when a ``resource_tags`` value violates AWS constraints."""


def validate_resource_tags(tags: Any, *, where: str) -> None:
    """Validate ``tags`` conforms to the shape AWS resource-tag APIs
    accept.

    Args:
        tags: the candidate value. ``None`` is accepted (no tags).
            Any other type is an error.
        where: a caller-friendly label (e.g.
            ``"GlueSparkRunner.resource_tags"`` or
            ``"overrides[model.p.a].resource_tags"``) prepended to
            the error message so users see the offending source.

    Raises:
        ResourceTagsError: any of the AWS constraints above are
            violated. Message names the offending key/value.
    """
    if tags is None:
        return
    if not isinstance(tags, dict):
        raise ResourceTagsError(f"{where}: must be a dict[str, str], got {type(tags).__name__}")
    for k, v in tags.items():
        if not isinstance(k, str) or not k:
            raise ResourceTagsError(f"{where}: tag keys must be non-empty strings, got {k!r}")
        if len(k) > _MAX_KEY_LEN:
            raise ResourceTagsError(
                f"{where}: tag key {k!r} exceeds {_MAX_KEY_LEN} chars (got {len(k)})"
            )
        if k.lower().startswith(_RESERVED_PREFIX):
            raise ResourceTagsError(
                f"{where}: tag key {k!r} uses the reserved {_RESERVED_PREFIX!r} "
                f"prefix (AWS-internal). Choose a different key."
            )
        if not isinstance(v, str):
            raise ResourceTagsError(f"{where}: tag values must be strings, got {v!r} for key {k!r}")
        if not v:
            raise ResourceTagsError(
                f"{where}: tag value for key {k!r} is empty; AWS accepts "
                f"empty values but this is almost always a typo."
            )
        if len(v) > _MAX_VALUE_LEN:
            raise ResourceTagsError(
                f"{where}: tag value for key {k!r} exceeds {_MAX_VALUE_LEN} chars (got {len(v)})"
            )


def merge_resource_tags(*layers: dict[str, str] | None) -> dict[str, str]:
    """Merge multiple ``resource_tags`` dicts, later layers winning
    per-key.

    Args:
        *layers: ordered from lowest to highest precedence. ``None``
            layers are skipped.

    Returns:
        A new dict. Never ``None``; empty when every layer was ``None``
        or empty.
    """
    merged: dict[str, str] = {}
    for layer in layers:
        if layer:
            merged.update(layer)
    return merged


def as_emr_tag_list(tags: dict[str, str] | None) -> list[dict[str, str]]:
    """Convert a ``{key: value}`` mapping into the ``[{Key, Value}, ...]``
    list-of-dicts shape EMR's ``RunJobFlow.Tags`` expects.

    Boto3's Glue and EMR Serverless APIs accept the plain
    ``dict[str, str]`` -- only EMR classic (``RunJobFlow``) needs the
    list form.

    Returns:
        Empty list when ``tags`` is falsy; otherwise one dict per key.
    """
    if not tags:
        return []
    return [{"Key": k, "Value": v} for k, v in tags.items()]


def make_glue_tag_sync_callback(
    *,
    job_name: str,
    resource_tags: dict[str, str] | None,
    aws_conn_id: str,
    region_name: str | None,
    strict: bool = False,
) -> Any:
    """Return an Airflow ``on_execute_callback`` that reconciles the
    Glue Job's tags to match ``resource_tags`` before the JobRun
    starts.

    Works around a real Glue API limitation: ``glue:UpdateJob`` does
    NOT accept ``Tags`` in the ``JobUpdate`` argument (it's rejected
    by boto3 with ``ParamValidationError``). Airflow's ``GlueJobHook``
    forwards ``create_job_kwargs`` verbatim to both
    ``glue:CreateJob`` (which DOES accept ``Tags``) AND
    ``glue:UpdateJob``, so the moment a Glue Job already exists and
    the runner is in ``mode='create'`` + ``update_config=True``, any
    ``Tags`` in ``create_job_kwargs`` crashes the DAG at
    task-execute time.

    Fix: keep ``Tags`` OUT of ``create_job_kwargs`` (see each glue
    runner's ``_build_create_job_kwargs``) and reconcile tags via
    ``glue:GetTags`` + ``glue:TagResource`` + ``glue:UntagResource``
    in this callback. Runs on the Airflow worker at task-execute
    time; no AWS calls happen at DAG-parse.

    The reconciliation is idempotent:

    1. Resolve the Glue Job ARN from the caller's connection.
    2. ``glue:GetTags(ResourceArn=<job arn>)`` -- read the current
       tag state.
    3. Compute the diff: keys to add / update + keys to remove.
    4. If ``resource_tags`` has additions or updates,
       ``glue:TagResource``.
    5. If keys need removal, ``glue:UntagResource``.

    When ``resource_tags`` is empty/None the callback is a no-op
    (returns immediately).

    Failure handling:

    * ``strict=False`` (default, best-effort). Failures are logged
      as WARNINGs; the JobRun proceeds. Matches earlier behaviour
      for backward compatibility -- an IAM misconfiguration or
      transient throttle can't take the data pipeline down.
    * ``strict=True`` (opt-in, compliance-grade). Failures RE-RAISE
      from the callback so the Airflow task fails visibly. Use when
      resource tags are a hard compliance requirement (cost-attribution,
      data-classification tagging, SOX) rather than a best-effort hint.
    """
    if not resource_tags:
        # Nothing to sync. Return a lightweight no-op so callers can
        # unconditionally attach the callback.
        def _noop(context: Any) -> None:
            return None

        return _noop

    def _sync_tags(context: Any) -> None:
        import logging

        _log = logging.getLogger(__name__)
        try:
            from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

            hook = AwsBaseHook(aws_conn_id=aws_conn_id, client_type="glue")
            client = hook.get_conn()
            # STS to build the ARN (needed for GetTags/TagResource).
            sts_hook = AwsBaseHook(aws_conn_id=aws_conn_id, client_type="sts")
            account_id = sts_hook.get_conn().get_caller_identity()["Account"]
            region = region_name or hook.get_session().region_name
            arn = f"arn:aws:glue:{region}:{account_id}:job/{job_name}"

            current = client.get_tags(ResourceArn=arn).get("Tags", {}) or {}

            to_add = {k: v for k, v in resource_tags.items() if current.get(k) != v}
            to_remove = [k for k in current if k not in resource_tags]

            if to_add:
                client.tag_resource(ResourceArn=arn, TagsToAdd=to_add)
                _log.info(
                    "glue tag sync: applied %d tag(s) on %s (keys=%s)",
                    len(to_add),
                    job_name,
                    sorted(to_add),
                )
            if to_remove:
                client.untag_resource(ResourceArn=arn, TagsToRemove=to_remove)
                _log.info(
                    "glue tag sync: removed %d tag(s) on %s (keys=%s)",
                    len(to_remove),
                    job_name,
                    sorted(to_remove),
                )
            if not to_add and not to_remove:
                _log.debug(
                    "glue tag sync: no drift on %s (%d tag(s) already in sync)",
                    job_name,
                    len(resource_tags),
                )
        except Exception as exc:  # noqa: BLE001 -- see strict handling below
            if strict:
                # compliance-grade opt-in. Fail the Airflow
                # task visibly so the on-call sees the misconfiguration.
                _log.error(
                    "glue tag sync: STRICT mode -- failed to reconcile tags on %s: %s",
                    job_name,
                    exc,
                )
                raise
            _log.warning(
                "glue tag sync: failed to reconcile tags on %s -- %s. "
                "Job runs unaffected; tags may drift from resource_tags= "
                "until the sync succeeds on a later run.",
                job_name,
                exc,
            )

    return _sync_tags
