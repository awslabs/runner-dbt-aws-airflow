"""Compose load + select + per-node task creation into Airflow DAGs
or embeddable TaskGroups via the class-style API.

Two public entry points share the same pipeline (load graph -> apply
selectors -> ``runner.make_task`` per node -> wire dependencies):

* :class:`DbtDag` -- a :class:`airflow.sdk.DAG` subclass populated
  with dbt tasks at construction time. Cosmos-equivalent name.
* :class:`DbtTaskGroup` -- a :class:`airflow.sdk.TaskGroup` subclass
  the caller embeds inside their own DAG. Cosmos-equivalent name.

For the third Cosmos variant (bare operators wired by hand), call
:meth:`Runner.make_task` directly with a :class:`DbtNode` you got
from :func:`load_graph`. The classes here are just convenience over
that.

``runner`` is whatever concrete :class:`~dbt_aws.common.runner.Runner`
subclass the caller picked -- ``GlueSparkRunner``, ``GluePythonShellRunner``,
``EmrServerlessRunner``, .... Both builders are backend-agnostic.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any

from dbt_aws.common._airflow_compat import DAG, TaskGroup
from dbt_aws.common.config import ProjectConfig
from dbt_aws.common.graph.collapse import CollapseStrategy, TagGroupSpec, collapse_graph
from dbt_aws.common.graph.graph import DbtGraph
from dbt_aws.common.graph.loader import load_graph
from dbt_aws.common.graph.node import DbtNode
from dbt_aws.common.runner.base import Runner, dbt_command_for
from dbt_aws.common.runner.config import LoadedRunnerConfig, TaskGroupingConfig
from dbt_aws.common.select.selector import apply_selectors

if TYPE_CHECKING:  # pragma: no cover
    from airflow.models import BaseOperator

_log = logging.getLogger(__name__)

#: Characters Airflow rejects in task ids -- replaced with underscores.
#: Airflow 3.x allows ASCII letters, digits, ``-``, ``_``, ``.``; we
#: turn the ``.`` separator inside dbt unique_ids into ``__`` because
#: dots interact badly with TaskGroup rendering.
_TASK_ID_SANITISE = re.compile(r"[^A-Za-z0-9_.-]")


# ----------------------------------------------------------------------
# Public entry points
# ----------------------------------------------------------------------
class DbtDag(DAG):
    """An Airflow :class:`DAG` populated with dbt tasks at construction.

    Cosmos-equivalent name. One task per filtered dbt node, wired by
    ``depends_on``.

    Args:
        dag_id: Airflow DAG id.
        project: how to load the dbt graph (see :class:`ProjectConfig`).
        runner: concrete :class:`Runner` (e.g. ``GlueSparkRunner``).
            Mutually exclusive with ``runners``.
        runners: ``{name: Runner}`` map for multi-runner DAGs. Per-node
            assignment via :class:`TaskGroupingConfig` or override.
        default_runner: which runner from ``runners`` to use for nodes
            that don't have an explicit assignment. Required when
            ``runners`` is given.
        project_archive_s3: ``s3://bucket/key.tar.gz`` URL the remote
            workers download. Typically the path returned by
            :func:`build_and_upload_project_archive` at parse time.
        target: dbt target name. Passed through to every runner.
        select: optional list of dbt-style selector expressions
            (``tag:foo``, ``+model_x``, ``+model_x+``, etc.). UNION
            semantics. ``None`` keeps every node.
        exclude: optional list of selector expressions to subtract.
        overrides: optional ``{unique_id: {field: value}}`` map of
            per-node overrides. Each key must be a field of the
            runner's ``OVERRIDE_TYPE``; unknown keys raise
            :class:`OverrideError` at DAG-parse time. Combines with
            (and wins over) ``node.meta['stratus']`` declared in the
            dbt project.
        tag_runners: optional bulk tag-to-runner routing. Compact
            alternative to writing one ``overrides`` entry per model
            when whole layers share a runner (e.g. send every
            ``tag:bronze`` model to ``glue_spark``). Accepts either:

            * a ``dict[str, str]`` -- keys are tags or comma-separated
              tag strings (``"silver,gold"``), values are runner names::

                  tag_runners={
                      "bronze":      "glue_spark",
                      "silver,gold": "session_warm",
                  }

            * a ``list[dict]`` -- explicit ``tags`` + ``runner`` per
              entry, where ``tags`` may be a list or csv string::

                  tag_runners=[
                      {"tags": ["bronze"],         "runner": "glue_spark"},
                      {"tags": ["silver", "gold"], "runner": "session_warm"},
                  ]

            Resolution precedence per node: ``overrides[uid].runner``
            > ``meta.stratus.runner`` > ``tag_runners`` >
            ``default_runner``. A node with two tags routing to
            different runners raises ``ValueError`` at DAG-parse time.
        tag_profiles: optional bulk tag-to-``profile_name`` routing.
            Same shape and semantics as ``tag_runners`` but drives
            the dbt ``--profile`` flag instead. Resolution precedence:
            ``overrides[uid].profile_name`` > ``meta.stratus.profile_name``
            > ``tag_profiles`` > ``runner.profile_name``. When no
            layer sets a value, no ``--profile`` flag is passed and
            dbt falls back to the profile named in ``dbt_project.yml``.
        tag_targets: optional bulk tag-to-``target`` routing. Same
            shape and semantics as ``tag_runners`` but drives the dbt
            ``--target`` flag. Resolution precedence:
            ``overrides[uid].target`` > ``meta.stratus.target``
            > ``tag_targets`` > ``runner.target`` > DAG-level
            ``target=``. Lets a single dbt project run different
            models against different dbt targets from one DAG.
        tag_groups: optional bulk-by-tag TASK GROUPING ().
            Different concept from ``tag_runners`` / ``tag_profiles``
            / ``tag_targets`` -- those route models to a runner /
            profile / target; ``tag_groups`` collapses every eligible
            tagged node into one Airflow task per
            ``(group_name, runner_name)`` bucket that runs
            ``dbt run --select <space-joined names>`` on the assigned
            runner. Composes with ``collapse_strategy``: structural
            collapse runs first, ``tag_groups`` runs on top of the
            result. Accepts two shapes:

            * Simple ``dict[tag, group_name]``::

                  tag_groups={"bronze": "bronze_batch"}

            * Rich form with group-level overrides::

                  tag_groups={
                      "gold": {
                          "name": "gold_batch",
                          "command": "build",
                          "target": "prod",
                          "profile_name": "gold_prof",
                          "full_refresh": True,
                      },
                  }

            Pull-out rule: a node carrying any per-model override
            in ``overrides[uid]`` or ``meta.stratus`` (beyond the
            dispatch-only ``runner`` key) is pulled OUT of the group
            and stays a singleton task -- preserves per-model
            behaviour when it disagrees with group-level defaults.
            Multi-runner sub-grouping: when a tag's nodes route to
            more than one runner via ``tag_runners``, the group
            splits into one task per runner
            (``<group_name>__<runner_name>``). Multi-component
            splits (disconnected subgraphs sharing a tag) get a
            ``__<idx>`` suffix. See
            :mod:`dbt_aws.common.graph.collapse` for the exact
            invariants.
        task_groups: optional :class:`TaskGroupingConfig` to bucket
            nodes into nested task groups (e.g. by tag).
        config: optional :class:`LoadedRunnerConfig` from
            :func:`load_runner_config`. Fills every routing kwarg
            (``runner`` / ``runners`` / ``default_runner`` /
            ``overrides`` / ``tag_runners`` / ``tag_profiles`` /
            ``tag_targets`` / ``tag_groups`` / ``task_groups``) the
            caller did not pass explicitly. Explicit kwargs still win
            per-field. Prefer this to hand-plumbing every field --
            the plumbing pattern silently drops any field the caller
            forgets, which is how ``tag_groups.command: build`` in
            ``runner.yaml`` ended up never reaching the dbt CLI in
            earlier releases.
        airflow_kwargs_per_task: extra kwargs forwarded to every task's
            underlying operator (``retries``, ``execution_timeout``,
            ``pool``, etc.).
        **dag_kwargs: passed straight through to :class:`airflow.sdk.DAG`
            (``schedule``, ``start_date``, ``tags``, etc.).

    Example::

        dag = DbtDag(
            dag_id="daily",
            project=ProjectConfig(mode="manifest", manifest_path=...),
            runner=GlueSparkRunner(mode="create", iam_role_name="..."),
            project_archive_s3="s3://...",
            schedule="@daily",
            start_date=datetime(2026, 1, 1),
        )
    """

    def __init__(
        self,
        *,
        dag_id: str,
        project: ProjectConfig,
        runner: Runner | None = None,
        runners: dict[str, Runner] | None = None,
        default_runner: str | None = None,
        project_archive_s3: str,
        target: str = "dev",
        select: list[str] | None = None,
        exclude: list[str] | None = None,
        overrides: dict[str, dict[str, Any]] | None = None,
        tag_overrides: dict[str, dict[str, Any]] | None = None,
        tag_runners: dict[str, str] | list[dict[str, Any]] | None = None,
        tag_profiles: dict[str, str] | list[dict[str, Any]] | None = None,
        tag_targets: dict[str, str] | list[dict[str, Any]] | None = None,
        tag_groups: dict[str, Any] | list[dict[str, Any]] | None = None,
        task_groups: TaskGroupingConfig | None = None,
        config: LoadedRunnerConfig | None = None,
        airflow_kwargs_per_task: dict[str, Any] | None = None,
        collapse_strategy: CollapseStrategy | None = None,
        drop_ephemeral: bool = True,
        **dag_kwargs: Any,
    ) -> None:
        super().__init__(dag_id=dag_id, **dag_kwargs)

        # Auto-wire every routing field from ``config`` (a
        # :class:`LoadedRunnerConfig`) when the caller opts in. Explicit
        # kwargs still win -- this only fills the gaps. Prevents the
        # silent-drop bug where ``tag_groups`` / ``tag_profiles`` /
        # ``tag_targets`` declared in ``runner.yaml`` never reach the DAG
        # because the caller forgot to forward them one-by-one.
        (
            runner,
            runners,
            default_runner,
            overrides,
            tag_overrides,
            tag_runners,
            tag_profiles,
            tag_targets,
            tag_groups,
            task_groups,
        ) = _apply_config_defaults(
            config,
            runner=runner,
            runners=runners,
            default_runner=default_runner,
            overrides=overrides,
            tag_overrides=tag_overrides,
            tag_runners=tag_runners,
            tag_profiles=tag_profiles,
            tag_targets=tag_targets,
            tag_groups=tag_groups,
            task_groups=task_groups,
        )

        # ``tag_single_name_prefixes`` is  metadata (per-tag
        # task-id prefixes for ``mode: single`` entries in
        # ``overrides[tag.<t>]``). It has no Python-level kwarg on
        # DbtDag today -- it comes exclusively from the YAML config.
        # Empty dict when ``config`` is ``None`` or has no prefixes.
        tag_single_name_prefixes = (
            dict(config.tag_single_name_prefixes)
            if config is not None and config.tag_single_name_prefixes
            else {}
        )
        # ``overrides.all`` metadata (fields dict, optional
        # group spec, optional task-id prefix). Same YAML-only pattern
        # as tag_single_name_prefixes -- no Python kwarg surface yet.
        all_override_fields = (
            dict(config.all_override_fields)
            if config is not None and config.all_override_fields
            else None
        )
        all_override_group_spec = (
            config.all_override_group_spec if config is not None else None
        )
        all_override_name_prefix = (
            config.all_override_name_prefix if config is not None else None
        )

        runners_dict, default_name = _normalise_runners(
            runner=runner, runners=runners, default_runner=default_runner
        )
        tag_routes = _normalise_tag_runners(tag_runners, runners=runners_dict)
        tag_profile_routes = _normalise_tag_map(
            tag_profiles, kind="tag_profiles", value_label="profile_name"
        )
        tag_target_routes = _normalise_tag_map(
            tag_targets, kind="tag_targets", value_label="target"
        )
        tag_group_specs = _normalise_tag_groups(tag_groups)
        _tp_suffix = _summarise_tag_map(tag_profile_routes, label="tag_profiles")
        _tt_suffix = _summarise_tag_map(tag_target_routes, label="tag_targets")
        _tg_suffix = _summarise_tag_group_specs(tag_group_specs)
        _log.info(
            "DbtDag: starting (dag_id=%s, project_mode=%s, target=%s, runners=%s%s%s%s)",
            dag_id,
            project.mode,
            target,
            sorted(runners_dict),
            _tp_suffix,
            _tt_suffix,
            _tg_suffix,
        )
        graph = _resolve_graph(project, select=select, exclude=exclude, label=dag_id)
        # Enter the DAG context so any nested TaskGroup ``_attach_tasks``
        # creates (when ``task_groups`` is set) sees an active DAG --
        # Airflow's TaskGroup base class refuses to instantiate outside
        # one.
        with self:
            tasks, edges = _attach_tasks(
                graph,
                runners=runners_dict,
                default_runner=default_name,
                target=target,
                project_archive_s3=project_archive_s3,
                overrides=overrides,
                tag_overrides=tag_overrides,
                tag_runners=tag_routes,
                tag_profiles=tag_profile_routes,
                tag_targets=tag_target_routes,
                tag_groups=tag_group_specs,
                tag_single_name_prefixes=tag_single_name_prefixes,
                all_override_fields=all_override_fields,
                all_override_group_spec=all_override_group_spec,
                all_override_name_prefix=all_override_name_prefix,
                airflow_kwargs_per_task=airflow_kwargs_per_task,
                task_groups=task_groups,
                collapse_strategy=collapse_strategy,
                drop_ephemeral=drop_ephemeral,
                dag=self,
            )
        _log.info(
            "DbtDag: built %s (%d task(s), %d edge(s))",
            dag_id,
            len(tasks),
            edges,
        )


class DbtTaskGroup(TaskGroup):
    """An Airflow :class:`TaskGroup` populated with dbt tasks at
    construction.

    Cosmos-equivalent name. Same pipeline as :class:`DbtDag` but
    produces a TaskGroup so the caller can embed dbt tasks alongside
    non-dbt tasks (sensors, branches, custom Python, etc.) inside
    their own DAG.

    Must be constructed inside an active DAG context::

        with DAG(dag_id="my_dag", ...) as dag:
            preflight = PythonOperator(task_id="preflight", ...)
            dbt_tg = DbtTaskGroup(
                group_id="dbt_run",
                project=ProjectConfig(...),
                runner=GlueSparkRunner(...),
                project_archive_s3="s3://...",
            )
            post = PythonOperator(task_id="notify", ...)
            preflight >> dbt_tg >> post

    Args:
        group_id: TaskGroup id.
        project: how to load the dbt graph.
        runner: concrete :class:`Runner`. Mutually exclusive with
            ``runners``.
        runners / default_runner: multi-runner map; see :class:`DbtDag`.
        project_archive_s3: ``s3://bucket/key.tar.gz`` URL.
        target: dbt target name.
        select / exclude / overrides / tag_runners /
            airflow_kwargs_per_task: same semantics as :class:`DbtDag`.
        config: optional :class:`LoadedRunnerConfig` from
            :func:`load_runner_config`; auto-wires every missing
            routing kwarg (see :class:`DbtDag` for the full list).
        **task_group_kwargs: passed straight through to
            :class:`airflow.sdk.TaskGroup`.
    """

    def __init__(
        self,
        *,
        group_id: str,
        project: ProjectConfig,
        runner: Runner | None = None,
        runners: dict[str, Runner] | None = None,
        default_runner: str | None = None,
        project_archive_s3: str,
        target: str = "dev",
        select: list[str] | None = None,
        exclude: list[str] | None = None,
        overrides: dict[str, dict[str, Any]] | None = None,
        tag_overrides: dict[str, dict[str, Any]] | None = None,
        tag_runners: dict[str, str] | list[dict[str, Any]] | None = None,
        tag_profiles: dict[str, str] | list[dict[str, Any]] | None = None,
        tag_targets: dict[str, str] | list[dict[str, Any]] | None = None,
        tag_groups: dict[str, Any] | list[dict[str, Any]] | None = None,
        task_groups: TaskGroupingConfig | None = None,
        config: LoadedRunnerConfig | None = None,
        airflow_kwargs_per_task: dict[str, Any] | None = None,
        collapse_strategy: CollapseStrategy | None = None,
        drop_ephemeral: bool = True,
        **task_group_kwargs: Any,
    ) -> None:
        super().__init__(group_id=group_id, **task_group_kwargs)

        # See :class:`DbtDag.__init__` -- same auto-wiring so a
        # ``LoadedRunnerConfig`` can drive a :class:`DbtTaskGroup`
        # without one-by-one kwarg plumbing.
        (
            runner,
            runners,
            default_runner,
            overrides,
            tag_overrides,
            tag_runners,
            tag_profiles,
            tag_targets,
            tag_groups,
            task_groups,
        ) = _apply_config_defaults(
            config,
            runner=runner,
            runners=runners,
            default_runner=default_runner,
            overrides=overrides,
            tag_overrides=tag_overrides,
            tag_runners=tag_runners,
            tag_profiles=tag_profiles,
            tag_targets=tag_targets,
            tag_groups=tag_groups,
            task_groups=task_groups,
        )

        # See DbtDag.__init__ for the rationale -- pulled straight
        # from the config since there is no Python kwarg for this yet.
        tag_single_name_prefixes = (
            dict(config.tag_single_name_prefixes)
            if config is not None and config.tag_single_name_prefixes
            else {}
        )
        # same YAML-only overrides.all metadata as DbtDag.
        all_override_fields = (
            dict(config.all_override_fields)
            if config is not None and config.all_override_fields
            else None
        )
        all_override_group_spec = (
            config.all_override_group_spec if config is not None else None
        )
        all_override_name_prefix = (
            config.all_override_name_prefix if config is not None else None
        )

        runners_dict, default_name = _normalise_runners(
            runner=runner, runners=runners, default_runner=default_runner
        )
        tag_routes = _normalise_tag_runners(tag_runners, runners=runners_dict)
        tag_profile_routes = _normalise_tag_map(
            tag_profiles, kind="tag_profiles", value_label="profile_name"
        )
        tag_target_routes = _normalise_tag_map(
            tag_targets, kind="tag_targets", value_label="target"
        )
        tag_group_specs = _normalise_tag_groups(tag_groups)
        _tp_suffix = _summarise_tag_map(tag_profile_routes, label="tag_profiles")
        _tt_suffix = _summarise_tag_map(tag_target_routes, label="tag_targets")
        _tg_suffix = _summarise_tag_group_specs(tag_group_specs)
        _log.info(
            "DbtTaskGroup: starting (group_id=%s, project_mode=%s, target=%s, runners=%s%s%s%s)",
            group_id,
            project.mode,
            target,
            sorted(runners_dict),
            _tp_suffix,
            _tt_suffix,
            _tg_suffix,
        )
        graph = _resolve_graph(project, select=select, exclude=exclude, label=group_id)
        # ``self`` is the active TaskGroup once ``super().__init__`` has
        # run, so operators created inside the loop attach to it via
        # Airflow's context stack.
        with self:
            tasks, edges = _attach_tasks(
                graph,
                runners=runners_dict,
                default_runner=default_name,
                target=target,
                project_archive_s3=project_archive_s3,
                overrides=overrides,
                tag_overrides=tag_overrides,
                tag_runners=tag_routes,
                tag_profiles=tag_profile_routes,
                tag_targets=tag_target_routes,
                tag_groups=tag_group_specs,
                tag_single_name_prefixes=tag_single_name_prefixes,
                all_override_fields=all_override_fields,
                all_override_group_spec=all_override_group_spec,
                all_override_name_prefix=all_override_name_prefix,
                airflow_kwargs_per_task=airflow_kwargs_per_task,
                task_groups=task_groups,
                collapse_strategy=collapse_strategy,
                drop_ephemeral=drop_ephemeral,
                dag=None,  # TaskGroup binds to the ambient DAG context
            )
        _log.info(
            "DbtTaskGroup: built %s (%d task(s), %d edge(s))",
            group_id,
            len(tasks),
            edges,
        )


# ----------------------------------------------------------------------
# Shared pipeline
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# LoadedRunnerConfig auto-wiring helper
# ----------------------------------------------------------------------
def _apply_config_defaults(
    config: LoadedRunnerConfig | None,
    *,
    runner: Runner | None,
    runners: dict[str, Runner] | None,
    default_runner: str | None,
    overrides: dict[str, dict[str, Any]] | None,
    tag_overrides: dict[str, dict[str, Any]] | None,
    tag_runners: dict[str, str] | list[dict[str, Any]] | None,
    tag_profiles: dict[str, str] | list[dict[str, Any]] | None,
    tag_targets: dict[str, str] | list[dict[str, Any]] | None,
    tag_groups: dict[str, Any] | list[dict[str, Any]] | None,
    task_groups: TaskGroupingConfig | None,
) -> tuple[
    Runner | None,
    dict[str, Runner] | None,
    str | None,
    dict[str, dict[str, Any]] | None,
    dict[str, dict[str, Any]] | None,
    dict[str, str] | list[dict[str, Any]] | None,
    dict[str, str] | list[dict[str, Any]] | None,
    dict[str, str] | list[dict[str, Any]] | None,
    dict[str, Any] | list[dict[str, Any]] | None,
    TaskGroupingConfig | None,
]:
    """Fill missing routing kwargs from a :class:`LoadedRunnerConfig`.

    Explicit kwargs still win -- ``config`` only supplies defaults for
    kwargs the caller left as ``None``. Prevents the silent-drop bug
    where routing fields on ``LoadedRunnerConfig`` (``tag_groups`` /
    ``tag_profiles`` / ``tag_targets``) declared in ``runner.yaml``
    never reach the DAG because the caller forgot to forward them
    one-by-one to :class:`DbtDag`.

    Runner mapping:

    * If the caller passed neither ``runner`` nor ``runners``,
      auto-wire from ``config.runners`` + ``config.default_runner``.
      When the YAML was single-runner, ``config.runners`` has one
      entry named ``"default"`` -- we hand that single ``Runner`` to
      ``runner=`` so the caller-facing API stays symmetric with the
      single-runner Python path.

    Returns the (possibly updated) kwargs in the same order the
    caller passed them so the caller can destructure the tuple.
    """
    if config is None:
        return (
            runner,
            runners,
            default_runner,
            overrides,
            tag_overrides,
            tag_runners,
            tag_profiles,
            tag_targets,
            tag_groups,
            task_groups,
        )

    if runner is None and runners is None:
        # Prefer the single-runner shape when the YAML was single-runner
        # -- one entry keyed 'default'. Otherwise expose the multi-runner
        # dict as-is.
        if len(config.runners) == 1 and "default" in config.runners:
            runner = config.runners["default"]
        else:
            runners = dict(config.runners)
            if default_runner is None:
                default_runner = config.default_runner

    if overrides is None and config.overrides:
        overrides = config.overrides
    if tag_overrides is None and config.tag_overrides:
        tag_overrides = config.tag_overrides
    # ``tag_runners`` / ``tag_profiles`` / ``tag_targets`` are
    # no longer stored on ``LoadedRunnerConfig``; they arrive via
    # ``tag_overrides`` (which the builder still fans out into the
    # internal ``tag_runners`` / ``tag_profiles`` / ``tag_targets``
    # kwargs the ladder consumes). Nothing to auto-wire here.
    if tag_groups is None and config.tag_group_specs:
        # ``config.tag_group_specs`` is already a ``{tag: TagGroupSpec}``
        # map (built from ``overrides[tag.<t>]: {mode: group, ...}``
        # entries). ``_normalise_tag_groups`` accepts either that shape
        # or the raw dict/list forms. Re-emit as the raw dict so the
        # downstream normaliser produces one canonical
        # ``{tag: TagGroupSpec}`` regardless of source.
        tag_groups = {
            tag: (
                {"name": spec.name, **spec.overrides}
                if spec.overrides
                else spec.name
            )
            for tag, spec in config.tag_group_specs.items()
        }
    if task_groups is None and config.task_groups is not None:
        task_groups = config.task_groups

    return (
        runner,
        runners,
        default_runner,
        overrides,
        tag_overrides,
        tag_runners,
        tag_profiles,
        tag_targets,
        tag_groups,
        task_groups,
    )


def _normalise_runners(
    *,
    runner: Runner | None,
    runners: dict[str, Runner] | None,
    default_runner: str | None,
) -> tuple[dict[str, Runner], str]:
    """Convert the caller's runner=/runners= input into a uniform
    ``(dict[str, Runner], default_name)`` pair the builder pipeline uses.

    Validates mutual exclusivity: exactly one of ``runner`` /
    ``runners`` must be set. When ``runners`` is set,
    ``default_runner`` is required and must reference a key in the dict.
    """
    if runner is not None and runners is not None:
        raise ValueError("DbtDag/DbtTaskGroup accept either runner= OR runners= (not both)")
    if runner is None and runners is None:
        raise ValueError("DbtDag/DbtTaskGroup require runner= (single) or runners= (multi)")
    if runner is not None:
        return {"default": runner}, "default"
    assert runners is not None
    if not runners:
        raise ValueError("runners= must contain at least one runner")
    if default_runner is None:
        raise ValueError(
            "runners= requires default_runner= (the name used when an "
            "override doesn't specify runner)"
        )
    if default_runner not in runners:
        raise ValueError(
            f"default_runner={default_runner!r} not in runners= (have {sorted(runners)})"
        )
    return dict(runners), default_runner


def _normalise_tag_runners(
    tag_runners: dict[str, str] | list[dict[str, Any]] | None,
    *,
    runners: dict[str, Runner],
) -> dict[str, str]:
    """Normalise the caller's ``tag_runners=`` input into a flat
    ``{tag: runner_name}`` map.

    Accepted shapes (mirror the YAML loader in
    :func:`dbt_aws.common.runner.config.load_runner_config`):

    * ``None`` -> empty map (feature off).
    * ``dict[str, str]`` where keys may be a single tag or a
      comma-separated string of tags.
    * ``list[dict]`` with ``tags`` (list or csv string) + ``runner``.

    Validates that every referenced runner name exists in ``runners``
    and that no tag is mapped to two different runners. Tag whitespace
    is trimmed; empty tags are rejected.
    """
    if tag_runners is None:
        return {}

    flat: dict[str, str] = {}

    def _record(tag: str, runner_name: str, where: str) -> None:
        tag = tag.strip()
        if not tag:
            raise ValueError(f"{where}: empty tag is not allowed")
        if runner_name not in runners:
            raise ValueError(
                f"{where}: runner {runner_name!r} not in runners= (have {sorted(runners)})"
            )
        prior = flat.get(tag)
        if prior is not None and prior != runner_name:
            raise ValueError(
                f"tag {tag!r} mapped to both {prior!r} and "
                f"{runner_name!r}; each tag must route to exactly one "
                f"runner"
            )
        flat[tag] = runner_name

    def _split_csv(value: str) -> list[str]:
        return [p.strip() for p in value.split(",") if p.strip()]

    if isinstance(tag_runners, dict):
        for key, value in tag_runners.items():
            if not isinstance(key, str) or not key.strip():
                raise ValueError(f"tag_runners keys must be non-empty strings, got {key!r}")
            if not isinstance(value, str) or not value:
                raise ValueError(
                    f"tag_runners[{key!r}] must be a runner name string, got {value!r}"
                )
            for tag in _split_csv(key):
                _record(tag, value, where=f"tag_runners[{key!r}]")

    elif isinstance(tag_runners, list):
        for i, entry in enumerate(tag_runners):
            if not isinstance(entry, dict):
                raise ValueError(
                    f"tag_runners[{i}] must be a mapping with 'tags' and "
                    f"'runner', got {type(entry).__name__}"
                )
            tags_raw = entry.get("tags")
            runner_name = entry.get("runner")
            if not isinstance(runner_name, str) or not runner_name:
                raise ValueError(
                    f"tag_runners[{i}].runner must be a non-empty string, got {runner_name!r}"
                )
            if isinstance(tags_raw, str):
                tag_list = _split_csv(tags_raw)
            elif isinstance(tags_raw, list):
                tag_list = []
                for t in tags_raw:
                    if not isinstance(t, str):
                        raise ValueError(
                            f"tag_runners[{i}].tags entries must be strings, got {t!r}"
                        )
                    tag_list.extend(_split_csv(t))
            else:
                raise ValueError(
                    f"tag_runners[{i}].tags must be a list or csv string, "
                    f"got {type(tags_raw).__name__}"
                )
            if not tag_list:
                raise ValueError(f"tag_runners[{i}].tags must contain at least one tag")
            for tag in tag_list:
                _record(tag, runner_name, where=f"tag_runners[{i}]")
    else:
        raise ValueError(
            f"tag_runners must be a dict, a list, or None; got {type(tag_runners).__name__}"
        )

    return flat


def _normalise_tag_map(
    tag_map: dict[str, str] | list[dict[str, Any]] | None,
    *,
    kind: str,
    value_label: str,
) -> dict[str, str]:
    """Generic ``tag -> value`` normaliser used by ``tag_profiles`` and
    ``tag_targets``.

    Same accepted shapes as :func:`_normalise_tag_runners` -- a dict
    (keys may be comma-separated tag lists) or a list of ``{tags, <value_label>}``
    entries. Difference from ``_normalise_tag_runners``: no ``runners=``
    membership check, because the value here is an opaque string
    (profile name / dbt target). The value is only required to be a
    non-empty string.

    Args:
        tag_map: caller input (may be ``None``).
        kind: name shown in error messages (``"tag_profiles"`` etc.).
        value_label: field name expected in the list-form entries
            (``"profile_name"`` or ``"target"``).

    Returns:
        Flat ``{tag: value}`` map. Empty when ``tag_map`` is ``None``.

    Raises:
        ValueError: on shape violations or when a tag maps to two
            different values.
    """
    if tag_map is None:
        return {}

    flat: dict[str, str] = {}

    def _record(tag: str, value: str, where: str) -> None:
        tag = tag.strip()
        if not tag:
            raise ValueError(f"{where}: empty tag is not allowed")
        prior = flat.get(tag)
        if prior is not None and prior != value:
            raise ValueError(
                f"tag {tag!r} mapped to both {prior!r} and "
                f"{value!r} in {kind}; each tag must route to exactly "
                f"one {value_label}"
            )
        flat[tag] = value

    def _split_csv(value: str) -> list[str]:
        return [p.strip() for p in value.split(",") if p.strip()]

    if isinstance(tag_map, dict):
        for key, value in tag_map.items():
            if not isinstance(key, str) or not key.strip():
                raise ValueError(f"{kind} keys must be non-empty strings, got {key!r}")
            if not isinstance(value, str) or not value:
                raise ValueError(
                    f"{kind}[{key!r}] must be a {value_label} string, got {value!r}"
                )
            for tag in _split_csv(key):
                _record(tag, value, where=f"{kind}[{key!r}]")

    elif isinstance(tag_map, list):
        for i, entry in enumerate(tag_map):
            if not isinstance(entry, dict):
                raise ValueError(
                    f"{kind}[{i}] must be a mapping with 'tags' and "
                    f"{value_label!r}, got {type(entry).__name__}"
                )
            tags_raw = entry.get("tags")
            entry_value = entry.get(value_label)
            if not isinstance(entry_value, str) or not entry_value:
                raise ValueError(
                    f"{kind}[{i}].{value_label} must be a non-empty string, got {entry_value!r}"
                )
            if isinstance(tags_raw, str):
                tag_list = _split_csv(tags_raw)
            elif isinstance(tags_raw, list):
                tag_list = []
                for t in tags_raw:
                    if not isinstance(t, str):
                        raise ValueError(
                            f"{kind}[{i}].tags entries must be strings, got {t!r}"
                        )
                    tag_list.extend(_split_csv(t))
            else:
                raise ValueError(
                    f"{kind}[{i}].tags must be a list or csv string, "
                    f"got {type(tags_raw).__name__}"
                )
            if not tag_list:
                raise ValueError(f"{kind}[{i}].tags must contain at least one tag")
            for tag in tag_list:
                _record(tag, entry_value, where=f"{kind}[{i}]")
    else:
        raise ValueError(
            f"{kind} must be a dict, a list, or None; got {type(tag_map).__name__}"
        )

    return flat


def _summarise_tag_map(routes: dict[str, str], *, label: str) -> str:
    """Compact one-line summary of a normalised tag map for the DAG-build
    startup log. Returns ``", <label>=[val1, val2]"`` when there are
    entries, or an empty string. Kept as a helper to stay under the
    100-column line limit at each call site.
    """
    if not routes:
        return ""
    return f", {label}={sorted(set(routes.values()))}"


#: Override keys that are handled by the builder for dispatch only --
#: NOT part of what triggers pull-out from a tag_group. ``runner``
#: (the dispatch-only key) is stripped everywhere else too.
_TAG_GROUPS_DISPATCH_ONLY_KEYS = frozenset({"runner"})


def _normalise_tag_groups(
    tag_groups: dict[str, Any] | list[dict[str, Any]] | None,
) -> dict[str, TagGroupSpec]:
    """Normalise the caller's ``tag_groups=`` input into a flat
    ``{tag: TagGroupSpec}`` map.

    Two accepted shapes:

    * Simple ``dict[tag, group_name]``::

          {"bronze": "bronze_batch"}

      Equivalent to ``TagGroupSpec(name="bronze_batch", overrides={})``.

    * Rich ``dict[tag, dict]`` with a ``name`` key and any number of
      override fields (``command``, ``target``, ``profile_name``,
      ``full_refresh``, ``vars_json``, ...)::

          {"gold": {"name": "gold_batch", "command": "build"}}

      Equivalent to
      ``TagGroupSpec(name="gold_batch", overrides={"command": "build"})``.

    * List form: ``[{"tags": [...], "name": "...", **overrides}]`` --
      same shape as the ``tag_runners`` list form.

    Validates that every entry has a non-empty ``name`` and doesn't
    smuggle in the dispatch-only ``runner`` key (routing lives in
    ``tag_runners`` / ``meta.stratus``, not in ``tag_groups``).
    """
    if tag_groups is None:
        return {}

    flat: dict[str, TagGroupSpec] = {}

    def _record(tag: str, spec: TagGroupSpec, where: str) -> None:
        tag = tag.strip()
        if not tag:
            raise ValueError(f"{where}: empty tag is not allowed")
        prior = flat.get(tag)
        if prior is not None and prior != spec:
            raise ValueError(
                f"tag {tag!r} mapped to both {prior.name!r} and "
                f"{spec.name!r} in tag_groups; each tag must route "
                f"to exactly one group_name"
            )
        flat[tag] = spec

    def _split_csv(value: str) -> list[str]:
        return [p.strip() for p in value.split(",") if p.strip()]

    def _make_spec(name: Any, extras: dict[str, Any], where: str) -> TagGroupSpec:
        if not isinstance(name, str) or not name.strip():
            raise ValueError(f"{where}.name must be a non-empty string, got {name!r}")
        bad = set(extras) & _TAG_GROUPS_DISPATCH_ONLY_KEYS
        if bad:
            raise ValueError(
                f"{where}: keys {sorted(bad)} are dispatch-only and cannot "
                f"appear in a tag_groups entry -- use tag_runners for runner "
                f"routing."
            )
        return TagGroupSpec(name=name.strip(), overrides=dict(extras))

    if isinstance(tag_groups, dict):
        for key, value in tag_groups.items():
            if not isinstance(key, str) or not key.strip():
                raise ValueError(f"tag_groups keys must be non-empty strings, got {key!r}")
            where = f"tag_groups[{key!r}]"
            if isinstance(value, str):
                spec = _make_spec(value, {}, where)
            elif isinstance(value, dict):
                extras = {k: v for k, v in value.items() if k != "name"}
                spec = _make_spec(value.get("name"), extras, where)
            else:
                raise ValueError(
                    f"{where} must be a group_name string or a mapping with "
                    f"'name', got {type(value).__name__}"
                )
            for tag in _split_csv(key):
                _record(tag, spec, where)
    elif isinstance(tag_groups, list):
        for i, entry in enumerate(tag_groups):
            if not isinstance(entry, dict):
                raise ValueError(
                    f"tag_groups[{i}] must be a mapping with 'tags' and "
                    f"'name', got {type(entry).__name__}"
                )
            tags_raw = entry.get("tags")
            extras = {k: v for k, v in entry.items() if k not in ("tags", "name")}
            spec = _make_spec(entry.get("name"), extras, f"tag_groups[{i}]")
            if isinstance(tags_raw, str):
                tag_list = _split_csv(tags_raw)
            elif isinstance(tags_raw, list):
                tag_list = []
                for t in tags_raw:
                    if not isinstance(t, str):
                        raise ValueError(
                            f"tag_groups[{i}].tags entries must be strings, got {t!r}"
                        )
                    tag_list.extend(_split_csv(t))
            else:
                raise ValueError(
                    f"tag_groups[{i}].tags must be a list or csv string, "
                    f"got {type(tags_raw).__name__}"
                )
            if not tag_list:
                raise ValueError(f"tag_groups[{i}].tags must contain at least one tag")
            for tag in tag_list:
                _record(tag, spec, f"tag_groups[{i}]")
    else:
        raise ValueError(
            f"tag_groups must be a dict, a list, or None; got {type(tag_groups).__name__}"
        )

    return flat


def _summarise_tag_group_specs(specs: dict[str, TagGroupSpec]) -> str:
    """One-line summary for the DAG-build startup log."""
    if not specs:
        return ""
    return f", tag_groups={sorted({s.name for s in specs.values()})}"


def _node_has_pullout_override_factory(
    graph: DbtGraph,
    overrides: dict[str, dict[str, Any]],
) -> Callable[[str], bool]:
    """Build a predicate that returns ``True`` when a node carries a
    per-model override (in ``overrides[uid]`` or
    ``node.meta.stratus``) beyond the dispatch-only ``runner`` key.

    Used by ``tag_groups`` to keep such nodes as singleton Airflow
    tasks so their per-model override isn't silently overridden by
    the group-level config.
    """
    per_uid_meta: dict[str, dict[str, Any]] = {}
    for node in graph:
        stratus = (node.meta or {}).get("stratus") or {}
        if isinstance(stratus, dict) and stratus:
            per_uid_meta[node.unique_id] = stratus

    def _pull_out(uid: str) -> bool:
        for source in (per_uid_meta.get(uid) or {}, overrides.get(uid) or {}):
            extras = {k for k in source if k not in _TAG_GROUPS_DISPATCH_ONLY_KEYS}
            if extras:
                return True
        return False

    return _pull_out


def _log_tag_groups_distribution(
    collapsed: Any,  # CollapsedGraph -- typed loosely to avoid import cycle at annotation eval
    tag_groups: dict[str, TagGroupSpec],
) -> None:
    """INFO-log the tag_groups distribution: which groups exist, how
    many members each has, and how many nodes were pulled out."""
    counts: dict[str, int] = {}
    produced_names: set[str] = set()  # spec.name values that landed in a task
    pulled_out: list[str] = []
    known_names = {s.name for s in tag_groups.values()}
    for g in collapsed:
        if g.group_id_override:
            counts[g.group_id_override] = len(g.members)
            # ``group_id_override`` looks like ``<spec.name>__<runner>``
            # (+ optional ``__<idx>`` for disconnected components).
            # Strip runner + component suffix so we can compare to
            # ``spec.name`` for the typo guard.
            for name in known_names:
                if g.group_id_override == name or g.group_id_override.startswith(name + "__"):
                    produced_names.add(name)
                    break
        elif g.is_singleton and any(
            t in tag_groups for t in (g.leader.tags or [])
        ):
            # Singleton that has a tag_groups tag but wasn't merged -> pulled out.
            pulled_out.append(g.leader.unique_id)
            # A pulled-out node still counts as "the spec produced
            # a task" for typo-guard purposes -- otherwise every
            # override in the DAG spuriously fires the warning.
            for tag in g.leader.tags or []:
                spec = tag_groups.get(tag)
                if spec is not None:
                    produced_names.add(spec.name)
    if counts:
        summary = ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
        _log.info("tag_groups distribution: %s", summary)
    if pulled_out:
        _log.info(
            "tag_groups: %d node(s) pulled out as singletons due to per-model overrides: %s",
            len(pulled_out),
            sorted(pulled_out),
        )
    unused = sorted(known_names - produced_names)
    if unused:
        _log.warning(
            "tag_groups declares group name(s) %s but no selected node "
            "produced them -- check tag mapping vs tag_groups keys",
            unused,
        )


def _resolve_graph(
    project: ProjectConfig,
    *,
    select: list[str] | None,
    exclude: list[str] | None,
    label: str,
) -> DbtGraph:
    """Load + filter the graph. Warns (but does not raise) when the
    result is empty."""
    graph = load_graph(project)
    graph = apply_selectors(graph, select=select, exclude=exclude)
    if len(graph) == 0:
        _log.warning(
            "%s: graph is empty after selectors -- the produced "
            "DAG/TaskGroup will have no tasks. Check select=%s exclude=%s",
            label,
            select,
            exclude,
        )
    return graph


def _attach_tasks(
    graph: DbtGraph,
    *,
    runners: dict[str, Runner],
    default_runner: str,
    target: str,
    project_archive_s3: str,
    overrides: dict[str, dict[str, Any]] | None,
    tag_overrides: dict[str, dict[str, Any]] | None = None,
    tag_runners: dict[str, str] | None,
    tag_profiles: dict[str, str] | None = None,
    tag_targets: dict[str, str] | None = None,
    tag_groups: dict[str, TagGroupSpec] | None = None,
    tag_single_name_prefixes: dict[str, str] | None = None,
    all_override_fields: dict[str, Any] | None = None,
    all_override_group_spec: TagGroupSpec | None = None,
    all_override_name_prefix: str | None = None,
    airflow_kwargs_per_task: dict[str, Any] | None,
    task_groups: TaskGroupingConfig | None,
    collapse_strategy: CollapseStrategy | None,
    drop_ephemeral: bool,
    dag: DAG | None,
) -> tuple[dict[str, BaseOperator], int]:
    """Iterate ``graph`` in topological order, dispatch each node to
    the runner named in its override (or ``default_runner``), wrap each
    task in the matching :class:`TaskGroup` from ``task_groups``, wire
    in-graph deps, and add setup/teardown brackets around each
    runner-subgroup that needs them.

    When ``collapse_strategy`` is set OR ``drop_ephemeral`` triggers,
    the dbt graph is first passed through
    :func:`dbt_aws.common.graph.collapse.collapse_graph`; the result
    is a graph of node-groups where each group becomes ONE Airflow
    task whose ``select=`` string joins every collapsed dbt node.

    ``tag_groups`` sits on top of structural collapse: nodes tagged
    for a group get bulk-collapsed into one task per
    ``(group_name, runner)`` unless they carry per-model overrides
    (see :func:`_node_has_pullout_override`), in which case they stay
    singletons.
    """
    overrides = overrides or {}
    tag_profiles = tag_profiles or {}
    tag_targets = tag_targets or {}
    tag_groups = tag_groups or {}
    tag_single_name_prefixes = tag_single_name_prefixes or {}

    # ``overrides.all`` collapse / prefix ride on the existing
    # tag-group and tag-prefix machinery via a synthetic reserved tag
    # key. Because ``DbtNode`` is a frozen dataclass we can't mutate
    # node.tags in-place; instead we plumb ``_ALL_SYNTHETIC_TAG`` down
    # to the two places that look up tag-based state (the collapse
    # pass via ``all_override_group_spec`` on ``collapse_graph``, and
    # the task-id prefix map via a graph-wide default).
    _ALL_SYNTHETIC_TAG = "__dbt_aws_all__"
    all_prefix_default: str | None = None
    if all_override_group_spec is not None:
        tag_groups = {**tag_groups, _ALL_SYNTHETIC_TAG: all_override_group_spec}
    if all_override_name_prefix is not None:
        all_prefix_default = all_override_name_prefix

    # build a per-node ``{unique_id -> task_id_prefix}`` map from
    # the tag-level prefixes. A node with two tags whose prefixes
    # disagree is a hard error at DAG-parse -- the same treatment as
    # any other tag override conflict on the same field.
    node_to_prefix: dict[str, str] = {}
    if tag_single_name_prefixes:
        for node in graph:
            matched_prefix: tuple[str, str] | None = None  # (source_tag, prefix)
            for tag in node.tags or []:
                prefix = tag_single_name_prefixes.get(tag)
                if prefix is None:
                    continue
                if matched_prefix is not None and matched_prefix[1] != prefix:
                    raise ValueError(
                        f"node {node.unique_id!r}: task-id prefix conflict -- "
                        f"tag {matched_prefix[0]!r} sets name={matched_prefix[1]!r}, "
                        f"tag {tag!r} sets name={prefix!r}. A node must not "
                        f"carry tags whose overrides[tag.*].name disagree. "
                        f"Fix by aligning name: on the two tag entries or "
                        f"removing one tag from the node."
                    )
                matched_prefix = (tag, prefix)
            if matched_prefix is not None:
                node_to_prefix[node.unique_id] = matched_prefix[1]
    # ``overrides.all: {mode: single, name: <prefix>}`` applies
    # a task-id prefix to every node that didn't already inherit one
    # from a tag-specific ``mode: single, name:`` entry. Tag-specific
    # wins per the precedence ladder (all: < tag:).
    if all_prefix_default is not None:
        for node in graph:
            node_to_prefix.setdefault(node.unique_id, all_prefix_default)

    # derive the flat ``{tag: value}`` dispatch maps from
    # ``tag_overrides`` for the three fields the ladder resolves
    # separately (``runner`` / ``profile_name`` / ``target``). The
    # caller can also pass those maps directly (Python API), in which
    # case the explicit map wins over anything derived from
    # ``tag_overrides`` -- lets tests + advanced callers override
    # without going through ``tag.<name>:`` entries.
    if tag_overrides:
        derived_runners = {
            t: entry["runner"]
            for t, entry in tag_overrides.items()
            if isinstance(entry.get("runner"), str)
        }
        derived_profiles = {
            t: entry["profile_name"]
            for t, entry in tag_overrides.items()
            if isinstance(entry.get("profile_name"), str)
        }
        derived_targets = {
            t: entry["target"]
            for t, entry in tag_overrides.items()
            if isinstance(entry.get("target"), str)
        }
        if not tag_runners and derived_runners:
            tag_runners = derived_runners
        if not tag_profiles and derived_profiles:
            tag_profiles = derived_profiles
        if not tag_targets and derived_targets:
            tag_targets = derived_targets

    # 1. Map every node to its assigned runner name.
    all_layer_runner: str | None = None
    all_layer_profile: str | None = None
    all_layer_target: str | None = None
    if all_override_fields:
        v = all_override_fields.get("runner")
        if isinstance(v, str) and v:
            all_layer_runner = v
        v = all_override_fields.get("profile_name")
        if isinstance(v, str) and v:
            all_layer_profile = v
        v = all_override_fields.get("target")
        if isinstance(v, str) and v:
            all_layer_target = v

    node_to_runner = _resolve_node_runners(
        graph,
        overrides=overrides,
        tag_runners=tag_runners or {},
        default_runner=default_runner,
        runners=runners,
        all_layer_runner=all_layer_runner,
    )

    # 1a. Per-node ``profile_name`` and ``target`` resolution. Same
    #     ladder as ``_resolve_node_runners`` (default < all < tag <
    #     meta < per-node), but the values are opaque strings (no
    #     ``runners`` membership check) and the tag map is optional.
    #     ``None`` means "no layer above the runner default set this
    #     field" -- the runner's own ``profile_name`` / ``target``
    #     attribute then kicks in inside ``_build_script_args``.
    node_to_profile = _resolve_node_field(
        graph,
        overrides=overrides,
        tag_map=tag_profiles,
        field="profile_name",
        all_layer_value=all_layer_profile,
    )
    node_to_target = _resolve_node_field(
        graph,
        overrides=overrides,
        tag_map=tag_targets,
        field="target",
        all_layer_value=all_layer_target,
    )

    # Rewrite ``overrides[uid][field]`` for any layer we resolved above
    # ``overrides`` itself (meta.stratus or tag_map) so the runner-side
    # ``effective(runner, override, field)`` picks it up without further
    # plumbing. We do NOT rewrite when the source was already
    # ``overrides[uid][field]`` (would be a no-op) nor when the value
    # is ``None`` (nothing to inject). The rewrite is scoped to a
    # local dict so caller-supplied ``overrides`` stays untouched.
    effective_overrides: dict[str, dict[str, Any]] = {
        uid: dict(v) for uid, v in overrides.items()
    }
    for uid, value in node_to_profile.items():
        if value is not None:
            effective_overrides.setdefault(uid, {}).setdefault("profile_name", value)
    for uid, value in node_to_target.items():
        if value is not None:
            effective_overrides.setdefault(uid, {}).setdefault("target", value)

    # 1a-bis. Fold ANY non-dispatch field from ``tag_overrides`` into
    # ``effective_overrides``. Dispatch (``runner``), profile, and
    # target already have dedicated resolution paths above; everything
    # else (``command``, ``full_refresh``, ``vars_json``, worker sizing
    # etc.) piggy-backs on the per-node ``overrides`` bucket so each
    # runner's ``_build_script_args`` / ``resolve_override`` picks it
    # up without new plumbing.
    #
    # Precedence per non-dispatch field (top wins):
    #   1. Caller ``overrides[uid][field]`` (already in effective_overrides)
    #   2. ``meta.stratus[field]`` -- resolved inside resolve_override
    #   3. ``tag_overrides[<tag>][field]`` (this pass; setdefault preserves 1)
    #   4. ``overrides.all[field]`` (, this pass; setdefault
    #      preserves 1-3 -- ``all:`` is the weakest override layer)
    #   5. Runner default (inside runner's build_script_args)
    _apply_tag_overrides_to_effective(
        effective_overrides,
        graph=graph,
        tag_overrides=tag_overrides or {},
    )
    _apply_all_override_to_effective(
        effective_overrides,
        graph=graph,
        all_override_fields=all_override_fields,
    )

    # Parse-time visibility: print how many nodes ended up on each
    # runner so DAG authors can spot mis-routes immediately in the
    # scheduler / dag-processor log when Airflow imports the file.
    _log_runner_distribution(
        node_to_runner,
        runners=runners,
        tag_runners=tag_runners or {},
        graph=graph,
    )
    def _runner_profile(uid: str) -> str | None:
        return getattr(runners[node_to_runner[uid]], "profile_name", None)

    def _runner_target(uid: str) -> str | None:
        return getattr(runners[node_to_runner[uid]], "target", None)

    _log_field_distribution(
        node_to_profile,
        field="profile_name",
        tag_map=tag_profiles,
        graph=graph,
        runner_default_lookup=_runner_profile,
        dag_default=None,
    )
    _log_field_distribution(
        node_to_target,
        field="target",
        tag_map=tag_targets,
        graph=graph,
        runner_default_lookup=_runner_target,
        dag_default=target,
    )

    # 1b. Guard: a node that carries two DIFFERENT tag_groups keys is a
    # config error -- like the ``tag_runners`` conflict guard, we
    # detect at parse time so the caller sees a clear error message.
    if tag_groups:
        for node in graph:
            matched = {t: tag_groups[t].name for t in (node.tags or []) if t in tag_groups}
            distinct = set(matched.values())
            if len(distinct) > 1:
                conflict = ", ".join(
                    f"{tag!r} -> {name!r}" for tag, name in sorted(matched.items())
                )
                raise ValueError(
                    f"node {node.unique_id!r}: tag_groups conflict "
                    f"({conflict}). A model can join at most one "
                    f"tag_group -- retag the model or remove the "
                    f"overlapping tag_groups entry."
                )

    # 1c. Collapse the graph. When strategy is None and drop_ephemeral
    # is True (default), only ephemeral nodes get dropped -- every
    # other node stays a singleton group. Behaviour is byte-identical
    # to earlier whenever the caller has no ephemeral models AND
    # doesn't pass ``collapse_strategy=``.
    #
    # ``pull_out`` keeps a node as a singleton when it carries any
    # per-model override (in ``overrides[uid]`` or
    # ``meta.stratus``) beyond the dispatch-only ``runner`` key --
    # preserves per-model behaviour when it disagrees with the
    # tag_group's group-level config.
    _pull_out_pred = _node_has_pullout_override_factory(graph, overrides) if tag_groups else None
    # when ``overrides.all.mode == 'group'`` we hand collapse
    # a graph whose nodes all carry the synthetic reserved tag
    # ``__dbt_aws_all__``. That tag is a key in ``tag_groups`` above,
    # so the existing tag-group machinery folds every eligible node
    # into one collapsed task. The original graph stays untouched.
    graph_for_collapse: DbtGraph = graph
    if all_override_group_spec is not None:
        from dataclasses import replace as _dc_replace

        graph_for_collapse = DbtGraph.from_nodes(
            _dc_replace(node, tags=[*(node.tags or []), _ALL_SYNTHETIC_TAG])
            for node in graph
        )
    collapsed = collapse_graph(
        graph_for_collapse,
        strategy=collapse_strategy,
        drop_ephemeral=drop_ephemeral,
        runner_for_node=(lambda n: node_to_runner[n.unique_id]),
        tag_groups=tag_groups or None,
        pull_out=_pull_out_pred,
    )
    if collapse_strategy is not None:
        _log.info(
            "collapse: %d dbt node(s) -> %d Airflow task(s) (strategy=%s)",
            sum(len(g.members) for g in collapsed),
            len(collapsed),
            collapse_strategy,
        )
    if tag_groups:
        # Extra visibility: emit a distribution line so users can see
        # how many nodes ended up in each tag_group task (and how many
        # got pulled out as singletons).
        _log_tag_groups_distribution(collapsed, tag_groups)

    # 2. Map every group's LEADER node to a task-group name (or None).
    #    Grouping is driven by the leader's tags because a merged
    #    group may span multiple layers -- we key on the leader so
    #    ``sv_dim_supplier + view_of_it`` visually lives in the
    #    downstream group.
    node_to_group = _resolve_node_groups(graph, task_groups=task_groups)

    # 3. Construct the TaskGroup contexts we'll need.
    from dbt_aws.common._airflow_compat import TaskGroup as _AirflowTaskGroup

    group_contexts: dict[str, Any] = {}
    if task_groups is not None:
        for name in sorted({g for g in node_to_group.values() if g}):
            group_contexts[name] = _AirflowTaskGroup(group_id=name)

    # 4. Per-GROUP make_task. Each collapsed group becomes exactly
    #    one Airflow task; the runner sees ``select=`` as the space-
    #    joined list of member dbt-node names.
    #
    #    When a ``tag_groups`` group carries a ``group_config`` block
    #    (e.g. ``{command: 'build', target: 'prod'}``), we merge it
    #    into ``effective_overrides[leader.unique_id]`` -- but only
    #    where the per-node override doesn't already have that key,
    #    so per-model still wins. The pull-out rule handled the
    #    conflicting case earlier by keeping that node as a singleton.
    tasks: dict[str, BaseOperator] = {}
    for cgroup in collapsed:
        if cgroup.group_config:
            leader_ov = effective_overrides.setdefault(cgroup.leader.unique_id, {})
            for k, v in cgroup.group_config.items():
                leader_ov.setdefault(k, v)

    for cgroup in collapsed:
        leader = cgroup.leader
        runner_name = node_to_runner[leader.unique_id]
        selected = runners[runner_name]
        tg_name = node_to_group.get(leader.unique_id)

        def _build_task(_sel=selected, _cg=cgroup, _rn=runner_name) -> BaseOperator:
            # Per-node effective target -- may come from override /
            # meta / tag layer (already merged into ``effective_overrides``
            # so ``effective(self, ov, "target")`` inside each runner's
            # ``_build_script_args`` picks it up). The ``target=``
            # kwarg we pass here is the DAG-level fallback used only
            # when NO other layer set a value AND the runner itself
            # has ``target=None``.
            return _sel.make_task(
                task_id=_task_id_for_group(_cg, _rn, node_to_prefix),
                node=_cg.leader,
                dbt_command=dbt_command_for(_cg.leader),
                # The magic: ``dbt run --select v1 v2 t`` when the
                # group has more than one member. dbt-core resolves
                # the internal order via its own graph.
                select=_cg.select_string if not _cg.is_singleton else _cg.leader.name,
                target=target,
                dag=dag,
                project_archive_s3=project_archive_s3,
                airflow_kwargs=airflow_kwargs_per_task,
                overrides=effective_overrides,
                tag_name=_cg.tag_name,
            )

        if tg_name is None:
            tasks[cgroup.group_id] = _build_task()
        else:
            with group_contexts[tg_name]:
                tasks[cgroup.group_id] = _build_task()

    # 5. In-graph dependency wiring at the GROUP level.
    edges = 0
    for cgroup in collapsed:
        for upstream_gid in cgroup.upstream_group_ids:
            tasks[upstream_gid] >> tasks[cgroup.group_id]
            edges += 1

    # 6. Setup/teardown brackets per runner-subgroup. Reusable runners
    # return non-None from make_setup_task / make_teardown_task; the
    # bracket is wired only around the groups that use THAT runner.
    for runner_name, selected in runners.items():
        subset = [g for g in collapsed if node_to_runner[g.leader.unique_id] == runner_name]
        if not subset:
            continue
        setup = selected.make_setup_task(dag=dag, airflow_kwargs=airflow_kwargs_per_task)
        teardown = selected.make_teardown_task(dag=dag, airflow_kwargs=airflow_kwargs_per_task)
        if setup is not None or teardown is not None:
            subset_gids = {g.group_id for g in subset}
            roots = [
                tasks[g.group_id]
                for g in subset
                if not (g.upstream_group_ids & subset_gids)
            ]
            leaves = [
                tasks[g.group_id]
                for g in subset
                if not (g.downstream_group_ids & subset_gids)
            ]
            if setup is not None:
                for root in roots:
                    setup >> root
                edges += len(roots)
            if teardown is not None:
                for leaf in leaves:
                    leaf >> teardown
                edges += len(leaves)
    return tasks, edges


def _log_field_distribution(
    node_to_value: dict[str, str | None],
    *,
    field: str,
    tag_map: dict[str, str],
    graph: DbtGraph,
    runner_default_lookup: Any,
    dag_default: str | None,
) -> None:
    """Log a one-line summary of how ``field`` (``profile_name`` or
    ``target``) resolved across nodes, plus a WARNING for any
    ``tag_map`` entries whose tags don't appear on the graph (typo
    guard, same pattern as :func:`_log_runner_distribution`).

    ``dag_default`` is the DAG-level fallback (``target=`` on
    ``DbtDag``; ``None`` for ``profile_name`` since there's no
    DAG-level knob). ``runner_default_lookup(uid)`` returns the
    runner-level default for the runner assigned to that node -- used
    only for the summary, not for resolution.

    Summary shape (INFO): ``<field> distribution: value1=N, value2=M,
    <runner-default>=K, <dag-default>=L``. Skipped entirely when no
    layer above the runner default sets any value AND the tag map is
    empty; that is the byte-identical pre-feature behaviour.
    """
    tag_map = tag_map or {}
    if not tag_map and not any(v is not None for v in node_to_value.values()):
        # No layer above the runner default touched this field for
        # any node. Nothing interesting to log -- avoid noise.
        return

    counts: dict[str, int] = {}
    for uid, value in node_to_value.items():
        if value is not None:
            key = value
        else:
            runner_val = runner_default_lookup(uid)
            if runner_val is not None:
                key = f"{runner_val} (runner-default)"
            elif dag_default is not None:
                key = f"{dag_default} (dag-default)"
            else:
                key = "(unset)"
        counts[key] = counts.get(key, 0) + 1
    summary = ", ".join(f"{name}={counts[name]}" for name in sorted(counts))
    _log.info("%s distribution: %s", field, summary)

    if tag_map:
        present_tags = {t for node in graph for t in (node.tags or [])}
        unused = sorted(set(tag_map) - present_tags)
        if unused:
            _log.warning(
                "tag_%ss declares tag(s) %s but no selected node carries "
                "them -- check for typos or update your selectors",
                field,
                unused,
            )


#: Dispatch/profile/target fields already resolved via dedicated code
#: paths -- ``_apply_tag_overrides_to_effective`` and
#: ``_apply_all_override_to_effective`` skip them so they don't
#: double-write.
_TAG_OVERRIDE_ALREADY_RESOLVED_FIELDS = frozenset({"runner", "profile_name", "target"})


def _apply_all_override_to_effective(
    effective_overrides: dict[str, dict[str, Any]],
    *,
    graph: DbtGraph,
    all_override_fields: dict[str, Any] | None,
) -> None:
    """Fold ``overrides.all[field]`` into ``effective_overrides[uid]``
    for every node in the graph.

     helper. ``all:`` is the weakest override layer -- higher
    layers (tag / meta.stratus / per-node) still win per key. The
    fold uses ``setdefault`` so any node bucket already carrying
    ``field`` (populated by ``_apply_tag_overrides_to_effective`` or
    a caller-supplied per-node ``overrides`` dict) is left untouched.

    Dispatch, profile, target are skipped -- those fields have their
    own dedicated resolvers (``_resolve_node_runners`` /
    ``_resolve_node_field``) that already know about the ``all``
    layer via their ``all_layer_*`` kwargs.
    """
    if not all_override_fields:
        return
    for node in graph:
        node_bucket = effective_overrides.setdefault(node.unique_id, {})
        for field, value in all_override_fields.items():
            if field in _TAG_OVERRIDE_ALREADY_RESOLVED_FIELDS:
                continue
            node_bucket.setdefault(field, value)


def _apply_tag_overrides_to_effective(
    effective_overrides: dict[str, dict[str, Any]],
    *,
    graph: DbtGraph,
    tag_overrides: dict[str, dict[str, Any]],
) -> None:
    """Fold ``tag_overrides[tag][field]`` into ``effective_overrides[uid]``.

    Every node in ``graph`` inherits fields from every tag it carries,
    UNLESS the caller already supplied a per-node value (``setdefault``
    preserves per-node precedence).

    Conflict detection: if a node has TWO tags mapping the same field
    to different values, raise :class:`ValueError` at DAG-build time.
    Matches the ``_resolve_node_field`` conflict pattern already used
    for profile/target -- users see a clear error instead of an
    arbitrary last-wins merge.

    Dispatch/profile/target are skipped -- those fields already have
    dedicated resolution passes upstream in :func:`_attach_tasks`.
    """
    if not tag_overrides:
        return
    # Fields whose values are dict-merged across tags (and vs the
    # per-node override) instead of the scalar last-wins+conflict rule.
    #
    _dict_merge_fields = frozenset({"resource_tags", "spark_conf"})
    for node in graph:
        node_tags = list(node.tags or [])
        if not node_tags:
            continue
        matched: dict[str, tuple[str, Any]] = {}  # field -> (source_tag, value)
        merged_dicts: dict[str, dict[str, Any]] = {}  # field -> shallow-merged dict
        for tag in node_tags:
            entry = tag_overrides.get(tag)
            if not entry:
                continue
            for field, value in entry.items():
                if field in _TAG_OVERRIDE_ALREADY_RESOLVED_FIELDS:
                    continue
                if field in _dict_merge_fields:
                    if not isinstance(value, dict):
                        raise ValueError(
                            f"node {node.unique_id!r}: tag_overrides[{tag!r}]."
                            f"{field} must be a dict, got {type(value).__name__}"
                        )
                    bucket = merged_dicts.setdefault(field, {})
                    # Later tag wins per key. Iteration order over
                    # ``node.tags`` is stable so the merge is deterministic.
                    bucket.update(value)
                    continue
                prior = matched.get(field)
                if prior is not None and prior[1] != value:
                    raise ValueError(
                        f"node {node.unique_id!r}: tag override conflict for "
                        f"field {field!r} -- tag {prior[0]!r} sets {prior[1]!r}, "
                        f"tag {tag!r} sets {value!r}. A node must not carry "
                        f"tags whose overrides[tag.*] disagree on the same "
                        f"field. Fix by removing the tag or aligning the "
                        f"override values."
                    )
                matched[field] = (tag, value)
        if not matched and not merged_dicts:
            continue
        node_bucket = effective_overrides.setdefault(node.unique_id, {})
        for field, (_source_tag, value) in matched.items():
            # setdefault preserves caller's per-node overrides -- if the
            # user already set ``overrides[uid][field]`` at DbtDag time,
            # we don't overwrite.
            node_bucket.setdefault(field, value)
        for field, tag_merged in merged_dicts.items():
            # Dict-merge fields: shallow-merge caller's per-node dict
            # ON TOP of the tag-merged baseline so per-node still wins
            # per key, but new keys from tags flow through.
            existing = node_bucket.get(field)
            if isinstance(existing, dict):
                node_bucket[field] = {**tag_merged, **existing}
            else:
                node_bucket[field] = dict(tag_merged)


def _resolve_node_field(
    graph: DbtGraph,
    *,
    overrides: dict[str, dict[str, Any]],
    tag_map: dict[str, str],
    field: str,
    all_layer_value: str | None = None,
) -> dict[str, str | None]:
    """Sibling of :func:`_resolve_node_runners` for opaque-value fields
    (``profile_name``, ``target``).

    Resolution order (last layer wins):
        1. ``None`` (fall through to runner default in the runner)
        2. ``all_layer_value`` ( ``overrides.all.<field>``)
        3. ``tag_map`` -- any tag on the node matching the map
        4. ``node.meta.stratus.<field>``
        5. ``overrides[unique_id].<field>``

    Returns a dict of ``{unique_id: value | None}``. ``None`` means
    "no layer above the runner default set this field"; the runner's
    own ``profile_name`` / ``target`` attribute then kicks in inside
    ``_build_script_args``.

    Errors:
        * Multiple tags on a node mapping to different values via
          ``tag_map`` -> :class:`ValueError` (typo/config error the
          user must fix).
    """
    out: dict[str, str | None] = {}
    for node in graph:
        layer_a = (node.meta or {}).get("stratus", {})
        layer_a_val = layer_a.get(field) if isinstance(layer_a, dict) else None
        layer_b_val = (overrides.get(node.unique_id, {}) or {}).get(field)

        tag_val: str | None = None
        if tag_map:
            matched: dict[str, str] = {
                tag: tag_map[tag] for tag in (node.tags or []) if tag in tag_map
            }
            distinct = set(matched.values())
            if len(distinct) > 1:
                conflict = ", ".join(f"{tag!r} -> {v!r}" for tag, v in sorted(matched.items()))
                raise ValueError(
                    f"node {node.unique_id!r}: tag_{field}s conflict "
                    f"({conflict}). A model can route to at most one "
                    f"{field} via tags -- retag the model or remove the "
                    f"overlapping tag entry."
                )
            if matched:
                tag_val = next(iter(distinct))

        chosen = layer_b_val or layer_a_val or tag_val or all_layer_value
        # Non-string, non-None means the user misconfigured meta/override.
        if chosen is not None and not isinstance(chosen, str):
            raise ValueError(
                f"node {node.unique_id!r}: {field!r} resolved to "
                f"non-string value {chosen!r}. Fix the corresponding "
                f"``overrides``/``meta.stratus.{field}`` entry."
            )
        out[node.unique_id] = chosen
    return out


def _log_runner_distribution(
    node_to_runner: dict[str, str],
    *,
    runners: dict[str, Runner],
    tag_runners: dict[str, str],
    graph: DbtGraph,
) -> None:
    """Log a one-line summary per runner naming the node count it owns
    + warn for ``tag_runners`` entries whose tags don't appear on any
    selected node (typo guard, soft: a warning, not an error)."""
    counts: dict[str, int] = dict.fromkeys(runners, 0)
    for runner_name in node_to_runner.values():
        counts[runner_name] = counts.get(runner_name, 0) + 1
    summary = ", ".join(f"{name}={counts[name]}" for name in sorted(counts))
    _log.info("runner distribution: %s", summary)

    if tag_runners:
        present_tags = {t for node in graph for t in (node.tags or [])}
        unused = sorted(set(tag_runners) - present_tags)
        if unused:
            _log.warning(
                "tag_runners declares tag(s) %s but no selected node "
                "carries them -- check for typos or update your selectors",
                unused,
            )


def _resolve_node_runners(
    graph: DbtGraph,
    *,
    overrides: dict[str, dict[str, Any]],
    tag_runners: dict[str, str],
    default_runner: str,
    runners: dict[str, Runner],
    all_layer_runner: str | None = None,
) -> dict[str, str]:
    """Map every node in the graph to its effective runner name.

    Resolution order (last layer wins):
        1. ``default_runner``
        2. ``all_layer_runner`` ( ``overrides.all.runner``)
        3. ``tag_runners`` (any tag on the node matching the map)
        4. ``node.meta.stratus.runner``
        5. ``overrides[unique_id].runner``

    Errors:
        * Multiple tags on a single node mapping to different runners
          via ``tag_runners`` -> :class:`ValueError` (the user must
          retag the node or remove the conflicting route).
        * Final chosen runner not in ``runners`` -> :class:`ValueError`.
    """
    node_to_runner: dict[str, str] = {}
    for node in graph:
        layer_a = (node.meta or {}).get("stratus", {})
        layer_a_runner = layer_a.get("runner") if isinstance(layer_a, dict) else None
        layer_b_runner = (overrides.get(node.unique_id, {}) or {}).get("runner")

        # Tag-based routing layer. Sits below the per-node layers so
        # ``overrides``/``meta.stratus`` keep their escape-hatch role.
        tag_runner: str | None = None
        if tag_runners:
            matched: dict[str, str] = {
                tag: tag_runners[tag] for tag in (node.tags or []) if tag in tag_runners
            }
            distinct = set(matched.values())
            if len(distinct) > 1:
                conflict = ", ".join(f"{tag!r} -> {r!r}" for tag, r in sorted(matched.items()))
                raise ValueError(
                    f"node {node.unique_id!r}: tag_runners conflict "
                    f"({conflict}). A model can route to at most one "
                    f"runner via tags -- retag the model or remove the "
                    f"overlapping tag_runners entry."
                )
            if matched:
                tag_runner = next(iter(distinct))

        chosen = (
            layer_b_runner
            or layer_a_runner
            or tag_runner
            or all_layer_runner
            or default_runner
        )
        if chosen not in runners:
            raise ValueError(
                f"node {node.unique_id!r}: resolved runner={chosen!r} "
                f"not in runners (have {sorted(runners)})"
            )
        node_to_runner[node.unique_id] = chosen
    return node_to_runner


def _resolve_node_groups(
    graph: DbtGraph,
    *,
    task_groups: TaskGroupingConfig | None,
) -> dict[str, str | None]:
    """Map every node to its task-group name (or ``None`` for ungrouped
    AND no ``ungrouped_group`` fallback configured).

    Strict: a node matching multiple groups raises ``ValueError``.
    """
    if task_groups is None:
        return {n.unique_id: None for n in graph}

    tag_to_group = task_groups.tag_to_group_name()
    fallback = task_groups.ungrouped_group  # may be None
    out: dict[str, str | None] = {}
    for node in graph:
        matched = {tag_to_group[t] for t in (node.tags or []) if t in tag_to_group}
        if len(matched) > 1:
            raise ValueError(
                f"node {node.unique_id!r} matches multiple task_groups "
                f"({sorted(matched)}) -- a model must belong to exactly "
                f"one group. Disambiguate by retagging or by removing "
                f"the overlapping group."
            )
        if matched:
            out[node.unique_id] = matched.pop()
        else:
            out[node.unique_id] = fallback  # None or the fallback group name
    return out


def _task_id_for(node: DbtNode, prefix: str | None = None) -> str:
    """Sanitise a dbt ``unique_id`` into an Airflow-legal task id,
    optionally prepending a ``<prefix>__`` namespace (used
    ``overrides[tag.<t>]: {mode: single, name: <prefix>}`` entries so
    tagged siblings sort together in the Airflow Graph view).
    """
    sanitised = node.unique_id.replace(".", "__")
    tid = _TASK_ID_SANITISE.sub("_", sanitised)
    if prefix:
        # ``name:`` was already validated at load time to match
        # ``[A-Za-z][A-Za-z0-9_]*`` so it is Airflow-legal as-is.
        return f"{prefix}__{tid}"
    return tid


def _task_id_for_group(
    cgroup: Any,  # NodeGroup -- loose to avoid import cycle at annotation eval
    runner_name: str,
    node_to_prefix: Mapping[str, str] | None = None,
) -> str:
    """Return the Airflow task id for a collapsed group.

    * For ``tag_groups`` groups (``group_id_override`` set): use
      ``dbt__<group_name_stripped>__<runner_name>`` -- the leader
      unique_id doesn't shape the id, so downstream tooling can grep
      for ``dbt__<group_name>`` reliably. ``node_to_prefix`` is not
      consulted here -- prefixes are a per-node concept and a group
      has multiple nodes.
    * For every other group (singletons + structural-collapse):
      fall back to :func:`_task_id_for(cgroup.leader, prefix)` where
      ``prefix`` comes from ``node_to_prefix[leader.unique_id]`` when
      set. Byte-identical to earlier when the prefix map is empty.
    """
    if getattr(cgroup, "group_id_override", None):
        sanitised = _TASK_ID_SANITISE.sub("_", f"dbt__{cgroup.group_id_override}")
        return sanitised
    prefix = None
    if node_to_prefix is not None:
        prefix = node_to_prefix.get(cgroup.leader.unique_id)
    return _task_id_for(cgroup.leader, prefix)
