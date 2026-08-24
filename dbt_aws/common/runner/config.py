"""Declarative YAML config for runners.

Supports two shapes:

1. **Single-runner** (simple, backward-compatible):

   .. code-block:: yaml

       runner:
         type: glue_spark
         job_name: my-job
       overrides:
         model.proj.x:
           worker_type: G.2X

2. **Multi-runner** (declare several named runners; per-model override
   can switch between them):

   .. code-block:: yaml

       runners:
         primary:
           type: glue_spark
           job_name: dbt-aws-primary
         heavy:
           type: glue_spark
           worker_type: G.4X
         athena:
           type: glue_python_shell
       default_runner: primary
       overrides:
         model.proj.heavy_agg:
           runner: heavy
         model.proj.warehouse:
           runner: athena

``overrides:`` entries also accept a ``tag.<name>:`` shape that folds
routing AND bulk-by-tag task collapse into one unified section.
The pre-existing top-level ``tag_groups:`` YAML key has
been replaced by two meta-keys on tag entries:

   .. code-block:: yaml

       overrides:
         # mode: single (default) -- one Airflow task per node, tag
         # entry only supplies bulk override defaults. Optional
         # ``name:`` becomes a task-id prefix so tagged siblings
         # sort together in the Graph view (``<name>__<uid>``).
         tag.landing:
           mode: single       # (default; can be omitted)
           name: landing      # optional -- task-id prefix
           runner: shell
           target: shell_dev

         # mode: group -- collapse every tagged node into ONE
         # Airflow task per (group_name, runner) bucket. ``name:``
         # defaults to the tag name.
         tag.bronze:
           mode: group
           name: bronze_batch  # optional -- defaults to 'bronze'
           runner: heavy
           command: build
           worker_type: G.2X

Validation runs at DAG-parse time:

* Unknown runner ``type:`` -> error
* Missing required runner kwargs -> error
* ``default_runner`` not in ``runners`` -> error
* Override ``runner:`` key not in ``runners`` -> error
* Override field not on selected runner's ``OVERRIDE_TYPE`` -> error
* ``mode:`` value not in ``{single, group}`` -> error
* ``name:`` not matching ``[A-Za-z][A-Za-z0-9_]*`` -> error
* Two ``mode: create`` runners with the same resolved ``job_name`` -> error
  (would race on ``glue:CreateJob``)
"""

from __future__ import annotations

import importlib
import logging
import re
from collections.abc import Mapping
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any

from dbt_aws.common.graph.collapse import TagGroupSpec
from dbt_aws.common.runner.base import Runner
from dbt_aws.common.runner.override import RunnerOverride
from dbt_aws.common.runner.tags import validate_resource_tags

_LOG = logging.getLogger(__name__)

# Matches either the escape sequence ``$${...}`` (captured as group 1)
# or a real reference ``${name}`` (captured as group 2). ``name`` is
# any run of characters that isn't ``{`` or ``}`` -- keeping the
# character class permissive is intentional: we don't want to define
# a variable-name grammar here, we just want to hand a well-formed
# key to the caller's ``vars`` mapping. Anything the caller didn't
# provide surfaces as a clear "undefined variable" error below.
_VAR_PATTERN = re.compile(r"(\$\$\{[^{}]*\})|\$\{([^{}]+)\}")

#: Map of ``type:`` strings to ``"module:class"`` paths. Imports are
#: resolved lazily so dbt-aws-common doesn't depend on runner packages.
#:
#: EMR entries (``emr_serverless`` / ``emr_cluster_step``) were removed
#: -- the runner source lives under ``attic/removed_0_9_1/emr/``
#: (gitignored). YAMLs still using those types will fail with the
#: standard "unknown runner type" error listing the currently-supported
#: types. Pin an earlier ``dbt-aws`` release if you still need them.
_RUNNER_REGISTRY: dict[str, str] = {
    "glue_spark": "dbt_aws.spark.runners.glue_job:GlueSparkRunner",
    "glue_python_shell": "dbt_aws.nonspark.runners.glue_python_shell:GluePythonShellRunner",
    "glue_interactive_session": "dbt_aws.spark.runners.glue_session:GlueInteractiveSessionRunner",
    "emr_serverless": "dbt_aws.spark.runners.emr_serverless:EmrServerlessRunner",
    "emr_cluster_step": "dbt_aws.spark.runners.emr_cluster_step:EmrClusterStepRunner",
}

_ALLOWED_TOP_LEVEL_KEYS = frozenset(
    {
        "runner",
        "runners",
        "default_runner",
        "overrides",
        "task_groups",
        "ungrouped_group",
        "openlineage",
        "resource_tags",
    }
)

#: Top-level YAML keys removed in favour of the unified ``overrides:``
#: shape (``tag_runners``, ``tag_profiles``, ``tag_targets``,
#: ``tag_groups``). The loader detects and rejects them with a hard
#: error that points at the equivalent ``overrides:`` form.
_REMOVED_TOP_LEVEL_KEYS: dict[str, str] = {
    "tag_runners": "runner",
    "tag_profiles": "profile_name",
    "tag_targets": "target",
    "tag_groups": "mode",
}

#: Metadata keys allowed inside ``overrides[tag.<name>]`` entries that
#: are NOT forwarded to the runner's ``OVERRIDE_TYPE`` field schema.
#: See :func:`_build_overrides` for how they are peeled off.
_TAG_ENTRY_META_KEYS = frozenset({"mode", "name"})

#: Allowed values for the ``mode:`` meta-key on a ``tag.<name>`` entry.
_TAG_MODE_VALUES = frozenset({"single", "group"})

#: Airflow task-id-legal names. The ``name:`` meta-key must match this.
_TAG_NAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")

_OL_ALLOWED_KEYS = frozenset(
    {
        "namespace",
        "s3_uri",
        "smus_domain_id",
        "smus_region",
        "parent_run_id_template",
        "parent_job_name_template",
        "parent_job_namespace",
        "extra_env",
    }
)


class RunnerConfigError(ValueError):
    """Any failure loading or validating a runner config YAML."""


@dataclass(frozen=True)
class TaskGroupConfig:
    """One named TaskGroup in the DAG: every dbt node carrying any of
    the listed tags ends up inside this group."""

    name: str
    tags: frozenset[str]


@dataclass(frozen=True)
class TaskGroupingConfig:
    """Full task-grouping spec.

    Attributes:
        groups: ordered list of named groups.
        ungrouped_group: optional name of a fallback group for models
            that don't match any group's tags. When ``None``, unmatched
            models live at the DAG root.
    """

    groups: tuple[TaskGroupConfig, ...]
    ungrouped_group: str | None = None

    def tag_to_group_name(self) -> dict[str, str]:
        """Flat ``{tag: group_name}`` index. Already validated for
        uniqueness at construction time."""
        return {tag: g.name for g in self.groups for tag in g.tags}

    @property
    def names(self) -> set[str]:
        return {g.name for g in self.groups} | (
            {self.ungrouped_group} if self.ungrouped_group is not None else set()
        )


@dataclass(frozen=True)
class LoadedRunnerConfig:
    """Result of :func:`load_runner_config`.

    Always exposes a ``runners`` dict + ``default_runner`` name, even
    when the YAML was single-runner (in which case ``runners`` has one
    entry named ``"default"``). The ``runner`` property is a convenience
    for single-runner consumers.
    """

    #: Named runners. Always at least one entry.
    runners: dict[str, Runner]
    #: Name of the default runner (key in ``runners``).
    default_runner: str
    #: ``{unique_id: {field: value}}`` per-model overrides.
    overrides: dict[str, dict[str, Any]]
    #: ``{tag: {field: value}}`` bulk-by-tag overrides for the
    #: default ``mode: single`` case (). Same field schema as
    #: :attr:`overrides`; every field on the selected runner's
    #: ``OVERRIDE_TYPE`` is accepted. Sits below per-node overrides in
    #: the precedence ladder. ``None`` when no ``tag.<name>:`` entries
    #: were declared. See :mod:`dbt_aws.common.builder` for the full
    #: ladder.
    tag_overrides: dict[str, dict[str, Any]] | None = None
    #: Optional tag-grouping spec. ``None`` means no grouping.
    task_groups: TaskGroupingConfig | None = None
    #: Optional ``{tag: TagGroupSpec}`` bulk-by-tag TASK collapse
    #: ( shape -- derived from ``overrides[tag.<t>]`` entries
    #: declaring ``mode: group``). ``None`` when no group-mode entries
    #: were declared. Each spec carries the target task name plus any
    #: group-level override fields (``command``, ``target``,
    #: ``profile_name``, etc.) declared on the same entry. The earlier
    #: top-level ``tag_groups:`` YAML key is a hard error at load time
    #: -- see :data:`_REMOVED_TOP_LEVEL_KEYS`.
    tag_group_specs: dict[str, TagGroupSpec] | None = None
    #: Optional ``{tag: name_prefix}`` map for ``mode: single``
    #: entries that supplied a ``name:``. Applied by the builder as a
    #: per-node task-id prefix (``<name>__<sanitised_unique_id>``) so
    #: siblings of the same tag sort together in the Airflow Graph view.
    #: ``None`` when no single-mode entry declared ``name:``.
    tag_single_name_prefixes: dict[str, str] | None = None
    #: Optional ``all:`` scope override (). Applies to every node
    #: in the rendered graph after ``select`` / ``exclude`` filtering,
    #: as the weakest override layer -- above runner defaults, below
    #: every other override source. Splits into three sub-fields
    #: mirroring the ``tag.<name>`` shape:
    #:
    #: * :attr:`all_override_fields` -- runner-facing fields (worker
    #:   sizing, command, dispatch, resource_tags, etc.) folded into
    #:   every node's effective overrides with ``setdefault`` so any
    #:   higher layer (tag / meta / per-node) wins per key.
    #: * :attr:`all_override_group_spec` -- populated when the entry
    #:   declared ``mode: group``. Collapses every eligible node into
    #:   ONE Airflow task; the group's name defaults to ``"all"``.
    #: * :attr:`all_override_name_prefix` -- populated when the entry
    #:   declared ``mode: single`` with a ``name:``. Applied by the
    #:   builder as a per-node task-id prefix.
    all_override_fields: dict[str, Any] | None = None
    all_override_group_spec: TagGroupSpec | None = None
    all_override_name_prefix: str | None = None

    @property
    def runner(self) -> Runner:
        """Convenience: the default runner."""
        return self.runners[self.default_runner]


def load_runner_config(
    path: str | Path,
    *,
    vars: Mapping[str, Any] | None = None,
) -> LoadedRunnerConfig:
    """Parse a YAML file and return a validated
    :class:`LoadedRunnerConfig`.

    Args:
        path: filesystem path to the YAML file.
        vars: optional ``{name: value}`` mapping. When set, every
            occurrence of ``${name}`` in the raw YAML text is replaced
            with ``str(vars[name])`` before the YAML parser runs.
            Undefined references raise :class:`RunnerConfigError`.
            ``$${...}`` is an escape hatch that renders as a literal
            ``${...}`` (useful for legitimate dollar-brace strings).
            Interpolation happens on the raw byte stream, so a
            variable can substitute part of any string -- bucket
            names, ARNs, prefixes, or fragments inside multi-line
            ``>-`` scalars.

            When ``None`` (default), no interpolation runs and
            behaviour is bit-for-bit identical to previous releases.

    Raises:
        RunnerConfigError: on any structural problem, including an
            undefined ``${var}`` reference.
    """
    p = Path(path)
    if not p.is_file():
        raise RunnerConfigError(f"runner config not found: {p}")

    raw = _read_yaml(p, vars=vars)
    if not isinstance(raw, dict):
        raise RunnerConfigError(f"{p}: top-level must be a mapping, got {type(raw).__name__}")

    # Reject the legacy tag-routing / tag-grouping top-level keys BEFORE
    # the generic ``unknown top-level key(s)`` check so the caller gets
    # an actionable migration hint instead of a bare "unknown key".
    legacy_present = sorted(k for k in raw if k in _REMOVED_TOP_LEVEL_KEYS)
    if legacy_present:
        example_lines = []
        for key in legacy_present:
            field = _REMOVED_TOP_LEVEL_KEYS[key]
            if key == "tag_groups":
                # tag_groups had two shapes -- illustrate the common one.
                example_lines.append(
                    f"    # {key}: {{tag_a: name_a}}   ->   "
                    f"overrides: {{tag.tag_a: {{mode: group, name: name_a}}}}"
                )
            else:
                example_lines.append(
                    f"    # {key}: {{tag_a: value_a}}   ->   "
                    f"overrides: {{tag.tag_a: {{{field}: value_a}}}}"
                )
        migration = "\n".join(example_lines)
        raise RunnerConfigError(
            f"{p}: top-level key(s) {legacy_present} were removed "
            f"(``tag_runners`` / ``tag_profiles`` / ``tag_targets``; "
            f"``tag_groups``). Migrate to 'overrides:' with a "
            f"'tag.<name>:' entry per tag. Examples:\n"
            f"{migration}\n"
            f"Use the modern ``runners:`` / ``overrides:`` schema "
            f"instead (see the runner-config-yaml reference)."
            f"full migration guides."
        )

    unknown = set(raw) - _ALLOWED_TOP_LEVEL_KEYS
    if unknown:
        raise RunnerConfigError(
            f"{p}: unknown top-level key(s) {sorted(unknown)}. "
            f"Valid keys: {sorted(_ALLOWED_TOP_LEVEL_KEYS)}"
        )

    # Exactly one of `runner` / `runners` must be present.
    has_single = "runner" in raw
    has_multi = "runners" in raw
    if has_single and has_multi:
        raise RunnerConfigError(f"{p}: use either 'runner:' OR 'runners:' (not both)")
    if not has_single and not has_multi:
        raise RunnerConfigError(f"{p}: must declare 'runner:' (single) or 'runners:' (multi)")

    if has_single:
        runners, default_name = _build_single(raw["runner"], path=p)
    else:
        runners, default_name = _build_multi(raw["runners"], raw.get("default_runner"), path=p)

    # Top-level ``openlineage:`` is applied to every runner that didn't
    # already receive its own ``openlineage:`` kwarg via the runner
    # section. Per-runner opts-out by setting ``openlineage: null``.
    _apply_top_level_openlineage(runners, raw.get("openlineage"), path=p)

    # Top-level ``resource_tags:`` cascades to every runner, per-key
    # merged with the runner's own ``resource_tags:`` (runner keys
    # win on conflict). Per-runner ``resource_tags: {}`` / ``null``
    # is a no-op (still inherits the top-level defaults).
    _apply_top_level_resource_tags(runners, raw.get("resource_tags"), path=p)

    _validate_no_create_job_name_clash(runners, path=p)

    (
        overrides,
        tag_overrides,
        tag_group_specs,
        tag_single_name_prefixes,
        all_override_fields,
        all_override_group_spec,
        all_override_name_prefix,
    ) = _build_overrides(
        raw.get("overrides"),
        runners=runners,
        default_name=default_name,
        path=p,
    )

    task_groups = _build_task_groups(raw.get("task_groups"), raw.get("ungrouped_group"), path=p)

    return LoadedRunnerConfig(
        runners=runners,
        default_runner=default_name,
        overrides=overrides,
        tag_overrides=tag_overrides or None,
        task_groups=task_groups,
        tag_group_specs=tag_group_specs or None,
        tag_single_name_prefixes=tag_single_name_prefixes or None,
        all_override_fields=all_override_fields,
        all_override_group_spec=all_override_group_spec,
        all_override_name_prefix=all_override_name_prefix,
    )


# ----------------------------------------------------------------------
# Internals
# ----------------------------------------------------------------------
def _read_yaml(path: Path, *, vars: Mapping[str, Any] | None = None) -> Any:
    """Read + parse a YAML file with ``safe_load``, optionally
    interpolating ``${name}`` references in STRING SCALARS after
    parsing.

     security fix (M4): interpolation now runs AFTER
    ``yaml.safe_load``, not on the raw text. Substituting into raw
    YAML let a variable value containing YAML special characters
    (``:``, ``{``, ``}``, ``|``, ``>``, ``"``, ``'``, ``#``, newline)
    break out of its intended string context and inject arbitrary
    keys or alter types. The post-parse walk confines substitution
    to string scalars, so a value like ``foo: bar`` remains a single
    string even if it looks like a YAML mapping.

    Backward compat: every existing ``${var}`` reference in every
    existing YAML still works. The only observable behaviour change
    is that a value which previously injected YAML structure now
    stays a string. That behaviour was never documented and was the
    injection vulnerability.
    """
    try:
        import yaml
    except ImportError as exc:  # pragma: no cover -- declared as dep
        raise RunnerConfigError("PyYAML is required to load runner config files.") from exc

    try:
        with path.open("r", encoding="utf-8") as fh:
            text = fh.read()
    except OSError as exc:  # pragma: no cover -- path.is_file() checked upstream
        raise RunnerConfigError(f"{path}: cannot read: {exc}") from exc

    try:
        parsed = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise RunnerConfigError(f"{path}: invalid YAML: {exc}") from exc

    if vars is not None:
        parsed = _interpolate_vars_tree(parsed, vars=vars, path=path)

    return parsed


def _interpolate_vars_tree(
    node: Any,
    *,
    vars: Mapping[str, Any],
    path: Path,
) -> Any:
    """Recursively walk a parsed YAML tree and substitute ``${name}``
    references in STRING SCALARS only.

    Non-string scalars (int / bool / float / None) are returned
    unchanged. Dicts and lists are copied so the input is never
    mutated. Undefined references raise :class:`RunnerConfigError`
    naming the missing key AND the provided key set.

    ``$${...}`` is a literal escape: it renders as ``${...}`` and
    the inner ``...`` is NOT looked up in ``vars``.

    Container-level substitution is INTENTIONALLY skipped -- a YAML
    key like ``${env}_bucket`` will NOT be substituted; substitution
    only touches values that YAML has already classified as strings.
    This is a deliberate scope reduction from earlier behaviour;
    it eliminates the injection surface. YAML keys are static config
    identifiers in every runner.yml we've seen, so the reduction has
    no known user impact.

    Emits ONE aggregated INFO log per file (not per string scalar) so
    the ``vars`` audit trail stays as compact as earlier.
    """
    resolved: set[str] = set()
    result = _interpolate_vars_tree_inner(node, vars=vars, path=path, resolved=resolved)
    if resolved:
        _LOG.info(
            "interpolated %d reference(s) in %s (keys: %s)",
            len(resolved),
            path,
            sorted(resolved),
        )
    return result


def _interpolate_vars_tree_inner(
    node: Any,
    *,
    vars: Mapping[str, Any],
    path: Path,
    resolved: set[str],
) -> Any:
    """Recursive worker for :func:`_interpolate_vars_tree`. Reuses one
    shared ``resolved`` set so the caller can emit a single aggregated
    log line for the whole file."""
    if isinstance(node, str):
        return _interpolate_vars(node, vars=vars, path=path, resolved=resolved)
    if isinstance(node, list):
        return [
            _interpolate_vars_tree_inner(item, vars=vars, path=path, resolved=resolved)
            for item in node
        ]
    if isinstance(node, dict):
        # reject ``${...}`` tokens in YAML keys. Keys are
        # never interpolated -- earlier they were silently passed
        # through, so a config like ``${env}_runner: ...`` would end
        # up with a literal key ``${env}_runner`` and fail validation
        # later with a confusing message. Fail early with a clear
        # error so users see the actual problem.
        for k in node:
            if isinstance(k, str) and _VAR_PATTERN.search(k):
                raise RunnerConfigError(
                    f"{path}: variable interpolation in YAML keys is "
                    f"not supported. Key {k!r} contains a ``${{...}}`` "
                    f"token; move the variable into the value or rename "
                    f"the key to a static identifier."
                )
        return {
            k: _interpolate_vars_tree_inner(v, vars=vars, path=path, resolved=resolved)
            for k, v in node.items()
        }
    return node


def _interpolate_vars(
    text: str,
    *,
    vars: Mapping[str, Any],
    path: Path,
    resolved: set[str] | None = None,
) -> str:
    """Replace ``${name}`` with ``str(vars[name])`` in ``text``.

    ``$${...}`` is a literal escape: it renders as ``${...}`` and the
    inner ``...`` is not looked up in ``vars``.

    Undefined references raise :class:`RunnerConfigError` naming the
    missing key AND the caller-provided key set.

    ``resolved`` is an optional shared set that the caller (typically
    :func:`_interpolate_vars_tree`) uses to aggregate the keys used
    across every string scalar in a YAML tree, so a single log line
    covers the whole file. When ``resolved`` is passed we do NOT
    emit a log here -- the caller emits after the walk completes.
    When ``resolved`` is ``None`` we behave stand-alone and emit our
    own log (kept for direct callers / tests).
    """
    caller_owns_log = resolved is not None
    if resolved is None:
        resolved = set()

    def _sub(match: re.Match[str]) -> str:
        escaped = match.group(1)
        if escaped is not None:
            # ``$${...}`` -> ``${...}`` (drop the leading dollar).
            return escaped[1:]
        name = match.group(2)
        if name not in vars:
            raise RunnerConfigError(
                f"{path}: references undefined variable {name!r}; provided vars are {sorted(vars)}"
            )
        resolved.add(name)
        return str(vars[name])

    interpolated = _VAR_PATTERN.sub(_sub, text)

    if resolved and not caller_owns_log:
        _LOG.info(
            "interpolated %d reference(s) in %s (keys: %s)",
            len(resolved),
            path,
            sorted(resolved),
        )
    return interpolated


def _build_single(section: Any, *, path: Path) -> tuple[dict[str, Runner], str]:
    """Single-runner case: wrap in a single-entry dict named 'default'."""
    if not isinstance(section, dict):
        raise RunnerConfigError(
            f"{path}: 'runner:' must be a mapping, got {type(section).__name__}"
        )
    runner = _construct_runner(section, name="<single>", path=path)
    return {"default": runner}, "default"


def _build_multi(section: Any, default_name: Any, *, path: Path) -> tuple[dict[str, Runner], str]:
    """Multi-runner case: parse each named runner + validate the default."""
    if not isinstance(section, dict):
        raise RunnerConfigError(
            f"{path}: 'runners:' must be a mapping, got {type(section).__name__}"
        )
    if not section:
        raise RunnerConfigError(f"{path}: 'runners:' is empty; declare at least one runner")
    if not isinstance(default_name, str):
        raise RunnerConfigError(
            f"{path}: 'default_runner:' must be set (a string referencing "
            f"a key in 'runners:'), got {default_name!r}"
        )
    if default_name not in section:
        raise RunnerConfigError(
            f"{path}: 'default_runner: {default_name!r}' not in 'runners:' (have {sorted(section)})"
        )

    runners: dict[str, Runner] = {}
    for name, runner_section in section.items():
        if not isinstance(runner_section, dict):
            raise RunnerConfigError(
                f"{path}: runners[{name!r}] must be a mapping, got {type(runner_section).__name__}"
            )
        runners[str(name)] = _construct_runner(runner_section, name=str(name), path=path)
    return runners, default_name


def _construct_runner(section: dict, *, name: str, path: Path) -> Runner:
    """Construct one Runner from a config section. ``name`` is used
    only for error messages.

    Rejects unknown YAML keys on the runner block with a clean
    :class:`RunnerConfigError` naming which key is invalid, rather
    than letting the key hit ``__init__`` and raise a Python-native
    ``TypeError``. For keys that live on the override side of the
    schema (``command``, ``full_refresh``, ``vars_json``, etc.) the
    error also hints at where they actually belong -- catches the
    common mistake of putting ``command: test`` on the runner block
    instead of under ``overrides.all:`` or ``overrides[tag.<t>]:``.
    """
    import inspect

    if "type" not in section:
        raise RunnerConfigError(
            f"{path}: runner {name!r} missing 'type:' (one of {sorted(_RUNNER_REGISTRY)})"
        )
    type_name = section["type"]
    if type_name not in _RUNNER_REGISTRY:
        raise RunnerConfigError(
            f"{path}: runner {name!r} unknown type {type_name!r}. "
            f"Available: {sorted(_RUNNER_REGISTRY)}"
        )
    runner_cls = _import_runner_class(type_name)
    kwargs = {k: v for k, v in section.items() if k != "type"}

    # Validate every YAML key against the runner class's __init__
    # signature BEFORE calling it. This catches typos + misplaced
    # override fields with a clean error instead of a Python
    # TypeError deep in the stack.
    sig = inspect.signature(runner_cls.__init__)
    valid = {p for p in sig.parameters if p != "self"}
    unknown = set(kwargs) - valid
    if unknown:
        override_fields = _override_field_names_for(runner_cls)
        override_mistakes = unknown & override_fields
        if override_mistakes:
            raise RunnerConfigError(
                f"{path}: runner {name!r}: key(s) {sorted(override_mistakes)} "
                f"are OVERRIDE fields, not runner constructor kwargs. "
                f"Move them under ``overrides.all:`` (for graph-wide), "
                f"``overrides[tag.<name>]:`` (bulk-by-tag), or "
                f"``overrides[<unique_id>]:`` (per-node). See the "
                f"precedence ladder in docs/reference/precedence.md."
            )
        raise RunnerConfigError(
            f"{path}: runner {name!r}: unknown key(s) {sorted(unknown)} "
            f"for {runner_cls.__name__}. Valid runner kwargs: "
            f"{sorted(valid)}."
        )

    # Track an explicit ``openlineage: null`` under a runner so the
    # top-level default can't later replace it. Presence of the key
    # (even with value ``None``) means the caller made a deliberate
    # per-runner decision.
    explicit_ol_optout = "openlineage" in kwargs and kwargs["openlineage"] is None
    # Convert an inline ``openlineage:`` mapping into an
    # ``OpenLineageConfig`` instance before handing it to the runner.
    if isinstance(kwargs.get("openlineage"), dict):
        kwargs["openlineage"] = _build_openlineage_config(
            kwargs["openlineage"],
            where=f"runner {name!r}",
            path=path,
        )
    try:
        runner = runner_cls(**kwargs)
    except TypeError as exc:
        raise RunnerConfigError(
            f"{path}: runner {name!r}: cannot construct {runner_cls.__name__}: {exc}"
        ) from exc
    except ValueError as exc:
        raise RunnerConfigError(f"{path}: runner {name!r}: invalid runner config: {exc}") from exc
    if explicit_ol_optout:
        # Sentinel attribute -- read by ``_apply_top_level_openlineage``
        # to skip this runner. Private (leading underscore) so it
        # doesn't collide with public runner attributes.
        object.__setattr__(runner, "_dbt_aws_ol_optout", True)
    return runner


def _build_openlineage_config(
    section: Any,
    *,
    where: str,
    path: Path,
):  # returns OpenLineageConfig; import lazy to avoid cycle at module import
    """Build an :class:`OpenLineageConfig` from a YAML mapping.

    Called from both :func:`_construct_runner` (per-runner
    ``openlineage:`` field) and :func:`_apply_top_level_openlineage`
    (top-level ``openlineage:`` block). Returns ``None`` when the
    caller explicitly passed ``null`` -- the runner then runs with
    lineage disabled.
    """
    if section is None:
        return None
    if not isinstance(section, dict):
        raise RunnerConfigError(
            f"{path}: {where}: 'openlineage' must be a mapping or null, "
            f"got {type(section).__name__}"
        )
    unknown = set(section) - _OL_ALLOWED_KEYS
    if unknown:
        raise RunnerConfigError(
            f"{path}: {where}: unknown openlineage key(s) {sorted(unknown)}. "
            f"Valid keys: {sorted(_OL_ALLOWED_KEYS)}"
        )
    from dbt_aws.common.lineage.config import OpenLineageConfig  # noqa: PLC0415

    try:
        return OpenLineageConfig(**section)
    except (TypeError, ValueError) as exc:
        raise RunnerConfigError(f"{path}: {where}: invalid openlineage config: {exc}") from exc


def _apply_top_level_openlineage(
    runners: dict[str, Runner],
    section: Any,
    *,
    path: Path,
) -> None:
    """When the YAML sets top-level ``openlineage:``, inject that
    config into every runner that:

    (a) accepts an ``openlineage`` attribute (all v0.4+ runners do), AND
    (b) has NOT already been constructed with a per-runner
        ``openlineage`` value.

    Per-runner ``openlineage: null`` opts that runner out entirely.
    Per-runner ``openlineage: {...}`` wins over the top-level default.
    """
    if section is None:
        return
    cfg = _build_openlineage_config(section, where="top-level", path=path)
    if cfg is None:
        return
    for name, runner in runners.items():
        if getattr(runner, "_dbt_aws_ol_optout", False):
            _LOG.debug(
                "openlineage: runner %r explicitly opted out; skipping top-level default",
                name,
            )
            continue
        existing = getattr(runner, "openlineage", None)
        if existing is not None:
            _LOG.debug(
                "openlineage: runner %r has its own config; top-level ignored for it",
                name,
            )
            continue
        if not hasattr(runner, "openlineage"):
            _LOG.warning(
                "openlineage: runner %r (%s) does not accept openlineage=; skipped",
                name,
                type(runner).__name__,
            )
            continue
        # Frozen dataclass instance -> ok. Runner attribute assignment
        # is a documented late-binding hook for this exact use case.
        try:
            object.__setattr__(runner, "openlineage", cfg)
        except Exception as exc:  # noqa: BLE001 -- defensive
            raise RunnerConfigError(
                f"{path}: cannot inject top-level openlineage into runner {name!r}: {exc}"
            ) from exc


def _apply_top_level_resource_tags(
    runners: dict[str, Runner],
    section: Any,
    *,
    path: Path,
) -> None:
    """When the YAML sets top-level ``resource_tags:``, merge the
    defaults into every runner that carries a ``resource_tags``
    attribute (all  concrete runners do).

    Precedence (per key, top wins):

    1. Runner-level ``resource_tags:`` on the runners: block
    2. Top-level ``resource_tags:``

    So the top-level entries are inherited by every runner but any
    runner can override individual keys (or add its own) by declaring
    its own ``resource_tags:`` block. Layers merge shallowly per-key
    -- there is no whole-dict replace; a runner that wants a
    completely different tag set simply declares every key.

    Validation delegates to :func:`validate_resource_tags`. The final
    merged dict is also validated so a runner-level override that
    e.g. exceeds AWS length limits still surfaces at DAG-parse time.

    Note: the EMR-cluster-step ``job_flow_overrides['Tags']``
    sync fell out with the EMR runner removal. Only Glue-based
    runners are left; they read ``self.resource_tags`` at task-build
    time so no post-merge refresh is needed.
    """
    if section is None:
        return
    validate_resource_tags(section, where=f"{path}: resource_tags")
    if not isinstance(section, dict):
        # validate_resource_tags handles the None / dict check but keep
        # this here for mypy's narrowing.
        return
    for name, runner in runners.items():
        # Runners that don't carry a ``resource_tags`` attribute are
        # not eligible for tagging. Log at DEBUG level -- this is only
        # a concern for third-party runners.
        if not hasattr(runner, "resource_tags"):
            _LOG.debug(
                "resource_tags: runner %r (%s) does not accept resource_tags; skipped",
                name,
                type(runner).__name__,
            )
            continue
        runner_tags = getattr(runner, "resource_tags", None) or {}
        merged: dict[str, str] = {**section, **runner_tags}
        # Runner keys win per-key; validate the merged dict again in
        # case somehow the merge produced a bad shape.
        validate_resource_tags(merged, where=f"{path}: resource_tags (merged for runner {name!r})")
        try:
            object.__setattr__(runner, "resource_tags", merged or None)
        except Exception as exc:  # noqa: BLE001 -- defensive
            raise RunnerConfigError(
                f"{path}: cannot inject top-level resource_tags into runner {name!r}: {exc}"
            ) from exc


def _build_overrides(
    section: Any,
    *,
    runners: dict[str, Runner],
    default_name: str,
    path: Path,
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, TagGroupSpec],
    dict[str, str],
    dict[str, Any] | None,
    TagGroupSpec | None,
    str | None,
]:
    """Validate the ``overrides:`` section and split entries by prefix.

    THREE entry shapes are recognised, all accepting the SAME per-runner
    ``OVERRIDE_TYPE`` field schema:

    * ``all:`` () -- broadest scope. Applies to every node in
      the rendered graph (after ``select`` / ``exclude`` filtering).
      Sits below every other override layer in the precedence ladder
      -- above runner defaults, below ``tag.<name>``, ``meta.stratus``,
      per-node. See :mod:`dbt_aws.common.builder` for the canonical
      ladder.
    * ``tag.<name>:`` -- bulk-by-tag override. Every dbt node carrying
      the tag inherits the fields declared here.
    * ``model.<pkg>.<name>:`` -- per-node override, keyed by the dbt
      ``unique_id`` (``model.pkg.name``, ``seed.pkg.name``,
      ``snapshot.pkg.name`` etc.). Per-node ALWAYS wins per-field.

    The per-node key MUST start with a dbt resource-type
    prefix (``model.``, ``seed.``, ``snapshot.``, ``test.``,
    ``analysis.``). Bare non-prefixed keys are rejected -- the
    ``all:`` and ``tag.<name>:`` shapes are the only bulk selectors
    accepted.

    ``tag.<name>:`` AND ``all:`` entries accept two
    meta-keys that are NOT forwarded to the runner's ``OVERRIDE_TYPE``:

    * ``mode: group | single`` (default: ``single``). ``group``
      collapses every eligible node into ONE Airflow task per
      ``(name, runner)`` bucket (see
      :mod:`dbt_aws.common.graph.collapse`). ``single`` keeps the
      per-node task fan-out and is the default.
    * ``name: <str>``. Under ``mode: group`` this is the collapsed
      task-id (default: the tag name, or ``"all"`` for ``all:``).
      Under ``mode: single`` it becomes a per-node task-id prefix
      (``<name>__<sanitised_unique_id>``).

    Each override may have a ``runner:`` field that selects a named
    runner; remaining non-meta fields must be valid for that runner's
    ``OVERRIDE_TYPE``.

    Returns:
        A 7-tuple ``(per_node_overrides, tag_overrides_single,
        tag_group_specs, tag_single_name_prefixes, all_override_fields,
        all_override_group_spec, all_override_name_prefix)``:

        * ``per_node_overrides`` -- keyed by ``unique_id``.
        * ``tag_overrides_single`` -- keyed by tag; entries where
          ``mode`` was ``single`` (or omitted).
        * ``tag_group_specs`` -- keyed by tag; :class:`TagGroupSpec`
          per ``mode: group`` tag entry.
        * ``tag_single_name_prefixes`` -- keyed by tag; the ``name:``
          from ``mode: single`` tag entries.
        * ``all_override_fields`` -- the runner-facing fields from an
          ``all:`` entry (), always ``None`` when no ``all:``
          entry was declared. Same field schema as the tag / per-node
          entries. Consumed by the builder as the weakest override
          layer.
        * ``all_override_group_spec`` -- populated when the ``all:``
          entry declared ``mode: group``. Feeds directly into
          :func:`dbt_aws.common.graph.collapse.collapse_graph` under
          a synthetic tag key.
        * ``all_override_name_prefix`` -- populated when the ``all:``
          entry declared ``mode: single`` with a ``name:``. Applied
          by the builder as a per-node task-id prefix on every node.

        The first four (``per_node`` / ``tag`` / ``tag_group`` /
        ``tag_prefix``) are always empty dicts (not ``None``) when
        their entries are absent; the three ``all_*`` slots are
        ``None`` when no ``all:`` entry was declared.

    Raises:
        RunnerConfigError: unknown field on any entry, unknown
            selected ``runner:``, malformed tag key, a comma-
            separated ``tag.<a>,tag.<b>`` key (not supported -- use
            two entries), a per-node key without a resource-type
            prefix, an invalid ``mode:`` value, or an invalid
            ``name:`` string.
    """
    per_node: dict[str, dict[str, Any]] = {}
    per_tag_single: dict[str, dict[str, Any]] = {}
    group_specs: dict[str, TagGroupSpec] = {}
    single_name_prefixes: dict[str, str] = {}
    all_fields: dict[str, Any] | None = None
    all_group_spec: TagGroupSpec | None = None
    all_name_prefix: str | None = None
    if section is None:
        return (
            per_node,
            per_tag_single,
            group_specs,
            single_name_prefixes,
            all_fields,
            all_group_spec,
            all_name_prefix,
        )
    if not isinstance(section, dict):
        raise RunnerConfigError(
            f"{path}: 'overrides:' must be a mapping, got {type(section).__name__}"
        )

    for raw_key, fields_for_entry in section.items():
        if not isinstance(raw_key, str) or not raw_key.strip():
            raise RunnerConfigError(
                f"{path}: overrides keys must be non-empty strings, got {raw_key!r}"
            )
        key = raw_key.strip()
        if not isinstance(fields_for_entry, dict):
            raise RunnerConfigError(
                f"{path}: overrides[{key!r}] must be a mapping, "
                f"got {type(fields_for_entry).__name__}"
            )

        is_all_entry = key == "all"
        is_tag_entry = key.startswith("tag.")
        tag: str | None = None
        if is_all_entry:
            if all_fields is not None:
                raise RunnerConfigError(f"{path}: overrides['all']: duplicate ``all:`` entry")
        elif is_tag_entry:
            tag = key[len("tag.") :].strip()
            if not tag:
                raise RunnerConfigError(
                    f"{path}: overrides[{key!r}]: empty tag name after 'tag.' prefix"
                )
            if "," in tag:
                raise RunnerConfigError(
                    f"{path}: overrides[{key!r}]: comma-separated tag keys are "
                    f"not supported -- declare one entry per tag."
                )
            if tag in per_tag_single or tag in group_specs:
                raise RunnerConfigError(f"{path}: overrides[{key!r}]: duplicate tag entry")
        else:
            # Per-node entry -- must be a proper dbt ``unique_id``.
            # The resource-type prefix is mandatory to
            # rule out ambiguity with legacy shorthand shapes.
            resource_type = key.split(".", 1)[0] if "." in key else ""
            if resource_type not in _VALID_NODE_PREFIXES:
                raise RunnerConfigError(
                    f"{path}: overrides[{key!r}]: per-node keys must start with a "
                    f"dbt resource-type prefix ({sorted(_VALID_NODE_PREFIXES)}) "
                    f"followed by ``.<package>.<name>``, or use ``tag.<name>:`` "
                    f"for bulk-by-tag. Example: ``model.my_project.orders``."
                )
            if key in per_node:
                raise RunnerConfigError(f"{path}: overrides[{key!r}]: duplicate per-node entry")

        # Peel off meta-keys (``mode``, ``name``) BEFORE OVERRIDE_TYPE
        # validation -- they are dbt-aws routing metadata, not runner
        # fields. Meta-keys are valid on ``all:`` and ``tag.<name>:``
        # entries; per-node entries reject them.
        entry_mode: str = "single"
        entry_name: str | None = None
        meta_present = set(fields_for_entry) & _TAG_ENTRY_META_KEYS
        if meta_present and not (is_tag_entry or is_all_entry):
            raise RunnerConfigError(
                f"{path}: overrides[{key!r}]: meta-keys {sorted(meta_present)} "
                f"are only valid on ``all:`` or ``tag.<name>:`` entries. "
                f"Per-node overrides do not accept ``mode:`` or ``name:``."
            )
        if is_tag_entry or is_all_entry:
            if "mode" in fields_for_entry:
                raw_mode = fields_for_entry["mode"]
                if not isinstance(raw_mode, str) or raw_mode not in _TAG_MODE_VALUES:
                    raise RunnerConfigError(
                        f"{path}: overrides[{key!r}]: mode must be one of "
                        f"{sorted(_TAG_MODE_VALUES)}, got {raw_mode!r}"
                    )
                entry_mode = raw_mode
            if "name" in fields_for_entry:
                raw_name = fields_for_entry["name"]
                if (
                    not isinstance(raw_name, str)
                    or not raw_name.strip()
                    or not _TAG_NAME_PATTERN.match(raw_name.strip())
                ):
                    raise RunnerConfigError(
                        f"{path}: overrides[{key!r}]: name must be a non-empty "
                        f"string matching ``[A-Za-z][A-Za-z0-9_]*`` (Airflow "
                        f"task-id legal), got {raw_name!r}"
                    )
                if len(raw_name.strip()) > 100:
                    raise RunnerConfigError(
                        f"{path}: overrides[{key!r}]: name must be <= 100 "
                        f"characters, got {len(raw_name.strip())}"
                    )
                entry_name = raw_name.strip()

        # Validate ``runner:`` dispatch (if any) then field schema on
        # the REMAINING (non-meta) fields.
        selected_name = fields_for_entry.get("runner", default_name)
        if selected_name not in runners:
            raise RunnerConfigError(
                f"{path}: overrides[{key!r}]: runner "
                f"{selected_name!r} not in 'runners:' "
                f"(have {sorted(runners)})"
            )
        override_class = _override_class_for(runners[selected_name])
        valid_keys = {f.name for f in fields(override_class)}
        remaining = {
            k: v
            for k, v in fields_for_entry.items()
            if k != "runner" and k not in _TAG_ENTRY_META_KEYS
        }
        unknown = set(remaining) - valid_keys
        if unknown:
            raise RunnerConfigError(
                f"{path}: overrides[{key!r}]: unknown key(s) "
                f"{sorted(unknown)}. Valid keys for "
                f"{override_class.__name__}: {sorted(valid_keys)}"
            )

        # Build the runner-facing entry (``runner:`` + validated
        # non-meta fields). This is what higher layers merge into the
        # per-node effective-override bucket.
        runner_entry = {k: v for k, v in fields_for_entry.items() if k not in _TAG_ENTRY_META_KEYS}

        if is_all_entry:
            all_fields = runner_entry
            if entry_mode == "group":
                group_name = entry_name if entry_name is not None else "all"
                all_group_spec = TagGroupSpec(
                    name=group_name,
                    overrides=runner_entry,
                )
            else:
                if entry_name is not None:
                    all_name_prefix = entry_name
        elif is_tag_entry:
            assert tag is not None  # for mypy -- set in the tag branch above
            if entry_mode == "group":
                group_name = entry_name if entry_name is not None else tag
                group_specs[tag] = TagGroupSpec(
                    name=group_name,
                    overrides=runner_entry,
                )
            else:
                per_tag_single[tag] = runner_entry
                if entry_name is not None:
                    single_name_prefixes[tag] = entry_name
        else:
            per_node[key] = runner_entry
    return (
        per_node,
        per_tag_single,
        group_specs,
        single_name_prefixes,
        all_fields,
        all_group_spec,
        all_name_prefix,
    )


#: dbt resource-type prefixes accepted as the leading segment of a
#: per-node ``overrides:`` key. Matches
#: :data:`dbt_aws.common.graph.node.RUNNABLE_RESOURCE_TYPES` plus
#: ``analysis`` for completeness -- ``source``, ``operation``,
#: ``macro``, ``exposure``, ``metric`` are metadata-only and never
#: become tasks, so overrides against them are rejected.
_VALID_NODE_PREFIXES: frozenset[str] = frozenset({"model", "seed", "snapshot", "test", "analysis"})


def _build_task_groups(
    raw_groups: Any,
    raw_ungrouped: Any,
    *,
    path: Path,
) -> TaskGroupingConfig | None:
    """Parse + validate the ``task_groups:`` / ``ungrouped_group:``
    blocks. Returns ``None`` if neither is set."""
    if raw_groups is None and raw_ungrouped is None:
        return None
    if raw_groups is None:
        raise RunnerConfigError(f"{path}: 'ungrouped_group' requires 'task_groups' to be set")
    if not isinstance(raw_groups, list):
        raise RunnerConfigError(
            f"{path}: 'task_groups' must be a list, got {type(raw_groups).__name__}"
        )
    if not raw_groups:
        return None

    groups: list[TaskGroupConfig] = []
    seen_names: set[str] = set()
    tag_to_group: dict[str, str] = {}

    for i, entry in enumerate(raw_groups):
        if not isinstance(entry, dict):
            raise RunnerConfigError(
                f"{path}: task_groups[{i}] must be a mapping, got {type(entry).__name__}"
            )
        name = entry.get("name")
        tags = entry.get("tags")
        if not isinstance(name, str) or not name:
            raise RunnerConfigError(f"{path}: task_groups[{i}].name must be a non-empty string")
        if name in seen_names:
            raise RunnerConfigError(f"{path}: task_groups[{i}].name {name!r} is duplicated")
        seen_names.add(name)
        if not isinstance(tags, list) or not tags:
            raise RunnerConfigError(f"{path}: task_groups[{i}].tags must be a non-empty list")
        normalised: set[str] = set()
        for tag in tags:
            if not isinstance(tag, str) or not tag:
                raise RunnerConfigError(
                    f"{path}: task_groups[{i}].tags entries must be non-empty strings, got {tag!r}"
                )
            if tag in tag_to_group:
                raise RunnerConfigError(
                    f"{path}: tag {tag!r} appears in both groups "
                    f"{tag_to_group[tag]!r} and {name!r}; tag-to-group "
                    f"must be unique"
                )
            tag_to_group[tag] = name
            normalised.add(tag)
        groups.append(TaskGroupConfig(name=name, tags=frozenset(normalised)))

    ungrouped_name: str | None = None
    if raw_ungrouped is not None:
        if not isinstance(raw_ungrouped, str) or not raw_ungrouped:
            raise RunnerConfigError(f"{path}: 'ungrouped_group' must be a non-empty string")
        ungrouped_name = raw_ungrouped

    return TaskGroupingConfig(groups=tuple(groups), ungrouped_group=ungrouped_name)


def _validate_no_create_job_name_clash(runners: dict[str, Runner], *, path: Path) -> None:
    """Two runners with mode=create and the same job_name would race on
    glue:CreateJob. Catch at load time."""
    seen: dict[str, str] = {}  # job_name -> runner name
    for name, runner in runners.items():
        # Only relevant for runners that have these attrs and are in create mode.
        mode = getattr(runner, "mode", None)
        job_name = getattr(runner, "job_name", None)
        if mode != "create" or not job_name:
            continue
        if job_name in seen:
            raise RunnerConfigError(
                f"{path}: runners {seen[job_name]!r} and {name!r} both "
                f"declare mode='create' with the same job_name="
                f"{job_name!r}; this would race on glue:CreateJob. "
                f"Use distinct job_names or set mode='attach' on one."
            )
        seen[job_name] = name


def _import_runner_class(type_name: str) -> type[Runner]:
    """Resolve a ``type:`` string to its concrete :class:`Runner`
    subclass via lazy import."""
    spec = _RUNNER_REGISTRY[type_name]
    mod_path, _, cls_name = spec.partition(":")
    try:
        module = importlib.import_module(mod_path)
    except ImportError as exc:
        raise RunnerConfigError(
            f"runner type {type_name!r} maps to {spec!r} which could "
            f"not be imported: {exc}. Is the corresponding package "
            f"installed?"
        ) from exc
    return getattr(module, cls_name)


def _override_class_for(runner: Runner) -> type[RunnerOverride]:
    """Find the ``OVERRIDE_TYPE`` declared on the runner class."""
    override_class = getattr(type(runner), "OVERRIDE_TYPE", None)
    if override_class is None or not isinstance(override_class, type):
        raise RunnerConfigError(
            f"runner {type(runner).__name__} does not declare an "
            f"OVERRIDE_TYPE; remove the 'overrides:' section."
        )
    return override_class


def _override_field_names_for(runner_cls: type[Runner]) -> frozenset[str]:
    """Return the set of field names declared on the runner class's
    ``OVERRIDE_TYPE`` dataclass (empty when the class has no
    ``OVERRIDE_TYPE`` attribute).

    Used by :func:`_construct_runner` to detect the common mistake
    of putting override-only fields (``command``, ``full_refresh``,
    ``vars_json``, ``target``, ``profile_name``, ...) on the runner
    block, where they'd otherwise be silently forwarded to
    ``__init__`` and raise a Python ``TypeError``.
    """
    override_class = getattr(runner_cls, "OVERRIDE_TYPE", None)
    if override_class is None or not isinstance(override_class, type):
        return frozenset()
    return frozenset(f.name for f in fields(override_class))
