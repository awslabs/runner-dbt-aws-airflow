"""Per-model override resolution.

Each concrete :class:`Runner` declares its own :class:`RunnerOverride`
subclass listing the fields it lets callers tweak per-node. The merge
between three layers happens in :func:`resolve_override`:

* **Layer A** \u2014 ``node.meta['stratus']`` (the per-model knob; lives
  next to the SQL inside the dbt project).
* **Layer B** \u2014 the ``overrides={unique_id: {...}}`` dict passed to
  :class:`DbtDag` (an escape hatch for cases where editing the dbt
  project isn't an option).

Layer B beats Layer A. Unknown keys in either layer raise
:class:`OverrideError` so typos surface at DAG-parse time.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Any

from dbt_aws.common.graph.node import DbtNode


@dataclass(frozen=True)
class RunnerOverride:
    """Marker base for per-model override dataclasses. Concrete runners
    subclass this and add their own fields (all optional, ``None`` =
    "use the runner default")."""


#: Override keys consumed by the DAG builder (multi-runner dispatch)
#: rather than the runner. Always stripped before per-runner field
#: validation so they don't trip ``OverrideError``.
_DISPATCH_ONLY_KEYS = frozenset({"runner"})

#: Fields whose values are merged (shallow, later-wins per key)
#: instead of replaced when the same field appears in multiple
#: layers. Currently limited to ``resource_tags`` -- the runner-level
#: default is combined with per-node / per-tag overrides so callers
#: can add tags per model / tag without redeclaring the runner-wide
#: baseline.
_DICT_MERGE_FIELDS = frozenset({"resource_tags", "spark_conf"})


class OverrideError(ValueError):
    """Raised when an override dict has keys the runner doesn't accept."""


def resolve_override(
    *,
    node: DbtNode,
    override_class: type[RunnerOverride],
    explicit_overrides: dict[str, dict[str, Any]] | None = None,
) -> RunnerOverride:
    """Merge per-model overrides for ``node`` into an instance of
    ``override_class``.

    Resolution order (last wins):
    1. Layer A \u2014 ``node.meta['stratus']``.
    2. Layer B \u2014 ``explicit_overrides[node.unique_id]``.

    Args:
        node: the dbt node being resolved.
        override_class: the runner's ``OVERRIDE_TYPE`` (a subclass of
            :class:`RunnerOverride`).
        explicit_overrides: optional ``{unique_id: {field: value}}`` map
            from :class:`DbtDag`.

    Raises:
        OverrideError: if any layer contains a key that isn't a field
            of ``override_class``.
    """
    valid_keys = {f.name for f in fields(override_class)}

    layer_a_raw = (node.meta or {}).get("stratus", {})
    if not isinstance(layer_a_raw, dict):
        raise OverrideError(
            f"node {node.unique_id!r}: meta.stratus must be a dict, "
            f"got {type(layer_a_raw).__name__}"
        )

    layer_b_raw = (explicit_overrides or {}).get(node.unique_id, {})
    if not isinstance(layer_b_raw, dict):
        raise OverrideError(
            f"overrides[{node.unique_id!r}] must be a dict, got {type(layer_b_raw).__name__}"
        )

    # Strip dispatch-only keys (e.g. 'runner') consumed by the builder
    # rather than the runner.
    layer_a_raw = {k: v for k, v in layer_a_raw.items() if k not in _DISPATCH_ONLY_KEYS}
    layer_b_raw = {k: v for k, v in layer_b_raw.items() if k not in _DISPATCH_ONLY_KEYS}

    _reject_unknown(
        layer_a_raw,
        valid_keys,
        where=f"node {node.unique_id!r} meta.stratus",
        runner_class=override_class,
    )
    _reject_unknown(
        layer_b_raw,
        valid_keys,
        where=f"overrides[{node.unique_id!r}]",
        runner_class=override_class,
    )

    # Layer B replaces Layer A per key, EXCEPT for dict-merge fields
    # (currently ``resource_tags``) where we shallow-merge both layers
    # so per-model tags augment the runner-wide baseline instead of
    # nuking it.
    merged: dict[str, Any] = {**layer_a_raw}
    for k, v in layer_b_raw.items():
        if k in _DICT_MERGE_FIELDS and isinstance(merged.get(k), dict) and isinstance(v, dict):
            merged[k] = {**merged[k], **v}
        else:
            merged[k] = v
    return override_class(**merged)


def effective(
    runner: Any,
    override: RunnerOverride,
    field: str,
    default: Any = None,
) -> Any:
    """Return the per-model-effective value for ``field``.

    Precedence:
        1. ``override.<field>`` if set and not ``None``
        2. ``runner.<field>`` if the runner has that attribute
        3. ``default``

    Used by runner ``make_task`` implementations to keep override
    resolution declarative: one call per field instead of inline
    ``override.x if override.x is not None else self.x`` everywhere.
    """
    ov_val = getattr(override, field, None)
    if ov_val is not None:
        return ov_val
    return getattr(runner, field, default)


def _reject_unknown(
    layer: dict[str, Any],
    valid_keys: set[str],
    *,
    where: str,
    runner_class: type[RunnerOverride],
) -> None:
    unknown = set(layer) - valid_keys
    if unknown:
        raise OverrideError(
            f"{where}: unknown key(s) {sorted(unknown)}. Valid keys "
            f"for {runner_class.__name__}: {sorted(valid_keys) or '(none)'}"
        )
