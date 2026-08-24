"""dbt-compatible selector grammar for filtering a :class:`DbtGraph`.

Supported subset:

* ``name`` — match by short name (``stg_orders``) or unique_id
  (``model.proj.stg_orders``).
* ``tag:<value>`` — match by ``config.tags`` membership.
* ``path:<glob>`` — match by ``original_file_path`` against a
  :mod:`fnmatch` glob (e.g. ``path:models/bronze/*.sql``). Note that
  fnmatch's ``*`` matches ``/`` as well — for directory-scoped
  matching, use a prefix without glob metacharacters
  (``path:models/bronze``), which matches every node whose path
  starts with ``models/bronze/`` OR equals ``models/bronze``.
* ``resource_type:<type>`` — one of ``model``, ``snapshot``,
  ``seed``, ``test``, ``analysis``, ``source``, ``operation``.
* ``package:<name>`` — match by ``package_name``.
* Graph walks (compose with any of the above bodies):

  * ``+expr`` — body + all transitive upstream ancestors.
  * ``expr+`` — body + all transitive downstream descendants.
  * ``+expr+`` — body + ancestors + descendants.
  * ``N+expr`` — body + ancestors up to N hops (``2+expr``).
  * ``expr+N`` — body + descendants up to N hops (``expr+3``).
  * Combined: ``2+expr+3``.

NOT supported (deferred): ``source:``, ``config:``, ``state:``,
set operators (``,`` intersection), ``@`` macro selectors,
``selectors.yml``.

Resolution rules — match what ``dbt --select`` does:

* ``select`` is the UNION across all expressions. An empty list means
  "all runnable nodes". ``None`` also means "all".
* ``exclude`` is the UNION across all expressions, SUBTRACTED from the
  select result.
"""

from __future__ import annotations

import fnmatch
import logging
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

from dbt_aws.common.graph.graph import DbtGraph
from dbt_aws.common.graph.node import DbtNode

_log = logging.getLogger(__name__)


class SelectorError(ValueError):
    """Raised for malformed selector expressions."""


# Body type — what the non-``+`` part of the expression matches against.
_BodyType = Literal["name", "tag", "path", "resource_type", "package"]

#: Body-type prefixes recognised in ``<prefix>:<value>`` expressions.
_BODY_PREFIXES: dict[str, _BodyType] = {
    "tag": "tag",
    "path": "path",
    "resource_type": "resource_type",
    "package": "package",
}


@dataclass(frozen=True)
class _Selector:
    """One parsed selector expression."""

    body: str
    body_type: _BodyType
    upstream_depth: int | None  # None = infinite, 0 = none, N>0 = max hops
    downstream_depth: int | None  # same convention

    @property
    def has_graph_walk(self) -> bool:
        return self.upstream_depth != 0 or self.downstream_depth != 0


_PARSE_RE = re.compile(
    r"""
    ^
    (?:(?P<up>\d*)\+)?    # optional upstream marker: '+', '2+', etc.
    (?P<body>[^+]+)        # the body: name or 'tag:value'
    (?:\+(?P<down>\d*))?  # optional downstream marker: '+', '+3', etc.
    $
    """,
    re.VERBOSE,
)


def parse_selector(expression: str) -> _Selector:
    """Parse one selector string. Raises :class:`SelectorError` on a
    malformed expression."""
    s = expression.strip()
    if not s:
        raise SelectorError("empty selector expression")
    match = _PARSE_RE.match(s)
    if not match:
        raise SelectorError(
            f"could not parse selector {expression!r}; expected "
            f"[N+]<name|tag:v|path:v|resource_type:v|package:v>[+N]"
        )
    up_raw = match.group("up")
    down_raw = match.group("down")
    body = match.group("body").strip()
    if not body:
        raise SelectorError(f"selector {expression!r} has an empty body")

    upstream_depth = _parse_depth(up_raw, marker_present=up_raw is not None)
    downstream_depth = _parse_depth(down_raw, marker_present=down_raw is not None)

    body_type: _BodyType = "name"
    for prefix, kind in _BODY_PREFIXES.items():
        marker = f"{prefix}:"
        if body.startswith(marker):
            body_type = kind
            value = body[len(marker) :].strip()
            if not value:
                raise SelectorError(f"selector {expression!r} has an empty {prefix} value")
            body = value
            break

    return _Selector(
        body=body,
        body_type=body_type,
        upstream_depth=upstream_depth,
        downstream_depth=downstream_depth,
    )


def _parse_depth(raw: str | None, *, marker_present: bool) -> int | None:
    """Decode the depth portion of a selector.

    * marker absent (``raw is None``)               → 0 (no walk on that side)
    * marker present with empty digits (``+`` only) → None (infinite)
    * marker present with digits (``2+``)           → that integer
    """
    if not marker_present:
        return 0
    if raw is None or raw == "":
        return None
    try:
        n = int(raw)
    except ValueError as exc:  # pragma: no cover — regex constrains this
        raise SelectorError(f"bad depth digits: {raw!r}") from exc
    if n < 0:
        raise SelectorError(f"depth must be non-negative, got {n}")
    return n


# ----------------------------------------------------------------------
# Matching
# ----------------------------------------------------------------------
def _match_root_set(graph: DbtGraph, sel: _Selector) -> set[str]:
    """Return the unique_ids that the BODY matches (before any graph
    walk)."""
    if sel.body_type == "tag":
        return {n.unique_id for n in graph if sel.body in (n.tags or [])}
    if sel.body_type == "resource_type":
        return {n.unique_id for n in graph if n.resource_type == sel.body}
    if sel.body_type == "package":
        return {n.unique_id for n in graph if n.package_name == sel.body}
    if sel.body_type == "path":
        return _match_path(graph, sel.body)
    # name | unique_id
    matched: set[str] = set()
    for n in graph:
        if n.unique_id == sel.body or n.name == sel.body:
            matched.add(n.unique_id)
    return matched


def _match_path(graph: DbtGraph, pattern: str) -> set[str]:
    """Match nodes by ``original_file_path``.

    Two forms:

    * A pattern containing any of ``*``, ``?``, ``[`` is a
      :mod:`fnmatch` glob applied to the node's
      ``original_file_path`` verbatim.
    * A pattern with no glob metacharacters is treated as a
      DIRECTORY prefix: ``models/bronze`` matches every node whose
      ``original_file_path`` starts with ``models/bronze/`` OR
      equals ``models/bronze`` exactly. Trailing slashes are
      tolerated.

    Nodes without ``original_file_path`` (rare — sources, older
    manifest schemas) never match.
    """
    is_glob = any(ch in pattern for ch in "*?[")
    prefix = pattern.rstrip("/")
    prefix_with_sep = prefix + "/"
    matched: set[str] = set()
    for n in graph:
        p = n.original_file_path
        if not p:
            continue
        if is_glob:
            if fnmatch.fnmatch(p, pattern):
                matched.add(n.unique_id)
        else:
            if p == prefix or p.startswith(prefix_with_sep):
                matched.add(n.unique_id)
    return matched


def _walk(
    start: set[str],
    *,
    step: Callable[[str], frozenset[str]],
    max_depth: int | None,
) -> set[str]:
    """BFS from ``start``, expanding via ``step`` up to ``max_depth``
    hops. ``max_depth=None`` means unlimited; ``max_depth=0`` returns
    just ``start``."""
    if max_depth == 0:
        return set(start)
    visited: set[str] = set(start)
    frontier: set[str] = set(start)
    depth = 0
    while frontier:
        if max_depth is not None and depth >= max_depth:
            break
        depth += 1
        next_frontier: set[str] = set()
        for uid in frontier:
            for neighbour in step(uid):
                if neighbour not in visited:
                    visited.add(neighbour)
                    next_frontier.add(neighbour)
        frontier = next_frontier
    return visited


def _expand(graph: DbtGraph, sel: _Selector) -> set[str]:
    """Apply one selector: match the body, then walk up/down per the
    selector's depth fields."""
    root = _match_root_set(graph, sel)
    if not root:
        return set()
    result: set[str] = set(root)
    if sel.upstream_depth != 0:
        result |= _walk(root, step=graph.upstream, max_depth=sel.upstream_depth)
    if sel.downstream_depth != 0:
        result |= _walk(root, step=graph.downstream, max_depth=sel.downstream_depth)
    return result


# ----------------------------------------------------------------------
# Public API
# ----------------------------------------------------------------------
def apply_selectors(
    graph: DbtGraph,
    *,
    select: list[str] | None = None,
    exclude: list[str] | None = None,
) -> DbtGraph:
    """Return a new :class:`DbtGraph` containing only the nodes that
    match the ``select`` rules and don't match any ``exclude`` rules.

    Args:
        graph: the source graph.
        select: list of selector expressions. UNIONed. ``None`` or
            empty list means "all nodes".
        exclude: list of selector expressions. UNIONed and SUBTRACTED
            from the select result.

    Raises:
        SelectorError: on any malformed expression.
    """
    initial_count = len(graph)

    if select is None or len(select) == 0:
        selected: set[str] = {n.unique_id for n in graph}
        _log.info(
            "selectors: no select rules — keeping all %d nodes",
            initial_count,
        )
    else:
        selected = set()
        for expr in select:
            matched = _expand(graph, parse_selector(expr))
            selected |= matched
            _log.info("selectors: select %r matched %d node(s)", expr, len(matched))
        _log.info(
            "selectors: after select union — %d of %d nodes kept",
            len(selected),
            initial_count,
        )

    if exclude:
        excluded: set[str] = set()
        for expr in exclude:
            matched = _expand(graph, parse_selector(expr))
            excluded |= matched
            _log.info("selectors: exclude %r matched %d node(s)", expr, len(matched))
        before = len(selected)
        selected -= excluded
        _log.info(
            "selectors: exclude removed %d node(s) (%d → %d)",
            before - len(selected),
            before,
            len(selected),
        )

    if not selected:
        _log.warning(
            "selectors: result is empty (started with %d nodes); the DAG will have no tasks",
            initial_count,
        )
        return DbtGraph.from_nodes([])

    nodes: list[DbtNode] = [graph[uid] for uid in selected]
    return DbtGraph.from_nodes(nodes)
