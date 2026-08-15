"""In-memory dbt graph + walk helpers.

``DbtGraph`` is an immutable view of the manifest restricted to runnable
nodes. It exposes the operations the rest of the lib actually needs:

* lookup by ``unique_id``
* one-hop upstream / downstream
* transitive upstream / downstream (graph walk)
* iteration in topological order (for the DAG builder)

No I/O happens here — graphs are built by loaders (see ``loader.py``).
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dbt_aws.common.graph.node import DbtNode


class DbtGraphError(ValueError):
    """Raised on structural problems in the graph (cycles, dangling deps)."""


@dataclass(frozen=True)
class DbtGraph:
    """Immutable graph of dbt nodes.

    Construct via :func:`DbtGraph.from_nodes` rather than calling
    ``__init__`` directly — that builder filters dangling edges and
    pre-computes the reverse adjacency.
    """

    #: All nodes keyed by ``unique_id``.
    nodes: Mapping[str, DbtNode]

    #: Reverse adjacency: ``_downstream_index[u]`` is the frozenset of
    #: ``unique_id``s that list ``u`` in their ``depends_on_nodes``.
    _downstream_index: Mapping[str, frozenset[str]]

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------
    @classmethod
    def from_nodes(cls, nodes: Iterable[DbtNode]) -> DbtGraph:
        """Build a graph from an iterable of nodes.

        Each node's ``depends_on_nodes`` is filtered down to edges whose
        target is also in the provided set — manifests routinely
        reference sources / macros / exposures that we don't surface as
        graph nodes, and those edges are silently dropped.
        """
        nodes_by_id: dict[str, DbtNode] = {n.unique_id: n for n in nodes}
        downstream: dict[str, set[str]] = {uid: set() for uid in nodes_by_id}
        for node in nodes_by_id.values():
            for dep in node.depends_on_nodes:
                if dep in nodes_by_id:
                    downstream[dep].add(node.unique_id)
        return cls(
            nodes=nodes_by_id,
            _downstream_index={uid: frozenset(s) for uid, s in downstream.items()},
        )

    # ------------------------------------------------------------------
    # Lookup
    # ------------------------------------------------------------------
    def __contains__(self, unique_id: object) -> bool:
        return unique_id in self.nodes

    def __getitem__(self, unique_id: str) -> DbtNode:
        return self.nodes[unique_id]

    def __len__(self) -> int:
        return len(self.nodes)

    def __iter__(self) -> Iterator[DbtNode]:
        return iter(self.nodes.values())

    def get(self, unique_id: str) -> DbtNode | None:
        """Return the node or ``None`` — never raises."""
        return self.nodes.get(unique_id)

    # ------------------------------------------------------------------
    # Edges
    # ------------------------------------------------------------------
    def upstream(self, unique_id: str) -> frozenset[str]:
        """One-hop upstream — the in-graph ``unique_id``s this node
        depends on. Empty if the node is absent or has no in-graph
        dependencies."""
        node = self.nodes.get(unique_id)
        if node is None:
            return frozenset()
        return frozenset(d for d in node.depends_on_nodes if d in self.nodes)

    def downstream(self, unique_id: str) -> frozenset[str]:
        """One-hop downstream — the ``unique_id``s that depend on this
        node."""
        return self._downstream_index.get(unique_id, frozenset())

    def upstream_closure(self, unique_id: str) -> frozenset[str]:
        """Transitive upstream (NOT including ``unique_id`` itself)."""
        return self._walk(unique_id, self.upstream)

    def downstream_closure(self, unique_id: str) -> frozenset[str]:
        """Transitive downstream (NOT including ``unique_id`` itself)."""
        return self._walk(unique_id, self.downstream)

    @staticmethod
    def _walk(start: str, step: Callable[[str], frozenset[str]]) -> frozenset[str]:
        """BFS walk; ``step(uid)`` returns the neighbours of ``uid``."""
        seen: set[str] = set()
        frontier: list[str] = list(step(start))
        while frontier:
            nxt = frontier.pop()
            if nxt in seen:
                continue
            seen.add(nxt)
            frontier.extend(step(nxt))
        return frozenset(seen)

    # ------------------------------------------------------------------
    # Topological order
    # ------------------------------------------------------------------
    def iter_topological(self) -> Iterator[DbtNode]:
        """Yield nodes such that every node comes after its in-graph
        upstreams.

        Uses Kahn's algorithm with deterministic tie-breaking on
        ``unique_id`` so the emitted order is stable across runs (and
        therefore the Airflow task wiring is stable across DAG parses).

        Raises:
            DbtGraphError: if the graph contains a cycle.
        """
        in_degree: dict[str, int] = {uid: len(self.upstream(uid)) for uid in self.nodes}
        ready: list[str] = sorted(uid for uid, deg in in_degree.items() if deg == 0)
        emitted = 0
        while ready:
            uid = ready.pop(0)
            yield self.nodes[uid]
            emitted += 1
            promoted: list[str] = []
            for child in self.downstream(uid):
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    promoted.append(child)
            if promoted:
                # Keep a deterministic order on every insertion.
                ready = sorted({*ready, *promoted})
        if emitted != len(self.nodes):
            remaining = sorted(uid for uid, deg in in_degree.items() if deg > 0)
            raise DbtGraphError(
                f"cycle in dbt graph; {len(remaining)} node(s) could not "
                f"be ordered. First few: {remaining[:5]}"
            )
