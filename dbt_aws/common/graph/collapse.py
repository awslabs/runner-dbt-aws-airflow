"""Collapse dbt-node subgraphs into merged Airflow tasks.

Motivation: today ``DbtDag`` produces one Airflow task per runnable
dbt node. That's simple but wasteful:

* ``ephemeral`` models are CTE-inlined by dbt at compile time --
  running ``dbt run --select my_ephemeral`` is a no-op. The current
  builder still creates a task for them.
* ``view`` models are almost free to materialise (a ``CREATE VIEW``)
  but each one costs a full worker cold-start on Glue Spark / EMR
  when it's its own task -- ~60-90 s of overhead per view.
* When a view has exactly one downstream consumer, running both in
  the same ``dbt run --select A B`` invocation lets dbt-core execute
  them in the same worker process -- one cold-start instead of two,
  and the view's DuckDB catalog entry survives long enough for the
  consumer to resolve ``ref()``.

This module implements the graph transformation. The builder calls
:func:`collapse_graph` after ``apply_selectors`` and before
``_attach_tasks``; the output is a new ``DbtGraph`` where each
remaining node represents a "group" that becomes one Airflow task
(the group's ``select`` string joins every collapsed member).

Two strategies are supported today:

* ``"view_chain"`` -- conservative: only fold a view into its single
  downstream consumer. Preserves per-model retry granularity for
  every non-view node.
* ``"aggressive"`` -- experimental: fold any linear chain of
  same-runner nodes into one group. Reduces Airflow task count more
  aggressively; trades granularity for wall-clock.

Invariants every strategy respects:

* Ephemeral nodes are always removed. Their upstream edges are
  transitively re-routed to downstream nodes so the resulting graph
  stays acyclic and preserves reachability.
* A group must map to ONE runner. Nodes routed to different runners
  cannot merge -- the merged ``dbt run`` has to run on one backend.
* A group must be a connected subgraph in the pre-collapse dbt DAG.
* Merging preserves topological order: if A depends on B before
  collapse and they end up in different groups, the group containing
  A depends on the group containing B.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Literal

from dbt_aws.common.graph.graph import DbtGraph
from dbt_aws.common.graph.node import DbtNode

_LOG = logging.getLogger(__name__)

#: Public choices for the ``collapse_strategy`` kwarg on ``DbtDag``.
CollapseStrategy = Literal["view_chain", "aggressive"]


@dataclass(frozen=True)
class TagGroupSpec:
    """One entry of the ``tag_groups`` map on ``DbtDag`` / ``DbtTaskGroup``.

    Attributes:
        name: the group name -- becomes the Airflow task id prefix
            (``<name>__<runner>``). Sanitised the same way as any
            other Airflow task id.
        overrides: group-level override fields applied to every
            member of the group at task-build time. Merged into the
            builder's per-node ``effective_overrides`` under the
            group's leader ``unique_id``, so the existing
            :func:`dbt_aws.common.runner.override.effective` lookup
            picks them up. Values in ``overrides[uid]`` at the caller
            level or in ``meta.stratus`` still win via the pull-out
            rule (:func:`dbt_aws.common.builder._node_has_pullout_override`).
    """

    name: str
    overrides: dict[str, Any] = field(default_factory=dict, hash=False, compare=False)


# ----------------------------------------------------------------------
# Public types
# ----------------------------------------------------------------------
@dataclass(frozen=True)
class NodeGroup:
    """One post-collapse group. Corresponds to exactly one Airflow task
    in the resulting DAG.

    The group's ``leader`` is the "representative" dbt node used for
    task-id derivation, callback wiring, and runner selection. In
    practice the leader is the group's most-downstream node -- the one
    whose completion signals the whole group is done.

    ``members`` is the topologically ordered list of dbt nodes in the
    group. dbt-core's own thread pool figures out the internal order
    when ``dbt run --select member1 member2 ...`` is invoked, but we
    still keep the order deterministic for logs and diffs.

    ``select_string`` is the space-joined list of member names ready
    to hand to a runner's ``select=`` kwarg.

    ``upstream_groups`` and ``downstream_groups`` reference other
    :class:`NodeGroup` objects; the builder wires these as Airflow
    dependencies between the group tasks.
    """

    leader: DbtNode
    members: tuple[DbtNode, ...]
    upstream_group_ids: frozenset[str] = field(default_factory=frozenset)
    downstream_group_ids: frozenset[str] = field(default_factory=frozenset)
    #: Group-level override config declared via ``tag_groups`` in the
    #: rich form (e.g. ``{command: 'build', target: 'prod'}``).
    #: Empty dict for structural-collapse groups and singletons.
    #: Excluded from ``eq`` / ``hash`` so ``NodeGroup`` stays hashable
    #: -- the dict itself is used only by the builder to merge into
    #: ``effective_overrides[leader.unique_id]``.
    group_config: dict[str, Any] = field(
        default_factory=dict, hash=False, compare=False
    )
    #: Optional explicit group id (used for ``tag_groups`` where the
    #: user picks the task name via the ``name:`` field of the rich
    #: form or the value of the simple form). When set, this replaces
    #: ``leader.unique_id`` as the group id -- the Airflow task id
    #: builder uses it verbatim, sanitised.
    group_id_override: str | None = None
    #: The dbt tag that drove this group (only set for groups formed by
    #: :func:`_merge_by_tag_groups`). The builder threads this into
    #: ``runner.make_task(tag_name=...)`` so runners can name AWS
    #: resources after the tag (e.g. ``dbt-aws_tag_nightly``). ``None``
    #: for singletons and structural-collapse groups.
    tag_name: str | None = field(default=None, hash=False, compare=False)

    @property
    def group_id(self) -> str:
        """Stable identifier for this group -- ``group_id_override``
        when set (``tag_groups`` groups), otherwise the leader's
        ``unique_id`` (singletons + structural-collapse groups)."""
        return self.group_id_override or self.leader.unique_id

    @property
    def select_string(self) -> str:
        """Space-separated member names for ``dbt run --select ...``.

        Order matches ``members`` (which is topological), so the
        rendered command reads naturally even though dbt-core picks
        its own internal execution order.
        """
        return " ".join(m.name for m in self.members)

    @property
    def is_singleton(self) -> bool:
        """``True`` when this group contains exactly one dbt node --
        i.e. no collapse happened. Handy for asserting invariants and
        for logging."""
        return len(self.members) == 1


@dataclass(frozen=True)
class CollapsedGraph:
    """Result of :func:`collapse_graph`. Ordered list of groups plus a
    fast lookup index.

    Every original dbt node appears in exactly one group. Ephemeral
    nodes are absent from every group (dropped in a preprocessing
    step).
    """

    groups: tuple[NodeGroup, ...]

    def __post_init__(self) -> None:
        # Sanity: no duplicate group ids, no duplicate members.
        seen_ids: set[str] = set()
        seen_members: set[str] = set()
        for g in self.groups:
            if g.group_id in seen_ids:
                raise ValueError(f"duplicate group_id {g.group_id!r}")
            seen_ids.add(g.group_id)
            for m in g.members:
                if m.unique_id in seen_members:
                    raise ValueError(
                        f"node {m.unique_id!r} present in more than one group"
                    )
                seen_members.add(m.unique_id)

    def __iter__(self):
        return iter(self.groups)

    def __len__(self) -> int:
        return len(self.groups)

    def group_for(self, unique_id: str) -> NodeGroup | None:
        """Return the group containing this dbt node, or ``None`` if
        the node was dropped (ephemeral)."""
        for g in self.groups:
            if any(m.unique_id == unique_id for m in g.members):
                return g
        return None


# ----------------------------------------------------------------------
# Public entry point
# ----------------------------------------------------------------------
def collapse_graph(
    graph: DbtGraph,
    *,
    strategy: CollapseStrategy | None = None,
    drop_ephemeral: bool = True,
    runner_for_node: Callable[[DbtNode], str] | None = None,
    tag_groups: Mapping[str, TagGroupSpec] | None = None,
    pull_out: Callable[[str], bool] | None = None,
) -> CollapsedGraph:
    """Fold a dbt graph into a smaller graph of Airflow-task groups.

    Args:
        graph: the pre-collapse :class:`DbtGraph` (typically after
            selectors have been applied).
        strategy: which merging rules to apply. ``None`` runs neither
            "view_chain" nor "aggressive" -- ephemeral drop still
            happens when ``drop_ephemeral=True``. Existing DAGs that
            don't opt in see a graph where every non-ephemeral node
            is its own singleton group -- byte-identical Airflow
            task count.
        drop_ephemeral: remove nodes materialised as ``ephemeral``.
            Default ``True`` because ``dbt run --select <ephemeral>``
            is a no-op that consumes an Airflow slot for nothing.
            Set ``False`` to keep the earlier behaviour.
        runner_for_node: optional callable that returns the runner
            NAME for a given node. When set, merging respects the
            runner boundary -- two nodes on different runners never
            merge. Pass the same function ``_resolve_node_runners``
            uses in the builder.
        tag_groups: optional bulk-by-tag grouping map. Keys are tags,
            values are :class:`TagGroupSpec`. Applied AFTER any
            ``strategy``-driven structural collapse, so
            ``collapse_strategy="view_chain"`` and ``tag_groups``
            compose cleanly.
        pull_out: optional predicate that receives a node's
            ``unique_id`` and returns ``True`` when the node must NOT
            join a tag_group (typically because it carries per-model
            overrides that would conflict with the group-level
            config). Pulled-out nodes stay singletons.

    Returns:
        :class:`CollapsedGraph` -- ordered groups + topology.

    Never raises for pure-graph reasons; malformed input (cycle,
    dangling reference) is expected to have been caught by
    :meth:`DbtGraph.from_nodes`.
    """
    # ------------------------------------------------------------------
    # Step 1: drop ephemeral nodes from the graph while preserving
    # transitive dependencies.
    # ------------------------------------------------------------------
    working = graph
    if drop_ephemeral:
        working = _strip_ephemeral(working)

    # ------------------------------------------------------------------
    # Step 2: build singleton groups for every remaining node. The
    # strategy step below merges singletons.
    # ------------------------------------------------------------------
    singletons: dict[str, NodeGroup] = {
        n.unique_id: NodeGroup(leader=n, members=(n,)) for n in working
    }

    if strategy is None:
        current: dict[str, NodeGroup] = singletons
    else:
        same_runner: Callable[[DbtNode, DbtNode], bool]
        if runner_for_node is None:
            same_runner = lambda _a, _b: True  # noqa: E731 -- terse local
        else:
            def _same_runner_impl(a: DbtNode, b: DbtNode) -> bool:
                return runner_for_node(a) == runner_for_node(b)

            same_runner = _same_runner_impl

        if strategy == "view_chain":
            current = _merge_view_chains(singletons, working, same_runner=same_runner)
        elif strategy == "aggressive":
            current = _merge_aggressive(singletons, working, same_runner=same_runner)
        else:  # pragma: no cover -- typing catches this at DAG-parse time
            raise ValueError(
                f"unknown collapse_strategy {strategy!r}; "
                f"valid: 'view_chain', 'aggressive' (or None)"
            )

    # tag_groups pass -- bulk-by-tag collapse, keyed by (group_name,
    # runner_name). Runs AFTER structural collapse so ``view_chain`` +
    # ``tag_groups`` compose cleanly. Skipped when no ``tag_groups``
    # map was passed -- byte-identical to pre-feature behaviour.
    if tag_groups:
        current = _merge_by_tag_groups(
            current,
            working,
            tag_groups=tag_groups,
            runner_for_node=runner_for_node,
            pull_out=pull_out,
        )

    return _finalise(current, working)


# ----------------------------------------------------------------------
# Ephemeral stripping
# ----------------------------------------------------------------------
def _is_ephemeral(node: DbtNode) -> bool:
    """dbt's ``materialized`` lives in the ``config`` block."""
    return (node.config or {}).get("materialized") == "ephemeral"


def _strip_ephemeral(graph: DbtGraph) -> DbtGraph:
    """Return a new :class:`DbtGraph` with all ephemeral nodes removed.

    An edge ``A -> ephemeral -> B`` is rewired to ``A -> B`` so
    downstream nodes still depend on the pre-ephemeral producer.
    Multi-hop chains through ephemerals are supported via the
    transitive-closure walk below.
    """
    keep = [n for n in graph if not _is_ephemeral(n)]
    if len(keep) == len(graph):
        return graph  # no-op fast path

    # Compute transitive upstream set for each surviving node by
    # walking through ephemeral hops.
    def _walk_up(uid: str, seen: set[str], out: set[str]) -> None:
        if uid in seen:
            return
        seen.add(uid)
        for dep in graph.upstream(uid):
            dep_node = graph.get(dep)
            if dep_node is None:
                continue
            if _is_ephemeral(dep_node):
                _walk_up(dep, seen, out)
            else:
                out.add(dep)

    rewritten_nodes: list[DbtNode] = []
    for node in keep:
        new_deps: set[str] = set()
        _walk_up(node.unique_id, set(), new_deps)
        # Preserve ordering + emit a new frozen DbtNode with rewritten deps.
        rewritten_nodes.append(
            DbtNode(
                unique_id=node.unique_id,
                name=node.name,
                resource_type=node.resource_type,
                package_name=node.package_name,
                depends_on_nodes=sorted(new_deps),
                config=node.config,
                tags=node.tags,
                meta=node.meta,
                database=node.database,
                schema=node.schema,
                fqn=node.fqn,
                original_file_path=node.original_file_path,
            )
        )

    dropped = len(graph) - len(keep)
    _LOG.info("collapse: dropped %d ephemeral node(s) from graph", dropped)
    return DbtGraph.from_nodes(rewritten_nodes)


# ----------------------------------------------------------------------
# view_chain strategy
# ----------------------------------------------------------------------
def _is_view(node: DbtNode) -> bool:
    return (node.config or {}).get("materialized") == "view"


def _merge_view_chains(
    singletons: dict[str, NodeGroup],
    graph: DbtGraph,
    *,
    same_runner: Callable[[DbtNode, DbtNode], bool],
) -> dict[str, NodeGroup]:
    """Merge every ``view`` with its single downstream consumer.

    Definition of "eligible view": a node whose ``materialized == 'view'``,
    whose one-hop downstream set has exactly one member, and where
    that downstream is routed to the same runner.

    Iterative because merging can enable new merges: if V1 -> V2 -> T
    and V2 has one consumer T, merge V2+T first; then V1's single
    downstream is now the merged group, so V1 merges into it too.
    """
    # Work with mutable copies keyed by group_id.
    groups: dict[str, NodeGroup] = dict(singletons)
    # ``member_to_group`` lets us look up which group any dbt node ended
    # up in as we iterate.
    member_to_group: dict[str, str] = {uid: uid for uid in groups}

    changed = True
    iterations = 0
    while changed:
        changed = False
        iterations += 1
        if iterations > 1000:  # pragma: no cover -- defensive
            raise RuntimeError(
                "collapse: view_chain didn't converge in 1000 iterations "
                "-- this is a bug in dbt-aws"
            )

        # Look for a view whose one downstream is a same-runner group.
        for view_uid, view_group in list(groups.items()):
            # Merge candidate: the group's LEADER is a view. This
            # includes both singleton view groups AND merged groups
            # whose most-downstream member happens to be a view --
            # e.g. after v1+v2 merged with v2 as the leader, v2 is
            # still a view and can now merge into t.
            view_node = view_group.leader
            if not _is_view(view_node):
                continue
            downstream_uids = graph.downstream(view_uid)
            if len(downstream_uids) != 1:
                continue
            consumer_uid = next(iter(downstream_uids))
            consumer_group_id = member_to_group[consumer_uid]
            consumer_group = groups[consumer_group_id]
            if consumer_group_id == view_uid:
                continue  # already the same group (shouldn't happen)
            if not same_runner(view_node, consumer_group.leader):
                continue
            # Merge: absorb the view group into the consumer group.
            merged_members = view_group.members + consumer_group.members
            merged = NodeGroup(leader=consumer_group.leader, members=merged_members)
            groups[consumer_group_id] = merged
            groups.pop(view_uid)
            # Every member of the absorbed group now points at the
            # consumer group.
            for m in view_group.members:
                member_to_group[m.unique_id] = consumer_group_id
            _LOG.debug(
                "collapse: merged view group %s into consumer group %s",
                view_uid,
                consumer_group_id,
            )
            changed = True
            break  # restart the outer loop to keep group indexes coherent

    return groups


# ----------------------------------------------------------------------
# aggressive strategy
# ----------------------------------------------------------------------
def _merge_aggressive(
    singletons: dict[str, NodeGroup],
    graph: DbtGraph,
    *,
    same_runner: Callable[[DbtNode, DbtNode], bool],
) -> dict[str, NodeGroup]:
    """Aggressive: apply view_chain first, then merge any node whose
    single downstream consumer is on the same runner (regardless of
    materialization).

    This aggressively reduces Airflow task count at the cost of
    retry granularity: a failure inside the group retries the whole
    group.
    """
    groups = _merge_view_chains(singletons, graph, same_runner=same_runner)
    member_to_group: dict[str, str] = {}
    for gid, g in groups.items():
        for m in g.members:
            member_to_group[m.unique_id] = gid

    changed = True
    iterations = 0
    while changed:
        changed = False
        iterations += 1
        if iterations > 1000:  # pragma: no cover -- defensive
            raise RuntimeError(
                "collapse: aggressive didn't converge in 1000 iterations "
                "-- this is a bug in dbt-aws"
            )

        for uid, group in list(groups.items()):
            # Compute one-hop downstream OF THE WHOLE GROUP -- unique
            # nodes downstream from any group member that are NOT in
            # this group themselves.
            outward: set[str] = set()
            for member in group.members:
                for d in graph.downstream(member.unique_id):
                    if d not in {m.unique_id for m in group.members}:
                        outward.add(member_to_group[d])
            if len(outward) != 1:
                continue
            consumer_gid = next(iter(outward))
            consumer = groups[consumer_gid]
            if consumer_gid == uid:
                continue
            if not same_runner(group.leader, consumer.leader):
                continue
            # Merge upstream group into consumer.
            merged_members = group.members + consumer.members
            merged = NodeGroup(leader=consumer.leader, members=merged_members)
            groups[consumer_gid] = merged
            groups.pop(uid)
            for m in group.members:
                member_to_group[m.unique_id] = consumer_gid
            _LOG.debug(
                "collapse: aggressive merged group %s into %s",
                uid,
                consumer_gid,
            )
            changed = True
            break

    return groups


# ----------------------------------------------------------------------
# tag_groups strategy -- bulk-by-tag collapse, one Airflow task per
# (group_name, runner_name) bucket.
# ----------------------------------------------------------------------
def _merge_by_tag_groups(
    groups: dict[str, NodeGroup],
    graph: DbtGraph,
    *,
    tag_groups: Mapping[str, TagGroupSpec],
    runner_for_node: Callable[[DbtNode], str] | None,
    pull_out: Callable[[str], bool] | None,
) -> dict[str, NodeGroup]:
    """Bulk-merge every eligible group into one task per
    ``(tag_group_name, runner_name)``.

    A group is eligible when ALL of the following hold:

    * Every member carries at least one tag present in ``tag_groups``.
    * All members' matching tags map to the SAME ``TagGroupSpec`` --
      a member carrying two different mapped tags is a config error
      caught at parse time in :mod:`dbt_aws.common.builder`.
    * ``pull_out(leader.unique_id)`` returns ``False`` (i.e. the
      leader has no per-model overrides that would conflict with the
      group-level config).
    * The group and the target bucket share the same runner (routing
      already grouped by runner at the singleton stage; the runner
      value comes from ``runner_for_node``).

    Post-merge invariants (checked by :func:`_finalise`): the merged
    subgraph is connected and acyclic. To preserve connectedness we
    partition the eligible nodes of each ``(group_name, runner)``
    bucket into weakly-connected components on the ORIGINAL graph;
    each component becomes one merged group.
    """
    if not groups:
        return groups

    # Bucket eligible NODES by (group_name, runner). Ineligible ones
    # (no matching tag / pulled out / structural-collapse groups with
    # multiple members carrying different mapped tags) skip the pass.
    def _matched_specs(node: DbtNode) -> list[TagGroupSpec]:
        return [tag_groups[t] for t in (node.tags or []) if t in tag_groups]

    def _runner(node: DbtNode) -> str:
        return runner_for_node(node) if runner_for_node is not None else ""

    # Bucket: (group_name, runner) -> ordered set of group_ids that
    # should merge into that bucket. We iterate the ``groups`` map in
    # its current order to keep results deterministic.
    buckets: dict[tuple[str, str], list[str]] = {}
    # gid -> which bucket (or None). Used to decide whether the group
    # participates in the tag_groups pass.
    gid_to_bucket: dict[str, tuple[str, str] | None] = {}
    for gid, g in groups.items():
        # Only merge groups whose EVERY member has the same spec.
        # (Structural collapse can hand us multi-member groups; a
        # mix of tagged and untagged members must NOT get merged
        # further by tag_groups because we can't attribute the group
        # to a single tag_group_name.)
        specs_per_member = [_matched_specs(m) for m in g.members]
        if any(len(s) == 0 for s in specs_per_member):
            gid_to_bucket[gid] = None
            continue
        # A member carrying multiple mapped tags with DIFFERENT specs
        # is a caller error, but we surface it at the builder layer
        # (higher context for the error message). Here we just skip
        # the group.
        distinct = {tuple(sorted({s.name for s in specs})) for specs in specs_per_member}
        if len(distinct) != 1 or len(next(iter(distinct))) != 1:
            gid_to_bucket[gid] = None
            continue
        spec = specs_per_member[0][0]
        # Pull-out: leader carries a per-model override that would
        # collide with the group's config -- keep singleton.
        if pull_out is not None and pull_out(g.leader.unique_id):
            _LOG.info(
                "tag_groups: pulling %s out of group %r (per-model override)",
                g.leader.unique_id,
                spec.name,
            )
            gid_to_bucket[gid] = None
            continue
        key = (spec.name, _runner(g.leader))
        buckets.setdefault(key, []).append(gid)
        gid_to_bucket[gid] = key

    if not buckets:
        return groups

    # Merge each bucket into ONE group per (group_name, runner) unless
    # doing so would create a cycle in the group-level graph. Cycles
    # happen when a node OUTSIDE the bucket sits between two members
    # of the same bucket (e.g. bucket={a,b}, graph a -> c -> b, with
    # c on a different runner or a different tag_group). In that
    # case we split the bucket into groups that stay acyclic; the
    # cheapest correct split is the weakly-connected components of
    # the bucket's induced subgraph, which is a sound conservative
    # rule: each component becomes its own group, and no group ever
    # merges nodes that already had an external hop between them.
    out: dict[str, NodeGroup] = {}
    merged_gids: set[str] = set()
    for (_group_name, runner_name), gid_list in buckets.items():
        members_all: list[DbtNode] = []
        for gid in gid_list:
            members_all.extend(groups[gid].members)
        by_uid = {m.unique_id: m for m in members_all}
        uids = [m.unique_id for m in members_all]

        # First try: single merged group. Check for cycle by seeing
        # whether any non-bucket node C has BOTH a bucket-upstream
        # AND a bucket-downstream (would produce group -> C -> group).
        bucket_set = set(uids)
        cycle_risk = _bucket_creates_cycle(bucket_set, graph)

        partitions = (
            [uids] if not cycle_risk else _split_into_connected_components(uids, graph)
        )

        spec = next(iter(_matched_specs(groups[gid_list[0]].leader)))
        for component_idx, comp_uids in enumerate(partitions):
            comp_members: list[DbtNode] = _topological_order(
                [by_uid[uid] for uid in comp_uids], graph
            )
            # Leader = most-downstream node in the component. If a
            # component has multiple sinks, pick a deterministic one
            # (last in topological order).
            leader = comp_members[-1]
            # Group id: <spec.name>__<runner> (+ suffix when >1 component)
            gid_override = f"{spec.name}__{runner_name}" if runner_name else spec.name
            if len(partitions) > 1:
                gid_override = f"{gid_override}__{component_idx}"
            merged_group = NodeGroup(
                leader=leader,
                members=tuple(comp_members),
                group_config=dict(spec.overrides),
                group_id_override=gid_override,
                tag_name=spec.name,
            )
            out[gid_override] = merged_group
            for gid in gid_list:
                if all(m.unique_id in comp_uids for m in groups[gid].members):
                    merged_gids.add(gid)

    # Keep every unmerged group as-is.
    for gid, g in groups.items():
        if gid in merged_gids:
            continue
        out[gid] = g
    return out


def _bucket_creates_cycle(bucket: set[str], graph: DbtGraph) -> bool:
    """Return True when merging ``bucket`` into a single group would
    create a cycle in the group-level graph.

    Cycle risk: ANY node ``c`` outside the bucket that has both an
    upstream in the bucket AND a downstream in the bucket. Merging
    the bucket would then need ``bucket -> c -> bucket``, which is a
    2-cycle at the group level.

    Runs in O(|bucket| * avg-fan-out); good enough for reasonable dbt
    graph sizes.
    """
    # Nodes that a bucket member depends on (upstream of the bucket).
    upstream_of_bucket: set[str] = set()
    for uid in bucket:
        for u in graph.upstream(uid):
            if u not in bucket:
                upstream_of_bucket.add(u)
    # Nodes that depend on a bucket member (downstream of the bucket).
    downstream_of_bucket: set[str] = set()
    for uid in bucket:
        for d in graph.downstream(uid):
            if d not in bucket:
                downstream_of_bucket.add(d)
    # Compute transitive downstreams of ``upstream_of_bucket``.
    # If any of those reach ``downstream_of_bucket``, we'd have a
    # path ``bucket -> ... -> external node -> bucket``.
    if not upstream_of_bucket or not downstream_of_bucket:
        return False
    seen: set[str] = set()
    stack = list(upstream_of_bucket)
    while stack:
        cur = stack.pop()
        if cur in seen:
            continue
        seen.add(cur)
        if cur in downstream_of_bucket:
            return True
        for d in graph.downstream(cur):
            if d in bucket:
                continue  # can't step back through the bucket
            if d not in seen:
                stack.append(d)
    return False


def _split_into_connected_components(
    uids: list[str],
    graph: DbtGraph,
) -> list[list[str]]:
    """Partition ``uids`` into weakly-connected components on ``graph``.

    Union-find over the induced subgraph (edges kept only when BOTH
    endpoints are in ``uids``). Returns components in the order their
    first member appears in ``uids`` so the output is deterministic.
    """
    uid_set = set(uids)
    parent: dict[str, str] = {u: u for u in uids}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]  # path compression
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for u in uids:
        for d in graph.downstream(u):
            if d in uid_set:
                union(u, d)

    seen_roots: dict[str, list[str]] = {}
    order: list[str] = []
    for u in uids:
        r = find(u)
        if r not in seen_roots:
            seen_roots[r] = []
            order.append(r)
        seen_roots[r].append(u)
    return [seen_roots[r] for r in order]


def _topological_order(nodes: list[DbtNode], graph: DbtGraph) -> list[DbtNode]:
    """Return ``nodes`` in a topological order relative to ``graph``.

    Uses Kahn's algorithm on the induced subgraph. Ties broken by
    ``unique_id`` for determinism.
    """
    uid_set = {n.unique_id for n in nodes}
    by_uid = {n.unique_id: n for n in nodes}
    in_deg: dict[str, int] = {n.unique_id: 0 for n in nodes}
    for n in nodes:
        for d in graph.downstream(n.unique_id):
            if d in uid_set:
                in_deg[d] = in_deg.get(d, 0) + 1
    ready = sorted(u for u, d in in_deg.items() if d == 0)
    out: list[DbtNode] = []
    while ready:
        u = ready.pop(0)
        out.append(by_uid[u])
        for d in sorted(graph.downstream(u)):
            if d in in_deg:
                in_deg[d] -= 1
                if in_deg[d] == 0:
                    ready.append(d)
    return out


# ----------------------------------------------------------------------
# Finalise -- compute topology + return the ordered CollapsedGraph.
# ----------------------------------------------------------------------
def _finalise(groups: dict[str, NodeGroup], graph: DbtGraph) -> CollapsedGraph:
    """Compute the between-group topology and return the result in
    topological order.

    Between-group edges are derived from the original graph: two
    groups have an edge iff any member of the upstream group has any
    downstream member in the downstream group.
    """
    member_to_group: dict[str, str] = {}
    for gid, g in groups.items():
        for m in g.members:
            member_to_group[m.unique_id] = gid

    up: dict[str, set[str]] = {gid: set() for gid in groups}
    down: dict[str, set[str]] = {gid: set() for gid in groups}

    for src_uid, src_gid in member_to_group.items():
        for dst_uid in graph.downstream(src_uid):
            dst_gid = member_to_group.get(dst_uid)
            if dst_gid is None or dst_gid == src_gid:
                continue
            up[dst_gid].add(src_gid)
            down[src_gid].add(dst_gid)

    # Topological sort using Kahn's algorithm on group ids.
    in_degree = {gid: len(up[gid]) for gid in groups}
    ready = sorted(gid for gid, d in in_degree.items() if d == 0)
    order: list[str] = []
    while ready:
        gid = ready.pop(0)
        order.append(gid)
        for dst in sorted(down[gid]):
            in_degree[dst] -= 1
            if in_degree[dst] == 0:
                ready.append(dst)
    if len(order) != len(groups):  # pragma: no cover -- upstream graph guarantees DAG
        raise RuntimeError("collapse: cycle detected -- this is a bug in dbt-aws")

    final = tuple(
        NodeGroup(
            leader=groups[gid].leader,
            members=groups[gid].members,
            upstream_group_ids=frozenset(up[gid]),
            downstream_group_ids=frozenset(down[gid]),
            group_config=groups[gid].group_config,
            group_id_override=groups[gid].group_id_override,
            tag_name=groups[gid].tag_name,
        )
        for gid in order
    )
    return CollapsedGraph(groups=final)


__all__ = [
    "CollapseStrategy",
    "CollapsedGraph",
    "NodeGroup",
    "TagGroupSpec",
    "collapse_graph",
]
