"""One normalised dbt node — the unit the rest of the lib consumes.

Built from a single entry inside ``manifest.json``'s ``nodes`` map. We
keep a deliberately tight field set so the downstream layers (selector,
DAG builder, runners) work the same regardless of how the manifest was
produced (live ``dbt parse`` vs. pre-generated artefact).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class DbtNode:
    """One node from ``manifest.json``, normalised.

    Attributes:
        unique_id: dbt's stable identifier, e.g.
            ``model.my_project.stg_orders``.
        name: short name (no project / resource_type prefix),
            e.g. ``stg_orders``.
        resource_type: one of ``model``, ``snapshot``, ``seed``, ``test``,
            ``analysis``, ``source``, ``operation``. Only the first four
            are runnable.
        package_name: dbt package the node lives in (the project name for
            first-party nodes, the dep name for nodes from ``packages.yml``).
        depends_on_nodes: upstream ``unique_id``s. Empty list for roots.
        config: raw config block from the manifest (materialization,
            tags, meta, hooks, etc.). Opaque — read-only.
        tags: convenience copy of ``config.tags``.
        meta: convenience copy of ``config.meta``.
        database: target database (often ``None`` in Spark setups).
        schema: target schema / Glue Catalog database.
        fqn: dbt's fully-qualified name segments, e.g.
            ``['my_proj', 'staging', 'stg_orders']``. Optional.
        original_file_path: file path relative to the project root,
            e.g. ``models/staging/stg_orders.sql``. Useful for error
            messages and ``path:`` selectors. Optional.
    """

    unique_id: str
    name: str
    resource_type: str
    package_name: str
    depends_on_nodes: list[str]
    config: dict[str, Any] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)
    database: str | None = None
    schema: str | None = None
    fqn: list[str] | None = None
    original_file_path: str | None = None


#: Resource types that become Airflow tasks. Everything else
#: (``analysis``, ``source``, ``operation``, ``macro``, ``exposure``,
#: ``metric``, ``group``, ``semantic_model``) is metadata-only.
RUNNABLE_RESOURCE_TYPES: frozenset[str] = frozenset({"model", "snapshot", "seed", "test"})


def node_from_manifest_entry(unique_id: str, raw: dict[str, Any]) -> DbtNode:
    """Build a :class:`DbtNode` from one entry of ``manifest.json``'s
    ``nodes`` map.

    Tolerant of missing optional fields — the manifest schema has changed
    several times across dbt versions, so we only require the fields we
    truly cannot work without (``resource_type``, ``name``).
    """
    config = raw.get("config") or {}
    return DbtNode(
        unique_id=unique_id,
        name=raw.get("name") or unique_id.split(".")[-1],
        resource_type=raw["resource_type"],
        package_name=raw.get("package_name", ""),
        depends_on_nodes=list((raw.get("depends_on") or {}).get("nodes") or []),
        config=dict(config),
        tags=list(config.get("tags") or raw.get("tags") or []),
        meta=dict(config.get("meta") or raw.get("meta") or {}),
        database=raw.get("database"),
        schema=raw.get("schema"),
        fqn=list(raw["fqn"]) if isinstance(raw.get("fqn"), list) else None,
        original_file_path=raw.get("original_file_path"),
    )
