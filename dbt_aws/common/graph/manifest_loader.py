"""Build a :class:`DbtGraph` from a ``manifest.json`` dict or file.

This is the shared parsing core used by both load modes — once the
manifest JSON is in hand, the rest of the pipeline is identical.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dbt_aws.common.graph.graph import DbtGraph
from dbt_aws.common.graph.node import (
    RUNNABLE_RESOURCE_TYPES,
    node_from_manifest_entry,
)


class ManifestParseError(ValueError):
    """Raised when ``manifest.json`` is missing required structure."""


#: Supported manifest schema versions. dbt-core has bumped this field
#: several times; the runnable-node shape we read (``nodes[*]`` with
#: ``unique_id`` / ``resource_type`` / ``depends_on.nodes``) has been
#: stable since v6. Anything older we reject explicitly rather than
#: silently misparsing.
MIN_SUPPORTED_MANIFEST_VERSION = 6


def load_graph_from_manifest_dict(manifest: dict[str, Any]) -> DbtGraph:
    """Build a :class:`DbtGraph` from an in-memory manifest dict.

    Only the four runnable resource types
    (:data:`~dbt_aws.common.graph.node.RUNNABLE_RESOURCE_TYPES`) become
    graph nodes. Sources / macros / exposures / etc. are kept as
    dependency targets but not surfaced — their edges land in
    ``DbtNode.depends_on_nodes`` and are filtered out by
    :meth:`DbtGraph.from_nodes`.
    """
    _validate_manifest_shape(manifest)

    raw_nodes = manifest.get("nodes") or {}
    if not isinstance(raw_nodes, dict):
        raise ManifestParseError(
            f"manifest['nodes'] must be a dict, got {type(raw_nodes).__name__}"
        )

    nodes = []
    for unique_id, raw in raw_nodes.items():
        if not isinstance(raw, dict):
            continue
        if raw.get("resource_type") not in RUNNABLE_RESOURCE_TYPES:
            continue
        try:
            nodes.append(node_from_manifest_entry(unique_id, raw))
        except KeyError as exc:
            raise ManifestParseError(
                f"manifest node {unique_id!r} missing required field {exc}"
            ) from exc

    return DbtGraph.from_nodes(nodes)


def load_graph_from_manifest_file(path: str | Path) -> DbtGraph:
    """Read ``manifest.json`` from disk and parse it."""
    p = Path(path)
    if not p.is_file():
        raise ManifestParseError(f"manifest file not found: {p}")
    try:
        with p.open("r", encoding="utf-8") as fh:
            manifest = json.load(fh)
    except json.JSONDecodeError as exc:
        raise ManifestParseError(f"manifest {p} is not valid JSON: {exc}") from exc
    if not isinstance(manifest, dict):
        raise ManifestParseError(
            f"manifest {p} top-level must be an object, got {type(manifest).__name__}"
        )
    return load_graph_from_manifest_dict(manifest)


def _validate_manifest_shape(manifest: dict[str, Any]) -> None:
    """Check the bits of manifest schema we depend on."""
    metadata = manifest.get("metadata") or {}
    version = metadata.get("dbt_schema_version")
    # ``dbt_schema_version`` looks like
    # ``https://schemas.getdbt.com/dbt/manifest/v12.json`` — extract the
    # integer. If the field is missing entirely we accept it (some
    # older dumps omit it); if it IS present we verify the major.
    if isinstance(version, str):
        major = _extract_major_version(version)
        if major is not None and major < MIN_SUPPORTED_MANIFEST_VERSION:
            raise ManifestParseError(
                f"manifest schema v{major} is below the minimum supported "
                f"v{MIN_SUPPORTED_MANIFEST_VERSION}. Regenerate with a "
                f"newer dbt-core."
            )

    if "nodes" not in manifest:
        raise ManifestParseError(
            "manifest is missing the 'nodes' key — was this produced by 'dbt parse'?"
        )


def _extract_major_version(schema_url: str) -> int | None:
    """Pull ``N`` out of strings like ``.../manifest/vN.json``."""
    # Look for the last ``/v<digits>`` segment.
    import re

    match = re.search(r"/v(\d+)\.json", schema_url)
    if match is None:
        return None
    try:
        return int(match.group(1))
    except ValueError:  # pragma: no cover
        return None
