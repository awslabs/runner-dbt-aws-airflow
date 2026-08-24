"""Dispatcher: pick the right loader based on
:class:`~dbt_aws.common.config.ProjectConfig.mode`.
"""

from __future__ import annotations

from dbt_aws.common.config import ProjectConfig
from dbt_aws.common.graph.graph import DbtGraph
from dbt_aws.common.graph.manifest_loader import (
    load_graph_from_manifest_dict,
    load_graph_from_manifest_file,
)
from dbt_aws.common.graph.mwaa_parse_loader import load_graph_via_dbt_parse


def load_graph(config: ProjectConfig) -> DbtGraph:
    """Load a :class:`DbtGraph` according to ``config.mode``.

    Modes:

    * ``"manifest"`` — read ``manifest_path`` from disk OR consume
      ``manifest_dict`` in-process. No subprocess.
    * ``"mwaa_parse"`` — run ``dbt parse`` inside ``project_dir``, then
      read ``target/manifest.json``.

    See :class:`~dbt_aws.common.config.ProjectConfig` for the field
    combinations each mode requires.
    """
    if config.mode == "manifest":
        if config.manifest_dict is not None:
            return load_graph_from_manifest_dict(config.manifest_dict)
        manifest_path = config.resolved_manifest_path()
        assert manifest_path is not None  # ProjectConfig.__post_init__ guarantees
        return load_graph_from_manifest_file(manifest_path)

    if config.mode == "mwaa_parse":
        project_dir = config.resolved_project_dir()
        assert project_dir is not None  # ProjectConfig.__post_init__ guarantees
        return load_graph_via_dbt_parse(
            project_dir=project_dir,
            profiles_dir=config.resolved_profiles_dir(),
            target=config.target,
            dbt_argv=config.dbt_argv,
            timeout=config.parse_timeout_seconds,
        )

    # ProjectConfig.__post_init__ should have rejected this already.
    raise ValueError(f"unknown mode: {config.mode!r}")  # pragma: no cover
