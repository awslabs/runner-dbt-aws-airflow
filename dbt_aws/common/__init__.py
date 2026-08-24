"""dbt-aws-common -- backend-agnostic core.

Public API (v0):

* :class:`ProjectConfig` -- how to obtain the dbt graph.
* :class:`ConfigError` -- invalid :class:`ProjectConfig`.
* :func:`load_graph` -- dispatcher over the two load modes.
* :class:`DbtGraph`, :class:`DbtNode`, :class:`DbtGraphError`.
* :class:`ManifestParseError`, :class:`DbtParseError`.

Airflow-side entry points (``DbtDag`` / ``DbtTaskGroup``) live in
:mod:`dbt_aws.common.builder`. They are NOT re-exported from this
package because importing them eagerly pulls in ``airflow.sdk`` --
Glue / EMR Serverless workers don't have Airflow installed, and the
worker entry point imports :mod:`dbt_aws.common.runtime` which would
then transitively crash with ``ModuleNotFoundError: No module named
'airflow'``. Callers explicitly import the classes::

    from dbt_aws.common.builder import DbtDag, DbtTaskGroup
"""

from dbt_aws.common.archive import (
    ArchiveError,
    ProjectArchive,
    build_project_archive,
    fingerprint_project,
)
from dbt_aws.common.config import ConfigError, LoadMode, ProjectConfig
from dbt_aws.common.graph import (
    RUNNABLE_RESOURCE_TYPES,
    DbtGraph,
    DbtGraphError,
    DbtNode,
    DbtParseError,
    ManifestParseError,
    load_graph,
    load_graph_from_manifest_dict,
    load_graph_from_manifest_file,
    load_graph_via_dbt_parse,
)
from dbt_aws.common.runner import (
    DBT_COMMAND_FOR_RESOURCE_TYPE,
    LoadedRunnerConfig,
    OverrideError,
    Runner,
    RunnerConfigError,
    RunnerOverride,
    TaskGroupConfig,
    TaskGroupingConfig,
    dbt_command_for,
    load_runner_config,
    resolve_override,
)
from dbt_aws.common.select import (
    SelectorError,
    apply_selectors,
    parse_selector,
)

__all__ = [
    "DBT_COMMAND_FOR_RESOURCE_TYPE",
    "RUNNABLE_RESOURCE_TYPES",
    "ArchiveError",
    "ConfigError",
    "DbtGraph",
    "DbtGraphError",
    "DbtNode",
    "DbtParseError",
    "LoadMode",
    "LoadedRunnerConfig",
    "ManifestParseError",
    "OverrideError",
    "ProjectArchive",
    "ProjectConfig",
    "Runner",
    "RunnerConfigError",
    "RunnerOverride",
    "SelectorError",
    "TaskGroupConfig",
    "TaskGroupingConfig",
    "apply_selectors",
    "build_project_archive",
    "dbt_command_for",
    "fingerprint_project",
    "load_graph",
    "load_graph_from_manifest_dict",
    "load_graph_from_manifest_file",
    "load_graph_via_dbt_parse",
    "load_runner_config",
    "parse_selector",
    "resolve_override",
]
