"""Per-node Airflow task factory ABC + helpers.

Public surface:

* :class:`Runner` -- contract every concrete runner implements.
* :func:`dbt_command_for` -- derive the dbt CLI verb from a node's
  resource type.
* :data:`DBT_COMMAND_FOR_RESOURCE_TYPE` -- the mapping directly.
* :class:`RunnerOverride` -- marker base for per-model override
  dataclasses.
* :func:`resolve_override` -- layered merge of ``node.meta['stratus']``
  + explicit ``overrides`` dict.
* :class:`OverrideError` -- malformed override dict.
* :func:`load_runner_config` -- parse + validate a YAML runner config.
* :class:`LoadedRunnerConfig` -- result of :func:`load_runner_config`.
* :class:`RunnerConfigError` -- malformed YAML / unknown type / bad kwargs.
"""

from dbt_aws.common.runner.base import (
    DBT_COMMAND_FOR_RESOURCE_TYPE,
    Runner,
    dbt_command_for,
)
from dbt_aws.common.runner.config import (
    LoadedRunnerConfig,
    RunnerConfigError,
    TaskGroupConfig,
    TaskGroupingConfig,
    load_runner_config,
)
from dbt_aws.common.runner.naming import (
    DEFAULT_NAME_PREFIX,
    DEFAULT_PER_MODEL_TEMPLATE,
    DEFAULT_PER_NODE_TEMPLATE,
    DEFAULT_PER_TAG_TEMPLATE,
    DEFAULT_RESOURCE_TEMPLATE,
    DEFAULT_SHARED_TEMPLATE,
    LEGACY_PER_NODE_TEMPLATE,
    resolve_resource_name,
    sanitize_resource_name,
)
from dbt_aws.common.runner.override import (
    OverrideError,
    RunnerOverride,
    effective,
    resolve_override,
)
from dbt_aws.common.runner.tags import (
    ResourceTagsError,
    as_emr_tag_list,
    make_glue_tag_sync_callback,
    merge_resource_tags,
    validate_resource_tags,
)

__all__ = [
    "DBT_COMMAND_FOR_RESOURCE_TYPE",
    "DEFAULT_NAME_PREFIX",
    "DEFAULT_PER_MODEL_TEMPLATE",
    "DEFAULT_PER_NODE_TEMPLATE",
    "DEFAULT_PER_TAG_TEMPLATE",
    "DEFAULT_RESOURCE_TEMPLATE",
    "DEFAULT_SHARED_TEMPLATE",
    "LEGACY_PER_NODE_TEMPLATE",
    "LoadedRunnerConfig",
    "OverrideError",
    "ResourceTagsError",
    "Runner",
    "RunnerConfigError",
    "RunnerOverride",
    "TaskGroupConfig",
    "TaskGroupingConfig",
    "as_emr_tag_list",
    "dbt_command_for",
    "effective",
    "load_runner_config",
    "make_glue_tag_sync_callback",
    "merge_resource_tags",
    "resolve_override",
    "resolve_resource_name",
    "sanitize_resource_name",
    "validate_resource_tags",
]
