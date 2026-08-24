"""Resource-name templating for runner-managed AWS resources.

Every runner manages at least one named AWS resource (Glue Job,
Glue Interactive Session, EMR Serverless application, Lambda
function, ...). The naming can be:

* Explicit -- caller provides ``job_name`` (or equivalent) on the
  runner or in an override.
* Templated -- caller provides ``job_name_template`` with token
  placeholders that this module resolves per task.
* Auto -- nothing set; lib falls back to the standard conventions:

    - **Per-model** (each dbt node gets its own AWS resource):
      ``"{prefix}_model_{model_name}"``.
    - **Per-tag** (all dbt nodes sharing a ``tag_groups`` tag share
      one AWS resource):
      ``"{prefix}_tag_{tag_name}"``.
    - **Shared** (EMR Serverless application, Glue Session -- one
      resource for the whole DAG): ``"{prefix}-{dag_id}-{runner_kind}"``.

Recognised tokens:

* ``{prefix}``       -> runner's ``name_prefix`` (default ``"dbt-aws"``)
* ``{model_name}``   -> dbt node ``name`` (per-model default)
* ``{tag_name}``     -> tag driving a ``tag_groups`` group (per-tag default)
* ``{dag_id}``       -> Airflow DAG id (still available for custom templates)
* ``{runner_kind}``  -> short suffix per runner type (``spark`` / ``pyshell`` / ``session``)
* ``{unique_id}``    -> dbt node ``unique_id``

Tokens that don't apply resolve to an empty string. The lib
sanitises every resolved name (lowercases, replaces disallowed
chars with ``_``, collapses repeats, caps length) so callers can
plug ``{model_name}`` = ``fct_orders__daily`` straight in without
worrying about AWS-side character constraints.

Note on the naming standard: the earlier defaults were
``"{prefix}-{dag_id}-{runner_kind}-{model_name}"`` (per-node) and
``"{prefix}-{dag_id}-{runner_kind}"`` (shared). The new defaults are
shorter and switch on scope (model vs tag). Users on ``mode='attach'``
who pre-provisioned Glue Jobs under the old names should either
rename the AWS resource or pass the legacy template explicitly via
``job_name_template=`` -- see :data:`LEGACY_PER_NODE_TEMPLATE`.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from dbt_aws.common.graph.node import DbtNode

#: Default name prefix used in the fallback convention.
DEFAULT_NAME_PREFIX = "dbt-aws"

#: Default template for **per-model** resources -- each dbt node gets
#: its own AWS resource. Simple, short, easy to grep in the AWS console.
DEFAULT_PER_MODEL_TEMPLATE = "{prefix}_model_{model_name}"

#: Default template for **per-tag** resources -- every dbt node in a
#: ``tag_groups`` group shares one AWS resource named after the driving
#: tag. Picks up when the builder passes ``tag_name=`` to the runner.
DEFAULT_PER_TAG_TEMPLATE = "{prefix}_tag_{tag_name}"

#: Default template for **shared** resources -- one resource for the
#: whole DAG (EMR Serverless *application*, Glue Interactive Session).
#: Kept in the old DAG-scoped shape because these resources are per-DAG,
#: not per-model or per-tag, and the ``{runner_kind}`` disambiguates
#: when a single DAG uses multiple reusable-resource runners.
DEFAULT_SHARED_TEMPLATE = "{prefix}-{dag_id}-{runner_kind}"

#: The earlier per-node template. No longer the auto default, but
#: available for users who want the DAG-scoped shape (e.g. one Glue
#: Job per (DAG, model) instead of one per model shared across DAGs).
#: Pass via ``job_name_template=`` on the runner constructor.
LEGACY_PER_NODE_TEMPLATE = "{prefix}-{dag_id}-{runner_kind}-{model_name}"

#: Alias kept for callers still importing the earlier name.
DEFAULT_PER_NODE_TEMPLATE = LEGACY_PER_NODE_TEMPLATE

#: Back-compat alias. Older callers imported ``DEFAULT_RESOURCE_TEMPLATE``.
DEFAULT_RESOURCE_TEMPLATE = DEFAULT_SHARED_TEMPLATE


#: AWS Glue Job / EMR application names allow ``[A-Za-z0-9_-]`` up
#: to 255 chars. We normalise to lowercase and swap anything else
#: for ``_`` so callers can drop dbt names in verbatim (dbt allows
#: dots, plus signs, etc. in model names that AWS rejects).
_ALLOWED_CHAR = re.compile(r"[^a-z0-9_-]")
_MAX_NAME_LEN = 255


def sanitize_resource_name(name: str) -> str:
    """Return a Glue/EMR-safe form of ``name``.

    Steps:
    1. lowercase
    2. replace any char outside ``[a-z0-9_-]`` with ``_``
    3. collapse consecutive ``_`` runs
    4. strip leading / trailing ``_`` and ``-``
    5. truncate to 255 chars (Glue Job name limit)

    Idempotent. Safe on already-sanitised inputs.
    """
    lowered = name.lower()
    swapped = _ALLOWED_CHAR.sub("_", lowered)
    collapsed = re.sub(r"_+", "_", swapped).strip("_-")
    if len(collapsed) > _MAX_NAME_LEN:
        collapsed = collapsed[:_MAX_NAME_LEN].rstrip("_-")
    return collapsed


def resolve_resource_name(
    *,
    explicit: str | None,
    template: str | None,
    name_prefix: str | None,
    dag_id: str,
    runner_kind: str,
    node: DbtNode | None = None,
    tag_name: str | None = None,
) -> str:
    """Resolve a runner-managed resource name using this precedence:

    1. ``explicit``  -> returned verbatim (NOT sanitised; caller's
       choice, caller's responsibility).
    2. ``template``  -> rendered with the available tokens, then
       sanitised.
    3. Default fallback:

       * ``tag_name`` is given -> :data:`DEFAULT_PER_TAG_TEMPLATE`
         (``{prefix}_tag_{tag_name}``)
       * ``node`` is given     -> :data:`DEFAULT_PER_MODEL_TEMPLATE`
         (``{prefix}_model_{model_name}``)
       * neither               -> :data:`DEFAULT_SHARED_TEMPLATE`
         (``{prefix}-{dag_id}-{runner_kind}``)

    Args:
        explicit: caller-supplied exact name (overrides everything).
        template: caller-supplied template using the recognised tokens.
        name_prefix: value for the ``{prefix}`` token (defaults to
            :data:`DEFAULT_NAME_PREFIX` when ``None``).
        dag_id: Airflow DAG id.
        runner_kind: short suffix identifying the runner type.
        node: optional dbt node, used to fill ``{model_name}`` /
            ``{unique_id}`` tokens.
        tag_name: optional driving tag when the runner is materialising
            a ``tag_groups`` group. Selects the per-tag default and
            fills the ``{tag_name}`` token.

    Returns:
        Sanitised resource name (safe for Glue / EMR APIs).
    """
    if explicit is not None:
        return explicit

    if template is None:
        if tag_name is not None:
            template = DEFAULT_PER_TAG_TEMPLATE
        elif node is not None:
            template = DEFAULT_PER_MODEL_TEMPLATE
        else:
            template = DEFAULT_SHARED_TEMPLATE

    rendered = template.format_map(
        _Tokens(
            prefix=name_prefix or DEFAULT_NAME_PREFIX,
            dag_id=dag_id,
            runner_kind=runner_kind,
            model_name=node.name if node is not None else "",
            unique_id=node.unique_id if node is not None else "",
            tag_name=tag_name or "",
        )
    )
    return sanitize_resource_name(rendered)


class _Tokens(dict):
    """Dict subclass that turns missing keys into empty strings.

    Lets templates reference ``{model_name}`` even when called outside
    a per-task context without raising ``KeyError``.
    """

    def __missing__(self, key: str) -> str:
        return ""
