# YAML config schema

`load_runner_config(path)` parses a YAML file into a `LoadedRunnerConfig` whose fields
plug straight into `DbtDag(...)`.

=== "Recommended"

    Pass the whole `LoadedRunnerConfig` in one shot. Every routing field
    (`runner` / `runners` / `default_runner` / `overrides` /
    `task_groups`) is auto-wired, including `mode: group | single`
    entries under `overrides[tag.<t>]` ( replacement for the
    removed top-level `tag_groups:` key).

    ```python
    from dbt_aws.common import load_runner_config
    from dbt_aws.common.builder import DbtDag

    cfg = load_runner_config("runners.yml")

    dag = DbtDag(
        dag_id="my_dbt",
        project=...,
        project_archive_s3="...",
        config=cfg,             # <-- one kwarg
    )
    ```

=== "Advanced (per-field, earlier releases)"

    Forward each field explicitly. Callers occasionally need this so
    they can mix Python-side overrides with YAML values on a field-by-
    field basis.

    ```python
    dag = DbtDag(
        runners=cfg.runners,
        default_runner=cfg.default_runner,
        overrides=cfg.overrides,
        tag_overrides=cfg.tag_overrides,
        task_groups=cfg.task_groups,
        project=...,
        project_archive_s3="...",
    )
    ```

    !!! warning " metadata not exposed as Python kwargs"

        `cfg.tag_group_specs` (from `overrides[tag.<t>]: {mode: group,
        ...}`) and `cfg.tag_single_name_prefixes` (from
        `overrides[tag.<t>]: {mode: single, name: <prefix>}`) do NOT
        have direct Python-level kwargs on `DbtDag`. They only flow
        through `config=cfg`. Use the recommended tab above if your
        YAML declares either shape.

## Variable interpolation (`vars=`)

`load_runner_config` accepts an optional `vars` kwarg. When set, every `${name}`
in the raw YAML text is replaced with `str(vars[name])` **before** the YAML
parser runs, so a single `runners.yml` can drive dev/qa/prd without file
duplication or wrapper scripts.

```python
import os
from dbt_aws.common import load_runner_config

cfg = load_runner_config(
    "runners.yml",
    vars={"env": os.environ["AIRFLOW__ENV__ENVIRONMENT"]},
)
```

```yaml
runners:
  glue_spark_lib:
    type: glue_spark
    mode: create
    iam_role_name: se-intelds-edh-euwe1-rdo-glue-service-${env}
    deploy_bucket: se-intelds-edh-euwe1-glue-scripts-${env}
    upload_artefacts_s3_prefix:
      "s3://se-intelds-edh-euwe1-rdo-stratus-${env}/dbt-aws-airflow/glue_spark_lib/"
    create_job_kwargs:
      SecurityConfiguration: se-intelds-edh-euwe1-security_conf-${env}
      DefaultArguments:
        "--conf": >-
          spark.sql.catalog.glue_catalog.warehouse=s3://se-intelds-edh-euwe1-rdo-dbt-data-gold-${env}/gold/
default_runner: glue_spark_lib
```

Semantics:

- **`vars=None` (default): no interpolation.** Behaviour is bit-for-bit
  identical to previous releases; a literal `${...}` in the YAML survives to
  the runner.
- **`vars=dict`: every `${name}` is replaced with `str(vars[name])`.**
  Substitution is a plain string replace on the raw byte stream (no Jinja,
  no filters, no arithmetic), so a variable can appear in the middle of any
  string — bucket names, ARNs, IAM role names, or fragments inside multi-line
  `>-` scalars.
- **Undefined references error at load time.** A `${roll_name}` with no
  matching entry in `vars` raises `RunnerConfigError` naming both the
  missing key AND the caller-provided key set, so users don’t have to
  guess which side is wrong.
- **Escape hatch: `$${...}` → literal `${...}`.** The inner name is NOT
  looked up in `vars`. Use this for the rare case where a legitimate
  dollar-brace substring must survive interpolation.
- **No implicit `os.environ` fallback.** Callers must pass `vars=`
  explicitly. This keeps the config’s dependencies obvious and prevents
  the library from accidentally capturing unrelated environment vars.
- **Existing validation runs after interpolation.** A typo in a variable
  name (`${roll_name}`) surfaces as an interpolation error; a typo in a
  YAML key (`iam-role-name` vs `iam_role_name`) still surfaces as a
  `RunnerConfigError` from `_construct_runner`.
- **INFO log at DAG-parse time** listing the set of resolved keys (values
  redacted), so logs make it obvious which env a file was rendered for.

## Top-level keys

```yaml
# === RUNNERS ===========================================================
# Exactly one of `runner` (single) OR `runners` (multi) must be present.

runner:                   # single-runner shortcut
  type: glue_spark        # glue_spark | glue_python_shell |
                          # glue_interactive_session | emr_serverless |
                          # emr_cluster_step
  # ... runner kwargs (see "Runner shapes" below) ...

# OR

runners:                  # multi-runner map
  spark:
    type: glue_spark
    # ... runner kwargs ...
  shell:
    type: glue_python_shell
    # ... runner kwargs ...

default_runner: spark     # required when `runners:` is set

# === OVERRIDES =========================================================
# One unified block. Three entry shapes, all with the SAME field schema:
#   * `all:` -- broadest scope, applies to every node in the
#     rendered graph (after select / exclude filtering).
#   * `tag.<name>:` -- bulk-by-tag override.
#   * `<unique_id>:` -- per-node override, keyed by dbt's unique_id.
#
# , `all:` and `tag.<name>:` entries accept two meta-keys
# that are NOT part of the runner's OVERRIDE_TYPE:
#   * `mode: group | single`  (default: single)
#       - `single` -- one Airflow task per node, entry supplies bulk
#         override defaults. This is the earlier releases behaviour.
#       - `group` -- collapse every eligible node into ONE Airflow
#         task per (name, runner). Replaces the earlier releases top-level
#         `tag_groups:` key.
#   * `name: <airflow-task-id-legal-string>`
#       - Under `mode: group`: the collapsed task's id (default:
#         the tag name for `tag.<t>`, the string "all" for `all:`).
#       - Under `mode: single`: a task-id PREFIX applied to every
#         matching node, so siblings sort together in the Graph view
#         (`<name>__<sanitised_uid>`).
#
# Precedence per field (higher wins). See
# ``docs/reference/precedence.md`` for the canonical ladder:
#   1. Runner default (on the `runners:` block)
#   2. `overrides.all.<field>`
#   3. `overrides[tag.<name>].<field>`
#   4. `node.meta.stratus.<field>` (declared in dbt project)
#   5. `overrides[<unique_id>].<field>`
#
# Per-node ALWAYS wins per-field over tag; tag wins over all.

overrides:
  # Broadest scope: applies to every node. Weakest override layer --
  # higher layers (tag / meta / per-node) can still override any key.
  all:
    worker_type: G.2X         # baseline sizing for every node

  # Bulk by tag, per-node fan-out (mode: single is the default).
  tag.landing:
    runner: shell             # dispatch: pick a named runner
    target: shell_dev         # dbt --target
    profile_name: shell_prof  # dbt --profile
    name: landing             # optional -- task-id prefix
                              # (yields ``landing__model__proj__x``)

  # Bulk by tag, GROUP mode -- collapses into one Airflow task.
  tag.bronze:
    mode: group
    name: bronze_batch        # optional -- defaults to "bronze"
    runner: spark
    worker_type: G.2X
    number_of_workers: 4
    command: build

  tag.gold:
    mode: group               # ``name:`` defaults to ``gold``
    runner: spark
    target: prod
    full_refresh: true

  # Per-node -- keyed by unique_id. Wins over tag entries per field.
  # Meta-keys ``mode:`` and ``name:`` are rejected on per-node entries.
  model.proj.heavy_aggregate:
    worker_type: G.4X
    number_of_workers: 8
    timeout_minutes: 240

  model.proj.hotfix:
    runner: shell
    target: shell_dev
    profile_name: shell_prof
    command: build

# === VISUAL GROUPING ===================================================
# task_groups: below is INDEPENDENT of overrides[tag.*].mode above --
# it controls collapsible folders in the Airflow Graph view (one task
# per dbt node still) rather than task collapse.
task_groups:
  - name: bronze
    tags: [bronze]
  - name: silver
    tags: [silver]
ungrouped_group: other    # optional; null/omitted -> unmatched at DAG root
```

Allowed top-level keys: `runner`, `runners`, `default_runner`, `overrides`,
`task_groups`, `ungrouped_group`, `openlineage`, `resource_tags`. Anything
else raises `RunnerConfigError`.

!!! danger "Removed top-level keys"

    The following top-level YAML keys were removed and now raise
    `RunnerConfigError` at load time with a per-key migration hint:

    | Removed key | Since | Replacement |
    |---|---|---|
    | `tag_runners:` | — | `overrides[tag.<t>]: {runner: ...}` |
    | `tag_profiles:` | — | `overrides[tag.<t>]: {profile_name: ...}` |
    | `tag_targets:` | — | `overrides[tag.<t>]: {target: ...}` |
    | `tag_groups:` | removed | `overrides[tag.<t>]: {mode: group, name: ...}` |

!!! note "Field schema is per-runner"

    Every field under `overrides[tag.<t>]` or `overrides[<uid>]` is validated
    against the SELECTED runner's `OVERRIDE_TYPE`. Unknown fields raise
    `RunnerConfigError` naming the accepted field list.

!!! note "Selectors are NOT in the YAML schema"

    `select` / `exclude` are deliberately omitted from the runner YAML -- they are
    **DAG-level** concerns, not runner-level. Pass them on `DbtDag(select=...,
    exclude=...)`. This keeps the runner YAML reusable across multiple DAGs (one
    `runners.yml`, N DAG files each with their own subset).

## `overrides.all:`

Broadest-scope override entry inside the ``overrides:`` block. Applies
to every node in the rendered graph (after ``select`` / ``exclude``
filtering). Sits at layer 2 in the [precedence ladder](precedence.md)
— above runner defaults, below tag / meta / per-node.

Same field schema as ``tag.<name>:`` entries:

- Accepts every field the selected runner's ``OVERRIDE_TYPE`` supports
  (``worker_type``, ``command``, ``full_refresh``, ``vars_json``,
  ``resource_tags``, ...).
- Accepts the meta-keys ``mode: single | group`` and ``name: <str>``.
- ``runner:`` sets a graph-wide dispatch default (weaker than any tag
  / meta / per-node ``runner:``).

### `mode: single` (default) with `name: <prefix>`

Every node's Airflow task-id gets prefixed with ``<name>__``. Useful
for "prefix every task with `dev_`" in a shared workspace.

```yaml
overrides:
  all:
    mode: single
    name: dev
    worker_type: G.2X          # baseline sizing for every node
```

Resulting task-ids: ``dev__model__proj__orders``,
``dev__model__proj__customers``, etc.

### `mode: group`

Collapses every eligible node into ONE Airflow task per
``(name, runner)`` bucket. ``name:`` defaults to the literal string
``"all"``.

```yaml
overrides:
  all:
    mode: group
    name: dbt_all              # optional -- defaults to "all"
    command: build             # every node in the group runs ``dbt build``
```

One task, id ``dbt__dbt_all__<runner>``, running
``dbt build --select <every node name>`` on the assigned runner.

### Pull-out via per-model override

A node with any per-model override (in ``overrides[<uid>]`` or
``node.meta.stratus``, beyond the dispatch-only ``runner`` key) gets
pulled OUT of the group into a singleton task. Same pull-out rule
as ``overrides[tag.<t>]: {mode: group}``.

```yaml
overrides:
  all:
    mode: group
    name: dbt_all

  # ``hotpath`` runs on its own Airflow task with G.8X. The rest of
  # the graph still batches under ``dbt__dbt_all__<runner>``.
  model.proj.hotpath:
    worker_type: G.8X
```

If pulling a node out disconnects the remaining graph into multiple
components, the collapse pass gives each component its own task with
a ``__<idx>`` suffix (``dbt__dbt_all__spark__0``, ``__1``, ...).

### Test-mode idiom

The expected use case for ``all: {mode: group}`` is one-shot
test / dev / CI runs where you want every model to run in a single
Glue Job / Session / Application. Not intended for production DAGs
— the collapsed task loses per-node retry granularity.

```yaml
runners:
  test_all:
    type: glue_spark
    mode: attach
    job_name: dbt-test-mode
    aws_conn_id: aws_default
    region_name: us-east-1

default_runner: test_all

overrides:
  all:
    mode: group
    name: dbt_all
    command: build
```

## Top-level `resource_tags:`

AWS resource tags applied to every runner in the file. Merges per key
over any runner-level `resource_tags:` (runner keys win on conflict).
Typical use: declare cost-centre / environment / owner once at the
top, override per-runner-specific bits where needed.

```yaml
# Cost / compliance tags applied to every AWS resource created by
# this DAG's runners.
resource_tags:
  CostCenter: data-platform
  Environment: prod
  Owner: analytics-team

runners:
  spark:
    type: glue_spark
    mode: create
    # No ``resource_tags:`` block -- inherits all three above.

  shell:
    type: glue_python_shell
    mode: create
    resource_tags:
      # Inherits CostCenter + Environment; overrides Owner;
      # adds Runtime.
      Owner: platform-team
      Runtime: python-shell
```

Semantics:

- Applies at AWS `create_*` time -- `mode='create'` runners only. Under
  `mode='attach'` the AWS resource pre-exists and IaC (Terraform /
  CloudFormation) owns its tags; `resource_tags` is silently ignored.
- **Layered.** `resource_tags` can also live under
  `overrides[<uid>].resource_tags`, `overrides[tag.<t>].resource_tags`,
  and dbt-side `meta.stratus.resource_tags`. All layers shallow-merge
  per key (later wins). Runner-level tags remain the baseline
  cascade.
- Caller-supplied `Tags` inside `create_job_kwargs` merges per Key
  on top of `resource_tags` (caller wins).
- AWS constraints enforced at DAG-parse time: keys and values must be
  non-empty strings, key <= 128 chars, value <= 256 chars, no `aws:`
  prefix. Violations raise `ResourceTagsError`.

Per-runner AWS API surface (what the boto3 call receives):

| Runner | AWS API | Tag kwarg shape |
|---|---|---|
| `glue_spark` | `glue:CreateJob` | `Tags: dict[str, str]` on `create_job_kwargs` |
| `glue_python_shell` | `glue:CreateJob` | `Tags: dict[str, str]` on `create_job_kwargs` |
| `glue_interactive_session` | `glue:CreateSession` | `Tags: dict[str, str]` (session-scoped) |

The library handles the per-service kwarg-shape conversion -- callers
always supply `resource_tags: dict[str, str]` regardless of the target
runner.

## Per-model + per-tag Spark config (`spark_conf`, )

> **See also:** [Example → `spark_conf` vs `--conf`](examples/spark-conf-demo.md)
> for a copy-paste demo, a walkthrough of the merged output at each
> layer, and a list of the common mistakes.

`GlueSparkRunner` accepts a `spark_conf: dict[str, str]` kwarg that
Glue Spark reads as `--conf key=value` fragments. The library resolves
an effective config per node from three layers, shallow-merged (later
wins per key):

1. `runners.<name>.spark_conf` -- runner-level baseline, applied to
   the Job's `DefaultArguments["--conf"]` at create/update.
2. `overrides[tag.<name>].spark_conf` -- bulk-by-tag layer.
3. `overrides[<unique_id>].spark_conf` or dbt-side
   `meta.stratus.spark_conf` -- per-model layer.

The merged result lands in the JobRun's `--conf` argument
(StartJobRun-level override), so concurrent runs of the same Job with
different per-node `spark_conf` don't race.

```yaml
runners:
  spark:
    type: glue_spark
    mode: create
    iam_role_name: AWSGlueServiceRole
    deploy_bucket: my-glue-bucket
    spark_conf:                                # runner-level baseline
      spark.sql.adaptive.enabled: "true"
      spark.sql.shuffle.partitions: "200"

overrides:
  tag.bronze:                                  # bulk-by-tag layer
    spark_conf:
      spark.sql.shuffle.partitions: "400"      # replaces the runner value

  model.my_project.fct_orders_daily:           # per-model layer (wins)
    spark_conf:
      spark.sql.shuffle.partitions: "800"      # beats tag.bronze
      spark.sql.autoBroadcastJoinThreshold: "-1"
```

For `fct_orders_daily` (tagged `bronze`), the effective `--conf` is:

```
--conf spark.sql.adaptive.enabled=true
--conf spark.sql.shuffle.partitions=800
--conf spark.sql.autoBroadcastJoinThreshold=-1
```

### Escape hatch: `spark_conf_replace`

Set `spark_conf_replace: {...}` on any layer (runner, tag, meta,
per-node) to REPLACE the merged `spark_conf` from all lower layers
entirely, instead of shallow-merging. Use when a single model needs a
completely different profile from the runner defaults.

```yaml
overrides:
  model.my_project.dim_special:
    spark_conf_replace:                        # discards runner + tag layers
      spark.sql.adaptive.enabled: "false"
      spark.sql.shuffle.partitions: "50"
```

Effective `--conf` for `dim_special`: `--conf spark.sql.adaptive.enabled=false --conf spark.sql.shuffle.partitions=50` (runner-level `spark.sql.adaptive.enabled=true` is dropped).

### Runtime-only limitation

`spark_conf` and `spark_conf_replace` land in the JobRun `--conf`
argument, so they cover DRIVER / EXECUTOR runtime configs
(`spark.sql.*`, `spark.default.parallelism`, `spark.rdd.compress`,
...). Configs that must be set before the JVM starts
(`spark.jars.packages`, `spark.hadoop.fs.*`, `spark.driver.memory`,
`spark.executor.memory`, ...) MUST live in the runner's
`create_job_kwargs.DefaultArguments` -- Glue silently ignores those
at StartJobRun time.

### Validation

All layers are validated at DAG-parse:

- keys must match `^[a-zA-Z][a-zA-Z0-9._-]*$` (Glue's `--conf` parser
  splits on spaces; any space in a key silently drops the config)
- keys must not start with `--conf` (footgun: copy-pasting from
  `spark-submit`)
- values must be non-empty strings (numbers cast to `str` at the YAML
  side)

Violations raise `RunnerConfigError` naming the offending key or
value.

## Runner shapes
Each `type:` key maps to a concrete Runner class. The rest of the runner block is
forwarded as kwargs to its constructor (so YAML keys mirror the Python kwargs 1:1).

### `type: glue_spark`

```yaml
runners:
  spark:
    type: glue_spark
    mode: create                        # or "attach"
    iam_role_name: AWSGlueServiceRole
    deploy_bucket: my-glue-bucket
    deploy_prefix: dbt-aws
    create_job_kwargs:
      DefaultArguments:
        "--additional-python-modules": "dbt-aws,dbt-core==1.11.11,dbt-duckdb==1.10.1"
        "--job-language": "python"
      GlueVersion: "5.0"
      WorkerType: G.1X
      NumberOfWorkers: 2
      ExecutionProperty:
        MaxConcurrentRuns: 5
    update_config: true                 # push CreateJob changes to existing Jobs
    aws_conn_id: aws_default
    region_name: eu-west-1
    upload_artefacts_s3_prefix: "s3://my-glue-bucket/dbt-aws/glue_spark/"
```

### `type: glue_interactive_session`

```yaml
runners:
  warm:
    type: glue_interactive_session
    iam_role_arn: arn:aws:iam::123:role/AWSGlueServiceRole
    reusable: true                      # one session shared (false = per-node)
    session_id_prefix: dbt-aws-warm
    additional_python_modules: "dbt-aws,dbt-core==1.11.11,dbt-duckdb==1.10.1"
    default_arguments:
    glue_version: "5.0"
    worker_type: G.1X
    number_of_workers: 2
    idle_timeout_minutes: 15
    timeout_minutes: 45
    aws_conn_id: aws_default
    region_name: eu-west-1
    upload_artefacts_s3_prefix: "s3://my-glue-bucket/dbt-aws/session_warm/"
```

### `type: glue_python_shell`

```yaml
runners:
  pyshell:
    type: glue_python_shell
    mode: create
    iam_role_name: AWSGlueServiceRole
    # ...
```

!!! warning "Glue 3.0 Python Shell limitations"

    See [Troubleshooting](../troubleshooting.md#glue-python-shell-30-install-pipeline).

### `type: emr_cluster_step`

```yaml
runners:
  emr_cluster:
    type: emr_cluster_step
    mode: create                        # or "attach" to an existing cluster
    reusable: true                      # share one cluster across all steps
    auto_terminate: true
    deploy_mode: client                 # recommended for dbt (no SparkContext needed)
    pyspark_python: /usr/bin/python3.11 # EMR 7.5+ has both 3.9 + 3.11; force 3.11
    driver_cores: 2
    driver_memory: 4g
    executor_cores: 2
    executor_memory: 4g
    num_executors: 1
    deploy_bucket: my-glue-bucket
    deploy_prefix: dbt-aws
    job_flow_overrides:
      Name: dbt-aws-cluster
      ReleaseLabel: emr-7.5.0
      # ... full EMR RunJobFlow spec (Instances, Roles, BootstrapActions, LogUri)
    aws_conn_id: aws_default
    region_name: eu-west-1
    upload_artefacts_s3_prefix: "s3://my-glue-bucket/dbt-aws/emr_cluster/"
```

### `type: emr_serverless`

```yaml
runners:
  emr_sl:
    type: emr_serverless
    mode: attach                        # "create" also supported
    application_id: 00abc123...
    execution_role_arn: arn:aws:iam::123:role/EmrServerlessJobRole
    deploy_bucket: my-glue-bucket
    deploy_prefix: dbt-aws
    aws_conn_id: aws_default
    region_name: eu-west-1
```

### Common kwargs on every runner

These apply to all runner shapes:

```yaml
runners:
  <name>:
    type: <shape>
    with_deps: true          # default true -- workers install packages.yml deps
    full_refresh: false
    vars_json: null
    upload_artefacts_s3_prefix: null
    state_s3: null
    defer: false
    profile_name: null
    env_vars_json: null
    dbt_extra_flags: []
```

## Validation

Every error raises `RunnerConfigError` at `load_runner_config(...)` call time:

| Failure | Result |
|---|---|
| Unknown top-level key | Error naming the key + valid keys |
| Both `runner:` and `runners:` set | Mutually exclusive |
| Neither set | Must declare one |
| `runners:` set without `default_runner:` | `default_runner` is required |
| `default_runner:` not a key in `runners:` | Error |
| Unknown runner `type:` | Error listing valid types |
| Missing required runner kwarg | Error from the runner's `__init__` |
| Override field not on the selected runner's `OVERRIDE_TYPE` | Error |
| Tag mapped to two different runners in `tag_runners:` | Error |
| Two `mode: create` runners with the same resolved `job_name` | Error (would race on `glue:CreateJob`) |

## Rejected top-level keys: `tag_runners`, `tag_profiles`, `tag_targets`, `tag_groups`

These four top-level tag routing / grouping keys are rejected by the
loader. Use `overrides[tag.<t>]:` entries instead:

```yaml
# NOT accepted -- loader raises RunnerConfigError
tag_runners:
  landing: shell
tag_profiles:
  landing: shell_prof
tag_targets:
  landing: shell_dev
tag_groups:
  gold:
    name: gold_batch
    command: build

# The unified form ---------------------------------------------
overrides:
  tag.landing:
    runner: shell
    profile_name: shell_prof
    target: shell_dev

  tag.gold:
    mode: group
    name: gold_batch
    command: build
```

Why the unified shape:

- **One place per tag.** All the settings that make a tag "landing" live under one
  key -- no split brain across four top-level maps.
- **More fields.** The unified form accepts every field the runner's
  `OVERRIDE_TYPE` supports -- `worker_type`, `command`, `full_refresh`,
  `vars_json`, `timeout_minutes`, etc. The rejected forms were limited
  to a single field each.
- **No silent drops.** Adding a fifth per-tag knob doesn't need a fifth
  top-level key.
- **Discoverable via error.** Loading a rejected YAML shape raises a
  hard error naming the key and printing an inline migration example.

## Mixing YAML + Python

Pass `config=cfg` for the YAML values and layer explicit kwargs on top for
Python-side changes. Explicit kwargs win per field.

```python
cfg = load_runner_config("runners.yml")

dag = DbtDag(
    dag_id="my_dbt",
    project=...,
    project_archive_s3="...",
    config=cfg,                                        # every YAML field
    overrides={                                        # merged with cfg.overrides
        **cfg.overrides,
        "model.proj.special": {"runner": "session_per_node"},
    },
)
```

Historically you had to forward every field manually. That pattern still works
(see the "Advanced" tab at the top of this page) but has to enumerate every field the
YAML uses -- forgetting one is a silent drop.
