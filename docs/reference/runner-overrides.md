# Runner overrides

Each runner declares an `OVERRIDE_TYPE` — a dataclass listing the per-model fields
callers can tweak. Pass overrides at the `unique_id` level via `DbtDag(overrides=...)` or
the YAML `overrides:` block.

```python
dag = DbtDag(
    runners={"spark": GlueSparkRunner(...)},
    default_runner="spark",
    overrides={
        "model.proj.huge_agg": {
            "worker_type": "G.4X",            # GlueSparkOverride field
            "number_of_workers": 8,
            "timeout_minutes": 180,
            "full_refresh": True,
        },
        "model.proj.tiny": {
            "runner": "another_runner",       # dispatch (handled by builder)
            "worker_type": "G.1X",            # validated against `another_runner`'s OVERRIDE_TYPE
        },
    },
)
```

Unknown fields raise `OverrideError` at parse time naming the valid fields for that runner.

## `GlueSparkOverride`

For `GlueSparkRunner`. All fields optional; `None` = use the runner default.

| Field | Type | Meaning |
|---|---|---|
| `job_name` | `str` | Specific Glue Job name (e.g. one managed by your IaC). Overrides the default per-model job naming. |
| `job_name_template` | `str` | Format string with `{node}` placeholder for derived names. |
| `mode` | `"attach" \| "create"` | Lifecycle mode for this one node. |
| `iam_role_name` | `str` | IAM role for this Job. |
| `script_location` | `str` | `s3://...` URI of the entry script. |
| `worker_type` | `str` | `G.1X`, `G.2X`, `G.4X`, `G.8X`, …. |
| `number_of_workers` | `int` | DPU count. |
| `timeout_minutes` | `int` | Glue Job timeout. |
| `full_refresh` | `bool` | dbt `--full-refresh` flag. |
| `vars_json` | `str` | JSON string for `dbt run --vars '{...}'`. |
| `profile_name` | `str` | dbt `--profile` for this single node. See [Routing → YAML unified shape](../concepts/routing.md#yaml-unified-shape). |
| `target` | `str` | dbt `--target` for this single node. Beats runner-level and DAG-level defaults. |
| `command` | `str` | Override the dbt CLI verb for this single node (e.g. `"build"` instead of the auto-derived `"run"`). Forwarded verbatim to the worker. |
| `concurrent_runs` | `"allow" \| "join" \| "queue"` | How the runner handles a duplicate run already in flight. |
| `name_prefix` | `str` | Override the `{prefix}` token used by `resolve_resource_name` for this single node. |
| `resource_tags` | `dict[str, str]` | AWS tags to layer on top of the runner-level `resource_tags`. Shallow-merged (later layers win per key), so a per-model override adds/overrides individual tags without nuking the runner-wide baseline. |
| `spark_conf` | `dict[str, str]` | Per-node Spark configuration overrides (`spark.sql.*`, `spark.default.parallelism`, ...). Shallow-merged with runner + tag layers and materialised into the per-JobRun `--conf` argument (safe under `concurrent_runs='allow'`). Runtime configs only; JVM-start configs like `spark.jars.packages` must live in `create_job_kwargs.DefaultArguments`. See [Example: `spark_conf` vs `--conf`](examples/spark-conf-demo.md) for the merged output at each layer. |
| `spark_conf_replace` | `dict[str, str]` | Escape hatch: REPLACES the merged `spark_conf` from all lower layers when set. Use when a single model needs a totally different Spark profile. Per-node replace beats runner replace; either bypasses the layered merge. |

## `GlueInteractiveSessionOverride`

For `GlueInteractiveSessionRunner` (both `reusable=True` and `=False`).

| Field | Type | Meaning |
|---|---|---|
| `full_refresh` | `bool` | dbt `--full-refresh` flag. |
| `vars_json` | `str` | JSON string for `dbt run --vars '{...}'`. |
| `profile_name` | `str` | dbt `--profile` for this single node. |
| `target` | `str` | dbt `--target` for this single node. |
| `command` | `str` | Override the dbt CLI verb for this single node (e.g. `"build"`). |
| `timeout_minutes` | `int` | Statement timeout. |

!!! note "Session sizing is set on the runner, not per-node"

    `worker_type` / `number_of_workers` are properties of the Glue *Session*, not
    the statement. To run different models on different sizes, declare two separate
    `GlueInteractiveSessionRunner` instances and route between them via
    `tag_runners` or `overrides[uid].runner`.

## `GluePythonShellOverride`

For `GluePythonShellRunner`. Mirrors `GlueSparkOverride` minus Spark-specific compute
fields.

| Field | Type | Meaning |
|---|---|---|
| `job_name` | `str` | Specific Glue Job name. |
| `job_name_template` | `str` | Format string. |
| `mode` | `"attach" \| "create"` | Lifecycle mode. |
| `iam_role_name` | `str` | IAM role. |
| `script_location` | `str` | `s3://...` URI of the entry script. |
| `max_capacity` | `float` | DPUs (0.0625 or 1). |
| `timeout_minutes` | `int` | Glue Job timeout. |
| `full_refresh` | `bool` | dbt `--full-refresh`. |
| `vars_json` | `str` | JSON string for `dbt run --vars '{...}'`. |
| `profile_name` | `str` | dbt `--profile` for this single node. |
| `target` | `str` | dbt `--target` for this single node. |
| `command` | `str` | Override the dbt CLI verb for this single node (e.g. `"build"`). |
| `concurrent_runs` | `"allow" \| "join" \| "queue"` | Duplicate-run policy. |

## `EmrClusterStepOverride`

For `EmrClusterStepRunner`. Applies to a single step submitted to the shared cluster.

| Field | Type | Meaning |
|---|---|---|
| `driver_cores` | `int` | Override `--conf spark.driver.cores`. |
| `driver_memory` | `str` | Override `--conf spark.driver.memory` (e.g. `"8g"`). |
| `executor_cores` | `int` | Override `--conf spark.executor.cores`. |
| `executor_memory` | `str` | Override `--conf spark.executor.memory`. |
| `num_executors` | `int` | Override `--conf spark.executor.instances`. |
| `full_refresh` | `bool` | dbt `--full-refresh`. |
| `vars_json` | `str` | JSON string for `dbt run --vars '{...}'`. |
| `profile_name` | `str` | dbt `--profile` for this single node. |
| `target` | `str` | dbt `--target` for this single node. |
| `command` | `str` | Override the dbt CLI verb (e.g. `"build"`). |

## `EmrServerlessOverride`

For `EmrServerlessRunner`. Applies to a single job submitted to the shared application.

| Field | Type | Meaning |
|---|---|---|
| `full_refresh` | `bool` | dbt `--full-refresh`. |
| `vars_json` | `str` | JSON string for `dbt run --vars '{...}'`. |
| `profile_name` | `str` | dbt `--profile` for this single node. |
| `target` | `str` | dbt `--target` for this single node. |
| `command` | `str` | Override the dbt CLI verb (e.g. `"build"`). |

## Dispatch keys

The builder strips these before validating per-runner fields, so they don't trip
`OverrideError`:

| Key | Layer | Purpose |
|---|---|---|
| `runner` | Dispatch (handled by `_resolve_node_runners`) | Pick which named runner from `runners=` executes this node. |

## Validation precedence

For a node `model.proj.x` with `overrides[model.proj.x] = {"runner": "spark", "worker_type": "G.4X"}`:

1. `runner: "spark"` → handled by builder; `"spark"` must be a key in `runners=`. Stripped
   before per-runner validation.
2. Remaining keys validated against `runners["spark"].OVERRIDE_TYPE` (`GlueSparkOverride`).
   `worker_type: "G.4X"` is a known field — OK. Anything not a `GlueSparkOverride` field
   → `OverrideError`.

If the override carries fields valid for a different runner (e.g. `max_capacity` —
Python-Shell-only) but `runner: "spark"` is set, validation fails because `worker_type`
fits but `max_capacity` doesn't.

## Per-model dbt-side declaration

The same override fields can live in the dbt project under `meta.stratus`:

```sql
{{ config(
    tags=['silver'],
    meta={'stratus': {
        'worker_type': 'G.2X',
        'number_of_workers': 4,
        'full_refresh': true,
    }},
) }}
```

These are merged with Python `overrides` at parse time (Python `overrides` wins).
