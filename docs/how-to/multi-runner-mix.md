# Multi-runner mix

Run different models on different runners in one DAG. The lib wires `parent >> child`
edges across runner boundaries automatically.

## Three runners, four routing layers

```python title="airflow_dag_all_runners_mix.py" hl_lines="2 3 4 9 10 11 18 19 20 24"
RUNNERS = {
    "glue_spark":       GlueSparkRunner(...),
    "session_warm":     GlueInteractiveSessionRunner(reusable=True, ...),
    "session_per_node": GlueInteractiveSessionRunner(reusable=False, ...),
}

# Layer 3: bulk by tag (NEW)
TAG_RUNNERS = {
    "bronze":      "glue_spark",
    "silver,gold": "session_warm",
}

# Layer 1: per-node escape hatch (highest priority)
OVERRIDES = {
    "seed.dbt_project.audit_log": {"runner": "session_per_node"},
}

dag = DbtDag(
    runners=RUNNERS,
    default_runner="session_warm",     # Layer 4: fallback
    tag_runners=TAG_RUNNERS,
    overrides=OVERRIDES,
    # ...
)
```

What ends up where (parse-time log):

```
runner distribution: glue_spark=9, session_per_node=1, session_warm=22
```

- **glue_spark = 9**: 8 bronze models (via `tag_runners`) + 1 `regions` seed (via `overrides`)
  *if you add it*
- **session_warm = 22**: 4 silver + 4 gold (via `tag_runners`) + intermediate + tests +
  remaining seeds (via `default_runner`)
- **session_per_node = 1**: `audit_log` seed (via `overrides`)

## Cross-runner data dependencies

A dbt `ref()` across runner boundaries means the consumer runs on a different runner
process than the producer. To make the ref resolve, materialise the producer's output
on **shared storage** (S3) instead of in-process state.

For `dbt-duckdb`, set `external_root` in `profiles.yml`:

```yaml title="profiles.yml"
dbt_project:
  outputs:
    dev:
      type: duckdb
      path: /tmp/dbt_project.duckdb
      external_root: s3://my-bucket/dbt-aws-airflow-test/external/
      secrets:
        - type: s3
          provider: credential_chain
  target: dev
```

And mark the producer model as `external`:

```yaml title="dbt_project.yml"
models:
  dbt_project:
    bronze:
      +tags: ["bronze"]
      +materialized: external
      +format: parquet
```

Now `br_customer` running in `glue_spark` writes
`s3://my-bucket/.../external/br_customer.parquet`, and `sv_dim_customer` running in
`session_warm` reads it via `read_parquet('s3://.../br_customer.parquet')` — across
runner boundaries.

## Setup/teardown framing

The lib injects per-runner setup/teardown brackets automatically:

```
my_dag
├── glue_spark/                            (no framing needed)
│   ├── seed__regions
│   └── model__br_customer (and 7 more bronze)
├── session_warm/
│   ├── dbt_aws_glue_session__setup        (CreateSession -- INJECTED)
│   ├── model__sv_dim_customer
│   ├── model__sv_fact_orders (and more)
│   ├── model__gd_top_customers (and more)
│   └── dbt_aws_glue_session__teardown     (DeleteSession -- INJECTED)
└── session_per_node/
    └── seed__audit_log.{setup,statement,teardown}    (per-node triplet -- INJECTED)
```

`make_subgroup_setup_task` and `make_subgroup_teardown_task` on each runner declare what
framing they need.

## See it end-to-end

The `airflow_dag_all_runners_mix.py` (Python config) and
`airflow_dag_all_runners_yaml.py` (YAML config) example DAGs are 1:1
mirrors demonstrating the full three-runner mix. See
[Examples](../examples.md).
