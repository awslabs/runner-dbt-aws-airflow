# Concepts

Three layers stacked together produce a dbt-aws DAG:

1. **[Architecture](architecture.md)** — how dbt-aws turns a dbt project into a set of
   Airflow tasks. The dispatch pipeline: manifest load → selector → per-node runner
   resolution → `make_task` → wire edges → wrap in TaskGroups.
2. **[Runners](runners.md)** — each runner is the *backend* that executes a single dbt
   model on AWS. Four shapes are first-class:
    - **Glue Spark Job** — one Glue Job per model (or per-DAG), `glue:CreateJob` + `StartJobRun`.
    - **Glue Interactive Session (warm)** — one shared session, `CreateSession` once, all
      models submit statements to it.
    - **Glue Interactive Session (per-node)** — one session per model, ephemeral.
    - **Glue Python Shell** — small non-Spark jobs. *(Currently disabled in demos — see
      [Known issues](../troubleshooting.md#glue-python-shell-30-install-pipeline).)*
3. **[Routing](routing.md)** + **[Visual grouping](visual-grouping.md)** — independent
   features that compose:
    - `tag_runners` decides *which runner* executes a model (bulk by tag).
    - `task_groups` decides *which UI folder* the task lives in (visual nesting).
    - `overrides` is the per-node escape hatch (wins over both).
4. **[Deployment helpers](deployment.md)** — how the lib gets your dbt project + worker
   entrypoint script to the Glue/EMR workers. Content-addressed S3 keys, idempotent
   uploads, parse-time `HEAD`-and-skip.
