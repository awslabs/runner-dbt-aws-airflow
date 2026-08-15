# Task-collapse feature

*Available in dbt-aws  (opt-in).*

**Problem.** By default `DbtDag` produces one Airflow task per runnable
dbt node. For a 17-model medallion project that's 17 Glue Job cold
starts (~60-90 s each) — a lot of wasted wall-clock on tiny models
like SQL views.

**Solution.** dbt-aws can fold a subgraph of dbt nodes into ONE
Airflow task. That single task then invokes
``dbt run --select nodeA nodeB nodeC``, so dbt-core runs the whole
subgraph inside a single worker process — one Glue Job startup, one
Spark session, one dbt manifest load.

Two orthogonal switches:

* ``drop_ephemeral=True`` (default) — filters ``ephemeral`` models
  from the Airflow graph. dbt inlines these as CTEs at compile time
  so running them as their own task is a no-op.
* ``collapse_strategy=...`` — merges adjacent same-runner nodes.
  ``None`` (default) keeps every node as its own singleton task.
  ``"view_chain"`` conservatively folds any ``view`` model with a
  single downstream consumer into that consumer's task.
  ``"aggressive"`` extends this to any linear chain of same-runner
  nodes.

## Enable it

```python
from dbt_aws.common.builder import DbtDag

dag = DbtDag(
    dag_id="medallion_optimised",
    project=ProjectConfig(...),
    runners={"glue_spark": runner},
    default_runner="glue_spark",
    project_archive_s3="s3://.../project.tar.gz",
    collapse_strategy="view_chain",   # <-- opt-in
    drop_ephemeral=True,              # <-- default
    start_date=datetime(2025, 1, 1),
)
```

## Rules the collapse respects

- **One runner per group.** Two nodes routed to different runners
  never merge — the merged ``dbt run`` has to run on one backend.
- **Connected subgraph.** No merging unrelated leaves.
- **Preserves topology.** Between-group Airflow edges match the
  transitive edges between the original dbt nodes.

## What survives, what shrinks

| Attribute | No collapse | ``view_chain`` |
|---|---|---|
| Airflow task count | 1 per dbt node | fewer — merged view+consumer chains become 1 |
| Retry granularity | per node | per group. A failure retries every node in the group |
| Airflow UI graph | flat | flat, fewer nodes |
| OpenLineage events | 1 START/COMPLETE pair per node | dbt-ol still emits per-node events, but they share a group parent |
| dbt-side execution | one invocation per node | one invocation per group |

Retry granularity is the main trade-off: a bug in `sv_fact_orders`
retries `sv_fact_orders + gd_top_customers + gd_monthly_revenue`
together if they've been folded. Fine for tight feedback loops; for
production with expensive downstream models, stay on ``None``.

## Example — TPC-H medallion

A demo project (``dbt_project_shapes``) contains 12 nodes:

- 1 seed / 1 root ``table`` model
- 4 ``view`` models forming fan-in and fan-out chains
- 4 downstream ``table`` consumers
- 1 ``incremental`` model
- 1 ``ephemeral`` hop
- (nested view over the ephemeral hop is a separate ``view`` model)

Under each strategy the shape of the Airflow graph:

```
strategy=None   -> 11 tasks (12 dbt nodes minus 1 ephemeral drop)
strategy=view_chain -> 6 tasks (view+consumer chains folded)
strategy=aggressive -> 6 tasks (same as view_chain here; more shrinkage possible on longer table chains)
```

DAGs to compare side-by-side: the ``dag_test_16_shapes_collapse.py``
example builds both DAGs (``test_16_shapes_no_collapse`` +
``test_16_shapes_view_chain``) against the same runner and archive so
the only difference is the collapse setting.

Real-AWS wall-clock on Glue 5.1 Spark:

| Test | Runners | Wall clock | Airflow tasks |
|---|---|---|---|
| no_collapse | Glue Spark | ~7-8 min (11 cold starts) | 11 |
| view_chain | Glue Spark | ~5 min (6 cold starts) | 6 |

## Iceberg + Materialized Views on Glue 5.1

For an Iceberg + Glue Data Catalog setup, the collapse feature works
identically. A companion demo project (``dbt_project_shapes_iceberg``)
targets Glue 5.1's native Iceberg materialised-view support. See
[docs/how-to/collapse-iceberg-glue.md](../how-to/collapse-iceberg-glue.md).

## Interaction with OpenLineage

Collapse and OpenLineage compose cleanly:

- Each collapsed Airflow task runs one ``dbt run --select a b c``.
- dbt-ol still emits per-node OL events (START/COMPLETE per dbt
  node), so the SMUS lineage graph has the same node-level
  granularity whether or not you collapse.
- The Airflow-level parent facet is still one per DAG run, so all
  events share the same parent_run_id.

You get lineage fidelity without paying for every cold start.
