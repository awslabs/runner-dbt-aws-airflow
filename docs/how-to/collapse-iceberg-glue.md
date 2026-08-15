# Run collapse + Iceberg materialized views on Glue 5.1

This guide runs a dbt project on AWS Glue 5.1 with:

- Native Iceberg tables in the AWS Glue Data Catalog
- Iceberg **materialized views** for models declared as ``+materialized: view``
- Task-collapse: view+consumer subgraphs folded into a single Glue Job

## Why this exists

Glue 5.1 (November 2025) shipped native support for Apache Iceberg
materialized views. Combined with dbt-aws's collapse feature, you get:

- Views that persist across Glue Jobs (Iceberg MVs are catalog-registered)
- Fewer Airflow tasks (collapse folds view+consumer into one dbt run)
- Fewer Glue Job cold starts (~60-90s each)

The catch: Iceberg's ``SparkCatalog`` doesn't support plain Spark
views (``CREATE OR REPLACE VIEW`` fails). Views must be created as
Iceberg **materialized views** — different DDL, different semantics.

## Setup

### 1. Glue version + Spark config

Set ``GlueVersion: "5.1"`` on the runner. Add these Spark options via
``--conf`` (single string, space-separated ``--conf`` prefixes):

```python
_ICEBERG_SPARK_CONF = " ".join([
    "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--conf spark.serializer=org.apache.spark.serializer.KryoSerializer",
    "--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog",
    "--conf spark.sql.catalog.glue_catalog.warehouse=s3://<bucket>/warehouse/",
    "--conf spark.sql.catalog.glue_catalog.type=glue",
    "--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
    "--conf spark.sql.catalog.glue_catalog.glue.region=<region>",
    "--conf spark.sql.catalog.glue_catalog.glue.id=<account-id>",
    "--conf spark.sql.catalog.glue_catalog.glue.account-id=<account-id>",
    # Required for Iceberg materialized views.
    "--conf spark.sql.optimizer.answerQueriesWithMVs.enabled=true",
    "--conf spark.sql.materializedViews.metadataCache.enabled=true",
    # Set Iceberg as the session default catalog.
    "--conf spark.sql.defaultCatalog=glue_catalog",
])
```

Job-level args on the Glue Job:

```python
create_job_kwargs={
    "DefaultArguments": {
        "--additional-python-modules": "...dbt-spark[session]==1.9.3,...",
        "--enable-glue-datacatalog": "true",
        "--datalake-formats": "iceberg",     # Glue 5.1 native Iceberg
        "--user-jars-first": "true",
        "--conf": _ICEBERG_SPARK_CONF,
    },
    "GlueVersion": "5.1",
    ...
}
```

### 2. dbt project setup

`dbt_project.yml`:

```yaml
models:
  <project_name>:
    +materialized: table
    +file_format: iceberg
```

`profiles.yml`:

```yaml
<project_name>:
  target: dev
  outputs:
    dev:
      type: spark
      method: session
      schema: my_iceberg_schema        # will be the Glue database
      host: "NA"
      threads: 1
```

### 3. Custom macro — the key ingredient

Iceberg's ``SparkCatalog`` refuses ``CREATE VIEW`` but accepts
``CREATE MATERIALIZED VIEW``. Add this file at
``<project>/macros/spark_create_view_as.sql``:

```jinja
{% macro spark__create_view_as(relation, sql) -%}
  {%- set catalog = 'glue_catalog' -%}
  {%- set schema = relation.schema -%}
  {#- Prepend catalog to every unqualified ``schema.table`` in the
      SELECT body. Iceberg MVs demand 3-part names. -#}
  {%- set fixed_sql = sql | replace(schema ~ '.', catalog ~ '.' ~ schema ~ '.') -%}
  create materialized view if not exists {{ catalog }}.{{ schema }}.{{ relation.identifier }}
  as
    {{ fixed_sql }}
{%- endmacro %}
```

This macro:

- Rewrites the target from ``<schema>.<view>`` to
  ``glue_catalog.<schema>.<view>``.
- Rewrites every reference to `<schema>.<table>` in the SELECT body to
  ``glue_catalog.<schema>.<table>``, satisfying Glue's
  ``MATERIALIZED_VIEW_REQUIRES_FULLY_QUALIFIED_TABLES`` rule.
- Emits ``CREATE MATERIALIZED VIEW IF NOT EXISTS``, so users still
  declare ``+materialized='view'`` in their model configs -- the
  upgrade is transparent.

**Trade-off**: ``IF NOT EXISTS`` means a second dbt run doesn't update
the view definition. To refresh, drop the MV manually:

```bash
spark-sql -e 'DROP MATERIALIZED VIEW glue_catalog.my_schema.my_view'
```

or add a manual refresh step to your DAG.

### 4. Wire the collapse feature

```python
dag = DbtDag(
    dag_id="my_iceberg_dag",
    project=ProjectConfig(mode="manifest", manifest_path="target/manifest.json"),
    runners={"glue_spark": runner},
    default_runner="glue_spark",
    project_archive_s3="s3://.../project.tar.gz",
    collapse_strategy="view_chain",    # <-- collapse view+consumer chains
    drop_ephemeral=True,
    start_date=datetime(2025, 1, 1),
)
```

## What happens end-to-end

For a project with:

- ``root_table`` (materialized=table, file_format=iceberg)
- ``fanout_view_a`` (materialized=view — upgraded to MV via macro)
- ``fanout_consumer_a`` (materialized=table, file_format=iceberg)

Without collapse: 3 Airflow tasks, 3 Glue Job cold starts.
Total wall-clock ~7-8 minutes.

With ``collapse_strategy="view_chain"``: 2 Airflow tasks
(`root_table`, `[fanout_view_a + fanout_consumer_a]`). Two Glue Job cold starts.
Wall-clock ~5 minutes.

## Measured savings

A representative Iceberg-on-Glue-5.1 pipeline of 12 dbt nodes
(1 seed + 1 root + 4 views + 4 tables + 1 incremental + 1 ephemeral)
with `collapse_strategy="view_chain"`:

- 6 Airflow tasks (from 11 without collapse; 5 view-chain groups
  collapsed).
- ~6 minutes wall-clock per DAG run.
- 4 Iceberg tables + 5 Iceberg materialized views registered in the
  Glue Data Catalog.

## Gotchas we hit and how to avoid

**Stale non-Iceberg tables in Glue Catalog.** If the same
schema previously held plain Spark views, Iceberg's MV creation fails
with ``NoSuchIcebergTableException: Input Glue table is not an iceberg
table``. Use a fresh schema name for the Iceberg + MV variant, or ask
your account admin to drop the stale entries (may need Lake Formation
permissions).

**`s3.legacy.allowNonEmptyLocationInCTAS`.** For plain parquet tables
(not Iceberg), if the S3 location is non-empty from a previous run,
add ``--conf spark.sql.legacy.allowNonEmptyLocationInCTAS=true`` to
your ``--conf`` list. Iceberg doesn't need this — its snapshotting
handles overwrites natively.

**Cross-schema refs.** The macro's rewrite only prepends
``glue_catalog.`` to references matching the CURRENT model's schema.
If a view refs a table in a different schema, extend the macro or use
fully-qualified names in the SQL.

## Related

- [Task collapse](../concepts/collapse.md) — collapse feature reference.
- [OpenLineage + SMUS](../concepts/lineage.md) — lineage on top of
  the collapsed shape.
