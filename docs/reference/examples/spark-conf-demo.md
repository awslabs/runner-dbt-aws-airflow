# Spark config: `spark_conf` vs `DefaultArguments["--conf"]`

Where each Spark config belongs and why. This is the single most common
source of confusion when tuning a `GlueSparkRunner`.

## The rule

| Config lives in... | When Glue applies it | Safe under `concurrent_runs='allow'`? |
| --- | --- | --- |
| `runners.<name>.create_job_kwargs.DefaultArguments["--conf"]` | Once per Job create/update; baked into every JobRun of that Job | Yes (immutable per JobRun) |
| `runners.<name>.spark_conf` | Every JobRun of that Job as a `--conf` argument | Yes |
| `overrides[tag.<t>].spark_conf` / `overrides[<uid>].spark_conf` / `meta.stratus.spark_conf` | Layered on top of the runner `spark_conf`, materialised into the per-JobRun `--conf` | Yes |

Two questions decide the placement:

1. **Does the config need to be set BEFORE Spark's JVM starts?**
   - Yes → `create_job_kwargs.DefaultArguments["--conf"]`.
   - No → `spark_conf` (runner-level, or under any `overrides:` layer).
2. **Do different nodes need different values for the same key?**
   - Yes → put the baseline in `spark_conf`, the deltas in `overrides[...].spark_conf`.
   - No → runner-level `spark_conf` is enough.

Glue **silently ignores** JVM-start configs passed per JobRun. This is
the failure mode you hit if you put `spark.sql.extensions=...` in
`spark_conf`: no error, but Iceberg extensions never load and every
`MERGE INTO` fails with `Table or view not found: glue_catalog.<db>.<t>`.

## Which configs go where

### `DefaultArguments["--conf"]` — JVM-start only

Anything Spark reads during SparkContext / SparkSession initialisation.
These MUST live here, not in `spark_conf`:

- `spark.sql.extensions` — extension classes loaded at driver startup (Iceberg, Delta, etc.)
- `spark.serializer` — Kryo vs Java, chosen at SparkContext init
- `spark.sql.defaultCatalog` + `spark.sql.catalog.<name>.*` — catalog impls registered at driver startup
- `spark.sql.catalog.<name>.http-client.*` — the boto3-shape HTTP client is created once per JVM
- `spark.jars.packages`, `spark.jars`, `spark.jars.repositories`
- `spark.hadoop.fs.*` (S3A tunings)
- `spark.driver.memory`, `spark.driver.cores` (JVM heap / cores are fixed at launch)
- `spark.executor.memory`, `spark.executor.cores`, `spark.executor.instances`
- `spark.dynamicAllocation.*` (executor pool sizing decided at startup)

### `spark_conf` — runtime, per-JobRun, layered per node

Everything Spark reads at query time, per session:

- `spark.sql.session.timeZone`
- `spark.sql.adaptive.*` (AQE toggles, thresholds)
- `spark.sql.autoBroadcastJoinThreshold`, `spark.sql.adaptive.autoBroadcastJoinThreshold`
- `spark.sql.shuffle.partitions`, `spark.sql.adaptive.advisoryPartitionSizeInBytes`
- `spark.sql.files.maxPartitionBytes`
- `spark.sql.legacy.parquet.*RebaseMode*`
- `spark.sql.parquet.outputTimestampType`, `spark.sql.parquet.compression.codec`
- `spark.sql.optimizer.dynamicPartitionPruning.enabled`
- `spark.sql.hive.convertMetastoreParquet`
- `spark.rdd.compress`, `spark.io.compression.codec`
- `spark.default.parallelism`

Borderline (works in `spark_conf` on Glue 5.x but safer on
`DefaultArguments["--conf"]` because some code paths read them at
SparkContext init):

- `spark.driver.maxResultSize`
- `spark.kryoserializer.buffer.max`
- `spark.locality.wait`

When in doubt, put it in `DefaultArguments["--conf"]`. The cost is
zero — the config just applies to every JobRun uniformly.

## Complete demo `runners.yml`

Copy-paste. Fill in bucket / role / account. Two runners, three
override layers, escape-hatch shown.

```yaml
# =====================================================================
# Runners config demonstrating:
#
# 1. DefaultArguments["--conf"] for JVM-start configs (Iceberg catalog,
#    Kryo serializer, HTTP client tunings). Baked into every JobRun.
# 2. Runner-level spark_conf for runtime baselines (AQE, timezone,
#    shuffle partitions, broadcast threshold).
# 3. Per-tag spark_conf that shallow-merges with runner defaults.
# 4. Per-model spark_conf that beats both.
# 5. Escape-hatch spark_conf_replace for models that need a totally
#    different profile.
# =====================================================================

resource_tags:
  CostCenter: data-platform
  Environment: prod

runners:

  glue_spark:
    type: glue_spark
    mode: create
    iam_role_name: AWSGlueServiceRole
    deploy_bucket: my-bucket
    region_name: eu-west-1
    glue_version: "5.1"
    worker_type: G.4X
    number_of_workers: 12
    timeout_minutes: 90
    name_prefix: acme-dbt              # jobs: acme-dbt_model_<name> / acme-dbt_tag_<name>

    resource_tags:
      Runner: glue-spark
      GlueVersion: "5.1"

    # -----------------------------------------------------------------
    # RUNTIME baseline. Shallow-merged with overrides[*].spark_conf
    # layers. Emitted as the JobRun's ``--conf`` argument.
    # -----------------------------------------------------------------
    spark_conf:
      # Session behaviour
      spark.sql.session.timeZone: UTC

      # Parquet compatibility (Spark 3.x rebase quirks)
      spark.sql.legacy.parquet.datetimeRebaseModeInRead: CORRECTED
      spark.sql.legacy.parquet.datetimeRebaseModeInWrite: CORRECTED
      spark.sql.legacy.parquet.int96RebaseModeInRead: CORRECTED
      spark.sql.parquet.outputTimestampType: TIMESTAMP_MICROS

      # AQE + broadcast baselines
      spark.sql.adaptive.enabled: "true"
      spark.sql.adaptive.coalescePartitions.enabled: "true"
      spark.sql.adaptive.skewJoin.enabled: "true"
      spark.sql.autoBroadcastJoinThreshold: "10485760"           # 10 MB (safe)
      spark.sql.adaptive.autoBroadcastJoinThreshold: "10485760"
      spark.sql.shuffle.partitions: "600"

      # DPP
      spark.sql.optimizer.dynamicPartitionPruning.enabled: "true"

    create_job_kwargs:
      ExecutionProperty:
        MaxConcurrentRuns: 2
      DefaultArguments:
        "--additional-python-modules": "dbt-aws,dbt-core==1.11.11,dbt-spark[session]==1.9.3"
        "--job-language": "python"
        "--enable-glue-datacatalog": "true"
        "--datalake-formats": "iceberg"
        "--enable-metrics": "true"
        "--enable-continuous-cloudwatch-log": "true"

        # -------------------------------------------------------------
        # JVM-START configs. Registered at driver startup. Every
        # JobRun of this Job inherits them; per-JobRun ``spark_conf``
        # CANNOT override these (Glue silently ignores them there).
        # -------------------------------------------------------------
        "--conf": >-
          spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
          --conf spark.serializer=org.apache.spark.serializer.KryoSerializer
          --conf spark.sql.defaultCatalog=glue_catalog
          --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog
          --conf spark.sql.catalog.glue_catalog.type=glue
          --conf spark.sql.catalog.glue_catalog.warehouse=s3://my-bucket/gold/
          --conf spark.sql.catalog.glue_catalog.http-client.type=apache
          --conf spark.sql.catalog.glue_catalog.http-client.apache.max-connections=2000
          --conf spark.driver.maxResultSize=8g

default_runner: glue_spark

# =====================================================================
# Overrides. spark_conf here layers on top of runners.glue_spark.spark_conf.
# =====================================================================
overrides:

  # -------------------------------------------------------------------
  # Tag layer: bump shuffle partitions + force skew-join for the
  # wide-fact chain. Every model carrying tag ``dwh_fact_sales`` picks
  # this up automatically.
  # -------------------------------------------------------------------
  tag.dwh_fact_sales:
    mode: group
    name: dwh_fact_sales
    worker_type: G.8X
    number_of_workers: 20
    timeout_minutes: 300
    spark_conf:
      spark.sql.shuffle.partitions: "2000"                        # beats runner's 600
      spark.sql.adaptive.forceOptimizeSkewedJoin: "true"          # additional
      spark.sql.adaptive.advisoryPartitionSizeInBytes: "128MB"
    resource_tags:
      Family: dwh_fact_sales
      SlaTier: gold

  # -------------------------------------------------------------------
  # Per-model layer: dim_customer benefits from a HIGHER broadcast
  # threshold (many small lookups). Same behaviour as the tag layer,
  # only scoped to one model.
  # -------------------------------------------------------------------
  model.my_project.dim_customer:
    worker_type: G.8X
    number_of_workers: 10
    spark_conf:
      spark.sql.autoBroadcastJoinThreshold: "104857600"           # 100 MB (beats runner's 10 MB)
      spark.sql.adaptive.autoBroadcastJoinThreshold: "104857600"

  # -------------------------------------------------------------------
  # Escape hatch: totally different Spark profile for one model.
  # ``spark_conf_replace`` DISCARDS the merged runner + tag layers
  # and only the dict below is sent to Glue.
  # -------------------------------------------------------------------
  model.my_project.dim_legacy_migration:
    spark_conf_replace:
      spark.sql.adaptive.enabled: "false"
      spark.sql.shuffle.partitions: "50"
      spark.sql.session.timeZone: UTC
```

## Effective `--conf` per node

Concrete walkthroughs for the three interesting shapes in the config
above.

### Ordinary model (no tag, no per-model override)

Runner baseline only. `run_job_kwargs.Arguments["--conf"]` becomes:

```
--conf spark.sql.adaptive.coalescePartitions.enabled=true
--conf spark.sql.adaptive.enabled=true
--conf spark.sql.adaptive.skewJoin.enabled=true
--conf spark.sql.autoBroadcastJoinThreshold=10485760
--conf spark.sql.adaptive.autoBroadcastJoinThreshold=10485760
--conf spark.sql.legacy.parquet.datetimeRebaseModeInRead=CORRECTED
--conf spark.sql.legacy.parquet.datetimeRebaseModeInWrite=CORRECTED
--conf spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED
--conf spark.sql.optimizer.dynamicPartitionPruning.enabled=true
--conf spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS
--conf spark.sql.session.timeZone=UTC
--conf spark.sql.shuffle.partitions=600
```

`DefaultArguments["--conf"]` (Iceberg + Kryo + catalogs + maxResultSize)
still applies — it's on the Job spec, not the JobRun.

### Model with tag `dwh_fact_sales`

Runner baseline PLUS three tag-layer keys, three-way merge (tag wins
on `spark.sql.shuffle.partitions`):

```
--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128MB       # from tag
--conf spark.sql.adaptive.coalescePartitions.enabled=true
--conf spark.sql.adaptive.enabled=true
--conf spark.sql.adaptive.forceOptimizeSkewedJoin=true             # from tag
--conf spark.sql.adaptive.skewJoin.enabled=true
--conf spark.sql.autoBroadcastJoinThreshold=10485760
--conf spark.sql.adaptive.autoBroadcastJoinThreshold=10485760
--conf spark.sql.legacy.parquet.datetimeRebaseModeInRead=CORRECTED
--conf spark.sql.legacy.parquet.datetimeRebaseModeInWrite=CORRECTED
--conf spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED
--conf spark.sql.optimizer.dynamicPartitionPruning.enabled=true
--conf spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS
--conf spark.sql.session.timeZone=UTC
--conf spark.sql.shuffle.partitions=2000                           # tag beats runner (600)
```

Job name: `acme-dbt_tag_dwh_fact_sales` (per-tag naming standard).
Airflow task-id prefix: `dwh_fact_sales__glue_spark` (from
`mode: group` + `name: dwh_fact_sales`).

### `dim_legacy_migration` — replace-mode

`spark_conf_replace` wins. Runner defaults and any tag layers are
DROPPED. `run_job_kwargs.Arguments["--conf"]` becomes only:

```
--conf spark.sql.adaptive.enabled=false
--conf spark.sql.session.timeZone=UTC
--conf spark.sql.shuffle.partitions=50
```

Iceberg catalog + Kryo + `spark.driver.maxResultSize` from
`DefaultArguments["--conf"]` still apply. Replace-mode only affects the
runtime layer.

## Common mistakes and how they fail

**1. Putting `spark.sql.extensions` in `spark_conf`.**

```yaml
# WRONG
spark_conf:
  spark.sql.extensions: org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
```

No error at DAG-parse. Glue starts the JVM without the extension.
Every `MERGE INTO glue_catalog.<db>.<t>` fails with:

```
AnalysisException: Table or view not found: glue_catalog.<db>.<t>
```

Move it under `create_job_kwargs.DefaultArguments["--conf"]`.

**2. Putting `spark.driver.memory` in per-model `spark_conf`.**

Same failure mode: silently ignored. Glue uses the Job's default
driver memory. Bump `worker_type` instead (which fixes both driver
AND executor memory in one knob), or hard-code
`spark.driver.memory` in `DefaultArguments["--conf"]` on the runner
if you really need it decoupled.

**3. `--conf` as a script arg key with space in the name.**

```yaml
# WRONG - Glue's --conf parser splits on spaces
spark_conf:
  "spark.sql adaptive.enabled": "true"
```

Rejected at DAG-parse:

```
RunnerConfigError: ... spark_conf key 'spark.sql adaptive.enabled'
contains characters Glue's --conf parser will trip on.
```

**4. `--conf` prefix on the key.**

```yaml
# WRONG - the runner adds --conf for you
spark_conf:
  "--conf spark.sql.shuffle.partitions": "400"
```

Rejected at DAG-parse:

```
RunnerConfigError: ... spark_conf key '--conf spark.sql.shuffle.partitions'
must not include the ``--conf`` prefix; the runner adds it.
```

**5. Numeric value.**

```yaml
# WRONG - Glue's DefaultArguments only accepts strings
spark_conf:
  spark.sql.shuffle.partitions: 400
```

Rejected at DAG-parse:

```
RunnerConfigError: ... spark_conf value for key 'spark.sql.shuffle.partitions'
must be a string, got int.
```

Cast to string: `"400"`.

**6. Assuming `spark_conf` overrides `DefaultArguments["--conf"]`.**

It doesn't. They occupy separate slots — `DefaultArguments` is Job-scoped
(all JobRuns), `spark_conf` is JobRun-scoped (this run only). If you set
`spark.sql.shuffle.partitions=600` in `DefaultArguments["--conf"]` AND
`spark.sql.shuffle.partitions=2000` in `spark_conf`, both go to Glue.
Spark takes the last-registered value (the `spark_conf` one), so the
per-JobRun value wins in practice — but this is Spark's behaviour, not
the library's. Keep the two lists disjoint to avoid confusion.

## When to use `spark_conf_replace`

Use it when a single model needs a Spark profile so different from the
runner defaults that shallow-merging would leave inherited keys
polluting the config. Real examples:

- A **legacy-compatibility migration model** that requires
  `spark.sql.adaptive.enabled=false` to produce byte-identical output
  to a pre-AQE pipeline. Every AQE knob inherited from the runner is
  wrong for this model; replace-mode drops them all.
- A **benchmark run** with `spark.sql.shuffle.partitions=1` to
  intentionally serialise for A/B measurement. You don't want any of
  the production AQE toggles.

Do NOT use `spark_conf_replace` as a shortcut for one-line changes.
The layering system exists so you can bump one key without
re-declaring the baseline. Replace-mode is an escape hatch, not the
default idiom.

## Related pages

- [Runner constructors](../runners.md) — `GlueSparkRunner` kwargs including `spark_conf`, `spark_conf_replace`.
- [Runner overrides](../runner-overrides.md) — `GlueSparkOverride.spark_conf` and `spark_conf_replace` fields.
- [YAML config](../runner-config-yaml.md#per-model-per-tag-spark-config-spark_conf) — full schema.
- [Precedence ladder](../precedence.md) — how layers merge for dict fields (same rule as `resource_tags`).
