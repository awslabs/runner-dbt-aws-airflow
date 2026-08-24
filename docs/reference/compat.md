# Package-version reference (`dbt_aws.compat`)

Every AWS runner has a different Python + pip + system-lib environment.
The `dbt_aws.compat` module encodes the version pins we have validated
end-to-end so you can drop ready-to-use install strings straight into
your runner config.

Nothing in `compat` runs at import time. Every value is either a plain
string constant or a pure function that returns a string. Safe to import
inside a DAG-parse hot path.

## Constants

| Name | Meaning |
| --- | --- |
| `DBT_AWS_VERSION` | Version of the library this module was released with. Bumped by the maintainer release flow. |
| `DBT_CORE_VERSION_PY311` | dbt-core for Python 3.10+ workers (Glue Spark, Glue Interactive Sessions). |
| `DBT_DUCKDB_VERSION_PY311` | dbt-duckdb adapter for Python 3.10+ workers. |
| `DBT_CORE_VERSION_PY39` | dbt-core for Python 3.9 workers (Glue Python Shell 3.0). Pinned to a stable release to avoid a broken PyPI shadow copy. |
| `DBT_DUCKDB_VERSION_PY39` | dbt-duckdb adapter for Python 3.9 workers. |
| `DUCKDB_VERSION_PY39` | DuckDB for Python 3.9 workers. **Must** be pinned — DuckDB 1.3.0+ wheels require GLIBC_2.28 but Glue Python Shell 3.0 ships GLIBC_2.26 (Amazon Linux 2). See [duckdb#17943](https://github.com/duckdb/duckdb/issues/17943). |
| `DBT_AWS_WHEEL_S3_URI` | S3 URI template for a self-hosted mirror of the wheel (Glue Python Shell fallback). `.format(bucket=..., version=...)`. |

## Ready-to-use install strings

Each is the return of one of the helper functions below, computed once at
module load with the default kwargs.

| Constant | Where to plug in |
| --- | --- |
| `GLUE_PY311_PACKAGES` | `DefaultArguments["--additional-python-modules"]` on Glue 5.0 Spark Job. |
| `GLUE_PY39_PACKAGES` | `DefaultArguments["--additional-python-modules"]` on Glue 3.0 Python Shell. Inline pip flags per-package; don't add `--python-modules-installer-option` (silently ignored on 3.0). |

## Helper functions

### `glue_py311_packages(*, dbt_aws_version, dbt_core_version, dbt_duckdb_version)`

Returns the `--additional-python-modules` string for a Python 3.10+ Glue
worker. All args default to the module-level constants. Override any of
them to test a newer dbt-core.

### `glue_py39_packages(*, dbt_aws_version, dbt_core_version, dbt_duckdb_version, duckdb_version)`

Returns the `--additional-python-modules` string for Glue Python Shell 3.0.
Handles three environment quirks that don't apply to Glue Spark:

1. `--python-modules-installer-option` is documented but silently ignored
   on Glue 3.0 Python Shell. `--extra-index-url` is inlined on the
   dbt-aws chunk instead.
2. PyPI hosts a broken shadow of one dbt-core patch release. Pinned
   to a version that only lives on real PyPI, so pip's resolver cleanly
   uses real-PyPI metadata.
3. DuckDB 1.3+ requires GLIBC_2.28. Glue AL2 has 2.26. Pinned to a
   compatible DuckDB release.

Also `--upgrade`s boto3 + botocore because Glue 3.0 pre-installs versions
too old for the dbt-aws worker entry code.

## Usage patterns

### Frozen constants (recommended for most cases)

```python
from dbt_aws.compat import GLUE_PY311_PACKAGES

runner = GlueSparkRunner(
    ...,
    create_job_kwargs={
        "DefaultArguments": {
            "--additional-python-modules": GLUE_PY311_PACKAGES,
        },
    },
)
```

### Parametrised (test a different dbt-core version)

```python
from dbt_aws.compat import glue_py311_packages

runner = GlueSparkRunner(
    ...,
    create_job_kwargs={
        "DefaultArguments": {
            "--additional-python-modules": glue_py311_packages(
                dbt_core_version="1.12.0",
            ),
        },
    },
)
```

### S3 URIs

```python
from dbt_aws.compat import DBT_AWS_VERSION, DBT_AWS_WHEEL_S3_URI

wheel_s3 = DBT_AWS_WHEEL_S3_URI.format(
    bucket="my-bucket",
    version=DBT_AWS_VERSION,
)
```

## Compatibility matrix

Glue and EMR runtime versions map to different Python and Spark
versions. The adapter you install (`dbt-spark`, `dbt-duckdb`,
`dbt-athena`, ...) must be compatible with both. Recommendations
below reflect what the maintainers verified end-to-end on real AWS.

| Glue version | Python | Spark | Recommended dbt-core | Recommended adapter | Notes |
|---|---|---|---|---|---|
| **3.0** (Python Shell) | 3.9 | — | 1.9.10 | `dbt-duckdb==1.9.6` + `duckdb==1.2.2` | Only version supporting Glue Python Shell. GLIBC_2.26. |
| 4.0 | 3.10 | 3.3 | 1.11.x | `dbt-spark==1.9.3` or `dbt-duckdb==1.10.1` | Previous default. |
| **5.0** (recommended) | 3.11 | 3.5.x | 1.11.11 | `dbt-spark[session]==1.9.3` or `dbt-duckdb==1.10.1` | Verified end-to-end (see below). |
| 5.1 | 3.11 | 3.5.6 | 1.11.11 | `dbt-spark[session]==1.9.3` or `dbt-duckdb==1.10.1` | Current Glue default. Adds native Iceberg. |
| 6.0 | 3.13 | 4.1.1 | — | — | dbt-core does not yet support Python 3.13; dbt-spark does not yet advertise Spark 4.x support. Wait for adapter updates. |

EMR-on-EC2 and EMR Serverless map similarly:

| EMR release | Python | Spark | Notes |
|---|---|---|---|
| emr-6.15.0 | 3.7 | 3.4.1 | Legacy; avoid for new work. |
| emr-7.2.0 | 3.9 | 3.5.1 | Compatible with dbt-core 1.9.10. |
| **emr-7.5.0** (recommended) | 3.11 | 3.5.x | Verified end-to-end (see below). |

## Verified end-to-end (2026-08-24)

Run against real AWS in `us-east-1`, using `runner-dbt-aws-airflow 1.0.0`
+ `dbt-core 1.11.11` + `dbt-spark[session] 1.9.3`:

| Runner | Backend | Status |
|---|---|---|
| `GlueSparkRunner` | Glue 5.0 Spark Job | PASS |
| `GlueInteractiveSessionRunner` | Glue 5.0 warm session, `RunStatement` | PASS |
| `EmrServerlessRunner` | emr-7.5.0 Spark job | PASS |
| `EmrClusterStepRunner` | emr-7.5.0 single-node cluster + step + auto-terminate | PASS |

Every runner reached its terminal SUCCESS state and cleaned up its
own AWS resources. Full driver + captured logs live in the
maintainer's local checkout (not committed).
