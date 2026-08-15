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
| `TESTPYPI_EXTRA_INDEX` | pip flag while `dbt-aws` is PyPI-only. Delete this once we ship to real PyPI. |
| `DBT_AWS_WHEEL_S3_URI` | S3 URI template. `.format(bucket=..., version=...)`. |

## Ready-to-use install strings

Each is the return of one of the helper functions below, computed once at
module load with the default kwargs.

| Constant | Where to plug in |
| --- | --- |
| `GLUE_PY311_PACKAGES` | `DefaultArguments["--additional-python-modules"]` on Glue 5.0 Spark Job. Pair with `"--python-modules-installer-option": TESTPYPI_EXTRA_INDEX`. |
| `GLUE_PY39_PACKAGES` | `DefaultArguments["--additional-python-modules"]` on Glue 3.0 Python Shell. `--extra-index-url` is inlined per-package — don't add `--python-modules-installer-option` (it's silently ignored). |

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
from dbt_aws.compat import GLUE_PY311_PACKAGES, TESTPYPI_EXTRA_INDEX

runner = GlueSparkRunner(
    ...,
    create_job_kwargs={
        "DefaultArguments": {
            "--additional-python-modules": GLUE_PY311_PACKAGES,
            "--python-modules-installer-option": TESTPYPI_EXTRA_INDEX,
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

## Bumping the pins

When we validate a new dbt-core / dbt-duckdb / duckdb combo on real AWS,
we bump the constants here and re-run the internal `dag_test_*` example
DAGs. If they pass, we ship the bump in the next `dbt-aws` release.

If you need to pin to a different set of versions for your own project,
call the helper functions with keyword args — don't monkey-patch the
constants (they're `Final` for a reason: the frozen `*_PACKAGES` strings
are computed at import time and won't see later mutations).
