"""Recommended package versions and worker-install recipes per runner.

Every runner has a slightly different Python + pip + system-lib
environment. This module encodes the version pins that we've
validated end-to-end so users can copy-paste ready-to-use
``--additional-python-modules`` strings, venv-pack references, and
EMR bootstrap args without hunting through the docs.

All values are STRING CONSTANTS -- import them, format them, use them
in your DAG's runner config. Nothing here is dynamic; nothing runs.

Example::

    from dbt_aws.compat import (
        GLUE_PY311_PACKAGES,       # Glue 5.0 Spark Job / warm Session
        GLUE_PY39_PACKAGES,        # Glue 3.0 Python Shell
        EMR_SERVERLESS_VENV_PATH,  # s3://... template
        EMR_CLUSTER_BOOTSTRAP_ARGS,
    )

    GlueSparkRunner(
        create_job_kwargs={
            "DefaultArguments": {
                "--additional-python-modules": GLUE_PY311_PACKAGES,
            },
        },
    )

Or dynamically::

    from dbt_aws.compat import glue_py311_packages

    GlueSparkRunner(
        create_job_kwargs={
            "DefaultArguments": {
                "--additional-python-modules": glue_py311_packages(
                    dbt_aws_version="<version>",
                ),
            },
        },
    )
"""

from __future__ import annotations

from typing import Final

# ---------------------------------------------------------------------------
# The lib's own version + companion package versions we validate against.
# ---------------------------------------------------------------------------

#: The version of ``runner-dbt-aws-airflow`` this file was released with.
#: Bumped by the release process (see
#: ``.github/workflows/publish-pypi.yml`` -- the workflow asserts this
#: constant equals ``pyproject.toml::project.version`` before
#: publishing); update the pins below when this bumps.
DBT_AWS_VERSION: Final[str] = "0.2.0"

#: dbt-core version pinned for Python 3.10+ workers (Glue Spark Job,
#: Glue Interactive Sessions, EMR-on-EC2, EMR Serverless). Latest stable
#: 1.11.x -- supports the ``external`` materialization + dbt-utils 1.3+.
DBT_CORE_VERSION_PY311: Final[str] = "1.11.11"

#: dbt-duckdb adapter for Python 3.10+ workers. Latest 1.10.x with
#: httpfs auto-load + credential_chain S3 secret provider.
DBT_DUCKDB_VERSION_PY311: Final[str] = "1.10.1"

#: dbt-core version pinned for Python 3.9 workers (Glue Python Shell 3.0).
#: dbt-core >= 1.10 requires Python 3.10+. 1.9.10 is the last 1.9.x line.
DBT_CORE_VERSION_PY39: Final[str] = "1.9.10"

#: dbt-duckdb adapter for Python 3.9 workers.
DBT_DUCKDB_VERSION_PY39: Final[str] = "1.9.6"

#: DuckDB Python wheel + extension version for Glue Python Shell 3.0.
#:
#: DuckDB wheels >= 1.3.0 require GLIBC_2.28. Glue Python Shell 3.0 runs
#: on Amazon Linux 2 which ships GLIBC_2.26. The extension files
#: (``httpfs.duckdb_extension``) inherit the same GLIBC dep, so
#: auto-download of extensions ALSO fails with
#: ``version 'GLIBC_2.28' not found``. Pin to the last GLIBC_2.26-
#: compatible line (1.2.x). See
#: https://github.com/duckdb/duckdb/issues/17943.
#:
#: Not needed for Python 3.10+ workers (Glue 5.0 uses AL2023, GLIBC_2.34).
DUCKDB_VERSION_PY39: Final[str] = "1.2.2"

# ---------------------------------------------------------------------------
# S3 URI templates.
# ---------------------------------------------------------------------------

#: S3 URI template for the ``runner-dbt-aws-airflow`` wheel (Glue Python
#: Shell fallback). Format with ``.format(bucket=..., version=...)``.
#:
#: The S3 layout prefix (``dbt-aws/wheels/``) is kept for backward
#: compatibility with existing deployments -- only the wheel filename
#: changes to match the new PyPI distribution name (hatchling emits
#: ``runner_dbt_aws_airflow-<version>-py3-none-any.whl``).
DBT_AWS_WHEEL_S3_URI: Final[str] = (
    "s3://{bucket}/dbt-aws/wheels/runner_dbt_aws_airflow-{version}-py3-none-any.whl"
)

#: S3 URI template for the EMR Serverless venv-pack archive. Contains
#: ``runner-dbt-aws-airflow`` + dbt-core + dbt-duckdb + boto3 +
#: pre-installed duckdb extensions (httpfs, aws) for offline
#: (private-subnet) deployments. See the troubleshooting guide's
#: "duckdb httpfs auto-download times out on EMR Serverless" section
#: for the venv-pack build recipe.
EMR_SERVERLESS_VENV_S3_URI: Final[str] = (
    "s3://{bucket}/dbt-aws/venvs/emr_serverless_venv-{version}.tar.gz"
)


# ---------------------------------------------------------------------------
# ``--additional-python-modules`` recipes.
# ---------------------------------------------------------------------------


def glue_py311_packages(
    *,
    dbt_aws_version: str = DBT_AWS_VERSION,
    dbt_core_version: str = DBT_CORE_VERSION_PY311,
    dbt_duckdb_version: str = DBT_DUCKDB_VERSION_PY311,
) -> str:
    """Return ``--additional-python-modules`` for Python 3.10+ Glue workers.

    Used by Glue Spark Job and Glue Interactive Sessions on Glue 5.0.
    Uses standard comma-separated package names; no inline pip options
    needed.
    """
    return (
        f"runner-dbt-aws-airflow=={dbt_aws_version}"
        f",dbt-core=={dbt_core_version}"
        f",dbt-duckdb=={dbt_duckdb_version}"
    )


def glue_py39_packages(
    *,
    dbt_aws_version: str = DBT_AWS_VERSION,
    dbt_core_version: str = DBT_CORE_VERSION_PY39,
    dbt_duckdb_version: str = DBT_DUCKDB_VERSION_PY39,
    duckdb_version: str = DUCKDB_VERSION_PY39,
) -> str:
    """Return ``--additional-python-modules`` for Glue Python Shell 3.0.

    Glue Python Shell 3.0 = Python 3.9 on Amazon Linux 2 (GLIBC_2.26).
    Two quirks handled here that don't apply to Glue Spark:

    1. ``--python-modules-installer-option`` is documented but silently
       IGNORED on Glue 3.0 Python Shell. Inline pip options are placed
       AFTER the package spec instead (AWS docs sanction this form).
    2. DuckDB >= 1.3 requires GLIBC_2.28; Glue AL2 has 2.26. Pin
       ``duckdb==1.2.2``. See
       https://github.com/duckdb/duckdb/issues/17943.
    """
    return (
        f"runner-dbt-aws-airflow=={dbt_aws_version}"
        f",dbt-core=={dbt_core_version}"
        f",dbt-duckdb=={dbt_duckdb_version}"
        f",duckdb=={duckdb_version}"
        f",boto3>=1.34 --upgrade"
        f",botocore>=1.34 --upgrade"
    )


# ---------------------------------------------------------------------------
# EMR-on-EC2 cluster bootstrap.
# ---------------------------------------------------------------------------


def emr_cluster_bootstrap_args(
    *,
    dbt_aws_version: str = DBT_AWS_VERSION,
    dbt_core_version: str = DBT_CORE_VERSION_PY311,
    dbt_duckdb_version: str = DBT_DUCKDB_VERSION_PY311,
) -> list[str]:
    """Return the ``ScriptBootstrapAction.Args`` list for EMR-on-EC2.

    Used with the ``install_dbt_aws.sh`` bootstrap script on EMR 7.5+
    (Python 3.11 on Amazon Linux 2023). Pass the return value to
    ``EmrClusterStepRunner.job_flow_overrides.BootstrapActions``.
    """
    return [
        f"runner-dbt-aws-airflow=={dbt_aws_version}",
        f"dbt-core=={dbt_core_version}",
        f"dbt-duckdb=={dbt_duckdb_version}",
    ]


# ---------------------------------------------------------------------------
# Convenience aliases (frozen strings, no version placeholders).
# ---------------------------------------------------------------------------

#: Ready-to-use ``--additional-python-modules`` for Glue Python 3.11.
GLUE_PY311_PACKAGES: Final[str] = glue_py311_packages()

#: Ready-to-use ``--additional-python-modules`` for Glue Python 3.9.
GLUE_PY39_PACKAGES: Final[str] = glue_py39_packages()

#: Ready-to-use bootstrap args for EMR-on-EC2.
EMR_CLUSTER_BOOTSTRAP_ARGS: Final[list[str]] = emr_cluster_bootstrap_args()


__all__ = [
    # versions
    "DBT_AWS_VERSION",
    "DBT_CORE_VERSION_PY311",
    "DBT_DUCKDB_VERSION_PY311",
    "DBT_CORE_VERSION_PY39",
    "DBT_DUCKDB_VERSION_PY39",
    "DUCKDB_VERSION_PY39",
    # URIs / URL templates
    "DBT_AWS_WHEEL_S3_URI",
    "EMR_SERVERLESS_VENV_S3_URI",
    # runner-ready strings
    "GLUE_PY311_PACKAGES",
    "GLUE_PY39_PACKAGES",
    "EMR_CLUSTER_BOOTSTRAP_ARGS",
    # constructors (parameterised)
    "glue_py311_packages",
    "glue_py39_packages",
    "emr_cluster_bootstrap_args",
]
