"""``dbt-aws.nonspark`` -- non-Spark runners.

Concrete :class:`dbt_aws.common.Runner` implementations for
warehouse-bound dbt adapters that do NOT need a Spark JVM
(``dbt-athena``, ``dbt-redshift``, ``dbt-snowflake``, ``dbt-postgres``,
``dbt-bigquery``, ``dbt-duckdb``, ``dbt-trino``):

* Glue Python Shell

Each runner is a sibling -- no inheritance across runners or across
the ``dbt-aws.spark`` boundary. Adapters that need Spark (e.g.
``dbt-spark[session]``) belong in ``dbt-aws.spark`` instead.
"""
