"""``dbt-aws.spark`` -- Spark-on-AWS runners.

Concrete :class:`dbt_aws.common.Runner` implementations for workloads
that need a Spark JVM:

* Glue Spark Job
* EMR Serverless (Spark application)
* EMR cluster step (existing EMR-on-EC2 cluster)
* Glue Interactive Session

Each runner is a sibling -- no inheritance across runners. Shared
behaviour lives in small composable helpers under
``dbt_aws.spark._shared``.
"""
