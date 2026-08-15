# Security

If you discover a potential security issue in this project we ask
that you notify AWS Security via our [vulnerability reporting page][1].
**Please do not create a public GitHub issue.**

[1]: https://aws.amazon.com/security/vulnerability-reporting/

Include as much of the following as possible when you report:

- A description of the issue and its impact.
- Steps to reproduce, ideally with a minimal test case.
- The affected `runner-dbt-aws-airflow` version(s) and any relevant AWS backends
  (Glue Spark Job, Glue Interactive Session, Glue Python Shell,
  EMR Serverless, EMR-on-EC2).
- Your assessment of severity and any suggested mitigations.

## What to expect

- We acknowledge receipt of your report as soon as we can.
- We investigate and confirm the issue.
- We agree a disclosure timeline with you and coordinate a fix.
- We publish a fix release and credit you in the release notes (with
  your consent).

## Scope

This policy covers the `runner-dbt-aws-airflow` library source code in
this repository. It does not cover:

- Third-party dependencies (`dbt-core`, `apache-airflow`, `boto3`,
  the `apache-airflow-providers-amazon` provider, etc.) — report
  those upstream.
- AWS service vulnerabilities (Glue, EMR, S3, MWAA) — report via
  the AWS security page above.
- Your own AWS account misconfiguration (IAM policies, S3 bucket
  policies, VPC setup). See the [Deployment guide][2] for
  least-privilege setup patterns.

[2]: docs/concepts/deployment.md

## Threat model

`runner-dbt-aws-airflow` runs dbt projects on customer-owned AWS compute
(Glue / EMR) orchestrated from customer-owned Apache Airflow. The
library:

- Reads customer dbt project code, uploads it to a customer-owned
  S3 bucket, and executes it on customer-owned Glue / EMR workers.
- Passes runtime configuration through Airflow task arguments,
  which are visible in the customer's Airflow logs and in the AWS
  Glue Job Run history.
- Assumes the customer's Glue / EMR execution IAM role has been
  scoped by the customer following AWS best practice.

The library does **not** manage the customer's S3 bucket, IAM roles,
VPC configuration, or Airflow instance. Those are customer
responsibilities and are covered in [Deployment → IAM & VPC][3].

[3]: docs/concepts/deployment.md#iam-and-vpc-prerequisites

## Reporting a docs issue

Non-security issues (bugs, feature requests, documentation gaps)
should be filed as [GitHub issues][4].

[4]: https://github.com/awslabs/runner-dbt-aws-airflow/issues
