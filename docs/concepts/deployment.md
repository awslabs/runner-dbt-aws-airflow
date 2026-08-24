# Deployment

The lib ships parse-time helpers that get your dbt project + worker entrypoint script to S3
where the Glue/EMR workers can read them. All of them are **idempotent** — they `HEAD`
the destination first and skip the upload if the content already matches.

## IAM and VPC prerequisites

`dbt-aws` does NOT provision AWS infrastructure. You provide:

- One S3 bucket for the project archive + worker entry script (the
  `deploy_bucket=` on each runner).
- One or more IAM execution roles (Glue / EMR) with least-privilege
  policies scoped to the resources this DAG touches.
- One IAM role for Airflow / MWAA to trigger and monitor Glue / EMR
  jobs.
- VPC + subnet + security groups if your workers or your MWAA
  instance run inside a private VPC.

### Airflow (MWAA) execution role

The role Airflow / MWAA assumes when calling Glue / EMR APIs. Minimum
policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "GlueJobLifecycle",
      "Effect": "Allow",
      "Action": [
        "glue:CreateJob", "glue:UpdateJob", "glue:GetJob", "glue:GetJobs",
        "glue:DeleteJob", "glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns",
        "glue:BatchStopJobRun",
        "glue:TagResource", "glue:UntagResource", "glue:GetTags"
      ],
      "Resource": "arn:aws:glue:<region>:<account>:job/dbt-aws-*"
    },
    {
      "Sid": "GlueSessionLifecycle",
      "Effect": "Allow",
      "Action": [
        "glue:CreateSession", "glue:DeleteSession", "glue:GetSession",
        "glue:RunStatement", "glue:CancelStatement", "glue:GetStatement",
        "glue:ListStatements", "glue:StopSession"
      ],
      "Resource": "arn:aws:glue:<region>:<account>:session/dbt-aws-*"
    },
    {
      "Sid": "EmrServerlessLifecycle",
      "Effect": "Allow",
      "Action": [
        "emr-serverless:CreateApplication", "emr-serverless:DeleteApplication",
        "emr-serverless:GetApplication", "emr-serverless:StartApplication",
        "emr-serverless:StopApplication", "emr-serverless:UpdateApplication",
        "emr-serverless:StartJobRun", "emr-serverless:CancelJobRun",
        "emr-serverless:GetJobRun", "emr-serverless:ListJobRuns",
        "emr-serverless:TagResource", "emr-serverless:UntagResource",
        "emr-serverless:ListTagsForResource"
      ],
      "Resource": "arn:aws:emr-serverless:<region>:<account>:/applications/*"
    },
    {
      "Sid": "EmrOnEc2Lifecycle",
      "Effect": "Allow",
      "Action": [
        "elasticmapreduce:RunJobFlow", "elasticmapreduce:TerminateJobFlows",
        "elasticmapreduce:DescribeCluster", "elasticmapreduce:AddJobFlowSteps",
        "elasticmapreduce:DescribeStep", "elasticmapreduce:ListSteps",
        "elasticmapreduce:CancelSteps", "elasticmapreduce:AddTags",
        "elasticmapreduce:RemoveTags"
      ],
      "Resource": "*"
    },
    {
      "Sid": "PassExecutionRoles",
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": [
        "arn:aws:iam::<account>:role/<glue-execution-role>",
        "arn:aws:iam::<account>:role/<emr-execution-role>"
      ]
    },
    {
      "Sid": "S3ArchiveAccess",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:HeadObject"],
      "Resource": "arn:aws:s3:::<deploy-bucket>/dbt-aws/*"
    },
    {
      "Sid": "S3BucketListForCache",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::<deploy-bucket>",
      "Condition": {"StringLike": {"s3:prefix": "dbt-aws/*"}}
    },
    {
      "Sid": "CloudWatchLogsRead",
      "Effect": "Allow",
      "Action": ["logs:GetLogEvents", "logs:FilterLogEvents", "logs:DescribeLogStreams"],
      "Resource": [
        "arn:aws:logs:<region>:<account>:log-group:/aws-glue/*",
        "arn:aws:logs:<region>:<account>:log-group:/aws/emr-serverless/*"
      ]
    }
  ]
}
```

Drop the EMR statements if you're not using either EMR runner. Drop
the Glue Session statement if you don't use `GlueInteractiveSessionRunner`.

### Glue execution role

The role that the Glue Job / Session assumes at runtime. This is
**separate** from the MWAA role above -- MWAA `PassRole`s this into
`glue:CreateJob`, and Glue then assumes it for the worker container.
Minimum policy for a Glue Spark / Session / Python Shell worker
running dbt models against S3 + Glue Data Catalog:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3DataAccess",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject",
                 "s3:ListBucket", "s3:HeadObject"],
      "Resource": [
        "arn:aws:s3:::<data-bucket>",
        "arn:aws:s3:::<data-bucket>/*",
        "arn:aws:s3:::<deploy-bucket>",
        "arn:aws:s3:::<deploy-bucket>/dbt-aws/*"
      ]
    },
    {
      "Sid": "GlueCatalogRead",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase", "glue:GetDatabases",
        "glue:GetTable", "glue:GetTables",
        "glue:GetPartition", "glue:GetPartitions",
        "glue:BatchGetPartition"
      ],
      "Resource": [
        "arn:aws:glue:<region>:<account>:catalog",
        "arn:aws:glue:<region>:<account>:database/<your-database>",
        "arn:aws:glue:<region>:<account>:table/<your-database>/*"
      ]
    },
    {
      "Sid": "GlueCatalogWrite",
      "Effect": "Allow",
      "Action": [
        "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
        "glue:CreatePartition", "glue:BatchCreatePartition",
        "glue:UpdatePartition", "glue:DeletePartition", "glue:BatchDeletePartition"
      ],
      "Resource": [
        "arn:aws:glue:<region>:<account>:catalog",
        "arn:aws:glue:<region>:<account>:database/<your-database>",
        "arn:aws:glue:<region>:<account>:table/<your-database>/*"
      ]
    },
    {
      "Sid": "CloudWatchLogsWrite",
      "Effect": "Allow",
      "Action": ["logs:CreateLogGroup", "logs:CreateLogStream",
                 "logs:PutLogEvents", "logs:AssociateKmsKey"],
      "Resource": "arn:aws:logs:<region>:<account>:log-group:/aws-glue/*"
    }
  ],
  "AssumeRolePolicyDocument": {
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Service": "glue.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }]
  }
}
```

Security guidance:

- Do NOT attach the AWS-managed `AWSGlueServiceRole` policy in
  production. It grants `s3:*` on every bucket in the account.
- Grant `s3:GetObject` / `s3:PutObject` on the exact bucket + prefix
  you use, not wildcard.
- If you use KMS-encrypted buckets, add `kms:Decrypt` /
  `kms:GenerateDataKey` on the specific key ARN.
- If you use Lake Formation for fine-grained catalog access, add
  `lakeformation:GetDataAccess` and drop the individual Glue table
  permissions (Lake Formation grants replace them).

### EMR execution role

Same shape as the Glue execution role above but the assume-role
principal is `elasticmapreduce.amazonaws.com` (EMR-on-EC2) or
`emr-serverless.amazonaws.com` (EMR Serverless). Add EMR-specific
actions:

- EMR-on-EC2: `ec2:CreateTags`, `ec2:DescribeInstances`,
  `ec2:DescribeVolumes` for cluster launch; the `EMR_EC2_DefaultRole`
  AWS-managed policy is a starting point but should be scoped down
  the same way.
- EMR Serverless: `emr-serverless:StartJobRun`,
  `emr-serverless:GetJobRun` if the workers themselves call back
  into the API (rare; usually only MWAA does).

### VPC configuration

When workers run in a private VPC (no NAT gateway), you MUST provide
endpoints so the worker can reach:

| Service | VPC endpoint type | Purpose |
|---|---|---|
| S3 | Gateway | Project archive + wheel download |
| Glue | Interface | Data Catalog reads/writes |
| CloudWatch Logs | Interface | Log delivery |
| STS | Interface | `sts:AssumeRole` for the execution role |
| Secrets Manager | Interface | If using `env_var()` with SecretsManager references |

Glue Spark Jobs pick up VPC config via a
[Glue Connection](https://docs.aws.amazon.com/glue/latest/dg/populate-add-connection.html)
(type `NETWORK`). Attach it via `create_job_kwargs.Connections`:

```python
GlueSparkRunner(
    mode="create",
    iam_role_name="<your-glue-role>",
    deploy_bucket="<your-deploy-bucket>",
    create_job_kwargs={
        "Connections": {"Connections": ["<your-glue-network-connection>"]},
    },
)
```

Glue Interactive Sessions accept the same connection under
`session_config.Connections`. EMR Serverless takes VPC config on the
application itself (subnet + security group IDs); pass them via
`create_application_kwargs.networkConfiguration`. EMR-on-EC2 takes
subnet + security groups on `job_flow_overrides.Instances`.

For private-subnet workers with no internet, you also need to solve
the dbt-core / dbt-duckdb install path -- see
[Two deployment variants](#two-deployment-variants) below.

## Two deployment variants

Before anything else, decide which of these two shapes matches your networking:

### Variant A — workers have internet (default)

> Most Glue/EMR default-VPC deployments, standard MWAA setups.

- Airflow / MWAA runs **no** `dbt deps`. **Zero `dbt-core` on the Airflow side.**
- CI pipeline (or your local `dbt parse`) generates `manifest.json` and uploads it to S3
  (or ships it alongside the DAG file).
- Workers install `packages.yml` deps themselves before running dbt.

```python
# Airflow / MWAA
ARCHIVE_S3 = build_and_upload_project_archive(
    project_dir=Path("/path/to/dbt_project"),
    cache_dir=Path("/tmp/dbt_aws_cache"),
    bucket="my-bucket",
    prefix="dbt-aws",
    # run_dbt_deps=False is the default -- no dbt-core needed here.
)

GlueSparkRunner(..., with_deps=True)   # default -- worker installs deps
```

**MWAA `requirements.txt` — only the wheel, nothing else:**

```txt title="requirements.txt"
/usr/local/airflow/plugins/runner_dbt_aws_airflow-<version>-py3-none-any.whl
```

**Guaranteed behaviour at MWAA DAG-parse time (checked in this repo):**

- **No `import dbt` anywhere in the lib.** `manifest.json` is parsed as plain JSON.
  You can confirm with `grep -rn '^from dbt\|^import dbt' src/dbt_aws/` — empty output.
- **`dbt deps` does not run.** `_run_dbt_deps` (the Airflow-side helper) short-circuits
  when `run_dbt_deps=False` (the default).
- **No `ModuleNotFoundError`** during DAG parse even without `dbt-core` installed.

### Variant B — workers are air-gapped (or partial-egress)

> Private-subnet Glue / EMR with only VPC endpoints, corporate egress firewalls that
> block `github.com`, or any setup where the wheel-from-S3 fallback is your only
> install path.

- **Bake `dbt_packages/` into the archive on the Airflow side** so workers never
  need to reach GitHub.
- Airflow scheduler / MWAA venv **does** need `dbt-core` (only for `dbt deps`).
- Workers can skip their own deps install — the archive already ships everything.

```python
# Airflow / MWAA
ARCHIVE_S3 = build_and_upload_project_archive(
    project_dir=Path("/path/to/dbt_project"),
    cache_dir=Path("/tmp/dbt_aws_cache"),
    bucket="my-bucket",
    prefix="dbt-aws",
    run_dbt_deps=True,   # <- opt in: bake dbt_packages/ into archive
)

GlueSparkRunner(..., with_deps=False)   # <- workers trust the archive
```

**MWAA `requirements.txt`:**

```txt title="requirements.txt"
dbt-core==1.11.11
/usr/local/airflow/plugins/runner_dbt_aws_airflow-<version>-py3-none-any.whl
```

### Cheat sheet

| Your workers | `run_dbt_deps` (Airflow) | `with_deps` (worker) | `dbt-core` in MWAA reqs |
|---|---|---|---|
| Have internet (default VPC + NAT) | `False` *(default)* | `True` *(default)* | **No** |
| Air-gapped (VPC endpoints only) | `True` | `False` | **Yes** |
| Mixed (some yes, some no) | `True` | `False` | **Yes** — unify on Variant B |

### Recommended pipeline (Variant A, most users)

Run this in CI on every dbt project change; MWAA never touches dbt:

```bash
# In CI / on your laptop
cd path/to/dbt_project
dbt parse --target dev                    # regenerates target/manifest.json
aws s3 cp target/manifest.json \
  s3://my-bucket/dbt-aws/manifests/manifest.json

# Airflow DAG (running on MWAA -- NO dbt-core needed)
dag = DbtDag(
    dag_id="medallion",
    project=ProjectConfig(
        mode="manifest",
        manifest_path="target/manifest.json",  # or an s3:// URI
    ),
    runners={...},
    project_archive_s3=ARCHIVE_S3,  # built by build_and_upload_project_archive
)
```

### Package pins per runner — `dbt_aws.compat`

Each runner has a different Python + pip + system-lib environment. The
[`dbt_aws.compat`](../reference/compat.md) module encodes validated pin
sets as ready-to-use strings so you don't have to work out the quirks:

```python
from dbt_aws.compat import (
    GLUE_PY311_PACKAGES,       # Glue 5.0 Spark Job + Interactive Sessions
    GLUE_PY39_PACKAGES,        # Glue 3.0 Python Shell (has PyPI + duckdb GLIBC pin)
    EMR_CLUSTER_BOOTSTRAP_ARGS,  # EMR-on-EC2 bootstrap args
    TESTPYPI_EXTRA_INDEX,
)

GlueSparkRunner(
    create_job_kwargs={
        "DefaultArguments": {
            "--additional-python-modules": GLUE_PY311_PACKAGES,
            "--python-modules-installer-option": TESTPYPI_EXTRA_INDEX,
        },
    },
)
```

#### EMR Serverless — pre-built venv archive

EMR Serverless doesn't accept `--additional-python-modules`. In
private-subnet deployments (no NAT), workers also can't reach
`extensions.duckdb.org` to auto-download duckdb extensions. Solution:
build a venv-pack archive once and reference it via `spark.archives`.

```bash
# On an AL2023 x86_64 machine (or a matching container). See the
# troubleshooting guide for the full recipe; short version:
python3.11 -m venv /tmp/emr_venv
source /tmp/emr_venv/bin/activate
pip install --upgrade pip venv-pack
pip install \
    "runner-dbt-aws-airflow==<version>" \
    "dbt-core==1.11.11" \
    "dbt-duckdb==1.10.1"
python -c "import duckdb; c = duckdb.connect(); c.execute('INSTALL httpfs'); c.execute('INSTALL aws')"
deactivate
venv-pack -o /tmp/emr_serverless_venv.tar.gz -p /tmp/emr_venv

# Upload to S3.
aws s3 cp /tmp/emr_serverless_venv.tar.gz \
    s3://my-bucket/dbt-aws/venvs/emr_serverless_venv.tar.gz
```

```python
from dbt_aws.compat import DBT_AWS_VERSION, EMR_SERVERLESS_VENV_S3_URI

venv_s3 = EMR_SERVERLESS_VENV_S3_URI.format(
    bucket="my-bucket", version=DBT_AWS_VERSION
)

EmrServerlessRunner(
    ...,
    configuration_overrides={
        "applicationConfiguration": [{
            "classification": "spark-defaults",
            "properties": {
                "spark.archives": f"{venv_s3}#env",
                "spark.pyspark.python": "./env/bin/python",
                "spark.pyspark.driver.python": "./env/bin/python",
                # HOME points at the extracted venv so duckdb finds its
                # pre-installed extensions in ``$HOME/.duckdb/extensions/``.
                # Absolute path required — duckdb rejects ``./env``.
                "spark.emr-serverless.driverEnv.HOME": "/home/hadoop/env",
                "spark.executorEnv.HOME": "/home/hadoop/env",
            },
        }],
    },
)
```

---

## Project archive — `build_and_upload_project_archive`

Tar-gzips the dbt project (skipping `target/`, `logs/`, `.git/`, `__pycache__/`, IDE
state), fingerprints it with sha256, and uploads to a content-addressed S3 key. The
`dbt_packages/` directory IS included so external dbt packages (installed via
`dbt deps`) ship with the project to workers.

```python
from dbt_aws.common.airflow_extras.auto_deploy import build_and_upload_project_archive

ARCHIVE_S3 = build_and_upload_project_archive(
    project_dir=Path("/path/to/dbt_project"),
    cache_dir=Path("/tmp/dbt_aws_cache"),
    bucket="my-glue-bucket",
    prefix="dbt-aws",                # keys become s3://my-glue-bucket/dbt-aws/archives/<sha256>.tar.gz
    region_name="eu-west-1",
    # run_dbt_deps=True is opt-in -- workers handle it by default.
    # Only set True if you want to bake dbt_packages/ into the archive at DAG-parse.
)
# ARCHIVE_S3 == "s3://my-glue-bucket/dbt-aws/archives/da965d...tar.gz"
```

What it does:

1. Walks `project_dir`, including `models/`, `seeds/`, `snapshots/`, `tests/`, `macros/`,
   `analyses/`, `dbt_packages/` and top-level files like `dbt_project.yml`,
   `profiles.yml`. Skips `target/`, `logs/`, `.git/`, IDE state.
2. Streams every file into a `tar.gz` while computing the running sha256.
3. The sha256 is the S3 key — same content → same key, regardless of mtime / order.
4. Caches the archive locally in `cache_dir` keyed by sha256 — re-parses of the DAG file
   skip the tar+upload entirely.
5. `HEAD`s the destination key. If it exists with matching `ContentLength`, skip the
   upload.

The returned `s3://...` URI is what every runner passes to its workers; each worker
`aws s3 cp`s it and untars locally before `cd`ing into the dbt project.

## dbt deps integration — `run_dbt_deps` (opt-in)

Opt-in helper that runs `dbt deps` before archiving so external dbt packages
(declared in `packages.yml` — `dbt_utils`, `dbt_expectations`, `audit_helper`, …)
ship to the workers as part of the archive.

**Default is `False`** — workers handle `dbt deps` themselves (see
the [Worker-side dbt deps](#worker-side-dbt-deps-with_deps-default-true)
section below). Only pass `run_dbt_deps=True` if you specifically want the
archive to include a pre-resolved `dbt_packages/` (e.g. no worker network
access to GitHub, or shaving 3-5s off first-time per-task startup).

```python
ARCHIVE_S3 = build_and_upload_project_archive(
    project_dir=Path("/path/to/dbt_project"),
    cache_dir=Path("/tmp/dbt_aws_cache"),
    bucket="my-glue-bucket",
    prefix="dbt-aws",
    region_name="eu-west-1",
    run_dbt_deps=True,   # explicit opt-in; requires dbt-core in this venv
)
```

What happens:

1. The helper checks for `packages.yml` in `project_dir`. If absent, skip
   silently (common case for projects without external dbt packages).
2. If present, the helper checks for `dbt-core` importability. If absent, warn +
   skip (the Airflow venv doesn't have dbt-core; the workers may install it at
   runtime via `--additional-python-modules`).
3. Otherwise, shell out to `python -m dbt deps --project-dir <project>` once at
   DAG-parse time and populate `dbt_packages/` locally.
4. The archive carries `dbt_packages/` (it was always in the included-dirs list).
5. Workers untar the archive and find every package ready — zero install cost per
   task.

**Graceful skip behaviour ():**

| Situation | Behaviour |
|---|---|
| No `packages.yml` | Skip silently (INFO log). Most projects without external dbt packages. |
| `packages.yml` present but `dbt-core` not in the Airflow venv | Skip with WARNING log (not an error). The DAG parse continues; workers' own dbt install handles the rest. |
| `dbt deps` exits non-zero (real failure) | Raise `ArchiveError` with stdout + stderr. The project DECLARED packages so silent failure would be wrong. |
| `subprocess` can't find `python -m dbt` (PATH oddity) | Raise `ArchiveError` with a clear hint. |

**Opt out:** pass `run_dbt_deps=False` explicitly:

```python
build_and_upload_project_archive(
    ...,
    run_dbt_deps=False,    # skip the helper entirely, even if packages.yml exists
)
```

### MWAA + projects with `packages.yml`

****, MWAA needs **NO extra `dbt-core` install**. Workers handle
dbt package installs themselves via `with_deps=True` (default on every
runner). Your MWAA `requirements.txt` only needs the dbt-aws wheel:

```txt title="requirements.txt for MWAA"
# Install the dbt-aws lib from plugins/ (mounted at /usr/local/airflow/plugins/).
/usr/local/airflow/plugins/runner_dbt_aws_airflow-<version>-py3-none-any.whl
```

Do **NOT** add the upstream Airflow constraints file
(`--constraint https://raw.githubusercontent.com/apache/airflow/constraints-<v>/...`)
to MWAA's `requirements.txt` — it conflicts with MWAA's base-image pins
(most commonly on `apache-airflow-providers-amazon` → `watchtower`) and
breaks the install. MWAA's pre-installed providers are already pinned to
compatible versions.

!!! note "How it works"

    On every worker (Glue Spark Job, Glue Interactive Session, Glue Python Shell,
    EMR Serverless, EMR Cluster Step), the runner adds `--with-deps true` to the
    entry script's argv. The entry script (`dbt_aws.common.runtime`) calls
    `_run_dbt_deps_on_worker(project_dir)` before invoking dbt, which runs
    `python -m dbt.cli.main deps --project-dir <project>` in-place. dbt packages
    (`dbt_utils`, etc.) land in the per-task `dbt_packages/` at worker runtime;
    the Airflow scheduler doesn't need `dbt-core` at all.

    Skip paths (so the worker doesn't pay double-cost when the archive
    already ships them):

    * No `packages.yml` in the archive → skip (info log).
    * `dbt_packages/` already populated with valid `dbt_project.yml` in each
      sub-package → skip (info log).
    * `dbt_packages/` populated but some sub-package is missing its own
      `dbt_project.yml` (partial extract) → wipe + re-run `dbt deps`
      (defensive,.
    * `dbt deps` exits non-zero → raise `RuntimeError`.

### MWAA decision tree

| Your project | What to do on MWAA |
|---|---|
| No `packages.yml` | Nothing. Workers detect + skip silently. |
| Has `packages.yml` | Nothing extra. `with_deps=True` (default) installs on the worker. |
| You want to bake `dbt_packages/` into the archive at DAG-parse | Set `run_dbt_deps=True` on `build_and_upload_project_archive` AND add `dbt-core` to MWAA `requirements.txt`. Rare — the worker path is simpler. |

## Worker-side dbt deps — `with_deps` (default `True`)

Workers install `packages.yml` deps themselves before each dbt invocation,
into the per-task `/tmp/dbt_aws/<run-id>/project/dbt_packages/` working
directory. ** this is the primary path** — the previous
Airflow-side helper (`run_dbt_deps`) is now off by default and only
relevant if you want to bake `dbt_packages/` into the archive at DAG-parse.

```python
GlueSparkRunner(..., with_deps=True)              # default True
GlueInteractiveSessionRunner(..., with_deps=True)
EmrServerlessRunner(..., with_deps=True)
EmrClusterStepRunner(..., with_deps=True)
GluePythonShellRunner(..., with_deps=True)
```

The runner adds `--with-deps "true"` to the worker's script_args (or sets
`with_deps=True` in the Glue Session in-process call kwargs). The worker
entry script (`dbt_aws.common.runtime`) reads the flag and runs
`python -m dbt.cli.main deps --project-dir <project>` via `sys.executable`
before the main dbt command.

!!! note "Why `python -m dbt.cli.main` and not `python -m dbt`?"

    `dbt` is a namespace package (no `__main__.py`), so `python -m dbt` fails.
    The console-script `dbt` isn't on PATH on Glue PySpark workers either
    (its bin/ directory isn't in the executor's PATH). `dbt.cli.main` is
    the entry-point module that dbt-core defines; it works uniformly on
    all worker shapes. Fixed.

**Skip paths** (so the worker doesn't pay double-cost when the archive
already ships them):

| Situation | Behaviour |
|---|---|
| No `packages.yml` in the archive | Skip (info log). |
| `dbt_packages/` already populated AND each sub-package has its own `dbt_project.yml` | Skip (info log). |
| `dbt_packages/` populated but some sub-package is missing `dbt_project.yml` (partial extract) | Wipe + re-run `dbt deps` (defensive,. |
| `dbt deps` exits non-zero | Raise `RuntimeError`. The project DECLARED packages so silent failure would be wrong. |

**Per-task overhead** when actually running: ~3-5 seconds for `dbt deps`
against a cached pip download. First-time runs against fresh workers
download from GitHub + pip; can be 10-20s.

### How the two layers compose

```
AIRFLOW SCHEDULER  (run_dbt_deps=False, default)
  Airflow-side helper is off. No dbt-core needed in the scheduler venv.
  Archive ships without dbt_packages/. Workers install fresh.

WORKER  (with_deps=True, default)
  + dbt_packages/ absent OR partial     -> shell out to ``dbt deps`` locally
                                           (~3-5s overhead per task)
  + dbt_packages/ fully populated       -> skip
  + with_deps=False                     -> caller opts out entirely
```

For MWAA users (default  config):

1. **Scheduler side**: `run_dbt_deps=False` (default). The archive-building
   helper skips entirely; no `dbt-core` needed in MWAA's `requirements.txt`.
2. **Worker side**: archive arrives without `dbt_packages/` → worker runs
   `dbt deps` into `/tmp/dbt_aws/<run-id>/project/dbt_packages/` → main dbt
   invocation finds packages → success.

**MWAA `requirements.txt` only needs the dbt-aws wheel.** Workers handle everything.

### Opting out

Set `with_deps=False` on the runner constructor:

```python
GlueSparkRunner(..., with_deps=False)
```

Use this when you've already shipped pre-resolved `dbt_packages/` via CI and want
strict zero-overhead on the worker.

## Worker entrypoint — content-addressed

The worker entrypoint Python script (the one Glue Spark Jobs / Sessions actually execute)
is shipped to S3 with a **content-addressed key** so old Glue Jobs don't break when the
lib upgrades:

```
s3://<bucket>/<prefix>/worker_entrypoint/<md5>.py
```

For dbt-aws version `X` the key is `s3://.../dbt-aws/worker_entrypoint/8f018c88...py`. When the
lib bumps to a new version and changes the entrypoint logic, a NEW key is uploaded; existing
Glue Jobs keep pointing at the old key until you re-parse + re-deploy.

Implementation lives in `auto_deploy.upload_worker_entrypoint`.

## Wheel fallback for Glue Python Shell

The maintainer publish flow builds the wheel, uploads it to PyPI, AND
uploads it to S3:

```
s3://<bucket>/dbt-aws/wheels/runner_dbt_aws_airflow-<version>-py3-none-any.whl
```

Reason: Glue 3.0 Python Shell can't install from PyPI (silently drops
`--python-modules-installer-option`). Glue Python Shell workers can
instead pull the wheel directly from S3 via
`--additional-python-modules`.

## What gets uploaded for one DAG run

Concrete example — a single `airflow dags trigger` on `dbt_project__all_runners_mix`:

```
At DAG-parse time (once per Airflow scheduler tick):
├── HEAD s3://...dbt-aws/archives/<sha256>.tar.gz       (skipped if present)
├── HEAD s3://...dbt-aws/worker_entrypoint/<md5>.py     (skipped if present)
└── (no actual S3 uploads if both keys already exist)

At task-runtime, every Glue worker:
├── pip install runner-dbt-aws-airflow ... (from PyPI extra-index)
├── aws s3 cp s3://...dbt-aws/archives/<sha256>.tar.gz .
├── tar xzf <sha256>.tar.gz
└── cd dbt_project && dbt run --select <node_name>
```

## Cache behaviour

The local `cache_dir` (e.g. `/tmp/dbt_aws_cache`) holds:

```
cache_dir/
├── archives/
│   └── da965d.../          # content-addressed dir per fingerprint
│       ├── project.tar.gz
│       └── meta.json       # {"fingerprint": ..., "size": ..., "files": 28}
```

A re-parse with no file changes hits the cache in milliseconds (we saw `cache hit
(fingerprint=da965d, size=83792 bytes, files=28, elapsed=3.2 ms)` in the parse log).

To force a fresh archive, delete the cache dir.

## Disable the auto-upload

If you manage the upload externally (CI/CD pipeline, ops-managed S3 sync), just pass the
final `s3://...` URI directly to `DbtDag(project_archive_s3=...)` and skip the helper.
The lib doesn't care how the archive got there.
