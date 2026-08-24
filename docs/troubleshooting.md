# Troubleshooting

Known issues, their root causes, and workarounds.

## Glue Python Shell 3.0 install pipeline

**Symptom:** any of

1. `ModuleNotFoundError: No module named 'dbt_aws'` after apparently-successful
   `--additional-python-modules` install.
2. `pip install dbt-core==1.9.9` pulls a Jinja2==3.1.2 that conflicts with
   `dbt-common`'s `jinja2>=3.1.3`.
3. `IO Error: Extension "httpfs.duckdb_extension" could not be loaded:
   /lib64/libc.so.6: version 'GLIBC_2.28' not found`.

**Root causes** (three independent quirks compound on Glue 3.0 Python Shell,
which runs Python 3.9 on Amazon Linux 2):

1. `--python-modules-installer-option` is documented but silently IGNORED
   on Glue 3.0 Python Shell. Only Glue Spark honours it. `pip` never sees
   the `--extra-index-url` -> `runner-dbt-aws-airflow` fails to resolve
   from PyPI.
2. PyPI hosts a **broken shadow copy** of `dbt-core==1.9.9` with a
   `Jinja2==3.1.2` exact pin. Real PyPI's 1.9.9 is fine, but pip's
   resolver picks PyPI's when both indexes are in the list.
3. DuckDB wheels >= 1.3.0 require GLIBC_2.28 (linked against
   manylinux_2_28); Glue AL2 has 2.26. The extension files inherit the
   same GLIBC dep so extension auto-download fails too. See
   [duckdb#17943](https://github.com/duckdb/duckdb/issues/17943).

**Fix:** use [`dbt_aws.compat.GLUE_PY39_PACKAGES`](reference/compat.md) as
the value of `--additional-python-modules` — all three quirks are
handled:

```python
from dbt_aws.compat import GLUE_PY39_PACKAGES
from dbt_aws.nonspark.runners import GluePythonShellRunner

runner = GluePythonShellRunner(
    ...,
    create_job_kwargs={
        "DefaultArguments": {
            "--additional-python-modules": GLUE_PY39_PACKAGES,
        },
        "GlueVersion": "3.0",
    },
)
```

The string it expands to:

```
runner-dbt-aws-airflow==<version>,
dbt-core==1.9.10,
dbt-duckdb==1.9.6,
duckdb==1.2.2,
boto3>=1.34 --upgrade,
botocore>=1.34 --upgrade
```

Glue parses this comma-separated, joins it into one `pip install ...`
command, and pip sees the per-package flags per AWS's [documented
syntax](https://docs.aws.amazon.com/glue/latest/dg/add-job-python.html).
The `--upgrade` on boto3+botocore replaces Glue's pre-installed
baseline (which the dbt-aws worker entry code needs).

**Why the `dbt-core` and `duckdb` pins?** DuckDB 1.3.0 (and every
version since)
ships wheels compiled against manylinux_2_28 which needs GLIBC 2.28.
Glue Python Shell 3.0's runtime is Amazon Linux 2 with GLIBC 2.26.
Same GLIBC constraint applies to the auto-downloaded extension files
(`httpfs.duckdb_extension`, `aws.duckdb_extension`). The last
GLIBC_2.26-compatible line is 1.2.x; 1.2.2 is our validated pin.

## Airflow triggerer wedges under high fan-out

**Symptom:** Tasks stay `deferred` indefinitely even though the underlying AWS work
has completed. Triggerer log shows `Triggerer is blocked for X.X seconds`.

**Root cause** — NOT a lib issue, NOT an Amazon-provider issue. The Amazon
provider's deferrable triggers (`GlueJobCompleteTrigger`, `EmrAddStepsTrigger`,
etc.) already use `aiobotocore` correctly via `await hook.get_async_conn()` and
`async_wait`. The wedge is a **deployment-level** problem, not a code-level one.

Three real causes, in order of likelihood:

1. **SQLite metastore contention.** Airflow standalone defaults to SQLite, which
   serialises all DB writes through a single lock. With N concurrent triggers all
   updating the `trigger` table, the lock contends and the triggerer's
   `cleanup_finished_triggers` cycle stalls. The `"blocked for X.X seconds"`
   warning is the triggerer noticing it couldn't yield to the loop for that long.
2. **One triggerer process.** Airflow standalone runs ONE triggerer. Under high
   trigger churn (e.g. 8 bronze Glue Job tasks all submitting + polling
   simultaneously), that single event loop has to schedule a lot of coroutines on
   one CPU core. Production setups run multiple triggerer replicas.
3. **`cleanup_finished_triggers` is synchronous.** When many triggers reach a
   terminal state in the same tick, the cleanup pass takes a noticeable wall-clock
   time and shows up as a "blocked" warning even though no actual code is wrong.

**Fixes** — every fix is deployment-level, no lib changes needed:

* **Production:** use MWAA or EKS-deployed Airflow. Both run multiple triggerer
  replicas + Postgres/MySQL metastore by default. Wedge gone.
* **Local development:** point Airflow at a local Postgres (or LocalStack RDS).
  The SQLite lock contention disappears. Example with `finch` / Docker:
  ```bash
  finch run -d --name airflow-pg -e POSTGRES_USER=airflow \
      -e POSTGRES_PASSWORD=airflow -e POSTGRES_DB=airflow \
      -p 5433:5432 postgres:15-alpine
  # then set in airflow.cfg:
  # sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@127.0.0.1:5433/airflow
  uv add --group dev psycopg2-binary asyncpg
  uv run airflow db migrate
  ```
* **Quick mitigation:** limit bronze fan-out via Airflow [pools](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/pools.html)
  so fewer triggers fire concurrently:
  ```python
  DbtDag(..., airflow_kwargs_per_task={"pool": "bronze", "pool_slots": 1})
  ```
  Then create a `bronze` pool with limited slots in the Airflow UI.
* **Non-deferrable fallback:** for the Glue Interactive Session runner
  specifically, pass ``deferrable=False`` on the runner. Tasks then sync-poll on
  a worker slot instead of handing off to the Triggerer. Costs one worker slot
  per active task; sidesteps the Triggerer entirely on constrained deployments.
  Prefer ``True`` on MWAA / production.
  ```python
  from dbt_aws.spark.runners import GlueInteractiveSessionRunner

  runner = GlueInteractiveSessionRunner(
      iam_role_arn="...",
      reusable=True,
      deferrable=False,  # only for constrained-Triggerer deployments
  )
  ```
  ``GlueSparkRunner``, ``EmrClusterStepRunner``, ``EmrServerlessRunner`` also
  accept ``deferrable=False`` (their default is ``True``).

**Why the lib doesn't "fix" this:** the lib's own custom triggers
(`GlueSessionReadyTrigger`, `GlueStatementTrigger`) already use
`asyncio.to_thread`-wrapped boto3 (via the shared
`dbt_aws.common.async_polling.poll_until_terminal` helper) and behave correctly
under fan-out. The Amazon-provider triggers we defer into are also async-correct.
The wedge is purely an Airflow-deployment scaling concern.
## Local Airflow on macOS: DAG parse hangs

**Symptom:** on macOS running `airflow standalone`, DAG-processor logs show
``dag_medallion_v030.py`` (or any DAG that calls ``boto3`` at import time)
running for 30-90 seconds without progress, then killed with
``exceeding the timeout of 50.00 seconds``. Same DAG parses in 2-3 seconds
from a plain shell.

**Root cause:** Airflow's dag-processor forks subprocesses (via multiprocessing
``fork`` start method). macOS 10.13+ isn't fork-safe once the Objective-C
runtime is initialised. ``boto3``'s config resolution transitively imports
``urllib.request`` → ``_scproxy`` (Apple's system proxy resolver), which
initialises Objective-C on first use; every subsequent ``fork()`` then
deadlocks in the child at the first proxy lookup.

**Fix (macOS local dev only):** set two env vars BEFORE starting Airflow:

```bash
export no_proxy='*'                          # bypass macOS proxy resolver
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES  # tell Objective-C to allow fork
uv run airflow standalone
```

Neither is needed on MWAA / Linux. The `no_proxy='*'` alone usually suffices
(disables the offending `_scproxy` call). The Objective-C env var is a
belt-and-braces backup.

See [Apple's fork-safety docs](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man3/pthread_atfork.3.html)
for background.
## Cross-runner refs: `Catalog Error: Table with name X does not exist`

**Symptom:** a downstream dbt model routed to a different runner than its
upstream fails at compile time with:

```
Runtime Error in model sv_dim_part (models/silver/sv_dim_part.sql)
  Catalog Error: Table with name br_part does not exist!
  LINE 15:     select * from "dbt_project"."main"."br_part"
```

**Root cause:** with dbt-duckdb's ``materialized: external``, dbt writes the
model to S3 Parquet AND creates a view over that file in the current session's
DuckDB catalog. Every runner (Glue Spark Job, Glue Session, EMR) has its own
ephemeral DuckDB, so the downstream node on a DIFFERENT runner has an empty
catalog and ``ref('br_part')`` fails.

**Fix:** register upstream external models as views on every dbt invocation.
Add an ``on-run-start`` hook to ``dbt_project.yml``:

```yaml
# dbt_project.yml
on-run-start:
  - "{{ register_upstream_external_models() }}"

models:
  my_project:
    bronze:
      +materialized: external
      +format: parquet
```

The macro is built into dbt-duckdb; it walks the manifest, finds every
materialization=external upstream, and creates a DuckDB view pointing at the
S3 Parquet URI (using the ``external_root`` from ``profiles.yml``). After the
hook runs, ``ref('br_part')`` resolves to a query-able view even in a fresh
DuckDB session.

See dbt-duckdb's [docs](https://docs.getdbt.com/reference/resource-configs/duckdb-configs)
for the full plugin API. Not needed if every model in the DAG runs on the
same reusable Glue Interactive Session (one shared DuckDB catalog).
## EMR Cluster Step: `A job flow that is shutting down ... may not be modified`

**Symptom:** the per-node ``.statement`` task (which submits
``AddJobFlowSteps`` to the cluster the ``.setup`` task just created) fails
with:

```
ClientError: An error occurred (ValidationException) when calling the
AddJobFlowSteps operation: A job flow that is shutting down, terminated,
or finished may not be modified.
```

**Root cause:** ``job_flow_overrides.Instances.KeepJobFlowAliveWhenNoSteps``
defaults to ``False``. When ``EmrCreateJobFlowOperator`` creates the cluster,
no steps are attached yet; EMR sees an empty step list and auto-terminates.
By the time the ``.statement`` task fires ``AddJobFlowSteps``, the cluster is
already in ``TERMINATING`` state.

**Fix:** set ``KeepJobFlowAliveWhenNoSteps=True`` in the runner's
``job_flow_overrides``. The lib's per-node teardown task terminates the cluster
explicitly after the statement completes (or fails), so ``KeepJobFlowAlive``
is the right shape for the setup / statement / teardown pattern:

```python
EmrClusterStepRunner(
    mode="create",
    reusable=False,
    auto_terminate=True,
    job_flow_overrides={
        "ReleaseLabel": "emr-7.5.0",
        "Applications": [{"Name": "Spark"}],
        "Instances": {
            "InstanceGroups": [...],
            "Ec2SubnetId": SUBNET,
            "KeepJobFlowAliveWhenNoSteps": True,   # <-- required
            "TerminationProtected": False,
        },
        "BootstrapActions": [...],
        "ServiceRole": "EMR_DefaultRole_V2",
        "JobFlowRole": "EMR_EC2_DefaultRole_V2",
    },
    ...
)
```
## EMR Serverless: `Application cannot be STARTED. Applications must be in [STOPPED, CREATED]`

**Symptom:** two tasks routed to the SAME ``serverless_create`` runner (both
call ``EmrServerlessStartJobOperator`` on the same lib-created application)
fail concurrently with:

```
ValidationException: An error occurred (ValidationException) when calling
the StartApplication operation: Application cannot be STARTED. Applications
must be in one of the following statuses: [STOPPED, CREATED]
```

**Root cause:** race in Airflow's Amazon-provider operator. It checks state
(``STARTED`` in ``APPLICATION_SUCCESS_STATES``) before calling ``StartApplication``,
but the check-then-start is not atomic. Timeline:

1. Task A: state=``STOPPED`` → call ``StartApplication`` → app goes to ``STARTING``
2. Task B (0.1s later): state=``STARTING`` → ``STARTING`` not in ``[CREATED,
   STARTED]`` → call ``StartApplication`` → ``ValidationException``

Airflow's ``APPLICATION_SUCCESS_STATES = {"CREATED", "STARTED"}`` should include
``STARTING`` — that's an upstream bug in `apache-airflow-providers-amazon`.

**Fixes:**

1. **Route concurrent tasks to different runners** so they don't call
   ``StartApplication`` on the same app. E.g. run ``sv_fact_lineitem`` on
   ``serverless_create`` and ``sv_fact_orders`` on ``serverless_attach``
   (a pre-warmed CFN application).
2. **Reduce parallelism:** ``DbtDag(..., max_active_tasks=1)`` for the
   serverless_create runner — blunt but effective.
3. **Retries + long IdleTimeout:** set ``IdleTimeoutMinutes: 60`` on the app
   (so it stays in ``STARTED`` between concurrent submissions) and add
   ``airflow_kwargs_per_task={"retries": 2}`` — the retry will find the app
   in ``STARTED`` on second attempt.
## Glue Interactive Session TIMES OUT during a long pipeline

**Symptom:** downstream tasks routed to a warm/reusable Glue Interactive
Session fail with:

```
InvalidInputException: An error occurred (InvalidInputException) when calling
the RunStatement operation: Session is not ready,
session_id=medallion-warm-..., status=TIMEOUT
```

**Root cause:** Glue Interactive Sessions have an ``IdleTimeout`` (default 30
minutes). If your bronze layer takes 10 minutes and silver-gold takes another
30 minutes with slow cross-runner steps (e.g. EMR Cluster Step setup takes
5 minutes), the warm session sits idle in-between and expires.

**Fix:** bump ``idle_timeout_minutes`` on the runner to comfortably exceed
your pipeline's worst-case duration. 60 minutes is a safe default for a full
medallion:

```python
GlueInteractiveSessionRunner(
    iam_role_arn="...",
    reusable=True,
    idle_timeout_minutes=60,   # was 15 -- too short for multi-runner pipelines
    timeout_minutes=90,        # hard ceiling on total session lifetime
    ...
)
```
## EMR Cluster Step teardown: `Cluster id 'None' is not valid`

**Symptom:** the per-node ``.teardown`` task in a non-reusable EMR Cluster
Step TaskGroup fails with:

```
InvalidRequestException: An error occurred (InvalidRequestException) when
calling the DescribeCluster operation: Cluster id 'None' is not valid.
```

**Root cause:** the ``.setup`` task's upstream (an earlier dbt model in the
DAG) failed, so ``.setup`` became ``upstream_failed`` (never ran, XCom empty).
The ``.teardown`` task's ``trigger_rule='all_done'`` fired anyway, and the
Jinja template ``{{ ti.xcom_pull(task_ids='.setup') }}`` rendered to the
string ``"None"``. ``DescribeCluster(cluster_id='None')`` errors out.

**Fixed automatically** — the lib wraps the teardown operator in
``_ResilientEmrTerminate`` which raises ``AirflowSkipException`` when the
resolved ``job_flow_id`` is ``None`` or empty. Present in
``runner-dbt-aws-airflow`` from the first public release.
## `retries=N` on EMR Cluster Step tasks races the teardown

**Symptom:** with ``default_args={"retries": 2}`` or
``airflow_kwargs_per_task={"retries": 2}`` set on EMR Cluster Step tasks, the
``.statement`` task fails after retries with:

```
ClientError: A job flow that is shutting down, terminated, or finished
may not be modified.
```

Different from the ``KeepJobFlowAliveWhenNoSteps=False`` symptom above:
``KeepJobFlowAlive`` is set to ``True`` here, and the cluster genuinely was
alive when the FIRST attempt started.

**Root cause:** the per-node teardown's ``trigger_rule='all_done'`` considers
``failed`` a terminal state. When the ``.statement`` task fails on attempt 1,
Airflow briefly marks it ``failed`` before transitioning to ``up_for_retry``.
In that transient ``failed`` window, the teardown fires, calls
``TerminateJobFlow``, and the cluster starts shutting down. The retry attempt
then hits ``AddJobFlowSteps`` on the shutting-down cluster.

**Fix:** don't set ``retries`` on EMR Cluster Step tasks. Set retries only on
runners where retry is safe (Glue Spark, EMR Serverless attach). Per-DAG:

```python
dag = DbtDag(
    ...,
    default_args={"retries": 0},  # safe default; opt in per-runner
)
```

If you need retries on EMR Cluster Step, use a coarser-grained retry pattern
(e.g. a wrapping cron DAG that reruns the whole ``medallion_v030`` dag_run
on failure) rather than task-level retries.
## ConcurrentRunsExceededException

**Symptom:** Tasks fail immediately with
`ConcurrentRunsExceededException` from `StartJobRun`.

**Root cause:** Glue Job default `MaxConcurrentRuns: 1`. A previous DAG run's Job
RunFinished on AWS but Airflow hasn't recorded it yet — or two consecutive DAG runs
overlap on the same model.

**Fix:** Bump `MaxConcurrentRuns` in the Glue Job config:

```python
create_job_kwargs={
    "ExecutionProperty": {"MaxConcurrentRuns": 5},   # tolerate dev re-triggers
    # ...
}
```

YAML:

```yaml
create_job_kwargs:
  ExecutionProperty:
    MaxConcurrentRuns: 5
```

Make sure `update_config: true` is set so the change pushes to existing Jobs.

## Tag-runner conflict at DAG parse

**Symptom:** `ValueError: node 'model.proj.x': tag_runners conflict ('a' -> 'r1', 'b' -> 'r2')`

**Root cause:** A node carries two tags that route to different runners.

**Fix:** Either retag the model (drop one tag), or remove one of the conflicting
`tag_runners` entries:

```python
# Option 1: retag
{{ config(tags=['silver']) }}   # not ['silver', 'heavy']

# Option 2: collapse the conflict
tag_runners = {"silver,heavy": "session_warm"}    # both route to same runner now
```

## Session statement fails with SystemExit: 1

**Symptom:**
```
AirflowException: Glue statement failed: state=AVAILABLE output_status=error
error=SystemExit: 1
```

**Root cause:** dbt errored inside the Glue Session, but the session was started with
`--ENABLE_ADDITIONAL_LOGGING=false` (the Glue default) so the actual dbt traceback
isn't in CloudWatch.

**Fix:** Enable additional logging on the session runner:

```python
GlueInteractiveSessionRunner(
    default_arguments={
        "--enable-additional-logging": "true",
        # ... rest ...
    },
)
```

Then the dbt stdout/stderr lands in `/aws-glue/sessions/output` and you can see the
actual error.

## Session statement fails with JsonParseException

**Symptom:** Airflow trigger event surfaces something like:

```
com.fasterxml.jackson.core.JsonParseException: Illegal character
((CTRL-CHAR, code 27)): only regular white space (\r, \n, \t) is allowed
between tokens at [Source: (String)"\x1b[0m17:22:57  Running with dbt=1.11.11"; line: 1, column: 2]
```

OR similar with `"Custom config keys should move ..."` etc.

**Root cause (earlier releases):** Glue Interactive Session's `RunStatement`
captures all stdout from the Python statement and tries to JSON-parse it into
the statement's `Output` field. Any non-JSON content — ANSI escape codes,
deprecation warnings, dbt-utils version notices — makes the parser fail and
surface a successful dbt run as an error.

**Fix:** the generated statement code ``os.dup2``'s fd 1 + fd 2 to
``/dev/null`` around the ``run_one_node`` call, so dbt's output never
reaches Glue's captured buffer. dbt's own ``logs/dbt.log`` and the
artefacts uploaded to S3 are unaffected. Present in
``runner-dbt-aws-airflow`` from the first public release.

## EMR Cluster Step: `ModuleNotFoundError: No module named 'dbt_aws'`

**Symptom:** step FAILS with the driver container's stdout showing:

```
File "/mnt/yarn/.../<md5>.py", line 24, in <module>
    from dbt_aws.common.runtime import main
ModuleNotFoundError: No module named 'dbt_aws'
```

Even though the bootstrap installed `dbt-aws` successfully.

**Root cause:** EMR 7.5+ on Amazon Linux 2023 ships **two** Pythons:
`/usr/bin/python3` (=3.9) and `/usr/bin/python3.11`. The bootstrap installs
into 3.11, but Spark's default `PYSPARK_PYTHON` is `/usr/bin/python3` (3.9)
which doesn't see the 3.11 site-packages.

**Fix:** the runner sets four ``--conf`` flags from ``pyspark_python``
(default ``/usr/bin/python3.11``) that cover both ``client`` and
``cluster`` deploy modes:

```
spark.pyspark.python              (canonical, both modes)
spark.pyspark.driver.python       (client-mode driver)
spark.yarn.appMasterEnv.PYSPARK_PYTHON  (cluster-mode AM)
spark.executorEnv.PYSPARK_PYTHON  (executors, any mode)
```

Present in ``runner-dbt-aws-airflow`` from the first public release.

## EMR Cluster Step: `Permission denied /home/.duckdb`

**Symptom:** dbt exits with:

```
_duckdb.IOException: IO Error: Failed to create directory "/home/.duckdb": Permission denied
```

**Root cause:** YARN containers on EMR ship a hardcoded `HOME="/home/"` (no
user) and `/home` isn't writable to the container user. dbt-duckdb's
`conn.install_extension(...)` tries to `mkdir $HOME/.duckdb` and fails.
Spark's `spark.yarn.appMasterEnv.HOME` config is overridden by YARN's
launcher AFTER Spark's env-passing.

**Fix:** the entry script's ``_ensure_writable_home()`` helper probes
``$HOME`` with ``tempfile.mkdtemp()`` (directory creation, matching
what dbt-duckdb needs) and repoints HOME to ``/tmp`` if the probe
fails. Runs before dbt is invoked. Present in
``runner-dbt-aws-airflow`` from the first public release.

## EMR Cluster Step: `User did not initialize spark context!`

**Symptom:** dbt logs show `Completed successfully` + `dbt exited with code 0`,
but the EMR step is marked FAILED with:

```
ApplicationMaster: Final app status: FAILED, exitCode: 13,
  (reason: User did not initialize spark context!)
```

**Root cause:** in `deploy_mode="cluster"`, the Spark AM expects the user
program to instantiate a `SparkContext`. The dbt-aws entry script runs dbt as
a Python subprocess and doesn't need Spark itself, so the AM errors even
after dbt exits 0.

**Fix:** set `deploy_mode="client"` on `EmrClusterStepRunner`. The driver
then runs on the master node (no YARN AM), so no `SparkContext` is required.

```python
EmrClusterStepRunner(..., deploy_mode="client")
```

## EMR Serverless: HTTP timeout to `extensions.duckdb.org`

**Symptom:** dbt run on EMR Serverless hangs during `INSTALL httpfs` /
`LOAD httpfs`, eventually failing with a connection timeout to
`extensions.duckdb.org`.

**Root cause:** EMR Serverless applications typically run in a private
subnet with no NAT / no route to the public internet. Duckdb tries to
auto-download extensions from `extensions.duckdb.org` on first use
and times out.

**Fix:** pre-bundle the extensions in a venv-pack archive and reference
it via `spark.archives`. Build the venv on Amazon Linux 2023 with
Python 3.11 (matches EMR Serverless's runtime); the outline below
assumes `venv-pack`:

```bash
# On an AL2023 x86_64 machine (or in a docker container that matches):
python3.11 -m venv /tmp/emr_venv
source /tmp/emr_venv/bin/activate
pip install --upgrade pip venv-pack
pip install \
    "runner-dbt-aws-airflow==<version>" \
    "dbt-core==1.11.11" \
    "dbt-duckdb==1.10.1" \
    "boto3>=1.34"

# Pre-load the duckdb extensions so runtime auto-download isn't
# needed. This is the whole point of the venv-pack -- private
# subnets have no route to extensions.duckdb.org.
python -c "
import duckdb
c = duckdb.connect()
c.execute('INSTALL httpfs')
c.execute('INSTALL aws')
"

deactivate
venv-pack -o /tmp/emr_serverless_venv.tar.gz -p /tmp/emr_venv

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
                "spark.emr-serverless.driverEnv.PYSPARK_PYTHON": "./env/bin/python",
                "spark.executorEnv.PYSPARK_PYTHON": "./env/bin/python",
                # HOME must be an absolute path; duckdb rejects
                # ``./env`` for home_directory.
                "spark.emr-serverless.driverEnv.HOME": "/home/hadoop/env",
                "spark.executorEnv.HOME": "/home/hadoop/env",
            },
        }],
    },
)
```

The build script:

* Creates a Python 3.11 venv in a fresh AL2023 container.
* Installs `dbt-aws + dbt-core + dbt-duckdb + boto3`.
* Pre-installs the `httpfs` and `aws` duckdb extensions into
  `$HOME/.duckdb/extensions/vX.Y.Z/linux_amd64/`.
* Runs `venv-pack` (which only whitelists standard venv dirs).
* Appends the `.duckdb/` extension dir to the tarball via `tar -rvf`
  so it lands at the archive root.

At runtime, Spark unpacks the archive to `/home/hadoop/env/` (path
determined by the `#env` alias in `spark.archives`). Setting `HOME` to
`/home/hadoop/env` makes duckdb find the pre-installed extensions in
`$HOME/.duckdb/extensions/...`.

## MWAA `pip install` fails with `apache-airflow-providers-amazon` conflict

**Symptom:** MWAA env update fails during `pip install`:

```
ERROR: Cannot install -r /usr/local/airflow/requirements/requirements.txt because
these package versions have conflicting dependencies.
  apache-airflow-providers-amazon 9.0.0 depends on watchtower!=3.3.0, <4 and >=3.0.0
  The user requested (constraint) watchtower==3.3.1,==3.4.0
```

**Root cause:** the upstream Airflow constraints file
(`--constraint https://raw.githubusercontent.com/apache/airflow/constraints-<v>/...`)
disagrees with MWAA's base-image pinned versions of some providers +
transitive deps.

**Fix:** remove the `--constraint` line from your MWAA `requirements.txt`.
MWAA's base image already carries compatible pinned versions of
`apache-airflow` + `apache-airflow-providers-amazon` + transitives; letting
pip resolve against those alone works.

```txt title="requirements.txt for MWAA (correct)"
# Only the runner-dbt-aws-airflow wheel; NO --constraint line, NO
# apache-airflow-*.
/usr/local/airflow/plugins/runner_dbt_aws_airflow-<version>-py3-none-any.whl
```

## Manifest not found at DAG parse

**Symptom:** `FileNotFoundError: target/manifest.json`

**Root cause:** dbt-aws reads `target/manifest.json` at DAG-parse time. It must exist
before Airflow imports the DAG file.

**Fixes:**

| Setup | Approach |
|---|---|
| dbt project lives in the same repo as the DAGs | Run `dbt parse` in CI before deploying, commit `manifest.json` to the repo. |
| Manifest lives in S3 | Use `ProjectConfig(mode="manifest", manifest_path="s3://...")` and let the lib download it. |
| Truly dynamic graph | Use `ProjectConfig(mode="mwaa_parse", project_dir=...)` to run `dbt parse` at DAG-import time. Slowest. |

## S3 archive upload fails

**Symptom:**
```
botocore.exceptions.NoCredentialsError: Unable to locate credentials
```
at DAG parse.

**Root cause:** `build_and_upload_project_archive` runs at DAG-import time; the
Airflow process must have AWS credentials. In production those usually come from the
worker's IAM role; locally they come from `AWS_PROFILE` / env vars.

**Fix:** Make sure the Airflow process has credentials:

```bash
AWS_PROFILE=my-profile  airflow scheduler
# or set AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY in the shell that runs airflow
```

For MWAA, the environment's execution role needs `s3:PutObject` on the deploy bucket.

## Worker entrypoint key collisions across versions

**Symptom (theoretical):** dbt-aws ships a new version, the entrypoint script changes, the
new version's S3 upload overwrites the key, all old Glue Jobs (still pointing at the
shared key) break.

**Why it doesn't happen:** the worker entrypoint key is
**content-addressed**: `s3://.../dbt-aws/worker_entrypoint/<md5>.py`.
Old Glue Jobs keep pointing at their original md5 forever; the new
version uploads a new md5 to a new key.

Confirm via parse log:

```
deploy: entry script already at
  s3://.../dbt-aws/worker_entrypoint/8f018c885fbdf668ee6cee16f0ff6a20.py (skipping)
```

## Other

For other issues open one on the [issue tracker](https://github.com/awslabs/runner-dbt-aws-airflow/issues).
Please include:

1. The parse-time log output (look for `runner distribution` and any `WARN`/`ERROR` lines).
2. The state of the failing task (`airflow tasks states-for-dag-run ...`).
3. AWS side: `aws glue get-job-run --job-name ... --run-id ...` or session state.
