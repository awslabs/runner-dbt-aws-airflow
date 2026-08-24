# runner-dbt-aws-airflow

[![PyPI](https://img.shields.io/pypi/v/runner-dbt-aws-airflow?logo=pypi&logoColor=white)](https://pypi.org/project/runner-dbt-aws-airflow/)
[![Python](https://img.shields.io/badge/python-3.9%20%7C%203.10%20%7C%203.11%20%7C%203.12-blue?logo=python&logoColor=white)](https://www.python.org/)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9%2B%20%7C%203.x-017CEE?logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![CI](https://github.com/awslabs/runner-dbt-aws-airflow/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/awslabs/runner-dbt-aws-airflow/actions/workflows/ci.yml)
[![Docs](https://img.shields.io/badge/docs-github.io-blue?logo=readthedocs&logoColor=white)](https://awslabs.github.io/runner-dbt-aws-airflow/)
[![mypy](https://img.shields.io/badge/mypy-strict%20clean-brightgreen)](#status)
[![ruff](https://img.shields.io/badge/ruff-clean-brightgreen)](#status)

Run [dbt](https://www.getdbt.com/) projects on **AWS Glue** (Spark Jobs,
Interactive Sessions, Python Shell) — orchestrated from **Apache Airflow**
(including **MWAA**). One library, five runner shapes, declarative YAML
routing, task-collapse, OpenLineage emission, AWS resource-tag compliance,
and **worker-side dbt package installs** so your Airflow deployment stays
lean.

> **Status:** stable 1.x on [PyPI](https://pypi.org/project/runner-dbt-aws-airflow/).
> Semver applies — breaking changes require a major bump.
>
> **Docs:** full mkdocs book hosted at
> **[awslabs.github.io/runner-dbt-aws-airflow](https://awslabs.github.io/runner-dbt-aws-airflow/)**
> (rebuilt on every push to `main` via `.github/workflows/docs.yml`).
> The same site ships bundled inside the wheel and is available
> offline via `runner-dbt-aws-airflow docs`.
---

## Table of Contents

- [Why runner-dbt-aws-airflow](#why-runner-dbt-aws-airflow)
- [Install (PyPI)](#install-pypi)
- [Quickstart — MWAA 3.2.1 + Glue Spark](#quickstart--mwaa-321--glue-spark)
    - [Prerequisites](#1-prerequisites)
    - [Sample dbt project](#2-sample-dbt-project)
    - [`runners.yml`](#3-runnersyml)
    - [Airflow DAG](#4-airflow-dag)
    - [MWAA `requirements.txt`](#5-mwaa-requirementstxt)
    - [Deploy to MWAA](#6-deploy-to-mwaa)
- [Features](#features)
- [Documentation](#documentation)
- [Repo layout](#repo-layout)
- [Development](#development)
- [Status](#status)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [Security](#security)
- [License](#license)

---

## Why runner-dbt-aws-airflow

Running dbt on AWS from Airflow gives you three real headaches:

1. **Compute selection.** You want cheap Spark for a bronze layer, a
   warm interactive session for silver/gold, and Python Shell for tiny
   Athena transforms — from **one DAG**, without hand-rolling six
   operators.
2. **Worker-side `dbt-core` install.** Baking `dbt-core` into MWAA's
   `requirements.txt` explodes install times and pins your whole
   Airflow to one dbt version. You want the runner to install `dbt`
   on-the-fly on the worker.
3. **Routing declaratively.** Which model runs on which compute, with
   which target / profile / vars, per-tag or per-model, needs to live
   in one YAML file — not scattered across five operator constructors.

`runner-dbt-aws-airflow` solves all three. It's a thin Airflow-native
library: no Cosmos wrapper, no separate scheduler, no vendored
dbt-core.

## Install (PyPI)

```bash
# Latest release
pip install runner-dbt-aws-airflow

# With Airflow + dev extras
pip install runner-dbt-aws-airflow[airflow]

# With OpenLineage emission
pip install runner-dbt-aws-airflow[lineage]
```

The Python import path stays `dbt_aws` (namespace package) — only the
PyPI distribution + CLI name follow the repo:

```python
from dbt_aws.common.builder import DbtDag, DbtTaskGroup
from dbt_aws.common import ProjectConfig, load_runner_config
```

Glue Python Shell can't reach PyPI directly (as of Glue 3.0). Use
the S3-hosted wheel instead:

```
--additional-python-modules
  s3://<your-glue-assets-bucket>/dbt-aws/wheels/runner_dbt_aws_airflow-<version>-py3-none-any.whl,dbt-core==1.11.11,dbt-duckdb==1.10.1
```

Every release publishes the wheel to PyPI via OIDC trusted publishing
(see `.github/workflows/publish-pypi.yml`). Mirroring to S3 for Glue
Python Shell consumers is a maintainer-side convenience, done via a
local (gitignored) `scripts/` folder. See
[Reference → `dbt_aws.compat`](docs/reference/compat.md) for pinned
version strings.

---

## Quickstart — MWAA 3.2.1 + Glue Spark

End-to-end setup, no library-specific tooling required beyond the
wheel install. Runs one dbt project through a Glue Spark Job managed
by MWAA. Total setup time: ~30 minutes if you already have an MWAA
environment; ~1 hour if you're creating one from scratch.

### 1. Prerequisites

- An AWS account with permissions to create an MWAA env, a Glue Spark
  Job, and an S3 bucket.
- MWAA environment on `mw1.small` (or larger) running **Airflow 3.2.1**.
- An IAM role Glue can assume (e.g. `AWSGlueServiceRole`) with S3
  read/write to your dbt project bucket.

Not covered here (see [Concepts → Deployment](docs/concepts/deployment.md)
for the full walkthrough): MWAA network setup (2 private subnets + NAT),
SecurityGroup wiring, DAG folder S3 sync.

### 2. Sample dbt project

Minimum shape — one seed, one model, `dbt-duckdb` locally / `dbt-glue`
on Glue:

```
my_dbt_project/
├── dbt_project.yml
├── profiles.yml
├── seeds/
│   └── raw_orders.csv
└── models/
    └── stg_orders.sql
```

`dbt_project.yml`:

```yaml
name: my_dbt_project
version: '1.0.0'
config-version: 2
profile: my_dbt_project

seeds:
  my_dbt_project:
    +schema: raw

models:
  my_dbt_project:
    +materialized: table
    +schema: analytics
```

`profiles.yml` (local dev; MWAA-side dbt runs use the Glue Spark
runtime's own catalog):

```yaml
my_dbt_project:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: dev.duckdb
      threads: 4
```

`models/stg_orders.sql`:

```sql
{{ config(tags=['bronze']) }}

select
    id,
    customer_id,
    order_total,
    ordered_at
from {{ ref('raw_orders') }}
where order_total > 0
```

### 3. `runners.yml`

Declarative compute + routing. One Glue Spark Job runs every bronze
model:

```yaml
# runners.yml -- runner-dbt-aws-airflow config

resource_tags:
  CostCenter: data-platform
  Environment: prod
  Owner: analytics-team

runners:
  glue_spark:
    type: glue_spark
    mode: create
    iam_role_name: AWSGlueServiceRole
    deploy_bucket: my-dbt-aws-bucket
    deploy_prefix: dbt-aws
    aws_conn_id: aws_default
    region_name: eu-west-1
    worker_type: G.1X
    number_of_workers: 2
    glue_version: "5.0"
    create_job_kwargs:
      DefaultArguments:
        "--additional-python-modules": "s3://my-dbt-aws-bucket/dbt-aws/wheels/runner_dbt_aws_airflow-<version>-py3-none-any.whl,dbt-core==1.11.11,dbt-duckdb==1.10.1"

default_runner: glue_spark

overrides:
  # Every bronze-tagged node runs on Glue Spark with 4 workers instead
  # of the runner default of 2. Task-id prefix ``bronze__`` so tagged
  # siblings sort together in the Airflow Graph view.
  tag.bronze:
    mode: single
    name: bronze
    worker_type: G.2X
    number_of_workers: 4
```

Full YAML schema: [Reference → YAML config](docs/reference/runner-config-yaml.md).

### 4. Airflow DAG

`dags/my_dbt_dag.py`:

```python
from datetime import datetime
from pathlib import Path

from dbt_aws.common import ProjectConfig, load_runner_config
from dbt_aws.common.airflow_extras.auto_deploy import build_and_upload_project_archive
from dbt_aws.common.builder import DbtDag

PROJECT = Path(__file__).parent / "my_dbt_project"
RUNNER_YML = Path(__file__).parent / "runners.yml"

# 1. Build + upload the project archive at DAG-parse time. Content-
#    fingerprinted -- re-uploads only when files change.
archive_s3 = build_and_upload_project_archive(
    project_dir=PROJECT,
    bucket="my-dbt-aws-bucket",
    prefix="dbt-aws/archives/",
    region_name="eu-west-1",
)

# 2. Load the runner config once.
cfg = load_runner_config(RUNNER_YML)

# 3. Build the DAG. ``config=cfg`` auto-wires every routing field.
dag = DbtDag(
    dag_id="my_dbt_daily",
    project=ProjectConfig(
        mode="manifest",
        manifest_path=PROJECT / "target/manifest.json",
    ),
    project_archive_s3=archive_s3,
    config=cfg,
    target="dev",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
)
```

Regenerate the manifest whenever your dbt project changes:

```bash
cd my_dbt_project && uv run dbt parse --target dev
```

Commit `target/manifest.json` (or regenerate it in your CI pipeline
before syncing DAGs to S3). See
[Concepts → Deployment → Project archive](docs/concepts/deployment.md#project-archive-build_and_upload_project_archive)
for the fingerprinting details.

### 5. MWAA `requirements.txt`

**Airflow 3.2.1 uses `constraints-3.2.1-python3.12.txt`.** Add:

```
--constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.2.1/constraints-3.2.1-python3.12.txt"
runner-dbt-aws-airflow[airflow]
```

That's it. **You do NOT need `dbt-core` in MWAA's requirements.txt**
— workers install it themselves at runtime via the `--additional-python-modules`
S3 wheel in step 3. This keeps MWAA installs fast and version-agnostic.

### 6. Deploy to MWAA

Standard MWAA workflow:

```bash
# Sync DAGs + config to MWAA's S3 bucket
aws s3 sync ./dags s3://<mwaa-bucket>/dags/
aws s3 cp requirements.txt s3://<mwaa-bucket>/requirements.txt

# Update MWAA to pick up the new requirements
aws mwaa update-environment --name <env-name> \
    --requirements-s3-object-version "$(aws s3api list-object-versions \
        --bucket <mwaa-bucket> --prefix requirements.txt \
        --query 'Versions[0].VersionId' --output text)"
```

MWAA takes ~10-15 minutes to reload. Once the environment is
`AVAILABLE`, unpause the DAG in the Airflow UI and trigger it.

Full MWAA network + IAM setup:
[Concepts → Deployment → MWAA](docs/concepts/deployment.md#mwaa).

---

## Features

**Five runner shapes** — one library, uniform API:

| Runner | Backend | Best for |
|---|---|---|
| `GlueSparkRunner` | Glue Spark Job (JobRun) | Bronze bulk transforms, medallion base layer |
| `GlueInteractiveSessionRunner` | Glue Interactive Session (warm or per-node) | Silver / gold with a shared warm session |
| `GluePythonShellRunner` | Glue Python Shell (Glue 3.0, up to 1 DPU) | dbt-athena / small Python-only transforms |
| `EmrServerlessRunner` | EMR Serverless job | High-concurrency Spark without cluster mgmt |
| `EmrClusterStepRunner` | EMR-on-EC2 cluster step | Iceberg / Spark 3.5+ / custom bootstrap |

**Declarative routing** — one `overrides:` block:

- `overrides[model.<pkg>.<name>]` — per-node.
- `overrides[tag.<name>]` — bulk-by-tag. With `mode: single` (one
  Airflow task per node) or `mode: group` (collapse all tagged nodes
  into one Airflow task).
- Runner-level defaults on the `runners:` block.
- Precedence per field is documented and validated at DAG-parse.

**AWS resource tags** — top-level `resource_tags:` cascades to every
runner. Applied to `glue:CreateJob` / `glue:CreateSession` (dict
shape). Per-runner blocks and per-node / per-tag overrides merge
per key.

**Worker-side dbt-core install** — MWAA's `requirements.txt` stays
lean; workers install `dbt-core` + `dbt-duckdb` + `packages.yml` deps
on the fly.

**OpenLineage + SageMaker Unified Studio (opt-in)** — emit
`START` / `COMPLETE` events to S3 (NDJSON) or SMUS via
`datazone:PostLineageEvent`. Multi-runner DAGs collapse to one
lineage graph via a shared parent facet.

**Task-collapse (opt-in)** — fold view+consumer chains and
ephemeral drops. Proven with Glue 5.1 + native Iceberg + materialised
views.

**Cosmos-compatible API** — `DbtDag` / `DbtTaskGroup` accept
Cosmos-style `ProjectConfig`. Drop-in from Cosmos-based DAGs.

## Documentation

Full docs live at **<https://awslabs.github.io/runner-dbt-aws-airflow/>**
(rebuilt on every push to `main`).

The same site ships bundled inside the released wheel for offline
viewing:

```bash
pip install runner-dbt-aws-airflow
runner-dbt-aws-airflow docs    # opens http://localhost:8000 with the bundled site
```

Built from `docs/` via `mkdocs-material`. Run locally:

```bash
uv sync --group docs
uv run mkdocs serve      # http://localhost:8000
```

| Topic | Page |
|---|---|
| First DAG in 5 minutes | [Getting started](https://awslabs.github.io/runner-dbt-aws-airflow/getting-started/) |
| Runner shapes | [Concepts → Runners](https://awslabs.github.io/runner-dbt-aws-airflow/concepts/runners/) |
| Routing (`overrides` / `tag.<name>` / `mode`) | [Concepts → Routing](https://awslabs.github.io/runner-dbt-aws-airflow/concepts/routing/) |
| Task-collapse | [Concepts → Task-collapse](https://awslabs.github.io/runner-dbt-aws-airflow/concepts/collapse/) |
| OpenLineage + SMUS | [Concepts → Lineage](https://awslabs.github.io/runner-dbt-aws-airflow/concepts/lineage/) |
| MWAA / VPC deployment | [Concepts → Deployment](https://awslabs.github.io/runner-dbt-aws-airflow/concepts/deployment/) |
| Every runner kwarg | [Reference → Runner constructors](https://awslabs.github.io/runner-dbt-aws-airflow/reference/runners/) |
| YAML config schema | [Reference → YAML config](https://awslabs.github.io/runner-dbt-aws-airflow/reference/runner-config-yaml/) |
| Per-model overrides | [Reference → Runner overrides](https://awslabs.github.io/runner-dbt-aws-airflow/reference/runner-overrides/) |
| DbtDag / DbtTaskGroup | [Reference → DbtDag](https://awslabs.github.io/runner-dbt-aws-airflow/reference/dbtdag/) |
| Package pins | [Reference → compat](https://awslabs.github.io/runner-dbt-aws-airflow/reference/compat/) |
| MWAA gotchas | [Troubleshooting](https://awslabs.github.io/runner-dbt-aws-airflow/troubleshooting/) |
| MWAA gotchas | [Troubleshooting](docs/troubleshooting.md) |

## Repo layout

```
runner-dbt-aws-airflow/
├── pyproject.toml              project + build config (hatchling), dev/docs deps
├── README.md                   this file
├── mkdocs.yml                  mkdocs book config
├── dbt_aws/                    PEP 420 namespace package (imported as `dbt_aws`;
│   │                           distributed on PyPI as `runner-dbt-aws-airflow`)
│   ├── common/                 core: builder, config, graph, runner base + tags helpers
│   ├── spark/                  Glue Spark Job + Session, EMR Serverless, EMR Cluster Step
│   ├── nonspark/               Glue Python Shell
│   ├── cli.py                  `runner-dbt-aws-airflow` CLI (offline docs server)
│   └── compat.py               version + resource-pin constants
└── docs/                       mkdocs book sources (markdown)
```

The test suite (unit + real-AWS integration) is maintained locally by
the maintainers for the initial public release and will land here in
a later drop. See `CONTRIBUTING.md` for the current dev loop.

## Development

```bash
# Install everything (creates .venv with dev + docs + runtime deps)
uv sync --group dev --group docs

# Lint + type-check
uv run ruff check dbt_aws
uv run mypy

# Build the wheel
uv run mkdocs build --strict                      # bundle docs into site/
uv build --wheel                                  # -> dist/runner_dbt_aws_airflow-*.whl

# Run all commit-time hooks
uv run pre-commit run --all-files
```

The test suite is maintained privately for the initial public
drop and will land in a follow-up release. Contributors sending PRs
are welcome to include local test evidence in the description.

## Status

| Check | State |
|---|---|
| ruff (`dbt_aws`) | clean |
| mypy (strict) | clean |
| mkdocs build (strict mode) | clean |
| Real-AWS integration (maintainer-local) | Glue Spark Job, Glue Interactive Session (warm + per-node), and Glue Python Shell validated end-to-end |
| Real-AWS OpenLineage (maintainer-local) | Glue Spark + Glue Session validated end-to-end. Python Shell blocked by dbt-core ≥1.10 requiring Py 3.10+ (Glue Python Shell caps at 3.9). |
| Real-AWS collapse + Iceberg + Glue 5.1 (maintainer-local) | Validated: Glue Data Catalog + parquet tables + view_chain (11 → 6 Airflow tasks) and Glue 5.1 native Iceberg + materialised views + view_chain. |
| MWAA end-to-end (maintainer-local) | Glue Spark + Glue Interactive Sessions validated on an MWAA test environment (Airflow 3.2.1). Python Shell verified on local Airflow 3.2.1. |
| Latest release | see [PyPI](https://pypi.org/project/runner-dbt-aws-airflow/) |

See [PyPI](https://pypi.org/project/runner-dbt-aws-airflow/) for release history.

## Roadmap

Actively working on:

- Publishing the test suite (currently maintainer-local).
- Growing the real-AWS regression matrix (EMR Serverless + EMR
  Cluster Step end-to-end coverage in CI once cost-friendly
  fixtures exist).

Have a feature request? Open an issue with your use case and current
workaround.

## Contributing

- Small fix / obvious bug — send a PR. Include the reproduction and a
  local test if you have one; the maintainers will fold it into the
  private test suite that will land publicly soon.
- New runner shape or config field — open an issue first so we can
  agree on the API surface before you write it.
- Docs — the mkdocs sources live under `docs/`. Preview with
  `uv run mkdocs serve`.

Style: `ruff` + `mypy --strict` are enforced by `pre-commit`. Bump
`pyproject.toml` version + `dbt_aws/compat.py::DBT_AWS_VERSION`
together when your PR ships user-visible changes (skip pure
refactors).

## Security

If you discover a potential security issue, please **do not** open a
public GitHub issue. Report it via the AWS Security
[vulnerability reporting page](https://aws.amazon.com/security/vulnerability-reporting/)
instead. Full policy, scope, threat model, and what to include in a
report live in [`SECURITY.md`](SECURITY.md).

## License

Apache-2.0. See [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE).
