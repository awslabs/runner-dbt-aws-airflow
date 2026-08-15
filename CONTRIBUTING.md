# Contributing to runner-dbt-aws-airflow

Thanks for the interest. This guide covers dev setup, style, testing,
and the release workflow.

> **Naming:** the PyPI distribution is `runner-dbt-aws-airflow`
> (matches the repo name); the Python import path stays `dbt_aws`
> (namespace package, backwards-compatible).

## Setup

```bash
git clone https://github.com/awslabs/runner-dbt-aws-airflow.git
cd runner-dbt-aws-airflow
uv sync --group dev --group docs        # creates .venv
uv run pre-commit install               # optional -- see below
```

The workspace resolves against Python 3.12 locally (see
`.python-version`). The wheel's floor is Python 3.9 (Glue Python Shell
compatibility); `[tool.uv.environments]` restricts dev resolution to
3.10+ so `apache-airflow==3.2.1` (dev-only) can co-exist with the
wider wheel floor.

## Style

- **`ruff`** — linter + formatter. `line-length = 100`. Config in
  `pyproject.toml` under `[tool.ruff]`.
- **`mypy --strict`** — type-check the `dbt_aws/` package. Config in
  `pyproject.toml` under `[tool.mypy]`.

Enforced automatically by `pre-commit` on every commit. Run manually
before pushing:

```bash
uv run ruff check dbt_aws --fix
uv run mypy
uv run mkdocs build --strict            # if you touched docs/
```

If you have a global `core.hooksPath` set (some corporate
environments do), the `pre-commit install` step is a no-op. Run
hooks manually:

```bash
uv run pre-commit run --all-files
```

## Testing

The test suite is currently kept in the maintainers' local checkouts
while we harden fixtures and the integration guide for external
contributors. Once tests land in this repo, this section will explain
how to run them; until then, CI runs `ruff` + `mypy` + `mkdocs build`
+ the wheel smoke test on every PR.

Contributors adding a runner or config field should still write tests
locally and share them alongside the PR description — they will be
folded into the public suite when it lands.

## Docs

Sources are under `docs/`. The mkdocs Material theme + `pymdown-extensions`.

```bash
uv run mkdocs serve                     # http://localhost:8000
uv run mkdocs build --strict            # writes to site/
```

`site/` is gitignored (except for a `.gitkeep` placeholder so editable
installs resolve before your first `mkdocs build`). The published
wheel bundles `site/` via
`[tool.hatch.build.targets.wheel.force-include]` in `pyproject.toml`
so `runner-dbt-aws-airflow docs` can serve the docs offline — run
`mkdocs build --strict` before `uv build` if you want the wheel to
carry the latest docs. CI does this automatically
(`.github/workflows/publish-*.yml`).

## Branch + PR conventions

- Branch off `main`. Name branches `feat/<short-desc>`,
  `fix/<short-desc>`, `docs/<short-desc>`, `chore/<short-desc>`.
- Commit messages follow the pattern already in the log — one-line
  subject describing the change, blank line, then a body that lists
  what changed and why. Reference issues where relevant.
- PRs need a title matching the branch prefix. Description should
  cover: summary of changes, what was tested, any user-visible impact.
- Squash on merge unless the commits tell a story worth preserving.

## Adding a new field or runner

Both the loader and the DAG builder validate at DAG-parse time. Follow
the same pattern:

1. Add the field to the concrete runner's `__init__` and to its
   `OVERRIDE_TYPE` dataclass if it's per-model-tweakable.
2. Add loader-side validation in `dbt_aws/common/runner/config.py`
   with an actionable error message when the shape is wrong.
3. Add DAG-parse-time enforcement so mistakes surface before any
   task runs.
4. Write a test asserting both the happy path and the error path.
5. Update `docs/reference/runners.md` (if a runner kwarg) or
   `docs/reference/runner-config-yaml.md` (if a YAML key).

## Release workflow

Releases are automated via GitHub Actions with **PyPI trusted
publishing (OIDC)** — no long-lived API tokens live in the repo or
any secret store.

**Cutting a release:**

1. Bump `version = "..."` in `pyproject.toml`.
2. Bump `DBT_AWS_VERSION = "..."` in `dbt_aws/compat.py` (must match).
3. Run the full verification suite locally:
   ```bash
   uv run ruff check dbt_aws
   uv run mypy
   uv run mkdocs build --strict
   ```
4. Commit + open a PR; get it merged to `main` once CI is green.
5. Tag + create a GitHub Release:
   ```bash
   git tag -a v0.13.0 -m "v0.13.0"
   git push origin v0.13.0
   gh release create v0.13.0 --generate-notes
   ```
   Publishing the GitHub Release triggers
   `.github/workflows/publish-pypi.yml`, which:
     - Builds the wheel + sdist (with mkdocs docs bundled).
     - Runs the wheel smoke test (via the inline
       `.github/actions/wheel-smoke` composite action).
     - Publishes to **PyPI** via the `pypi` environment (OIDC trusted
       publisher, `id-token: write`).
     - Attaches the built artefacts to the GitHub Release.

**Publishing to TestPyPI (pre-release smoke):**

Run `.github/workflows/publish-testpypi.yml` from the Actions tab
(`workflow_dispatch`). Same flow, targets TestPyPI's trusted
publisher via the `testpypi` environment. Also fires automatically on
every push to `main` (skipped if the current version already exists
on TestPyPI).

**Local / manual publish (fallback only):**

When CI is unavailable, maintainers keep a local `scripts/` folder on
their workstations with a TestPyPI upload helper. That folder is NOT
tracked in git (see `.gitignore`) -- it's a local convenience, not a
supported release path. The production release path is always the
OIDC trusted publisher workflows in `.github/workflows/publish-*.yml`.

**Prerequisites for OIDC publishing (one-time repo setup):**

- Register `awslabs/runner-dbt-aws-airflow` on PyPI as a
  [Trusted Publisher](https://docs.pypi.org/trusted-publishers/) with
  workflow `publish-pypi.yml` and environment `pypi`.
- Same on TestPyPI with workflow `publish-testpypi.yml` and
  environment `testpypi`.
- Create two GitHub Environments (`pypi`, `testpypi`) in repo
  settings. Optionally require reviewer approval on `pypi`.

## Real-AWS integration testing

The real-AWS integration suite is currently maintainer-local (see
"Testing" above). Environment variables the suite expects, for
reference / when the suite lands publicly:

- `DBT_AWS_INT_BUCKET` — S3 bucket for archives / worker entrypoints.
- `DBT_AWS_INT_REGION` — AWS region (e.g. `eu-west-1`).
- `DBT_AWS_INT_IAM_ROLE` — Glue / EMR execution role ARN.
- `DBT_AWS_INT_WHEEL_S3` — S3 URI for the published wheel (Glue
  Python Shell fallback).
- `DBT_AWS_INT_ENTRYPOINT_S3` — S3 URI for the worker entry script.

Each integration test creates and tears down its own AWS resources.
Failures leave behind logs pointing at the CloudWatch group; check
there before rerunning.

## Reporting issues

- **Security issue** — **do NOT open a public GitHub issue.** Report
  security vulnerabilities via the AWS Security
  [vulnerability reporting page](https://aws.amazon.com/security/vulnerability-reporting/).
  See [`SECURITY.md`](SECURITY.md) for the full policy, scope, and
  what to include in a report.
- **Bug** — include: the runner shape, AWS region,
  `runner-dbt-aws-airflow` version, minimal reproduction (a redacted
  `runners.yml` + DAG file when possible), and the failing traceback.
  If it involves a specific AWS resource state (Glue Job doesn't
  exist, EMR cluster stuck), say so.
- **Feature request** — describe the use case first, then propose an
  API shape. We prefer additive changes; breaking changes need a
  strong motivation and go into the next minor.

## Code of Conduct

See [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md).

## License

Contributions are accepted under the Apache-2.0 license (the project
license). By submitting a PR you certify that you have the right to
license the contribution under those terms. Adding a `Signed-off-by:`
trailer (`git commit -s`) is welcome but not required.
