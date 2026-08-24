# TODO

Forward-looking work only. Tracks work not yet cut into GitHub issues.

## In flight

- **Real-AWS verification of `resource_tags`**. Every runner has
  unit-test coverage that the tag dict lands in the correct
  AWS-facing kwargs shape, but the eu-west-1 / us-east-1 round-trip
  (does the Glue Job / Session actually come out tagged?) hasn't
  been run yet. Add one integration-test assertion per runner that
  reads tags back from the AWS resource.
- **Public PyPI release under the new distribution name
  (`runner-dbt-aws-airflow`).** Blockers before 1.0: a real-AWS
  regression suite for every runner, a stable public docs URL
  (GitHub Pages via `.github/workflows/docs.yml`), and the OIDC
  trusted-publisher registration on PyPI and TestPyPI.

## Nice-to-have

- **`resource_tags` at per-model / per-tag scope.** Delivered in
  Layered via ``_apply_tag_overrides_to_effective`` in
  ``dbt_aws/common/builder.py`` with a shallow-fold merge across
  every layer.
- **PR preview docs** on GitHub Pages. Publish per-PR previews of the
  mkdocs site so reviewers can click a link instead of building
  locally. Skipped for now to keep CI minimal.

## Scope decisions (not planned)

- **Cosmos-style rendering of task IDs**. The current
  `<name>__<uid>` scheme is byte-stable and grep-friendly for
  downstream tooling; Cosmos's `run.<node_name>` style adds nothing
  we don't already have via `mode: single, name: <prefix>`.
- **Per-JobRun / per-statement tagging** on Glue. Glue's
  `StartJobRun` doesn't accept `Tags`; workaround via
  `--additional-python-modules` args isn't a real tag. Users needing
  per-run cost allocation can query the JobRun cost by
  `--JOB_RUN_ID` in CloudTrail.
