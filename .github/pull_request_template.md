## Summary

<!-- One or two sentences describing the change. -->

## Motivation / context

<!-- Why is this needed? Link the issue (Fixes #NNN) or explain the use case. -->

## What changed

<!-- Bullet list of user-visible changes; skip internal refactors. -->

-
-

## How it was tested

- [ ] `uv run ruff check dbt_aws` — clean
- [ ] `uv run mypy` — clean
- [ ] `uv run mkdocs build --strict` — passing (if docs changed)
- [ ] Local test run (the public test suite is pending; describe what
      you exercised locally, and paste output for anything AWS-facing)
- [ ] Real-AWS integration exercised (if applicable): _which runner + region_

## Breaking changes?

<!-- If yes, describe the migration path. Pre-1.0 we accept minor
     breaks but they need to be called out in the release notes. -->

- [ ] No breaking changes
- [ ] Breaking (details below)

## Checklist

- [ ] Version bumped in `pyproject.toml` AND `dbt_aws/compat.py`
      (if this PR ships in a release)
- [ ] Docs updated (`docs/`) if user-visible behavior changed
- [ ] Tests added / updated
