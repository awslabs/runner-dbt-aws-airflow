# Reference

API contracts and full configuration schemas.

- **[Precedence ladder](precedence.md)** — the canonical five-layer
  override ladder (runner defaults → `all:` → `tag:` → `meta.stratus`
  → per-node). Referenced by every other doc page.
- **[Runner constructors](runners.md)** — per-runner constructor kwargs
  (Glue Spark, Glue Python Shell, Glue Interactive Session) plus the
  cross-cutting kwargs every runner accepts (including `resource_tags`,
  `openlineage`, `with_deps`, etc.).
- **[DbtDag / DbtTaskGroup](dbtdag.md)** — every constructor kwarg, return semantics,
  validation order.
- **[Package-version reference (`dbt_aws.compat`)](compat.md)** — validated pin sets and
  ready-to-use install strings per runner shape (Glue Spark, Glue Python Shell).
- **[YAML config](runner-config-yaml.md)** — full top-level schema for `runners_*.yml`
  files consumed by `load_runner_config()`.
- **[Runner overrides](runner-overrides.md)** — the per-runner `OVERRIDE_TYPE` dataclasses
  listing every per-node-tweakable field.

For library internals (modules, runner interface, dispatch pipeline) see
[Concepts → Architecture](../concepts/architecture.md).
