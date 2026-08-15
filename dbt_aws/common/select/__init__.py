"""dbt-compatible selectors for filtering a :class:`DbtGraph`.

Public surface:

* :func:`apply_selectors` — main entry point.
* :func:`parse_selector` — exposed for callers that want to validate a
  selector string without applying it.
* :class:`SelectorError` — malformed selector expression.
"""

from dbt_aws.common.select.selector import (
    SelectorError,
    apply_selectors,
    parse_selector,
)

__all__ = ["SelectorError", "apply_selectors", "parse_selector"]
