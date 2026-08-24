"""Public configuration objects.

Kept deliberately small — only the knobs the orchestration layer needs.
Concrete runners declare their own config objects in their own packages.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

#: The two supported graph-loading modes.
#:
#: * ``"manifest"`` — read a pre-generated ``manifest.json``. Fast,
#:   deterministic, no ``dbt-core`` required at orchestration time.
#:   Recommended for production.
#: * ``"mwaa_parse"`` — shell out to ``dbt parse`` at DAG-parse time.
#:   Requires ``dbt-core`` + the adapter to be installed in the same
#:   Python environment that imports this module (typical on MWAA via
#:   ``requirements.txt``).
LoadMode = Literal["manifest", "mwaa_parse"]


class ConfigError(ValueError):
    """Raised for invalid :class:`ProjectConfig` combinations."""


@dataclass(frozen=True)
class ProjectConfig:
    """How to obtain the dbt graph.

    Pick exactly one mode and supply only the fields for that mode.
    Validation happens in ``__post_init__`` so callers see configuration
    errors at construction time, not deep inside a loader.

    Manifest mode::

        ProjectConfig(mode="manifest", manifest_path="/path/to/manifest.json")
        # or
        ProjectConfig(mode="manifest", manifest_dict={...})

    MWAA-parse mode::

        ProjectConfig(
            mode="mwaa_parse",
            project_dir="/usr/local/airflow/dags/dbt/my_project",
            profiles_dir="/usr/local/airflow/dags/dbt/my_project",
            target="prod",
        )
    """

    mode: LoadMode

    # ── Mode A: manifest ────────────────────────────────────────────
    manifest_path: str | os.PathLike[str] | None = None
    manifest_dict: dict[str, Any] | None = None

    # ── Mode B: mwaa_parse ───────────────────────────────────────────
    project_dir: str | os.PathLike[str] | None = None
    profiles_dir: str | os.PathLike[str] | None = None
    target: str | None = None

    #: Override for how to invoke dbt in ``mwaa_parse`` mode. Default is
    #: ``[sys.executable, "-m", "dbt.cli.main"]``, which uses the same
    #: Python interpreter that imported this module — typically the
    #: right thing on MWAA.
    dbt_argv: tuple[str, ...] | None = None

    #: Subprocess timeout in seconds for ``mwaa_parse``. ``None`` =
    #: wait forever (dbt's own ``--target-path`` will kill the run if
    #: the project is genuinely broken).
    parse_timeout_seconds: float | None = 600.0

    def __post_init__(self) -> None:
        if self.mode == "manifest":
            if self.manifest_path is None and self.manifest_dict is None:
                raise ConfigError(
                    "mode='manifest' requires either manifest_path= or manifest_dict="
                )
            if self.manifest_path is not None and self.manifest_dict is not None:
                raise ConfigError(
                    "mode='manifest' accepts manifest_path= OR manifest_dict=, not both"
                )
            for field_name in ("project_dir", "profiles_dir", "target"):
                if getattr(self, field_name) is not None:
                    raise ConfigError(
                        f"mode='manifest' does not use {field_name}=; "
                        f"got {getattr(self, field_name)!r}"
                    )
        elif self.mode == "mwaa_parse":
            if self.project_dir is None:
                raise ConfigError("mode='mwaa_parse' requires project_dir=")
            for field_name in ("manifest_path", "manifest_dict"):
                if getattr(self, field_name) is not None:
                    raise ConfigError(
                        f"mode='mwaa_parse' does not use {field_name}=; "
                        f"got {getattr(self, field_name)!r}"
                    )
        else:
            raise ConfigError(f"mode must be 'manifest' or 'mwaa_parse', got {self.mode!r}")

    # ------------------------------------------------------------------
    # Convenience accessors (return resolved Paths for the loaders)
    # ------------------------------------------------------------------
    def resolved_manifest_path(self) -> Path | None:
        return Path(self.manifest_path) if self.manifest_path is not None else None

    def resolved_project_dir(self) -> Path | None:
        return Path(self.project_dir) if self.project_dir is not None else None

    def resolved_profiles_dir(self) -> Path | None:
        return Path(self.profiles_dir) if self.profiles_dir is not None else None
