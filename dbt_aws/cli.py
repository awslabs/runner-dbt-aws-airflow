"""``runner-dbt-aws-airflow`` command-line entry point.

Currently exposes a single subcommand:

* ``runner-dbt-aws-airflow docs [--port N] [--host H] [--no-open]`` --
  serve the bundled documentation site on ``http://localhost:8000/``
  (default).

The docs bundle ships inside the wheel at ``dbt_aws/_docs/`` so users
can browse the reference without an internet connection. Rebuilt from
``docs/`` on every release via ``mkdocs build``; see
``.github/workflows/publish-pypi.yml`` and
``.github/workflows/docs.yml``.

The Python import path is still ``dbt_aws`` (namespace package); only
the PyPI distribution + console-script name follow the awslabs repo
name (``runner-dbt-aws-airflow``).

Design notes:

* No third-party deps -- the runtime path uses stdlib
  ``http.server`` only. mkdocs is a build-time dep, not runtime.
* Non-forking single-process server -- fine for local browsing.
* Refuses to serve when the bundle is missing (dev checkouts that
  didn't run ``mkdocs build`` first) and prints how to rebuild.
"""

from __future__ import annotations

import argparse
import contextlib
import http.server
import importlib.resources
import logging
import socket
import socketserver
import sys
import webbrowser
from importlib.abc import Traversable
from pathlib import Path

_log = logging.getLogger(__name__)


def _resolve_docs_root() -> Traversable | None:
    """Return the bundled ``_docs/`` root inside the installed wheel.

    Returns ``None`` when the bundle isn't present -- typically an
    editable install where the docs haven't been built.
    """
    try:
        root = importlib.resources.files("dbt_aws").joinpath("_docs")
    except (ModuleNotFoundError, FileNotFoundError):
        return None
    # ``joinpath`` succeeds even for missing dirs on some backends; probe.
    index = root.joinpath("index.html")
    try:
        with importlib.resources.as_file(index) as p:
            if not p.is_file():
                return None
    except (FileNotFoundError, IsADirectoryError):
        return None
    return root


def _pick_free_port(preferred: int) -> int:
    """Return ``preferred`` when it's free, else the first free port
    above it."""
    port = preferred
    while port < preferred + 100:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind(("127.0.0.1", port))
                return port
            except OSError:
                port += 1
    raise RuntimeError(
        f"could not find a free port in [{preferred}, {preferred + 100})"
    )


def _serve_docs(host: str, port: int, open_browser: bool) -> int:
    """Serve the bundled docs directory. Returns process exit code."""
    root = _resolve_docs_root()
    if root is None:
        print(
            "runner-dbt-aws-airflow: bundled documentation not found.\n"
            "\n"
            "This can happen in a dev checkout when 'mkdocs build' has not\n"
            "run yet. From the repository root:\n"
            "\n"
            "    uv sync --group docs\n"
            "    uv run mkdocs build\n"
            "\n"
            "The installed wheel from PyPI ships the bundle by default.",
            file=sys.stderr,
        )
        return 1

    # ``importlib.resources.as_file`` may return a Path (installed wheel)
    # or a synthesised path (zip). Materialise to a directory we can
    # serve from.
    with importlib.resources.as_file(root) as docs_dir:
        docs_path = Path(docs_dir)

        # Some environments give us the ZipPath -> tempdir hop; ensure
        # the resolved path is a directory.
        if not docs_path.is_dir():
            print(
                f"runner-dbt-aws-airflow: resolved docs path {docs_path!s} "
                f"is not a directory. Rebuild the docs and reinstall.",
                file=sys.stderr,
            )
            return 1

        chosen_port = _pick_free_port(port)
        if chosen_port != port:
            _log.info(
                "runner-dbt-aws-airflow docs: port %d taken; serving on %d instead",
                port,
                chosen_port,
            )

        # warn when the caller opts out of localhost-only
        # binding. The default is 127.0.0.1 (safe); values like
        # 0.0.0.0 or an interface IP expose the docs server to the
        # LAN / cloud network -- fine on developer boxes but a
        # potential recon vector on EC2 / MWAA workers.
        if host not in ("127.0.0.1", "localhost", "::1"):
            _log.warning(
                "runner-dbt-aws-airflow docs: --host=%s exposes the server "
                "BEYOND localhost. On cloud instances with permissive "
                "security groups this may reach the internet. Use "
                "--host 127.0.0.1 (default) unless you deliberately want "
                "LAN sharing.",
                host,
            )

        handler_cls = _make_handler(docs_path)
        socketserver.TCPServer.allow_reuse_address = True
        with socketserver.TCPServer((host, chosen_port), handler_cls) as httpd:
            url = f"http://{host}:{chosen_port}/"
            print(f"runner-dbt-aws-airflow docs: serving bundled docs at {url}")
            print("Press Ctrl+C to stop.")
            if open_browser:
                with contextlib.suppress(webbrowser.Error):
                    # Headless environment -- ignore and keep serving.
                    webbrowser.open_new_tab(url)
            try:
                httpd.serve_forever()
            except KeyboardInterrupt:
                print("\nrunner-dbt-aws-airflow docs: stopped.")
    return 0


def _make_handler(docs_path: Path) -> type[http.server.SimpleHTTPRequestHandler]:
    """Build a ``SimpleHTTPRequestHandler`` subclass whose root is
    ``docs_path``. Python 3.9+ accepts ``directory=`` on the parent
    constructor -- we forward it so the handler is stateless
    per-instance."""
    root_str = str(docs_path)

    class _DocsHandler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args: object, **kwargs: object) -> None:
            super().__init__(*args, directory=root_str, **kwargs)  # type: ignore[arg-type]

        def log_message(  # noqa: A003 -- overriding stdlib name intentionally
            self,
            format: str,  # noqa: A002 -- signature imposed by stdlib
            *args: object,
        ) -> None:
            # Quieter default log; drop the "GET /... 200" spam.
            return

    return _DocsHandler


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="runner-dbt-aws-airflow",
        description="runner-dbt-aws-airflow command-line utilities.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    docs = sub.add_parser(
        "docs",
        help="serve the bundled documentation site on localhost",
        description=(
            "Serve the offline copy of the runner-dbt-aws-airflow docs that "
            "ships inside the wheel. Useful before the docs are hosted "
            "publicly, or on networks that block GitHub Pages."
        ),
    )
    docs.add_argument(
        "--port",
        type=int,
        default=8765,
        help="port to bind (default: 8765; picks the next free port if taken).",
    )
    docs.add_argument(
        "--host",
        default="127.0.0.1",
        help="interface to bind (default: 127.0.0.1). Use 0.0.0.0 for LAN sharing.",
    )
    docs.add_argument(
        "--no-open",
        action="store_true",
        help="don't open the default browser (useful in scripts / SSH sessions).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = _build_parser().parse_args(argv)
    if args.command == "docs":
        return _serve_docs(args.host, args.port, open_browser=not args.no_open)
    # argparse's ``required=True`` catches this, but static analysers appreciate.
    return 2  # pragma: no cover


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
