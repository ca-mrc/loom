"""`python -m loom_benchmark_tool <subcommand>` — deprecation shim.

The five subcommands (`list`, `import`, `publish`, `register`, `verify`)
moved to `loom datasets <subcommand>` (modular-D). This entry-point
stays for back-compat: prints a one-line deprecation note to stderr,
then re-runs the same argv under `loom_cli.datasets_cmd.dispatch`.

The old HF/direct publication implementation is retired. Publication now uses
`loom datasets publish PATH` or `loom datasets publish --benchmark SLUG`.
Other historical lifecycle modules remain available.
"""

from __future__ import annotations

import sys


def main() -> None:
    sys.stderr.write(
        "warning: `python -m loom_benchmark_tool` is deprecated; use "
        "`loom datasets <subcommand>` instead. "
        "this shim simply forwards to the new entry point.\n",
    )

    # The old CLI had a bare `list` subcommand that called run_list()
    # against the entry-point REGISTRY (not the discovery union). Keep
    # that exact behavior — operators were used to seeing the raw
    # registry rows.
    argv = sys.argv[1:]
    if argv and argv[0] == "list":
        from loom_benchmark_tool.list_cmd import run_list

        print(run_list())
        return

    from loom_cli.datasets_cmd import dispatch as datasets_dispatch

    rc = datasets_dispatch(argv)
    sys.exit(rc)


if __name__ == "__main__":
    main()
