"""Run the data quality audit and render a report.

Usage::

    python3 scripts/data_audit.py                 # markdown to stdout
    python3 scripts/data_audit.py -o report.md    # markdown to file
    python3 scripts/data_audit.py --json          # JSON to stdout
    python3 scripts/data_audit.py --strict        # exit 2 if any error

The default lookback window for calendar-gap checks is 60 trading days.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.data.audit import (  # noqa: E402
    render_json,
    render_markdown,
    run_all_checks,
    summarise,
)
from src.data.storage import init_schema  # noqa: E402
from src.data.utils import configure_logger  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the data quality audit")
    parser.add_argument(
        "-o", "--output", type=Path,
        help="Write report to file instead of stdout",
    )
    parser.add_argument(
        "--json", action="store_true",
        help="Emit JSON instead of Markdown",
    )
    parser.add_argument(
        "--strict", action="store_true",
        help="Exit with non-zero status if any check has severity=error",
    )
    args = parser.parse_args(argv)

    configure_logger()
    init_schema()

    results = run_all_checks()
    text = render_json(results) if args.json else render_markdown(results)

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
        print(f"Report written to {args.output}")
    else:
        print(text)

    summary = summarise(results)
    if args.strict and summary["error"] > 0:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
