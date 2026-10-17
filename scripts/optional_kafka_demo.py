"""Check the optional Kafka extra. This script does not open a socket.

CI does not run it. A compose file in the repo root can start Postgres and
Redpanda for a person who installs `cdc-lab[kafka]` and wires `KafkaBroker`
themselves. This file only reports whether the extra imports.
"""

from __future__ import annotations

import sys


def main() -> int:
    try:
        import confluent_kafka  # noqa: F401
    except ImportError:
        sys.stderr.write(
            "Kafka extra is not installed. Use: pip install 'cdc-lab[kafka]'\n"
        )
        return 2
    sys.stdout.write(
        "confluent-kafka imports. No broker was contacted. "
        "See docker-compose.yml if you want a local Redpanda.\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
