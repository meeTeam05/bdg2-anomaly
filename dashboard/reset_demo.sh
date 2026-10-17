#!/usr/bin/env bash
# Reset a demo run: Kafka, Spark checkpoint and Delta output must go together,
# otherwise Spark fails on missing offsets or re-reads and duplicates rows.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(dirname "$here")"

docker compose -f "$here/docker-compose.yml" down
rm -rf "$repo/streaming/output"
docker compose -f "$here/docker-compose.yml" up -d --wait
echo "Reset done. Kafka is up at localhost:9094, streaming/output removed."
