#!/usr/bin/env bash
# start.sh — streaming lakehouse workshop bootstrap
#
# The streaming lakehouse labs on open source — Apache Flink 1.20, Apache Fluss 0.9 and
# Apache Iceberg (Lakekeeper + MinIO), read back with Trino — all in Docker Compose.
#
# Usage:
#   cd streaming_lakehouse
#   ./start.sh                   bring everything up, start the pipeline and the tiering
#   ./start.sh --services-only   bring the stack up, submit nothing (the workshop mode)
#   ./start.sh --reset           docker compose down -v first: empty Fluss, empty lake

set -euo pipefail
cd "$(dirname "$0")"

# ── Options ────────────────────────────────────────────────────────────────────

SERVICES_ONLY=0
RESET=0

usage() {
  cat <<'USAGE'
Usage: ./start.sh [--services-only] [--reset]

  (no options)      Start the stack, submit every Flink SQL job, enable tiering and
                    start the tiering service — the finished state of all the labs.
  --services-only   Start the stack (containers, databases, Flink cluster, Fluss, the
                    lake) but submit nothing. This is the state attendees start the
                    workshop from; the manual steps are printed at the end either way.
  --reset           Remove every container and volume first. Fluss tables, Iceberg
                    snapshots, Kafka topics and the Postgres data are all rebuilt.
  -h, --help        Show this message.
USAGE
}

while [ $# -gt 0 ]; do
  case "$1" in
    --services-only) SERVICES_ONLY=1; shift ;;
    --reset)         RESET=1; shift ;;
    -h|--help)       usage; exit 0 ;;
    *)               echo "ERROR: unknown option '$1'" >&2; echo >&2; usage >&2; exit 2 ;;
  esac
done

# ── Prerequisite checks ────────────────────────────────────────────────────────

require_cmd() { command -v "$1" &>/dev/null || { echo "ERROR: '$1' not found"; exit 1; }; }
require_cmd docker
require_cmd curl
require_cmd jq

# ── The manual runbook ─────────────────────────────────────────────────────────
# Printed at the end of every run: after --services-only it is the list of what to
# do next, and after a normal run it is how to redo any single step by hand.

print_manual_steps() {
  cat <<'STEPS'

  ── Run the labs step by step ────────────────────────────────────────────────

  WORKSHOP.md walks through every step. In short:

  1. Open the Flink SQL editor: http://localhost:8084
     The examples menu has each flink_sql/ddl/ file, every query from
     flink_sql/explore.sql, and each job. Run the DDL files first.

  2. Submit the jobs — from the editor (the "Jobs" examples), or from a terminal:

       docker compose exec sql-client /opt/submit.sh kafka     # Lab 1  Kafka -> Fluss
       docker compose exec sql-client /opt/submit.sh cdc       # Lab 2  Postgres CDC -> Fluss
       docker compose exec sql-client /opt/submit.sh enrich    # Lab 2  lookup join

  3. Lab 3 — start the tiering service, then opt a table in. The service is already
     deployed: click "Start" next to "Tiering service" in the SQL editor's header.
     From a terminal instead:

       docker compose exec jobmanager /opt/tiering.sh
       docker compose exec sql-client /opt/submit.sh lake

  4. Lab 4 — query the lake from Trino in CloudBeaver: http://localhost:8978
     (the queries are in trino_sql/lakehouse.sql), or from a terminal:

       docker compose exec trino trino --catalog lakehouse --schema orders

  5. Lab 5:

       docker compose exec sql-client /opt/submit.sh revenue   # -> Grafana

  ── Useful commands ──────────────────────────────────────────────────────────

  Running jobs:
       curl -s http://localhost:8081/jobs/overview | jq -r '.jobs[] | "\(.state) \(.name)"'

  Raw events on the topic:
       docker compose exec redpanda rpk topic consume orders_log --num 3

  Change a product and watch CDC carry it through to Fluss:
       docker compose exec postgres psql -U root -d shop -c \
         "UPDATE product_catalog SET unit_price = 1.99 WHERE product_id = 'PRD-00001';"

  What Fluss has written to MinIO:
       docker compose run --rm --entrypoint /bin/sh minio-init -c \
         'mc alias set l http://minio:9000 admin password >/dev/null && mc tree l/warehouse'

STEPS
}

# ── Step 1: Download connector JARs ───────────────────────────────────────────

if [ ! -f lib/tiering/fluss-flink-tiering-1.0.0.jar ] || [ -z "$(ls -A lib/*.jar 2>/dev/null)" ]; then
  echo "==> Downloading connector JARs..."
  bash scripts/download-jars.sh
else
  echo "==> [skip] lib/ already populated ($(ls lib/*.jar 2>/dev/null | wc -l | tr -d ' ') JARs + tiering)"
fi

# ── Step 2: Start Docker Compose services ─────────────────────────────────────

if [ "$RESET" = "1" ]; then
  echo ""
  echo "==> Removing containers and volumes..."
  docker compose down -v --remove-orphans
fi

echo ""
echo "==> Starting Docker Compose services..."
docker compose up -d --remove-orphans

echo ""
echo "==> Waiting for core services to be healthy..."

wait_healthy() {
  local service="$1" max_attempts="${2:-30}" attempt=0
  echo -n "    $service "
  until [ "$(docker compose ps "$service" --format '{{.Health}}' 2>/dev/null)" = "healthy" ]; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge "$max_attempts" ]; then
      echo " TIMEOUT"
      echo "ERROR: $service did not become healthy within $(( max_attempts * 5 ))s"
      docker compose logs "$service" --tail=20
      exit 1
    fi
    echo -n "."
    sleep 5
  done
  echo " OK"
}

wait_healthy redpanda 24
wait_healthy postgres 24
wait_healthy postgres-dwh 24
wait_healthy lakekeeper 24
wait_healthy coordinator-server 36
wait_healthy jobmanager 24
wait_healthy sql-gateway 24
wait_healthy trino 36

# The warehouse is created by a one-shot container, so its absence here means that
# container failed rather than that something is still starting.
if ! curl -sf http://localhost:8181/management/v1/warehouse | grep -q '"name":"lakehouse"'; then
  echo ""
  echo "ERROR: the 'lakehouse' warehouse was not created in Lakekeeper."
  docker compose logs lakekeeper-warehouse --tail=20
  exit 1
fi

# The TaskManager has no healthcheck; a job submitted before it registers just queues.
echo -n "    taskmanager "
attempt=0
until [ "$(curl -s http://localhost:8081/overview | jq -r '.taskmanagers // 0')" -ge 1 ]; do
  attempt=$((attempt + 1))
  [ "$attempt" -ge 24 ] && { echo " TIMEOUT"; docker compose logs taskmanager --tail=20; exit 1; }
  echo -n "."
  sleep 5
done
echo " OK"

echo ""
echo "==> All core services healthy."

# ── Deploy — but do not start — the tiering service ───────────────────────────
# Flink has no stopped deployments, so "deployed" means the JAR is uploaded to the
# JobManager: it shows under Submit New Job, and the SQL editor's "Tiering service:
# Start" button runs it with tiering.args. The editor does the upload, so it is done
# the same way its Start button would redo it after a JobManager restart.

echo ""
echo -n "==> Deploying the tiering service (not starting it) "
attempt=0
until curl -sf http://localhost:8084/api/config >/dev/null; do
  attempt=$((attempt + 1))
  [ "$attempt" -ge 36 ] && { echo " TIMEOUT"; docker compose logs sql-editor --tail=20; exit 1; }
  echo -n "."
  sleep 5
done
if curl -sf -X POST http://localhost:8084/api/tiering/deploy | jq -e '.deployed' >/dev/null; then
  echo " OK"
else
  echo " FAILED"
  echo "WARNING: could not upload the tiering JAR — the editor's Start button will retry."
fi

# ── --services-only stops here ────────────────────────────────────────────────

if [ "$SERVICES_ONLY" = "1" ]; then
  cat <<'BANNER'

══════════════════════════════════════════════════════════════
  Streaming Lakehouse OSS — services running, no jobs submitted
══════════════════════════════════════════════════════════════

  Environment home    →  http://localhost        links to everything below
  Flink SQL editor    →  http://localhost:8084   start here
  Flink Web UI        →  http://localhost:8081   (no jobs yet; tiering JAR deployed)
  CloudBeaver (Trino) →  http://localhost:8978
  Lakekeeper UI       →  http://localhost:8181/ui
  MinIO Console       →  http://localhost:9001   admin / password
  Redpanda Console    →  http://localhost:8082
  Grafana             →  http://localhost:3000   (empty until Lab 5)

  Kafka (from host):      localhost:19092   topic orders_log
  PostgreSQL shop (from host): localhost:5432   shop_user/admin1   db shop
  PostgreSQL dwh  (from host): localhost:5433   dwh_user/admin1    db dwh
BANNER
  print_manual_steps
  echo "  Stop everything:  docker compose down -v"
  echo "══════════════════════════════════════════════════════════════"
  exit 0
fi

# ── Step 3: Submit the pipeline ───────────────────────────────────────────────
# submit.sh runs inside the sql-client container: it loads all DDL into each session
# with -i and submits each job with -f. The tiering service starts first, so that the
# `lake` step's ALTER TABLE is picked up on its first round.

echo ""
echo "==> Starting the Fluss tiering service (Fluss -> Iceberg)..."
docker compose exec -T jobmanager /opt/tiering.sh || {
  echo ""
  echo "WARNING: the tiering service did not start — see the output above."
}

echo ""
echo "==> Submitting Flink SQL jobs..."
docker compose exec -T sql-client /opt/submit.sh || {
  echo ""
  echo "WARNING: at least one step failed — see the output above."
}

echo ""
echo "==> Verifying..."
sleep 5
curl -s http://localhost:8081/jobs/overview | jq -r '.jobs[] | "    \(.state)  \(.name)"' | sort

# ── Step 4: Status summary ────────────────────────────────────────────────────

cat <<'BANNER'

══════════════════════════════════════════════════════════════
  Streaming Lakehouse OSS — Setup Complete
══════════════════════════════════════════════════════════════

  Environment home    →  http://localhost        links to everything below
  Flink SQL editor    →  http://localhost:8084
  Flink Web UI        →  http://localhost:8081
  CloudBeaver (Trino) →  http://localhost:8978   first rows in ~1 minute
  Lakekeeper UI       →  http://localhost:8181/ui
  MinIO Console       →  http://localhost:9001   admin / password
  Redpanda Console    →  http://localhost:8082
  Grafana             →  http://localhost:3000   first 1-minute window in ~2 minutes

  Kafka (from host):      localhost:19092   topic orders_log
  PostgreSQL shop (from host): localhost:5432   shop_user/admin1   db shop
  PostgreSQL dwh  (from host): localhost:5433   dwh_user/admin1    db dwh

  Start again without submitting anything:
    ./start.sh --reset --services-only
BANNER
print_manual_steps
echo "  Stop everything:  docker compose down -v"
echo "══════════════════════════════════════════════════════════════"
