#!/usr/bin/env bash
# scripts/download-jars.sh — fetch connector JARs into ./lib/ for Flink and Fluss.
#
# Layout, and who loads what:
#
#   lib/*.jar           mounted read-only at /opt/flink/lib/extra/ in every Flink container
#                       (jobmanager, taskmanager, sql-client, sql-gateway) and copied into
#                       /opt/flink/lib/ by their entrypoints. The iceberg-aws, iceberg-aws-bundle
#                       and failsafe JARs are ALSO copied into the Fluss servers'
#                       plugins/iceberg/ directory — see the coordinator-server entrypoint.
#   lib/tiering/*.jar   NOT on any classpath. The tiering service is a Flink job, submitted
#                       with `flink run` by tiering.sh; its JAR is the job's main artifact.
#
# Safe to re-run — present JARs are skipped.

set -euo pipefail

MAVEN="https://repo1.maven.org/maven2"
FLINK_VERSION="1.20"
FLUSS_VERSION="1.0.0"
# Must match the Iceberg build bundled inside fluss-lake-iceberg (1.0.0 ships
# Iceberg 1.10.1). A mismatch surfaces as NoSuchMethodError at the first tiering commit.
# Verify with:  unzip -p lib/fluss-lake-iceberg-*.jar iceberg-build.properties
ICEBERG_VERSION="1.10.1"
KAFKA_CONNECTOR_VERSION="3.3.0-1.20"
CDC_CONNECTOR_VERSION="3.2.1"
JDBC_CONNECTOR_VERSION="3.3.0-1.20"   # core + postgres modules, see below
POSTGRES_DRIVER_VERSION="42.7.4"

LIB_DIR="$(dirname "$0")/../lib"
mkdir -p "$LIB_DIR/tiering"

download() {
  local filename="$1" url="$2"
  if [ -f "$LIB_DIR/$filename" ]; then
    echo "  [skip] $filename already present"
    return
  fi
  echo "  [download] $filename"
  curl -fsSL -o "$LIB_DIR/$filename.part" "$url"
  mv "$LIB_DIR/$filename.part" "$LIB_DIR/$filename"
}

# Fluss JARs of any other version, left by an earlier run. Every lib/*.jar goes onto the
# Flink classpath, and tiering.sh / the SQL editor take the first fluss-flink-tiering-*.jar,
# so a stale one would load two Fluss versions side by side.
for stale in "$LIB_DIR"/fluss-*.jar "$LIB_DIR"/tiering/fluss-*.jar; do
  [ -e "$stale" ] || continue
  case "$(basename "$stale")" in
    *"-${FLUSS_VERSION}.jar") ;;
    *) echo "  [remove] $(basename "$stale") (not Fluss ${FLUSS_VERSION})"; rm -f "$stale" ;;
  esac
done

echo "==> Fluss: Flink connector + the tiering service..."
download "fluss-flink-${FLINK_VERSION}-${FLUSS_VERSION}.jar" \
  "$MAVEN/org/apache/fluss/fluss-flink-${FLINK_VERSION}/${FLUSS_VERSION}/fluss-flink-${FLINK_VERSION}-${FLUSS_VERSION}.jar"

download "tiering/fluss-flink-tiering-${FLUSS_VERSION}.jar" \
  "$MAVEN/org/apache/fluss/fluss-flink-tiering/${FLUSS_VERSION}/fluss-flink-tiering-${FLUSS_VERSION}.jar"

echo "==> Fluss ↔ Iceberg: the lake format plugin + S3FileIO for MinIO..."
# fluss-lake-iceberg is what both the tiering job (writing) and the Fluss connector's
# union read (reading) use to talk to Iceberg. It bundles iceberg-core but NOT S3FileIO,
# which lives in iceberg-aws and needs the AWS SDK from iceberg-aws-bundle plus failsafe
# (iceberg-aws's retry library). Without the three, the first commit fails with
#   ClassNotFoundException: org.apache.iceberg.aws.s3.S3FileIO
download "fluss-lake-iceberg-${FLUSS_VERSION}.jar" \
  "$MAVEN/org/apache/fluss/fluss-lake-iceberg/${FLUSS_VERSION}/fluss-lake-iceberg-${FLUSS_VERSION}.jar"

download "iceberg-aws-${ICEBERG_VERSION}.jar" \
  "$MAVEN/org/apache/iceberg/iceberg-aws/${ICEBERG_VERSION}/iceberg-aws-${ICEBERG_VERSION}.jar"

download "iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
  "$MAVEN/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"

download "failsafe-3.3.2.jar" \
  "$MAVEN/dev/failsafe/failsafe/3.3.2/failsafe-3.3.2.jar"

echo "==> Iceberg's own Flink catalog (optional — the 'iceberg' catalog in explore.sql)..."
download "iceberg-flink-runtime-${FLINK_VERSION}-${ICEBERG_VERSION}.jar" \
  "$MAVEN/org/apache/iceberg/iceberg-flink-runtime-${FLINK_VERSION}/${ICEBERG_VERSION}/iceberg-flink-runtime-${FLINK_VERSION}-${ICEBERG_VERSION}.jar"

# The apache/flink image ships without Hadoop, and Iceberg code paths — the Flink
# catalog factory, and the Iceberg writer inside the tiering job — instantiate
# org.apache.hadoop.conf.Configuration even when every byte goes through S3FileIO.
#
# Trino's hadoop-apache, NOT flink-shaded-hadoop-2-uber. The uber JAR bundles Avro 1.7,
# which shadows the Avro 1.12 Iceberg is built against, and the tiering job restart-loops
# on its first write with
#   NoSuchMethodError: 'org.apache.avro.LogicalTypes$TimestampNanos
#                       org.apache.avro.LogicalTypes.timestampNanos()'
# hadoop-apache relocates all of Hadoop's third-party dependencies, Avro included.
download "hadoop-apache-3.3.5-2.jar" \
  "$MAVEN/io/trino/hadoop/hadoop-apache/3.3.5-2/hadoop-apache-3.3.5-2.jar"

# A lib/ populated by an earlier version of this script would still carry the uber JAR,
# and every JAR in lib/ is copied onto the Flink classpath.
if [ -f "$LIB_DIR/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar" ]; then
  echo "  [remove] flink-shaded-hadoop-2-uber-2.8.3-10.0.jar (Avro 1.7 clash, see above)"
  rm -f "$LIB_DIR/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar"
fi

echo "==> Flink SQL connectors: Kafka, PostgreSQL CDC, JDBC..."
download "flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" \
  "$MAVEN/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar"

download "flink-sql-connector-postgres-cdc-${CDC_CONNECTOR_VERSION}.jar" \
  "$MAVEN/org/apache/flink/flink-sql-connector-postgres-cdc/${CDC_CONNECTOR_VERSION}/flink-sql-connector-postgres-cdc-${CDC_CONNECTOR_VERSION}.jar"

# Lab 5 only: the postgres JDBC catalog (ddl/03_postgres.sql) and its revenue_1m sink.
#
# The core and postgres modules, NOT the all-in-one flink-connector-jdbc JAR. That one
# registers both the deprecated and the new JdbcCatalogFactory under 'jdbc', and every
# CREATE CATALOG ... 'type' = 'jdbc' fails with
#   Multiple factories for identifier 'jdbc' that implement 'CatalogFactory'
# The core module also carries the 'connector' = 'jdbc' table factory.
download "flink-connector-jdbc-core-${JDBC_CONNECTOR_VERSION}.jar" \
  "$MAVEN/org/apache/flink/flink-connector-jdbc-core/${JDBC_CONNECTOR_VERSION}/flink-connector-jdbc-core-${JDBC_CONNECTOR_VERSION}.jar"

download "flink-connector-jdbc-postgres-${JDBC_CONNECTOR_VERSION}.jar" \
  "$MAVEN/org/apache/flink/flink-connector-jdbc-postgres/${JDBC_CONNECTOR_VERSION}/flink-connector-jdbc-postgres-${JDBC_CONNECTOR_VERSION}.jar"

# A lib/ populated by an earlier version of this script would still carry the all-in-one
# JAR, and both on the classpath bring the duplicate factory back.
if [ -f "$LIB_DIR/flink-connector-jdbc-${JDBC_CONNECTOR_VERSION}.jar" ]; then
  echo "  [remove] flink-connector-jdbc-${JDBC_CONNECTOR_VERSION}.jar (duplicate 'jdbc' catalog factory, see above)"
  rm -f "$LIB_DIR/flink-connector-jdbc-${JDBC_CONNECTOR_VERSION}.jar"
fi

download "postgresql-${POSTGRES_DRIVER_VERSION}.jar" \
  "$MAVEN/org/postgresql/postgresql/${POSTGRES_DRIVER_VERSION}/postgresql-${POSTGRES_DRIVER_VERSION}.jar"

echo ""
echo "==> JARs in $LIB_DIR:"
ls -lh "$LIB_DIR" "$LIB_DIR/tiering"
echo ""
echo "Done. Run ./start.sh (or 'docker compose up -d') to start the stack."
