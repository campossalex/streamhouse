# Streaming Lakehouse (open-source edition): reference

How the environment is built and why, for whoever runs or changes it. To deploy it, see
[README.md](../README.md); for the attendee walkthrough, [WORKSHOP.md](../WORKSHOP.md).

---

## Architecture

```
  ┌───────────┐   orders_log    ┌──────────────────────────────┐
  │ order-gen │ ──────────────► │ Redpanda (Kafka)             │
  └───────────┘                 └──────────────┬───────────────┘
                                               │ job: kafka-to-fluss
                                               ▼
                                 fluss.orders.orders_log  (Log table)
                                               │
   PostgreSQL shop.product_catalog             │ job: product-enrichment
     │ job: pgcdc-to-fluss (postgres-cdc)      │ lookup join FOR SYSTEM_TIME AS OF
     ▼                                         ▼
   fluss.orders.product_lookup (PK) ──► fluss.orders.orders_enriched  (Log table)
                                               │          │
                  job: Fluss Lake Tiering      │          │ job: revenue-analytics-sink
                  Service (tiering.sh)         ▼          ▼   (TUMBLE 1 min)
                                               │   PostgreSQL dwh.revenue_1m ──► Grafana
                                               ▼
               Iceberg  lakehouse.orders.orders_enriched
               ├─ metadata ── Lakekeeper (REST catalog) ── catalog-db (Postgres)
               └─ Parquet ─── MinIO  s3://warehouse/lakehouse/
                                               │
                           Trino ◄─────────────┘──► CloudBeaver
```

| Component | Container(s) | Role |
|-----------|--------------|------|
| Fluss | `zookeeper`, `coordinator-server`, `tablet-server` | Log and PK tables; creates the Iceberg table when one is ALTERed to `table.datalake.enabled` |
| Flink | `jobmanager`, `taskmanager` | Session cluster: the pipeline jobs, the tiering service, ad hoc queries |
| Flink SQL | `sql-gateway`, `sql-editor`, `sql-client` | Web editor (via the gateway's REST API) and the terminal client |
| Lake | `minio`, `lakekeeper`, `catalog-db` | Object storage and the Iceberg REST catalog |
| Readers | `trino`, `cloudbeaver` | Plain Iceberg reads with no Fluss in the path |
| Homepage | `homepage` | nginx serving `homepage/index.html` on port 80: a card per web UI, links built from the host the page was opened on, and a reachability check every 15 s |
| Sources/sinks | `redpanda`, `postgres` (shop), `postgres-dwh` (data warehouse), `order-gen`, `grafana` | Order events, product catalog (CDC), the warehouse table revenue_1m, order analytics dashboard |

---

## Running the steps by hand

The attendee path is the SQL editor. Every step also has a terminal equivalent, which is
what `start.sh` uses:

```bash
# Lab 1 and 2 — the pipeline jobs
docker compose exec sql-client /opt/submit.sh kafka cdc enrich

# Lab 3 — the tiering service, then opt the tables in. Attendees use the editor's
# "Tiering service: Start" button instead (see "The tiering service" below)
docker compose exec jobmanager /opt/tiering.sh
docker compose exec sql-client /opt/submit.sh lake

# Lab 5
docker compose exec sql-client /opt/submit.sh revenue

# An interactive SQL client. Only 02_sources.sql is needed: see "What survives a session"
docker compose exec sql-client /opt/flink/bin/sql-client.sh -i /opt/sql/ddl/02_sources.sql

# Trino from a terminal
docker compose exec trino trino --catalog lakehouse --schema orders
```

`submit.sh` concatenates `flink_sql/ddl/*.sql` into one init file, loads it with `-i`,
and runs one job file with `-f` per name; it fails a step that prints `[ERROR]` or that
should have produced a Job ID and did not. `tiering.sh` refuses to start a second copy of
the tiering service.

### Files

| File | Purpose |
|------|---------|
| `flink_sql/ddl/01_fluss.sql` | The `fluss` catalog, the `orders` database and the three Fluss tables |
| `flink_sql/ddl/02_sources.sql` | Kafka source and postgres-cdc source |
| `flink_sql/ddl/03_postgres.sql` | The `postgres` JDBC catalog; Lab 5 writes `postgres.dwh.revenue_1m` through it (column meanings: `postgres/pg_dwh_ddl.sql`) |
| `flink_sql/jobs/10…50_*.sql` | One long-running `INSERT INTO` each, in lab order |
| `flink_sql/lake/enable_tiering.sql` | The `ALTER TABLE ... 'table.datalake.enabled'` that opts `orders_enriched` in to tiering |
| `flink_sql/explore.sql` | The labs' SELECTs and ALTERs; one editor example per `-- ---- title` section |
| `trino_sql/lakehouse.sql` | The Trino queries for CloudBeaver: Steps 13 and 16–18 |
| `sql-editor/` | Flask proxy (`app.py`) in front of the SQL Gateway, and the one-page UI |
| `scripts/tiering.sh`, `scripts/submit.sh` | Mounted into `jobmanager` and `sql-client` respectively |
| `scripts/capture-screenshots.mjs` | Retakes WORKSHOP.md's screenshots (`docs/screenshots/`) from a running full-mode stack, through headless Chrome; Node 22+, no packages |
| `scripts/tiering.args` | The tiering service's program arguments — the one copy, used by `tiering.sh` and the editor's Start button |
| `postgres/pg_shop_ddl.sql`, `postgres/pg_dwh_ddl.sql` | The two PostgreSQL servers' setup, run once each via `docker-entrypoint-initdb.d`: `shop` seeds `product_catalog` (500 rows), its users (`shop_user`, `cdc_user`) and the CDC slot and publication; `dwh` creates `revenue_1m` and `dwh_user` |

---

## The web SQL editor

Two services provide http://localhost:8084:

- **`sql-gateway`**: Flink's SQL Gateway, the engine the SQL Client embeds, exposed as a
  REST API on port 8083. Sessions never expire (`sql-gateway.session.idle-timeout=0`), so
  a coffee break does not drop an attendee's tables.
- **`sql-editor`**: a small Flask app that serves the page and proxies to the gateway,
  which sends no CORS headers.

One session per
browser, kept across reloads; statements run in order, each with its own result panel; a
streaming `SELECT` stays live until **Cancel**, which cancels its Flink job; an `INSERT
INTO` is a real job that outlives the tab. The examples menu has each `ddl/` file, every
`explore.sql` section and each job file (with a trailing `RESET 'pipeline.name'`).

Details worth knowing:

- **Catalog browser.** The header's **Catalog** button opens a tree of catalogs,
  databases, tables and columns (`/api/catalog/<session>`, which runs `SHOW` and
  `DESCRIBE`). It runs in the page's own session, so it also lists that session's
  in-memory tables. After a script, only the levels its successful `CREATE`, `DROP` and
  `ALTER` statements touched are re-fetched (the catalog list, one catalog's databases,
  or one database's tables, plus an altered table's columns), in place: open nodes stay
  open and new ones flash. Unqualified names resolve against the session's current
  catalog and database, followed through the script's `USE` statements. Views are
  listed with the tables: Flink 1.20's `SHOW VIEWS` has no `IN` clause.
  Each table's **Info** button opens its structure in a dialog
  (`/api/catalog/<session>/structure`: `DESCRIBE` plus `SHOW CREATE TABLE`, or
  `SHOW CREATE VIEW` for a view): columns with keys and watermarks, the `WITH` options,
  tiering and merge-engine settings at a glance, and the full CREATE statement.
- **Waiting indicator.** A running statement's panel shows a spinner and the elapsed
  time until its first row arrives, with a hint after 10 s about slow starts on tiered
  tables.
- **Queries run at-least-once.** A query's rows reach the client through Flink's collect
  sink, which under exactly-once checkpointing (every session has a 30 s interval) holds
  them until a checkpoint completes: first rows after up to 30 s, then a batch every
  30 s. The editor sends each `SELECT`, `WITH`, `VALUES` and `TABLE` statement with
  `execution.checkpointing.mode = AT_LEAST_ONCE` (the gateway's per-statement
  `executionConfig`), so rows stream as they come, typically within 1–2 s. At worst a
  failover repeats some rows. `INSERT` jobs keep the session's exactly-once mode.
- **No placeholder substitution and no redaction.** The workshop has no secrets, so the
  SQL files use literal hostnames and statements reach the gateway verbatim.
- **Optimizer hints are kept.** The statement splitter drops `/* ... */` comments but
  passes `/*+ ... */` through: dropping `/*+ OPTIONS('snapshot-id' = '...') */` would
  silently make a time-travel query read the latest snapshot instead.
- **`PRELOAD_DDL=true` is safe to combine with attendees' own DDL**: every statement in
  `ddl/` is `IF NOT EXISTS`.

---

## The tiering service: deployed, then started

In the VVP edition the tiering service is a Deployment that attendees open and click
**Start** on. Flink has no stopped deployments — a job either runs or does not exist —
so this edition splits it the nearest way it can:

- **Deploy:** `start.sh` (both modes) has the editor upload `lib/tiering/fluss-flink-tiering-*.jar`
  to the JobManager. An uploaded JAR is listed in the Flink Web UI under *Submit New Job*,
  ready to run.
- **Start:** the editor's header shows *Tiering service: deployed, not running* and a
  **Start** button, which runs the uploaded JAR with `tiering.args` through the
  JobManager's REST API (`POST /jars/<id>/run`) — the same call the Web UI makes. The
  header then shows the job's state, polled every 10 s, with a link to it.

Details worth knowing:

- **Checkpointing.** A job submitted this way is built by the JobManager with the
  cluster's `config.yaml`, so it gets the 30 s checkpoint interval it needs to commit.
- **Only one copy.** Start does nothing when a tiering job is already active, however it
  was started, and so does `tiering.sh`. The Web UI's Submit New Job has no such check:
  submitting from there twice runs two tiering services that compete for the same tables.
- **JobManager restarts.** Uploaded JARs live in the JobManager's `/tmp`, so a restart
  forgets them and the header shows *not running* instead of *deployed, not running*.
  Start re-uploads the JAR itself first, so it still works.
- **Where it shows.** The editor's `/api/config` reports `tiering: true` only when it finds
  the JAR and `tiering.args` (mounted into `sql-editor` in `docker-compose.yml`); without them the
  control stays hidden.

---

## What survives a session

This is the one concept that differs most from the VVP edition, where tables live in a
persistent platform catalog.

| Object | Where it is stored | After *New session*, a gateway restart, or a new `sql-client` |
|--------|--------------------|------------------------------------------------------------------|
| `fluss.orders.*` tables | In Fluss | **Still there** |
| The `fluss`, `postgres` (and `iceberg`) catalog registrations | File-based CatalogStore, volume `catalog-store`, shared by `sql-client` and `sql-gateway` | **Still there** |
| `orders_log_kafka`, `product_catalog_cdc` | Flink's in-memory `source_catalog` (its built-in catalog, renamed with `table.builtin-catalog-name`) | **Gone** — re-run `ddl/02_sources.sql` |
| Running jobs | Flink cluster | **Unaffected**: connector options were baked into the JobGraph at submission |

So after a gateway restart, attendees only re-run `02_sources.sql`, and only if they
want to submit another job that reads Kafka or CDC.

---

## Implementation notes

| Concern | How this workshop does it |
|---------|---------------------------|
| Versions | Flink 1.20 (`apache/flink:1.20-java17`), Fluss 1.0.0, Iceberg 1.10.1 (the Iceberg build bundled in `fluss-lake-iceberg` 1.0.0) |
| Fluss config | `FLUSS_PROPERTIES` env var, appended to `server.yaml` by the image's entrypoint. Not a bind-mounted `server.yaml`: the entrypoint edits that file in place with `sed -i` |
| Fluss remote storage | A shared named volume at `/fluss/remote-data`, not `s3://` — see below |
| Iceberg catalog | Lakekeeper warehouse `lakehouse`, `sts-enabled: false`, `remote-signing-enabled: false`, static MinIO keys everywhere |
| Tiering service | `fluss-flink-tiering` JAR via `flink run -d` from the `jobmanager` container, so it inherits the cluster's 30s checkpoint interval — it commits to Iceberg on checkpoints |
| Iceberg plugin in Fluss | The image's `plugins/iceberg/` has `fluss-lake-iceberg` but not S3FileIO; the entrypoints copy `iceberg-aws`, `iceberg-aws-bundle` and `failsafe` in from `lib/` |
| Hadoop | Trino's relocated `hadoop-apache`, not `flink-shaded-hadoop-2-uber` — see below |
| CDC | `wal_level=logical` on the Postgres command line; slot and publication created by the init script |
| Revenue sink | `postgres.dwh.revenue_1m` (server `postgres-dwh`, user `dwh_user`) through a JDBC catalog, which reads the table's PRIMARY KEY from PostgreSQL (upsert). Needs the `flink-connector-jdbc-core` and `-postgres` JARs: the all-in-one `flink-connector-jdbc` 3.3.0 JAR registers two `jdbc` catalog factories and every `CREATE CATALOG` fails |
| Grafana | Datasource and the *Order Analytics* dashboard (`grafana/dashboards/order_analytics.json`) provisioned, pinned to the datasource uid `lakehouse_dwh` |

Four of these cost real debugging time and are worth knowing about before changing
anything.

**Fluss's remote data is a local volume, not MinIO.** With `remote.data.dir` on `s3://`,
Fluss hands clients short-lived STS credentials (`GetSessionToken`), which MinIO does not
implement, so every Flink read that touches remote data fails with
`NoAwsCredentialsException`. The Iceberg lake still lives on MinIO: Iceberg's S3FileIO
takes static keys. The volume is mounted at the same path in the Fluss servers and all
Flink containers, and is made world-writable by `fluss-remote-init`, because both sides
write to it — the servers their segments and snapshots, the tiering job its per-commit
`*.offsets` files.

**It is not mounted under `/tmp/fluss`.** Docker creates a mount point's missing parents
as `root:root 0755`, and `/tmp/fluss` is the Fluss client's scratch directory. Flink runs
as uid 9999, so a streaming read of a PK table — which downloads the table's KV snapshot
there first — failed with
`IOException: Failed to create directory /tmp/fluss/kv-snapshots-...`.

**Flink needs S3 keys as `AWS_*` environment variables.** Fluss gives clients a table's
`table.datalake.iceberg.*` options (see `SHOW CREATE TABLE`) but drops the credential
ones. Without `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION` on the Flink
containers, the union read, the `$lake` tables and the `$lake$snapshots` table all fail
with `SdkClientException: Unable to load credentials from any of the providers in the
chain`. The tiering job does not need them: `tiering.sh` passes the keys as arguments.

**No `flink-shaded-hadoop-2-uber`.** Iceberg needs Hadoop's `Configuration` class on the
classpath, and the usual way to get it on Flink is the uber JAR. But it bundles Avro 1.7,
which shadows the Avro 1.12 Iceberg is built against, and the tiering job restart-loops
on its first write with `NoSuchMethodError: ... LogicalTypes.timestampNanos()`. Trino's
`hadoop-apache` relocates all of Hadoop's dependencies, Avro included.
`scripts/download-jars.sh` deletes an uber JAR it finds left over in `lib/`.

---

## Troubleshooting

**`orders_enriched` never shows up in Trino.** Tiering needs all three of: the tiering
job running (`curl -s localhost:8081/jobs/overview | jq -r '.jobs[].name'` lists
`Fluss Lake Tiering Service - iceberg`), the `ALTER TABLE` having run, and new data in
the table. Then look at the TaskManager:

```bash
docker compose logs taskmanager | grep -E "Committed snapshot|Exception" | tail
```

**A read of a newly added column fails with `Projected fields [N] is out of bound for
schema with N fields`.** `ALTER TABLE ... ADD` on a tiered table updates Fluss and the
Iceberg table, but a job already writing to it keeps writing the old schema, and any read
that selects the new column fails on those records. Stop the writer with a savepoint, add
the column to its `SELECT` (Fluss rejects an `INSERT` column list on a Log table as a
partial update), and restore it from the savepoint. The read works again once tiering has
committed past the last old-schema record.

**A Fluss table can't be tiered: the ALTER fails, or its Iceberg table never fills.**
The table was created before the servers had `datalake.format: iceberg`. That setting is
copied into the table at `CREATE TABLE` time and fixes its bucketing; DROP and re-create
the table.

**`NoSuchMethodError: ... org.apache.avro.LogicalTypes.timestampNanos()`.** An old Avro
is on the Flink classpath — almost always `flink-shaded-hadoop-2-uber` in `lib/`. Re-run
`./scripts/download-jars.sh` (it removes it), then
`docker compose up -d --force-recreate jobmanager taskmanager sql-client sql-gateway`
and resubmit: the recreated JobManager has no jobs.

**`VerifyError` mentioning `S3V4RestSignerClient` at the first commit.** The Lakekeeper
warehouse was created with remote signing enabled. It must be `false`, as in
`lakekeeper/create-warehouse.json`; check with
`curl -s 'localhost:8181/catalog/v1/config?warehouse=lakehouse' | jq .overrides`.

**The CDC job fails with `replication slot ... is active`.** Only one reader per slot:
another `pgcdc-to-fluss` is already running, possibly one an attendee submitted. Cancel
the duplicate in the Flink Web UI.

**Ad hoc queries stay in `CREATED` / never return rows.** The TaskManager has 12 slots
and each running job holds one. Six pipeline jobs plus several forgotten streaming
SELECTs fill it up: cancel what you are not using, from the editor or the Flink Web UI.

**A streaming `SELECT` shows rows only every 30 s.** It ran exactly-once: rows are
released on checkpoints (see "Queries run at-least-once" above). The editor avoids this;
in the `sql-client` container, run `SET 'execution.checkpointing.mode' = 'AT_LEAST_ONCE';`
first — and `RESET` it before submitting an `INSERT`.

**Grafana panels are empty.** A window is written only once it closes. On a table with
history, every past window is written as soon as `revenue-analytics-sink` has
caught up; on an empty one, the first row lands about a minute after it starts. Check with
`docker compose exec postgres-dwh psql -U root -d dwh -c 'SELECT count(*) FROM revenue_1m;'`.

---

## Teardown

```bash
docker compose down -v   # remove containers and every volume — the normal way to stop
```

A plain `docker compose down` (no `-v`) is not a useful middle ground: PostgreSQL, MinIO
and the Lakekeeper catalog are on named volumes and survive, but ZooKeeper and the tablet
server keep Fluss's metadata and data inside their containers, so the Fluss tables are
gone while their Iceberg copies remain. Use `./start.sh --reset` to start clean (see [README.md](../README.md#run-locally)).

Whatever survives, the jobs do not: the session cluster has no HA. And resubmitting
`kafka-to-fluss` on top of existing Fluss data duplicates it, because the Kafka table
reads from `earliest-offset`. Between groups, `./start.sh --reset` is the only clean
restart.
