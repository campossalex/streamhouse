# Adding a column to a tiered Fluss Log table while a writer is running

**Asking for advice:** what is the recommended way to run `ALTER TABLE ... ADD` on a
datalake-enabled (Iceberg) Fluss Log table that streaming jobs are writing to and reading
from? The ALTER itself works on 1.0.0. The trouble is the running writer: until it is
restarted, any read that selects the new column fails. Below is what we saw, the runbook
we pieced together, and the points we are unsure about.

## Environment

| | |
|---|---|
| Fluss | 1.0.0 (`apache/fluss:1.0.0`, one coordinator, one tablet server) |
| Flink | 1.20 (`apache/flink:1.20-java17`), `fluss-flink-1.20-1.0.0.jar` |
| Lake | Iceberg 1.10.1 through `fluss-lake-iceberg-1.0.0`, Lakekeeper REST catalog, MinIO (S3FileIO) |
| Tiering | `fluss-flink-tiering-1.0.0`, one Flink job, `table.datalake.freshness = 30s` |

The table is an append-only Log table (no primary key, 3 buckets) with tiering enabled:

```sql
CREATE TABLE fluss.orders.orders_enriched (
  `order_id` STRING, `customer_id` STRING, `product_id` STRING,
  `product_name` STRING, `category` STRING, `status` STRING,
  `amount` DECIMAL(10, 2), `unit_price` DECIMAL(10, 2), `event_time` TIMESTAMP(3),
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
) WITH ('bucket.num' = '3');

ALTER TABLE fluss.orders.orders_enriched
SET ('table.datalake.enabled' = 'true', 'table.datalake.freshness' = '30s');
```

Jobs touching it at the time of the change:

- **Writer:** `product-enrichment`, a lookup join with
  `INSERT INTO fluss.orders.orders_enriched SELECT <9 columns> ...`.
- **Streaming reader:** `revenue-analytics-sink`, a 1-minute `TUMBLE` over the table.
  It doesn't read the new column.
- **Tiering service:** commits the table to Iceberg.

## What we did and saw

1. **The ALTER succeeds.** We waited for the first tiering commit (1,338 rows in Iceberg),
   then ran:
   ```sql
   ALTER TABLE fluss.orders.orders_enriched ADD (`discount` DECIMAL(10, 2) COMMENT 'add-column test');
   ```
   - `DESCRIBE` in Flink shows the 10th column.
   - `DESCRIBE` in Trino shows it on the Iceberg table as well.
   - (On 0.9.1 the same statement failed with
     `UnsupportedOperationException: Unsupported table change: ...TableChange$AddColumn`.
     It works there only on tables that aren't tiered.)

2. **Writes with the new column work.** A batch `INSERT` that fills `discount` wrote a
   row, and the row reached Iceberg with `discount = 1.50`. The rows tiered earlier read
   `discount` as NULL in Trino. Tiering kept committing, and all jobs stayed `RUNNING`.

3. **Selecting the new column through Fluss fails.**
   - A batch `SELECT COUNT(*) FROM fluss.orders.orders_enriched` (union read) works.
   - `SELECT COUNT(*), COUNT(discount) FROM fluss.orders.orders_enriched` fails:
     ```
     org.apache.fluss.exception.FetchException: Unexpected error code INVALID_COLUMN_PROJECTION
     while fetching at offset 724 from bucket TableBucket{tableId=2, bucket=1}:
     org.apache.fluss.exception.InvalidColumnProjectionException:
     Projected fields [9] is out of bound for schema with 9 fields.
       at org.apache.fluss.client.table.scanner.log.AbstractLogFetchCollector.handleInitializeErrors(AbstractLogFetchCollector.java:246)
       at org.apache.fluss.client.table.scanner.log.AbstractLogFetchCollector.initialize(AbstractLogFetchCollector.java:159)
       ...
       at org.apache.fluss.flink.source.reader.FlinkSourceSplitReader.fetch(FlinkSourceSplitReader.java:195)
     ```
   - With the default restart strategy, the query job sits in `RESTARTING` forever.
   - A few minutes later, the same query failed at offset **1801** of the same bucket.
     So records were still being written with the old 9-field schema after the ALTER.
     The running writer kept the schema it started with.

4. **The writer can't simply be resubmitted.**
   - The 9-column `INSERT INTO ... SELECT` no longer matches the 10-column table.
   - An explicit column list
     (`INSERT INTO t (order_id, ..., event_time) SELECT ...`) is rejected:
     ```
     ValidationException: Fluss table sink does not support partial updates for table
     without primary key. Please make sure the number of specified columns in INSERT INTO
     matches columns of the Fluss table.
     ```

5. **What fixed it:**
   - Stop the writer with a savepoint.
   - Add `CAST(NULL AS DECIMAL(10, 2)) AS discount` to its `SELECT`.
   - Restore it from the savepoint.

   Within about 5 minutes (we didn't time it exactly), `COUNT(discount)` returned
   `9174 | 1`. We think it started working once tiering had committed past the last
   old-schema record. That way, the union read reads those records from Iceberg (where
   the column fills as NULL) instead of from the Fluss log.

## The runbook we use now

1. **Check first.**
   - The new column must be nullable and goes at the end.
   - The table's lake format must be Paimon or Iceberg, not Hudi or Lance.
   - List every job that writes to the table.
2. **Stop each writer with a savepoint.**
   ```bash
   curl -X POST localhost:8081/jobs/<jid>/stop -H 'Content-Type: application/json' \
     -d '{"targetDirectory":"file:///tmp/flink-checkpoints/savepoints","drain":false}'
   ```
3. **Run the ALTER.**
   ```sql
   ALTER TABLE fluss.orders.orders_enriched ADD (`discount` DECIMAL(10, 2));
   ```
   Never add the column to the Iceberg table from another engine. The docs warn that a
   schema mismatch stops tiering.
4. **Change each writer's SQL so its `SELECT` returns every column of the new schema,**
   in order. Use `CAST(NULL AS <type>) AS <col>` until the job fills the column.
5. **Restore each writer from its savepoint:**
   `SET 'execution.savepoint.path' = '<path>';` then the `INSERT`.
6. **Hold off on readers of the new column** until tiering has committed past the last
   record written with the old schema. Check `orders_enriched$snapshots` in Trino or
   `$lake$snapshots` in Flink.
7. **Leave existing readers alone.** Streaming readers that don't select the new column
   kept running without a restart.

We put the ALTER *between* stopping and restoring the writers. With that order, no
old-schema records are written after the ALTER. Records written before it remain in the
Fluss log until they are tiered and, later, expire.

## Questions

1. **Is the projection failure expected or a bug?** Each record batch carries a schema
   id, so we expected the log reader to fill a column missing from an older-schema batch
   with NULL (as the Iceberg side does) rather than fail. If it's expected, does it also
   hit a streaming reader that starts from `earliest`, until the old-schema segments
   expire?
2. **Can a running writer pick up the new schema** without a stop and restore? Or is
   restarting every writer the intended procedure?
3. **Is there a way to `INSERT` with fewer columns into a Log table,** with the missing
   nullable columns written as NULL? Today a column list counts as a partial update and
   is rejected without a primary key. That forces every writer's SQL to change with each
   added column.
4. **Is restoring from the savepoint safe** after adding a constant column to the
   `SELECT`? It worked here: the topology didn't change and the source offsets resumed.
   Is there anything about the Fluss sink or source state we should watch for, or should
   we set operator UIDs explicitly?
5. **Is waiting for tiering the right way to make the new column readable?** Is there a
   better signal than watching snapshots, and is the 30 s freshness the bound we should
   expect?
6. **Does any of this differ for primary-key tables** (KV snapshots, changelog,
   lookups), or for Paimon instead of Iceberg? We have only tested an Iceberg-tiered Log
   table.
7. **Is `DROP COLUMN` or `RENAME COLUMN` planned for tiered tables?** Without them, a
   mistaken ADD can only be undone by recreating the table.
