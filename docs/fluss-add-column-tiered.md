# ADD COLUMN on a tiered Log table with a running writer: recommended procedure?

**Setup:**
- Fluss 1.0.0, Flink 1.20, Iceberg 1.10.1 (REST catalog).
- The table is an append-only Log table with `table.datalake.enabled = true`.
- One streaming job writes to it with `INSERT INTO t SELECT <9 cols> ...`.

**What happens**

1. `ALTER TABLE t ADD (discount DECIMAL(10, 2))` succeeds, and the column also appears
   on the Iceberg table. New rows tier with the column, and old rows read NULL.
2. While the writer keeps running, it writes records with the old 9-field schema. Any read
   that selects the new column then fails:
   ```
   InvalidColumnProjectionException: Projected fields [9] is out of bound for schema with 9 fields.
   ```
   Reads that don't select it work fine.
3. The writer can't be resubmitted unchanged. Its 9-column `SELECT` no longer matches the
   table, and an explicit column list is rejected:
   `Fluss table sink does not support partial updates for table without primary key`.

**Workaround we use**

1. Stop the writer with a savepoint.
2. Run the `ALTER TABLE ... ADD`.
3. Add `CAST(NULL AS DECIMAL(10, 2)) AS discount` to the writer's `SELECT`.
4. Restore the writer from the savepoint.
5. Wait for tiering to commit past the old-schema records before reading the new column.

**Questions**

1. Is the projection failure on older-schema records expected, or should the reader fill
   the missing column with NULL?
2. Is stopping and restoring every writer the intended procedure, or can a running writer
   pick up the new schema?
3. Can an `INSERT` into a Log table leave out nullable columns, so they are written as NULL?
4. Does anything differ for primary-key tables or Paimon?
