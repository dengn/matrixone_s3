> 历史实验入口：这里使用 `snapshot.json` 检查点发布协议，不能直接读取新版 `head.json` 数据库。完整的读写和增量同步教程请见仓库根目录 README，入口为 `cmd/mo-bucket`。

# MO bucket snapshot query experiment

This command opens a published native MatrixOne checkpoint from S3-compatible
storage and runs a restricted read-only SELECT in a new process. It starts no
CN/TN/LogService/HAKeeper service and creates no transaction or logtail client.

It reuses the native checkpoint catalog/lifecycle/tombstone readers, the MO SQL
parser and optimizer, and production projection/filter/group/order/limit
operators. A small local pipeline compiler connects them. No DuckDB, CSV import,
Parquet conversion or replacement database engine is involved. Some reused
catalog-listing internals format catalog rows through an in-memory CSV helper;
the user-table scan passes typed native batches directly to MO operators.

## Build

Build MO's native dependencies first, using the repository's normal `make build`
workflow. On Linux, with the repository's Go toolchain selected:

```sh
repo=$(pwd)
CGO_CFLAGS="-I$repo/cgo -I$repo/thirdparties/install/include" \
  go build -ldflags="-extldflags '-L$repo/cgo -lmo -L$repo/thirdparties/install/lib -Wl,-rpath,$repo/lib -fopenmp'" \
  -o work/mo-bucket-query ./cmd/mo-bucket-query
```

## Query a published snapshot

Only immutable, complete prefixes are supported. Stop/quiesce the source writer,
flush the user table and catalog, force a checkpoint, copy the native shared
fileservice objects with their exact paths/bytes into a fresh bucket prefix,
then write `snapshot.json` last. The experiment publisher and fixtures are in
`/workspace/work/mo-bucket/`; they use synthetic data and a local Moto S3 server.

The publication marker has this shape; inventory entries are key/bytes/sha256:

```json
{
  "format": "matrixone-native-checkpoint-experiment-v1",
  "count": 145,
  "objects": ["inventory entries omitted here"]
}
```

The reader checks the marker's format and inventory count, then opens the
native checkpoint. It does not verify every inventory checksum. A missing
referenced object fails the query. Prefix immutability is a deployment rule;
this prototype implements neither an IAM policy nor object-version pinning.

```sh
export MO_BUCKET_S3='bucket=mo-bucket-experiment,endpoint=http://127.0.0.1:9000,region=us-east-1,key-prefix=snapshots/v1,key-id=experiment,key-secret=experiment'
./work/mo-bucket-query --backend MINIO --database bucket_demo \
  --sql 'select region, count(*) as n, sum(amount) as total from orders group by region order by region'
```

The credentials above are dummy credentials for the local emulator. Use a
read-only storage identity/default credential chain for an actual deployment.

`--snapshot physical:logical` pins an explicit timestamp; by default the latest
available checkpoint end is used. Future timestamps are rejected. `--inspect`
shows the catalog and timestamp, and `--plan` writes the native MO plan to stderr.
An offline directory can be read with `--directory PATH --kind local2` instead.
Some MO initialization messages precede the final JSON line on stdout.

## Evidence from the executed experiment

The source MO service was stopped. Its local data paths were renamed away.
Each query ran in a new process in an empty directory, with only S3 settings and
SQL supplied. Both prefixes were also queried by simultaneous independent
readers. The backend was Moto's local S3-compatible HTTP API, including ranged
GETs, not an actual cloud bucket.

| Snapshot | Rows | SUM(id) | SUM(amount) |
|---|---:|---:|---:|
| v1 | 12000 | 71994000 | 59940.00 |
| v2 | 10825 | 65100300 | 175062.50 |

v2 deletes 1200 rows, updates the amounts of 1200 other rows, and inserts 25 rows.
Eight SQL statements per snapshot matched results collected from the normal MO
service: projection, filters, decimal arithmetic, NULL, COUNT DISTINCT,
SUM/MIN/MAX/AVG, GROUP BY/HAVING, ORDER BY and LIMIT. Four simultaneous readers
and explicit timestamp selection passed. Eight negative cases passed: writes,
multiple statements, joins, future timestamps, outfile, missing table, missing
publication marker, and missing referenced object.

The test run also covers owned native batch lifetime, column-order binding,
failure cleanup, catalog decimal precision/scale, aggregate output formatting,
and existing object-lifecycle/logical-view regressions.

Reader base: `175a645a85` (full hash recorded by verify.py). Fixture writer:
`23d8f61252f3017dff218b530817a240206fd762`; native compatibility was exercised
across these two revisions.

## Scope

This is a read-only feasibility prototype, not a new complete MO deployment.
One ordinary table per query is supported. Unknown plan nodes, joins, views,
index scans and writes are rejected. Schema-evolution defaults and complete
collation fidelity are not implemented or validated. The reader binds account 0
and provides no SQL authorization layer. Other accounts are not a supported
interface. Supported JSON value types are bool, integers, floats, decimal and
char/varchar/text; other non-null result types fail explicitly.

Native blocks are read incrementally. JSON results are buffered and capped at
100000 rows and 64 MiB. Snapshot publication currently copies a stopped writer's
shared objects to an independent immutable prefix. Live publication, automatic
following of new snapshots, GC integration, write fencing and bucket-only WAL
durability are outside this experiment. No cloud-latency, IAM or crash-recovery
benchmark was performed.
