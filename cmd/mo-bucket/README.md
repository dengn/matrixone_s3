# Experimental local MatrixOne on an object-storage bucket

This is the writable, full native SQL implementation. `cmd/mo-bucket-query`
contains the earlier read-only, single-table snapshot experiment.

The application starts one local `mo-service` process through `runtime.py`.
Its CN, TN, LogService and HAKeeper roles run inside that process. There is no
separately deployed MO cluster. The authoritative database lives in a bucket;
local directories contain disposable launch files, ETL/scratch files and logs.
This is substantially heavier than an embedded DuckDB library.

## Application interface

Build the modified `cmd/mo-service` using the repository's normal native build
prerequisites. Install Python dependencies `pymysql` and `boto3`. Add this directory
to your Python module path and use:

```python
from runtime import BucketDatabase

config = {
    "bucket": "my-bucket",
    "prefix": "databases/example",
    "region": "us-east-1",
    # "endpoint": "http://localhost:9000",  # optional S3-compatible endpoint
}
binary = "/path/to/patched/mo-service"

with BucketDatabase(config, binary, mode="writer") as db:
    db.execute("create database demo")
    db.execute("use demo")
    db.execute("create table orders(id bigint primary key, amount decimal(12,2))")
    db.execute("insert into orders values(1,10.50)")

with BucketDatabase(config, binary, mode="reader", database="demo") as db:
    print(db.execute("select sum(amount) from orders"))
```

Credentials use the AWS SDK chain, or `MO_BUCKET_ACCESS_KEY` and
`MO_BUCKET_SECRET_KEY`. Static `access_key`/`secret_key` fields are accepted for
local tests but are not written to generated child configuration files. Native
MO authentication defaults to root/111 in the experimental local launcher;
change the normal native authentication settings before exposing any listener.
The launcher currently assumes loopback use and does not provision accounts.

Only one writer can attach to a prefix. Independent readers can attach while it
runs. Native SQL executes unchanged through the MO frontend, optimizer and TAE
engine: DDL/DML, joins, subqueries, windows, native constraints and transactions.
Successful procedure calls also publish a committed reader generation.
The SQL dialect is MatrixOne's, not DuckDB's.

A reader checks the committed generation before each new transaction. The
launcher enables `live_updates` by default: it prefetches a contiguous batch of
native transaction WAL commands, applies them to the existing TAE instance, and
updates the existing CN's subscribed table partitions and catalog before admitting
the SQL query. The native process and MySQL connection remain alive; session
variables and prepared statements survive. This is native transaction replay,
not replaying SQL text. `BEGIN` pins the current generation until `COMMIT` or
`ROLLBACK`; the following query applies missed source commits. Supply
`snapshot=<generation>` to retain a historical version.

A raw MySQL connection to the child reader remains pinned; automatic refresh and
its admission barrier are features of this driver. One driver instance serializes
its connection. Use several instances for independent concurrent readers. Set
`live_updates: false` in the config to use the older runtime-replacement path;
this costs seconds and restores only selected database and recorded SET commands.
An existing legacy prefix/snapshot needs a fresh attachment to an enabled writer's
journal frontier; readers cannot infer missing historical transaction commands.

## Durability and on-bucket format

The bucket prefix holds `head.json`, `manifests/<uuid>.json` and
`chunks/<sha256>`. Chunk size is 256 KiB. A manifest maps native shared objects,
persistent LOCAL control metadata and WAL filesystem paths into immutable chunks.
These are MatrixOne-native object/checkpoint/log bytes, not Parquet or a DuckDB
`.duckdb` file. WAL hard links retain their identities across recovery. Paths are
independent of the original machine's working directory.

The writer owns a conditional-PUT lease on `head.json`. Manifest publication uses
`If-Match` on its current ETag; acquisition uses `If-None-Match` for a new prefix.
A takeover changes ownership and fences the previous writer. An uncertain HEAD
write fences conservatively rather than risking further acknowledgement.

Native WAL `Sync` uploads that file's synchronized bytes and publishes the durable
manifest before returning. Unrelated publication cannot capture unsynchronized
WAL bytes. Successful user mutations and explicit commits advance
`visible_version` before the frontend returns success. The recovery head can move
for durable internal maintenance without forcing readers to refresh. Readers
restore a committed manifest and replay the WAL locally. Native initialization
and maintenance writes stay in a reader-only memory overlay: the reader never
publishes S3 writes. Restored service discovery is rebound exclusively to the
current process, so readers cannot subscribe to the original writer's logtail.
Async SQL task execution is disabled for readers. Live readers also disable
private flushing, merging and maintenance cron jobs, so local maintenance cannot
fork the source object lifecycle or collect MVCC versions required by delayed
source commits. Imported appendable objects are frozen for local appenders while
native source replay can continue appending. Reader-local control transactions
allocate separate objects, preserving the source WAL row-offset namespace. Native
query service and heartbeats remain active.

With `live_updates`, manifests also contain a `live_lsn` and native transaction
payload objects under `data:__live__/wal/<lsn>` in the same chunk store. The writer
exports each payload after durable WAL sync and before native WAL completion, so
acknowledged commits include their incremental commands. Readers check contiguous
LSNs and content hashes and decode the whole batch before native mutation. Native
apply failures poison further synchronization rather than serving a partially
applied version as fresh. Incremental import retains old immutable shared objects
and preserves the reader's private control-plane WAL and LOCAL metadata.

## Reproduce the integration experiment

Create an empty S3 bucket, provide its configuration in JSON, then run:

```console
python integration.py --config bucket.json --binary /path/to/patched/mo-service \
    --evidence integration-result.json
```

The test creates a unique child prefix and preserves existing data. It exercises
native checkpointing, joins/windows/subqueries, constraints, rollback,
independent readers, automatic refresh, transaction pinning, historical snapshots,
competing writer rejection, SIGKILL without checkpoint, deletion of all local
runtime files, and a new writer that recovers and continues writing from S3.

Filesystem integration tests use `MO_BUCKET_TEST_ENDPOINT` and the bucket
`mo-full-experiment`: `go test ./pkg/objectstoragefs -count=1`.

## Measured optimization and benchmark

Recovery now retires the restored CN ingress with the existing committed
heartbeat protocol before allocating the new local generation. This is enabled
only in the isolated bucket runtime; ordinary distributed cluster restarts retain
their liveness and generation fences. The new CN still performs native catalog,
protocol and epoch admission. The local launcher uses a 50 ms loopback Raft RTT.

Unchanged synchronized filesystem state reuses its immutable manifest. Every
publication still performs the conditional HEAD write to validate writer
ownership; a paused old writer cannot succeed through a no-change shortcut.

In the local Moto S3 experiment, the previous full-run reader refresh was
33.928 seconds. The optimized full run took 3.046 seconds and passed all 14
functional checks, including pinned transactions, historical snapshots, writer
exclusion and bucket-only SIGKILL recovery. A separate three-refresh benchmark
measured a 3.162 second median; steady native SQL plus the S3 head check had a
3.372 ms median, and acknowledged writes had a 19.700 ms median. These numbers
include full runtime creation and retirement. They describe this small test on
an emulator, not real cloud latency or large-database performance.

Reproduce the independent benchmark with:

```console
python benchmark.py --config bucket.json --binary /path/to/patched/mo-service \
    --repeats 3 --output benchmark-result.json
```

## Incremental interactive experiment

`interactive.py` verifies repeated acknowledged updates through a prepared query while
asserting stable process PID, connection ID and a session variable. It also checks
transaction pinning, catching up 15 commits, CREATE/ALTER/DROP/recreate and native
checkpoint transitions without reconnecting. It keeps the same reader alive across
writer SIGKILL, lease takeover, recovery and another source commit.

```console
python interactive.py --config bucket.json --binary /path/to/patched/mo-service \
    --repeats 100 --output interactive-result.json
```

The final incremental run on local Moto passed all eight interaction checks with
100 updates: refresh median 9.604 ms, P95 15.666 ms; steady query median 3.817 ms,
P95 8.229 ms; 15 missed commits applied in 35.996 ms. The native reader PID and
MySQL connection ID remained identical even across source writer recovery. An
earlier independent 25-update benchmark run concurrently with the integration
test gave refresh median 17.179 ms and steady query median 6.107 ms. Cold reader
attach remains about 3 seconds. Timings include the S3 HEAD check and native SQL
execution; they are small-data emulator results, not real-cloud or large-data
claims.

The extra durability publication increased acknowledged writer-update median
from the earlier 19.700 ms baseline to 44.687 ms in the concurrent benchmark.
Batching journal publication is a future optimization; reader smoothness does not
make this cost disappear.

## Background prefetch and bounded freshness

Strict latest visibility remains the default (`freshness_ms=0`). Every new
transaction checks remote HEAD; background prefetch can remove manifest/WAL
fetches from its critical path but cannot eliminate that strict HEAD round trip.
For interactive exploration, explicitly allow a short observation window:

```python
with BucketDatabase(config, binary, database="demo", freshness_ms=250) as db:
    result = db.execute("select sum(amount) from orders")
    print(result["rows"], result["freshness"])
    db.sync()  # Force remote HEAD + committed catch-up before a critical read.
```

`freshness_ms=250` permits a source observation at most 250 ms old **at SQL
admission**. Its age starts when the remote HEAD request starts, so slow downloads
cannot renew freshness by finishing late. The driver only uses observations whose
native WAL is prepared or whose version is already applied. An expired observation
triggers a foreground refresh; a failed refresh returns an error instead of
silently serving an old version. A slow successful sync retries its observation
once before rejecting an expired freshness window. `sync()` checks latest HEAD
regardless of the configured window and rejects an open transaction. Explicit
transactions and fixed snapshots deliberately retain their pinned version.

The background worker polls every 50 ms during query activity, backs off to
1 second after 2 seconds of inactivity, and wakes when queries resume. Tune
`poll_interval_ms` and `idle_poll_interval_ms`, or set `background_sync=False`.
`status()` reports local policy, background errors and request counters without
remote I/O. `close()` signals and joins its worker before retiring the native
runtime. Results include observation age and whether the transaction is pinned.

The native `/_bucket/prefetch` endpoint stages an immutable batch without changing
native SQL visibility or importing file references. Remote fetches release the
filesystem cache lock; concurrent requests share in-flight work. Two prepared
versions are retained, with at most 64 MiB of payloads/4096 records each. Admission
retains its own batch reference even if the cache evicts that version. Newly
referenced data chunks are warmed within an 8 MiB budget; normal native range reads
handle colder data. S3 hash/size checks happen before admission. A reader behind
these batch limits must reattach to a current snapshot.

Reproduce the network experiment with a local HTTP S3 endpoint:

```console
python network_interactive.py --config bucket.json --binary /path/to/mo-service \
    --delay-ms 30 --output network-result.json
```

It adds 30 ms to **every reader S3 GET**, leaving writer traffic unaffected. Checks
include 60 cached queries without foreground HEAD, 10 prefetched updates through a
prepared statement, stable connection/session, strict latest visibility, a stalled
WAL download, pinned transactions, expiry during HTTP 503, recovery on the same
connection, idle polling backoff and worker shutdown. This is controlled latency
and fault injection around a local emulator, not a real cloud benchmark. The update
query timing starts **after prefetch finishes** and does not measure commit-to-
visibility delay or promise the same latency for a query racing a new commit.

The controlled 30 ms injection run passed all 10 network/interaction checks:
strict query median 36.471 ms (15 samples); bounded warm query median 0.442 ms,
P95 1.221 ms (60 samples, zero foreground HEAD requests); prepared query after
completed update prefetch median 2.209 ms, P95 15.741 ms (10 samples). A query
while a WAL download was deliberately stalled took 1.427 ms and retained its
permitted recent version. The idle worker made one HEAD request in 1.2 seconds.

The final source also passed the original 14 integration checks, all eight
100-update interaction checks (including writer recovery on the same reader),
filesystem tests under the Go race detector, and native bucket/replay regressions.
The strict 100-update run shared CPU/S3 with integration and Go compilation; its
51.853 ms refresh and 18.553 ms steady medians describe that loaded run, not the
isolated network experiment above or a cloud service latency target.

## Current engineering limits

This is an end-to-end functional experiment, not a production release. It restores
all recovery WAL into memory. Ordinary bucket filesystem operations are serialized
under one mutex; background remote prefetch releases that lock during network I/O.
Incremental sync currently downloads a full manifest and retains all journal
entries; metadata and catch-up cost grow with database history. Live reader
background maintenance is disabled, and sustained memory usage needs validation.
Readers are still read-only: temporary-table creation and DML are rejected. The
driver does not replace the complete DB-API interface or support transparent
connection recovery for writers. Raw SQL clients bypass the driver's freshness
barrier; exposing multiple SQL connections into one child is unsupported.

All immutable manifests and chunks are retained. Physical bucket garbage
collection, retention leases and storage compaction must be added before sustained
production use. Native TAE garbage collection is disabled by the launcher to keep
historical manifests available. Large databases, cloud WAN latency, real AWS IAM,
clock skew, prolonged partitions and all engine SQL features require further
validation. Each strict-freshness query checks remote HEAD, so real S3 adds a network
round trip even when no data changed. Bounded freshness trades immediate visibility
for a declared short observation window and still waits when data is not ready.
Background polling adds S3 GET traffic even without source updates. Native network roles retain
MO's normal resource footprint and the launcher is not an embedded library.
