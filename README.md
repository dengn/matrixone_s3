# MatrixOne on S3：本地查询、云端持久化、读者自动同步

**本地 Python 连接现有 MOCloud 的数据科学用例：** 见[五个 DuckDB / MatrixOne 对照场景](examples/data_science/README.md)。这些用例使用现有 Python SDK，不需要本地安装 MatrixOne，也不需要运行下面的 S3 原型。

这个实验让应用在本机运行 MatrixOne，把数据库的权威状态保存在 S3 兼容对象存储中。一个写者负责提交，多个独立读者从同一 bucket/prefix 恢复数据库，并通过原生事务日志增量同步新提交。

**热态交互已经接近“本地打开数据库、直接查询云端数据”的体验**：读者保持同一个进程和连接，更新时不重启；会话变量和预处理语句保留。严格最新模式每次查询检查云端，交互模式可显式允许 250 毫秒的新鲜度窗口，把大部分网络等待移到后台。

它仍是实验版本：每个 attachment 启动一个包含 CN、TN、LogService、HAKeeper 的本地 `mo-service` 进程，冷启动约 3 秒，资源占用比嵌入式 DuckDB 高。真实 AWS S3、大数据集和长期运行尚未验证。本仓库包含完整 MatrixOne 源码及实验改动，可以直接编译。

## 1. 准备环境与编译

已验证的环境是 Linux x86_64，使用 **Go 1.27.1** 和 Python 3.12。源码的 `go.mod` 要求 Go 1.27.1；请使用匹配或更新的工具链。准备 C/C++ 编译器、GNU Make、CMake、Git、curl、unzip，以及 Python 3.10+。

Ubuntu/Debian 的系统依赖：

```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake git curl unzip python3 python3-venv
```

安装 Go 后，确认版本并 clone：

```bash
go version  # 应为 go1.27.1 或更新版本

git clone https://github.com/dengn/matrixone_s3.git
cd matrixone_s3

# 首次编译会下载并编译原生依赖，需要联网和一定时间。
make build NATIVE_BUILD_JOBS=4
./mo-service --help
```

`mo-service` 会动态加载同目录 `lib/` 下的原生库。运行时请保留 `mo-service`、`lib/`、`dict/` 的相对位置；不要只复制可执行文件。原生依赖及下载校验逻辑沿用上游 Makefile。

安装 Python 依赖：

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r cmd/mo-bucket/requirements.txt
```

## 2. 先用本地 S3 模拟器跑通

以下步骤不需要真实 AWS 账号。Moto 是内存中的 S3 API 模拟器，只适合实验；停止它会丢失其对象。真实对象存储用于保留数据库。

**终端 A：启动模拟器**，在仓库根目录运行：

```bash
source .venv/bin/activate
python -m pip install -r cmd/mo-bucket/requirements-dev.txt
python -m moto.server -H 127.0.0.1 -p 9000
```

**终端 B：创建 bucket 与配置**，也在仓库根目录运行：

```bash
source .venv/bin/activate

# 仅用于本地模拟器的公开测试凭据。
export MO_BUCKET_ACCESS_KEY=experiment
export MO_BUCKET_SECRET_KEY=experiment

python - <<'PY'
import boto3
s3 = boto3.client('s3', endpoint_url='http://127.0.0.1:9000',
                  region_name='us-east-1',
                  aws_access_key_id='experiment', aws_secret_access_key='experiment')
s3.create_bucket(Bucket='mo-full-experiment')
print('bucket ready')
PY

cat > bucket.local.json <<'JSON'
{
  "bucket": "mo-full-experiment",
  "prefix": "databases/example",
  "endpoint": "http://127.0.0.1:9000",
  "region": "us-east-1",
  "live_updates": true,
  "lease_seconds": 60
}
JSON
```

同一个 bucket 可以放多个数据库，使用不同 `prefix` 隔离。`bucket + prefix` 标识一个数据库，不是表名。

## 3. 一条命令体验写入和自动同步

```bash
python cmd/mo-bucket/demo.py \
  --config bucket.local.json \
  --binary ./mo-service \
  --freshness-ms 250 \
  --output work/demo-config.json
```

Demo 创建一个带随机 UUID 的新子 prefix，不改动已有数据库，并演示：

1. 写者创建 `demo.orders`，读者恢复后读到两行，金额合计 `31.00`。
2. 写者事务更新并插入数据，读者自动追到三行、合计 `45.00`。
3. 读者进程、连接、会话变量及预处理语句保持。
4. 读者打开事务后，写者再提交更新，读者事务内继续读到 `45.00`。
5. 读者结束事务并调用 `sync()`，读到 `46.00`。

最后生成 `work/demo-config.json`，保存此次数据库的 bucket/prefix，**不保存凭据**。Demo 退出后数据库仍在对象存储中；可以用这份配置重新连接。

## 4. Python 应用接入

在仓库根目录设置模块路径：

```bash
export PYTHONPATH="$PWD/cmd/mo-bucket${PYTHONPATH:+:$PYTHONPATH}"
```

下面是完整的基础链路。请选择一个尚未使用的 prefix，重复运行建库语句会报已存在：

```python
from runtime import BucketDatabase

config = {
    "bucket": "mo-full-experiment",
    "prefix": "databases/my-app",
    "endpoint": "http://127.0.0.1:9000",  # AWS S3 可省略
    "region": "us-east-1",
}
binary = "./mo-service"

with BucketDatabase(config, binary, mode="writer") as writer:
    writer.execute("create database demo")
    writer.execute("use demo")
    writer.execute("create table orders(id bigint primary key, amount decimal(12,2))")
    writer.execute("insert into orders values(1,10.50),(2,20.50)")

    # 独立的本地进程，从 bucket 恢复，不连接写者的查询服务。
    with BucketDatabase(config, binary, database="demo", freshness_ms=250) as reader:
        print(reader.execute("select sum(amount) from orders")["rows"])
        writer.execute("insert into orders values(3,9.00)")

        # 交互模式可能在声明的窗口内读到旧版本。
        # 关键读取前显式同步，强制检查并追上云端提交。
        reader.sync()
        print(reader.execute("select sum(amount) from orders")["rows"])
```

`execute(sql, params=None)` 返回 `columns`、`rows`、`affected_rows` 和 `snapshot`。读者还返回 `freshness`：新鲜度模式、声明窗口、查询进入时的版本检查年龄，以及是否固定在事务/历史快照中。Decimal 结果转换为字符串，例如 `"40.00"`。SQL 方言是 MatrixOne 的。

### 选择新鲜度模式

| 配置 | 行为 | 适用场景 |
| --- | --- | --- |
| `freshness_ms=0`，默认 | 每个新查询/事务检查云端最新提交，再执行 SQL | 需要立即读取已提交数据 |
| `freshness_ms=250` | 复用不超过 250 毫秒的有效云端检查结果；后台预取增量 | 交互探索、连续查询 |
| `snapshot="<UUID>"` | 固定一个历史版本，不自动追新 | 重现历史查询 |

窗口从云端 HEAD 请求开始计时，慢下载不能延长新鲜度。超出窗口会刷新；刷新失败则报错，不会静默把过期数据作为新数据返回。显式事务会固定版本，优先于窗口设置；`sync()` 不允许推进一个正在打开的事务。

后台默认活跃时每 50 毫秒轮询；空闲 2 秒后降为每秒一次，新查询会唤醒它。可以设置 `poll_interval_ms`、`idle_poll_interval_ms`，或者 `background_sync=False` 关闭后台预取。`reader.status()` 可查看后台错误及请求计数，不发起远端请求。

### 事务与历史快照

```python
with BucketDatabase(config, binary, database="demo") as reader:
    reader.execute("begin")
    first = reader.execute("select sum(amount) from orders")
    # 此时即使另一个写者提交更新，事务内仍读旧版本。
    reader.execute("select sum(amount) from orders")
    reader.execute("commit")
    latest = reader.execute("select sum(amount) from orders")
    snapshot_id = latest["snapshot"]

with BucketDatabase(config, binary, database="demo", snapshot=snapshot_id) as historical:
    print(historical.execute("select sum(amount) from orders")["rows"])
```

同一个 prefix 只允许一个活跃写者，可以有多个独立读者。一个 `BucketDatabase` 实例串行使用一条连接；并行读者请创建多个实例。请使用 `with` 或 `close()` 释放进程、连接与后台线程。

## 5. 两个终端观察实时变化

先跑完第 3 节 Demo。两个终端都激活 `.venv`，设置本地模拟器凭据和第 4 节的 `PYTHONPATH`。

**读者终端**：保持同一个读者，不断查询：

```bash
python - <<'PY'
import json, time
from runtime import BucketDatabase
config = json.load(open('work/demo-config.json'))
with BucketDatabase(config, './mo-service', database='demo', freshness_ms=250) as db:
    while True:
        result = db.execute('select count(*),sum(amount) from orders')
        print(result['rows'], result['snapshot'], flush=True)
        time.sleep(.3)
PY
```

**写者终端**：从同一个 bucket 恢复写者，提交一次更新并退出：

```bash
python cmd/mo-bucket/runtime.py \
  --config work/demo-config.json --binary ./mo-service \
  --mode writer --database demo \
  --sql "update orders set amount=amount+1 where id=1"
```

读者随后看到金额变化，连接不重建。也可以使用 CLI 做一次严格最新读取：

```bash
python cmd/mo-bucket/runtime.py \
  --config work/demo-config.json --binary ./mo-service \
  --mode reader --database demo --freshness-ms 0 \
  --sql "select * from orders order by id"
```

自动同步和查询进入时的版本保证由 Python driver 提供。直接绕过 driver 连接子进程的 MySQL 端口，不具备这套自动同步保证。

## 6. 换成 AWS S3 或持久化的 S3 兼容存储

准备一个已有 bucket 和独立 prefix，使用 SDK 默认凭据链，例如 AWS profile，或环境变量：

```bash
export AWS_PROFILE=my-profile
```

也可使用 `AWS_ACCESS_KEY_ID`、`AWS_SECRET_ACCESS_KEY`、临时凭据对应的 `AWS_SESSION_TOKEN`。不要把真实密钥提交到配置文件。写者需要此 prefix 的对象读取/写入权限；读者只读取远端对象。写者协议依赖对象存储支持条件 PUT（`If-Match` / `If-None-Match`），并正确支持 HEAD 对象的一致性语义。

AWS S3 配置不需要 `endpoint`：

```json
{
  "bucket": "your-existing-bucket",
  "prefix": "databases/matrixone-experiment",
  "region": "ap-southeast-1",
  "live_updates": true,
  "lease_seconds": 60
}
```

S3 兼容存储可补充 `endpoint`。真实 S3 尚未完成端到端验证，不能把模拟器结果直接当作云端 SLA。示例本地 MO 账户为 `root/111`，启动器面向本机隔离实验，不提供账号配置或生产部署方案。

## 7. 崩溃恢复与数据格式

正常 `close()` 会释放写者租约。写者异常退出后，新的写者需要等待原租约到期，然后用同一个 bucket/prefix 重新 `attach()`。它从 S3 的原生 WAL、检查点及对象恢复，可以继续写入；不需要复制旧机器的本地目录。测试使用 6 秒短租约，通常配置示例保留 60 秒。

对象存储保存的格式：

```text
<prefix>/
  head.json                  # 写者租约、持久版本和查询可见版本
  manifests/<uuid>.json      # 原生路径、事务日志序号、分块引用
  chunks/<sha256>            # 256 KiB 不可变内容块
```

内容是 **MatrixOne 原生对象、检查点和 WAL**，不是 Parquet，也不是 DuckDB `.duckdb` 文件。提交返回成功前，原生 WAL 与增量事务命令已持久化。读者下载并校验新增命令，在查询边界应用它们，同时更新本地查询缓存；读者的内部维护写入保留在自己的内存 overlay 中，不向 S3 写入。

目前不删除历史 manifest/chunk，没有生产级回收与保留策略。不要手工删除某些 chunk 来节省空间，否则可能破坏恢复或历史快照。

## 8. 跑验证与性能实验

以下测试创建随机子 prefix。`integration.py` 和 `interactive.py` 包含故意杀掉写者的恢复测试；不要把测试当作生产运维脚本。

```bash
mkdir -p work/results

python cmd/mo-bucket/integration.py \
  --config bucket.local.json --binary ./mo-service \
  --evidence work/results/integration.json

python cmd/mo-bucket/interactive.py \
  --config bucket.local.json --binary ./mo-service \
  --repeats 100 --output work/results/interactive.json

python cmd/mo-bucket/network_interactive.py \
  --config bucket.local.json --binary ./mo-service \
  --delay-ms 30 --output work/results/network.json

MO_BUCKET_TEST_ENDPOINT=http://127.0.0.1:9000 \
  go test -race ./pkg/objectstoragefs -count=1
```

发布源码还完成了 `make build-with-prebuilt-native` 构建检查：Go 部分从本仓库重新编译，原生库复用已校验的依赖；一键 Demo 及退出后重新读取链路均实际执行。三个连续完整 Demo 复测通过。

最后一个 Go 测试使用本地 `mo-full-experiment` bucket 与公开测试凭据。网络实验只接受没有路径前缀的 HTTP S3 endpoint；它给每个读者 S3 GET 增加延迟并注入阻塞/503，不是针对生产 AWS 的负载测试。

已运行的验证：14 项原生 SQL、约束、隔离和恢复检查；100 次连续更新的 8 项会话/DDL/检查点/恢复检查；10 项网络与故障检查；文件系统 race 检测及相关原生重放回归。

| 实验场景 | 中位数 | 说明 |
| --- | --- | --- |
| 严格最新普通查询 | 36.5 ms | 每个读者 S3 GET 额外延迟 30 ms |
| 250 ms 窗口下的普通热态查询 | 0.44 ms | 60 次查询，没有前台 HEAD 请求；P95 1.22 ms |
| 更新预取完成后的查询 | 2.2 ms | 10 次更新；不包含预取时间，不代表提交到可见的总延迟 |
| 读者冷启动 | 约 3 s | 本地无额外网络延迟、小数据 |

这些是本地模拟器及受控网络注入结果。加载大数据、查询复杂度、CPU 竞争、真实云网络都会改变延迟。原始通过结果放在 [docs/experiments](docs/experiments/)，详细实现说明在 [cmd/mo-bucket/README.md](cmd/mo-bucket/README.md)。

## 9. 当前边界与常见问题

- **不能启动第二个写者**：同一个 prefix 已有活跃写者，或崩溃后的租约尚未到期。正常关闭原写者，或等待租约到期。
- **`freshness window expired`**：网络或增量同步超过了声明窗口。可以重试、增大窗口，或对关键读取调用 `sync()`；不要依赖错误后静默降级读取旧数据。
- **找不到原生库**：请在仓库根目录完成 `make build`，保留 `lib/` 的运行时位置。
- **尚未定位的间歇性异常**：发布前的一次 Demo 在事务结束后等待新值超时，随后多次同路径复测正常，根因尚未定位。Demo 失败时会保存 `*.reader.log`，并报告预期/实际值及两侧明细；不要把该实验当作已验证的生产强一致方案。
- **首条查询较慢**：仍需启动完整本地原生角色并恢复 WAL；该启动器不是嵌入式库。
- **读者写入被拒绝**：读者模式只读，临时表创建及 DML 同样拒绝；请通过唯一写者提交。
- **积压太多增量**：每批上限 4096 条或 64 MiB WAL payload；超过时需重新 attach 当前快照。缓存保留两个预取版本，新增数据块预热预算 8 MiB。
- **长期内存/对象存储增长**：恢复 WAL 在内存中，manifest 下载仍与历史大小有关，历史对象保留且读者后台维护被禁用。大规模与长期运行仍需进一步工程验证。

本仓库保留上游 Apache 2.0 许可证，源码基于 MatrixOne 提交 `175a645a85994f2c95992d3aa8d6dacb6d9e167f`。原上游 README 保存在 [README_UPSTREAM.md](README_UPSTREAM.md)，基准与改动来源见 [EXPERIMENT_PROVENANCE.json](EXPERIMENT_PROVENANCE.json)。`cmd/mo-bucket-query` 是早期只读检查点实验，使用另一种发布协议；本文教程统一使用 `cmd/mo-bucket` 的完整原生读写实现。

上游 CI 工作流保存在 [docs/upstream-workflows](docs/upstream-workflows/)，本独立实验仓库没有启用上游发布和定时任务；验证入口为第 8 节的手动实验。
