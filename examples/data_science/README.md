# 用 Python 做数据科学：DuckDB 与现有 MatrixOne 的五个对照用例

结论：**数据已经在 MatrixOne 里时，Notebook → SQL → Pandas → 模型的链路已经能完成；本地 DataFrame、临时文件直接参与 SQL 时，DuckDB 更省步骤。** 主要差距是 Python 数据交互接口，而不是 S3 存储能力。

这里使用已有 MatrixOne Python SDK，连接一个普通 MO SQL 服务。用户本地只安装 Python 依赖，不需要安装、编译 MatrixOne，也不需要运行本仓库的 `mo-bucket` 原型。MOCloud 负责后台数据库与存储，客户端不需要持有数据库底层 S3 凭证。

## 五个场景

| 场景 | 用户想做什么 | DuckDB 的使用感受 | 现有 MO 的完成方式 | 体验判断 |
|---|---|---|---|---|
| 1. Notebook 探索 | 看各地区订单量和销售额，再画图 | SQL 查询后 `.df()` | 云端聚合，`execute()`，结果转换为 DataFrame | 数据已在 MO 时很顺，结果转换可以再简化 |
| 2. 本地实验名单关联业务数据 | 手上四个客户的分组名单，关联共享订单 | 注册 DataFrame 后直接 JOIN | 建一次性表、上传名单、JOIN、清理表 | 能做，MO 多了建表和传输步骤 |
| 3. 临时 CSV 探索 | 收到文件后过滤、汇总 | 直接对文件执行 SQL | 显式建表，把小 CSV 内容传给导入接口，再查询 | 能做，当前接口没有“文件变成可查表”的一步操作 |
| 4. 增量处理与共享 | ETL 更新数据，另一个 Notebook 查询 | 单进程事务已有；跨进程共享更新由 S3 插件提供 | SDK 事务提交，独立连接查询 | MO 原有数据库能力就能完成 |
| 5. 可复现训练 | 固定特征数据版本，在本地训练模型，之后复跑 | 本地固定副本或显式固定读取版本 | MO 数据库快照查询 → Pandas → scikit-learn | 已能完成；需要快照权限与匹配的后端版本 |

前三个场景用普通 DuckDB Python API 做结果对照。没有安装或测试原文的对象存储插件；第 4、5 个场景验证的是 MO 自身功能，不声称两种产品具有相同的一致性或版本协议。

### 1. 云端算完，回到熟悉的 Pandas

用户继续在本地 Notebook 工作，只把重的数据扫描与聚合交给 MO：

```python
result = client.execute("""
    SELECT region, SUM(amount) AS revenue
    FROM sales
    GROUP BY region
""")
df = pd.DataFrame(result.fetchall(), columns=result.columns)
df.plot.bar(x="region", y="revenue")
```

这里的 `sales` 是示意业务表。可执行脚本会生成自己的订单和客户表，并验证三组汇总结果。`pd.DataFrame(...)` 是调用方的转换，当前 SDK 没有内置的 DuckDB `.df()` 对应接口。

结果只有几十行时，本地无需下载整个业务数据集。每次查询仍有到云端的网络往返，不能据此保证比本地 DuckDB 更快。

### 2. 本地 DataFrame 和云端订单关联

Notebook 中已有 `cohort` DataFrame，标记哪些客户属于实验组。

DuckDB 可以直接注册 Python 对象：

```python
con.register("cohort", cohort)
df = con.sql("""
    SELECT c.experiment_group, SUM(o.amount) AS revenue
    FROM cohort c JOIN orders o USING (customer_id)
    GROUP BY c.experiment_group
""").df()
```

MO 的 SQL 在云端执行，因此云端必须先拿到这份名单。脚本使用现有接口：

```python
client.execute("CREATE TABLE scratch_cohort (customer_id BIGINT, experiment_group VARCHAR(20))")
client.batch_insert("scratch_cohort", cohort.to_dict("records"))
# SQL JOIN；完成后删除 scratch_cohort。
```

脚本实际使用随机表名，并在 `finally` 中清理。这里显式指定类型，只演示小而简单的 DataFrame，没有实现通用类型推断或复杂类型转换。

如果 MO 聚合出的客户级数据已经足够小，也可以下载聚合结果后在本地 `cohort.merge(...)`，省掉上传名单。这是现有 Pandas 工作流就能做的选择；不要为了 JOIN 把巨大的订单明细全部拉回本地。

### 3. 本地文件：方法名相似，语义不同

DuckDB 的 `read_csv` 可以把文件作为查询数据源。MO SDK 的 `load_data.read_csv(..., table=...)` 是**把内容导入已有表**，不是返回 DataFrame，也不会自动推断并创建目标表。

在核对的 SDK 中，普通文件路径会生成 `LOAD DATA INFILE`，由服务器读取路径。`client.load_data.read_csv('/my-laptop/orders.csv', table=...)` 并不自动把电脑里的文件传给远端 MO。

脚本显式读取小 CSV 并发送内容：

```python
payload = pd.read_csv("orders.csv").to_csv(index=False, header=False)
client.load_data.read_csv(payload, table="scratch_orders", inline=True)
```

这条路径已验证简单数字 CSV。它会在本地解析、序列化、上传，并消耗客户端内存，不适合作为大文件方案，也没有验证任意嵌套类型或所有 CSV 转义情况。

大文件可以使用已配置的外部 stage 与导入接口；本地文件传输可考察 `from_local_file()`，但它需要数据库和客户端驱动支持 `LOCAL INFILE`。这些路径没有纳入本次实测。S3 上的 Parquet 探索还可考察 MO 外部表，不能把“支持导入 Parquet”等同于“无需建表就直接查任意路径”。

### 4. ETL 更新后，另一个 Notebook 接着查

```python
with client.session() as session:
    session.execute("UPDATE orders SET amount = amount + 5 WHERE order_id = 11")
    session.batch_insert("orders", [{"order_id": 999, "customer_id": 1, "amount": 7.0}])

# reader 是另一条 SDK 连接；这是新的查询事务。
result = reader.execute("SELECT SUM(amount) FROM orders")
```

脚本验证提交后的总额从 `1542` 变成 `1554`，还验证异常退出事务会回滚。Notebook 已显示的结果不会自己变化，需要重新执行查询；长事务的可见性仍由数据库事务规则决定。

这条链路直接使用 MO 数据库服务，不需要重写 S3 同步协议。

### 5. 固定训练数据，再交给 scikit-learn

```python
client.snapshots.create("training_v1", level="database", database="analytics")
result = client.execute("""
    SELECT customer_id, amount
    FROM orders {snapshot = 'training_v1'}
""")
df = pd.DataFrame(result.fetchall(), columns=result.columns)
```

脚本把订单和客户表固定在同一数据库快照，提取 12 行客户级特征，训练一个 LogisticRegression；再修改订单，验证快照查询仍得到相同特征，最新查询已经变化。模型只用于验证接口衔接，不评估预测效果。

如果特征数据很大，现有 `client.execute()` 会先 `fetchall()`，再封装成 `ResultSet`；`fetchmany()` 只是切分已经加载的列表，**不能用它证明结果在网络上逐批传输或内存有界**。真实分批训练需要真正的流式查询接口或显式分批提取；多批次应固定同一数据版本。

## 运行全部用例

```bash
git clone https://github.com/dengn/matrixone_s3.git
cd matrixone_s3
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r examples/data_science/requirements.txt

export MO_HOST='你的云端SQL地址'
export MO_PORT='6001'  # 使用云服务提供的实际端口
export MO_USER='你的数据库登录名'
export MO_DATABASE='已有的实验数据库'
export MO_SSL_MODE='required'  # 根据云服务 TLS 配置设置
read -rsp 'MO password: ' MO_PASSWORD
export MO_PASSWORD

python examples/data_science/run_cases.py --output work/data-science-results.json
```

数据库账号需要能查询、创建/删除实验表，以及创建/删除数据库快照。建议使用已有的实验数据库。脚本只修改它自己生成的随机表，不创建数据库，完成或失败时清理这些表和快照；清理失败会明确报错。断电或强制杀进程仍可能留下随机前缀资源。

如果云端账号不能创建快照，第 5 个用例会报错，而不会降级成不固定版本的查询。TLS、账号登录名、网络可达性与后端版本要按实际云端配置提供。

## 实测范围

实测包括 SDK 最新源码与 PyPI 发布包 `matrixone-python-sdk==0.1.16`，使用 Python 3.12、普通原生 `mo-service`，通过 MySQL 协议连接。没有修改 SDK，也没有使用 `--bucket-config`。

- 样本：12 个客户、24 笔初始订单、4 行实验名单。
- 前三个场景与 DuckDB 结果一致。
- 后两个场景验证独立连接可见性、回滚、快照复现与本地模型训练。
- 实验资源已清理，验证用原生服务已正常退出。
- [发布包验证结果](validation.json)记录依赖版本与执行结果。
- 没有真实 MOCloud 地址与凭证，因此没有验证生产云端权限、网络延迟、底层 S3 访问或大规模性能。结果中的本机耗时不能用于判断“云端是否丝滑”。

核对的上游 SDK 源码提交：`944b721d1b3705cdb51d1e26f1a04611a08c8401`；验证服务器使用此前构建的普通 MO 二进制，构建元数据记录提交 `23d8f61252f3017dff218b530817a240206fd762`，`vcs.modified=false`，开发版返回 `999.0.0`，不是生产云端版本。源码与发布包的 `client.py`、`snapshot.py` 完全相同；`load_data.py` 的差异是 Parquet 字典编码说明，本次 CSV 路径两者均通过。

## 优先补什么体验

以下是建议接口，**不是现有 SDK API**：

1. `read_sql(..., as_dataframe=True)`：让 SQL 结果直接回到 Pandas，避免每次写转换代码。
2. `with client.dataframe("cohort", df): ...`：自动创建有作用域的上传表，完成后清理。云端仍需实际传输，不能把名字做成“零拷贝”。
3. `client.read_csv(local_path)` / `read_parquet(...)`：区分客户端文件、服务器路径、stage，封装上传与类型推断，让用户一次调用得到可查询对象。
4. `iter_dataframes(..., batch_size=...)`：实现真正的流式读取与版本固定，避免大结果先全部进内存。

对 MOCloud 已有用户，我会先补这几层 Python 工作流接口；底层 SQL、事务和快照已经可以承接这些场景。

参考：[SDK 源码](https://github.com/matrixorigin/matrixone/tree/944b721d1b3705cdb51d1e26f1a04611a08c8401/clients/python)、[云服务 SQL 执行](https://github.com/matrixorigin/mocloud-services/blob/main/src/local-service/pkg/handlers/sqlquery/query.go)、[DuckDB 对象存储原文](https://dentiny.github.io/posts/duckdb-on-object-storage/)。
