# MPE MongoDB 插件（第三方开发示例）

> **定位**：本插件是 MPE 插件的**第三方独立开发示例**——不依赖宿主仓库的任何代码，
> 只依赖公开的 `mpe-plugin-sdk`（Sidecar 进程 + JSON-RPC over stdio）。
> 宿主启动时扫描插件目录、`describe` 握手、注册节点、执行时经
> `execute` RPC 调用本插件进程。
>
> 本目录已被宿主仓库 `.gitignore` 忽略（`/plugins/`），它属于"插件作者"的
> 独立项目，不参与宿主编译，也不会被宿主 CI 构建。

---

## 0. 一分钟理解插件如何工作

```
宿主 (mpe / mpe-cli)                    插件进程 (本 crate)
   │  扫描 plugins/ 目录                       │
   │  ── describe ───────────────────────────► │  返回节点描述（类型、端口、config schema）
   │  ◄─────────── 节点清单 ──────────────────  │
   │  ── execute(config, execution_id) ──────► │  执行 Mongo 操作
   │  ◄─────────── 结果 / 变量更新 ───────────  │
   │  ── flowEnded(execution_id) ────────────► │  释放 per-execution 连接池
```

- **传输**：stdin/stdout，一行一个 JSON（JSON-RPC 2.0，LF 分隔）
- **驻留**：`capabilities.streaming: true` → 进程常驻，连接池跨执行复用
- **无共享内存**：插件是独立进程，宿主只能通过 JSON 传值

---

## 1. 项目结构

```
plugins/mongo/
├── Cargo.toml            # 独立 package，不依赖宿主 workspace
├── plugin.json           # 宿主扫描用清单（启动描述、驻留模式）
├── src/
│   ├── main.rs           # mpe_plugin_main! 入口
│   ├── lib.rs            # MongoPlugin：Plugin trait 实现 + 节点分发
│   ├── pool.rs           # per-execution 连接池（execution_id → Client）
│   └── nodes/
│       ├── mod.rs        # 各节点 execute 分派
│       ├── connect.rs    # mongo:connect
│       ├── find.rs       # mongo:find
│       ├── insert.rs     # mongo:insert
│       ├── update.rs     # mongo:update
│       ├── delete.rs     # mongo:delete
│       ├── aggregate.rs  # mongo:aggregate
│       └── close.rs      # mongo:close
└── tests/
    └── pool_test.rs      # 连接池 per-execution 隔离单测
```

---

## 2. Cargo.toml（独立构建）

```toml
[package]
name = "mpe-plugin-mongo"
version = "0.1.0"
edition = "2021"

[dependencies]
# 第三方开发：只用公开 SDK，绝不 import 宿主类型（flow-engine-core 等）
mpe-plugin-sdk = { path = "../../src-tauri/crates/mpe-plugin-sdk" }
# 官方 Mongo 驱动（异步，tokio 运行时）
mongodb = { version = "3", features = ["tokio-runtime"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tokio = { version = "1", features = ["full"] }

[profile.release]
opt-level = 3

[[bin]]
name = "mpe_mongo_plugin"
path = "src/main.rs"
```

> **SDK 获取方式（二选一）**：
> 1. **git 依赖（tag 固定）**：`mpe-plugin-sdk = { git = "https://github.com/multi-protocol-flow/mpe-plugin-sdk.git", tag = "v0.1.0" }`（默认特性，含 runtime）。
>    已验证可独立编译——cargo 按 git tag 拉取，无需宿主目录。
> 2. **本地联调**：`[patch."https://github.com/multi-protocol-flow/mpe-plugin-sdk.git"] mpe-plugin-sdk = { path = "../mpe-plugin-sdk" }` 指向本地 checkout。

---

## 3. plugin.json（宿主扫描清单）

```json
{
  "name": "mongo",
  "version": "0.1.0",
  "description": "MongoDB protocol plugin: connect/find/insert/update/delete/aggregate/close",
  "entry": {
    "command": "./mpe_mongo_plugin"
  },
  "env": {},
  "permissions": [],
  "capabilities": { "streaming": true }
}
```

关键字段：

| 字段 | 值 | 说明 |
|---|---|---|
| `name` | `mongo` | 唯一；目录名建议一致 |
| `entry.command` | 插件二进制路径 | 建议绝对路径；开发期用 `target/debug` |
| `capabilities.streaming` | `true` | **必须**：进程常驻，Mongo 连接池不随 60s 空闲回收而断 |
| `min_host_version` | 可选 | 版本门槛，不满足则宿主跳过 |

> **`streaming: true` 为什么必须**：默认 `false` 时宿主按需拉起进程并在
> 60s 空闲后回收——进程一死连接池全没，每次执行都要重建 TCP+TLS 握手。
> Mongo 是长连接协议，必须常驻。

---

## 4. 节点设计

所有节点端口：`in`（左）/ `out`（右）。失败走宿主 `on_error` 策略。

### 4.1 `mongo:connect` — 建立连接

```json
{ "uri": "mongodb://localhost:27017", "database": "mydb", "timeout_ms": 5000 }
```

- 用 `execution_id` 作 key 建池（`get_or_insert`），幂等：同一执行重复 connect 复用已有连接
- 输出：`{ "connected": true, "database": "mydb" }`
- 失败：报错 → 宿主按 `on_error` 路由

### 4.2 `mongo:find` — 查询

```json
{ "collection": "users", "filter": { "age": { "$gt": 18 } }, "project": null, "limit": 100 }
```

- 从当前 `execution_id` 的池取 `Client`，未 connect 则报错"请先 connect"
- 输出：`{ "count": N, "documents": [ ... ] }`

### 4.3 `mongo:insert` / `mongo:update` / `mongo:delete` / `mongo:aggregate`

| 节点 | 配置 | 输出 |
|---|---|---|
| `insert` | `{ collection, documents: [...] }` | `{ inserted_count }` |
| `update` | `{ collection, filter, update, upsert? }` | `{ matched_count, modified_count }` |
| `delete` | `{ collection, filter, delete_many? }` | `{ deleted_count }` |
| `aggregate` | `{ collection, pipeline: [...] }` | `{ documents: [...] }` |

### 4.5 `mongo:close` — 主动释放

```json
{ }
```

- `pool.remove(execution_id)` → 断开该执行的所有连接
- 用于流程中途主动断连（比如长流程后半段不再需要 DB）

---

## 5. 连接池设计（核心：per-execution 隔离）

```rust
// pool.rs
pub struct MongoPool {
    inner: mpe_plugin_sdk::pool::ConnectionPool,  // key → Arc<dyn Any>
}

impl MongoPool {
    /// connect 节点：按 execution_id 建池（幂等复用）
    pub fn connect(&self, execution_id: &str, uri: &str) -> Result<Client> {
        self.inner.get_or_insert(execution_id, || /* Client::with_uri_str */)
    }
    /// find 等节点：取当前执行的连接，没有则报错
    pub fn client(&self, execution_id: &str) -> Option<Arc<Client>> {
        self.inner.get::<Client>(execution_id)
    }
    /// close 节点 / flow_ended 回调：释放
    pub fn release(&self, execution_id: &str) {
        self.inner.remove(execution_id);
    }
}
```

**为什么 key 用 `execution_id`**：
- 一次流程执行 = 一个 `execution_id`，同流程所有节点共享 → connect 建的池 find 能取到
- 不同执行（重复运行 / 压测虚拟用户 / 数据集行）各自唯一 → 互不串池
- `execution_id` 由宿主每次执行生成，插件把它当**不透明 key**，绝不要假设它等于 flow uuid

**释放路径（双保险）**：

```
1. 正常路径：宿主执行完广播 flowEnded(execution_id)
   → 插件 Plugin::flow_ended() 回调 → pool.release(execution_id)
2. 流程内主动：mongo:close 节点 → pool.release(execution_id)
```

---

## 6. 插件主实现（骨架）

```rust
// src/lib.rs
use mpe_plugin_sdk::prelude::*;

#[derive(Default)]
pub struct MongoPlugin {
    pub pool: MongoPool,   // 进程级单例
}

impl Plugin for MongoPlugin {
    fn describe(&self) -> Vec<NodeDescription> {
        vec![
            NodeDescription::new("mongo:connect", "Mongo Connect"),
            NodeDescription::new("mongo:find", "Mongo Find"),
            NodeDescription::new("mongo:insert", "Mongo Insert"),
            NodeDescription::new("mongo:update", "Mongo Update"),
            NodeDescription::new("mongo:delete", "Mongo Delete"),
            NodeDescription::new("mongo:aggregate", "Mongo Aggregate"),
            NodeDescription::new("mongo:close", "Mongo Close"),
        ]
    }

    fn execute(&self, ctx: &mut ExecuteContext) -> impl Future<Output = ExecuteResult> + Send {
        // ctx.execution_id() 取当前执行 id；ctx.config() 取宿主 resolve 后的配置
        // 按 config 的节点类型分派到 nodes/*.rs
        async move { /* ... */ }
    }

    fn flow_ended(&self, execution_id: &str) {
        self.pool.release(execution_id);   // 流程结束自动释放连接池
    }
}

// src/main.rs
use mpe_plugin_sdk::prelude::*;
mpe_plugin_main!(MongoPlugin);
```

---

## 7. 开发 & 测试

```bash
# 构建（独立于宿主 workspace）
cd plugins/mongo
cargo build --release
# 产物: target/release/mpe_mongo_plugin.exe

# 单元测试（连接池隔离、配置解析）
cargo test

# 手动冒烟：模拟宿主对话（describe → execute）
echo '{"jsonrpc":"2.0","id":1,"method":"describe","params":{}}' | target/debug/mpe_mongo_plugin.exe
```

> **注意（todo-1/todo-12 实测）**：`entry.command`（plugin.json）按宿主文档
> （docs/plugin-sdk.md:72）是相对**宿主进程 cwd** 解析，而非插件目录。
> todo-12 起 plugin.json 已改为平台正确的相对路径 `target/release/mpe_mongo_plugin`
> （Linux 构建产物无 `.exe` 后缀），运行时需保证宿主进程 cwd 在插件目录内
> （见下方"端到端"命令）；宿主 cwd 不可控的部署场景应改填绝对路径。

### 端到端（连真宿主）

```bash
# 把插件目录给宿主
export MPE_PLUGIN_DIR=/path/to/plugins   # 或复制到宿主数据目录 plugins/
mpe run -f flow_with_mongo.json
```

### 与宿主一起跑（todo-12 实测 e2e）

宿主 conformance 套件（`flow-engine-plugin/tests/conformance.rs`）只驱动
`echo_mock` 与模板插件，**不会扫描本插件**——真正驱动本插件的是宿主 CLI 的
插件扫描（`MPE_PLUGIN_DIR`）。已实测通过的端到端步骤：

```bash
# 1. 构建插件 release（产物 target/release/mpe_mongo_plugin，Linux 无 .exe）
cd plugins/mongo && cargo build --release

# 2. 构建纯 CLI 宿主
cd src-tauri && cargo build -p mpe-cli --release

# 3. 从插件目录运行（相对 entry.command 按宿主进程 cwd 解析；flow 路径必须用绝对路径）
cd plugins/mongo
MPE_PLUGIN_DIR=/path/to/plugins /path/to/src-tauri/target/release/mpe-cli run \
  -f /path/to/plugins/mongo/tests/fixtures/mongo_e2e_flow.json
```

实测结果（todo-12，连本地 mongod）：`mpe-cli run` 退出码 0，报告
`executed_count=5`（entry→connect→insert→find→end），insert 节点
`inserted_count=2`、find 节点 `count=2`。注意 `mpe-cli validate` 只使用内置
registry，插件节点类型会报 `E_VAL_UNKNOWN_NODE_TYPE`——属预期行为，验证插件
流程以 `run` 的报告为准。

---

## 8. 约束清单（第三方必须遵守）

| # | 约束 | 原因 |
|---|---|---|
| 1 | 只用 `mpe-plugin-sdk` 公开 API | 插件是子进程，禁止继承宿主内部类型 |
| 2 | `type_id` 用 `vendor:name` 命名空间 | 避免与 91 个宿主内置类型冲突 |
| 3 | `variable_updates` key 必须是**扁平顶层名**（无点号） | 宿主 VariableStorage 是扁平结构，点号 key 会被警告+丢弃 |
| 4 | `report_data` ≤ 1 MiB | 宿主 wire 上限 |
| 5 | 大结果集避免塞进输出 | 转成变量/分页/聚合，防超时与内存 |
| 6 | `execute` 返回 `Send` future | SDK 按请求 spawn task 流水线 |
| 7 | 每帧 ≤ 2 MiB（双向） | framing 协议上限 |
| 8 | `streaming: true` 常驻 | 否则连接池随进程回收失效 |

---

## 9. 下一步（可迭代项）

- [x] connect/find 跑通端到端（连本地 mongod）
- [x] insert/update/delete/aggregate
- [ ] 配置面板（P1 iframe / inline HTML）——非必须（配置经 describe 的 `config_schema` 提供，见决策 D8，未做自定义面板）
- [ ] 发布 SDK 到 crates.io 后，插件切 crates.io 依赖彻底脱离宿主目录
