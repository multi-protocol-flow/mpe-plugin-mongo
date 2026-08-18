//! MongoDB 插件的 stdio 集成测试：针对真实 mongod，JSON-RPC 2.0 over piped
//! stdio 驱动编译产物 `mpe_mongo_plugin`，与宿主走完全相同的传输通道。
//!
//! 运行方式（需要 docker 中有 mongo:7）：
//! ```bash
//! docker run -d --name mpe-mongo-test -p 27017:27017 mongo:7
//! cd plugins/mongo && cargo test -- --include-ignored
//! docker rm -f mpe-mongo-test
//! ```
//!
//! 全部用例默认 `#[ignore = "requires MongoDB"]`：无 mongod 时 `cargo test`
//! 只编译、不执行。每个用例：独立测试库（进程 pid + 用例名，天然隔离并行
//! 测试线程）→ 种子数据（driver 仅用于 setup/teardown；插件交互全部走
//! stdio）→ 逐请求断言 → `db.drop()` 清理（`DbGuard` 的 `Drop` 保证断言
//! 失败也清理）。

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::Duration;

use mongodb::bson::doc;
use mongodb::bson::Document;
use mongodb::options::ClientOptions;
use mongodb::Client;
use serde_json::{json, Value};

/// 编译产物路径（cargo 为包内 `[[bin]]` 注入）。
const PLUGIN_BIN: &str = env!("CARGO_BIN_EXE_mpe_mongo_plugin");

/// 连接串：`MPE_MONGO_URI` 可覆盖，默认本地 mongod。
fn mongo_uri() -> String {
    std::env::var("MPE_MONGO_URI").unwrap_or_else(|_| "mongodb://localhost:27017".to_string())
}

/// 每次运行的随机测试库：进程 pid 隔离并发运行，用例名隔离并行测试线程。
fn test_db(case: &str) -> String {
    format!("mpe_plugin_test_{}_{}", std::process::id(), case)
}

/// `mongo:connect` 节点配置（连到本用例的测试库）。
fn connect_config(uri: &str, db: &str) -> Value {
    json!({ "type": "mongo:connect", "uri": uri, "database": db, "timeout_ms": 10_000 })
}

/// connect 请求参数：宿主每次 execute 都带 `node_instance_id`（节点实例
/// uuid），connect 节点以 `(execution_id, node_instance_id)` 为池 key 注册
/// 连接；操作节点的 `connection_uuid` 必须指向该实例 id（宿主经 schema 的
/// `x-node-selector` 注入，见 src/lib.rs）。
fn connect_params(exec: &str, instance: &str, uri: &str, db: &str) -> Value {
    json!({ "execution_id": exec, "node_instance_id": instance, "config": connect_config(uri, db) })
}

// ---------------------------------------------------------------------------
// stdio 驱动（插件进程）
// ---------------------------------------------------------------------------

/// 一个插件子进程 + 双向 stdio 管道。同一进程内可连续发多个请求
/// （per-execution 隔离用例需要）。
struct PluginProcess {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
}

impl PluginProcess {
    /// 拉起编译产物（每次用例一个全新进程，模拟宿主的"拉起→常驻→EOF 回收"）。
    fn spawn() -> PluginProcess {
        let mut child = Command::new(PLUGIN_BIN)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("无法拉起 mpe_mongo_plugin");
        let stdin = child.stdin.take().expect("插件 stdin 不可用");
        let stdout = child.stdout.take().expect("插件 stdout 不可用");
        PluginProcess {
            child,
            stdin: Some(stdin),
            stdout: BufReader::new(stdout),
            next_id: 0,
        }
    }

    /// 发一个 JSON-RPC 请求并等待其响应（按 id 关联；log 通知帧无 id，跳过）。
    /// 请求顺序发送，同一时刻在途请求至多一个，响应按序返回。
    fn request(&mut self, method: &str, params: Value) -> Value {
        self.next_id += 1;
        let id = self.next_id;
        self.send_frame(&json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }));
        loop {
            let value: Value =
                serde_json::from_str(&self.read_frame()).expect("响应必须是合法 JSON");
            if value.get("id").and_then(Value::as_u64) == Some(id) {
                return value;
            }
            // 无 id 的帧是 log 通知（失败路径会伴随错误日志），直接丢弃。
        }
    }

    /// 发一个 fire-and-forget 通知（如 `flowEnded`），不读响应。
    fn notify(&mut self, method: &str, params: Value) {
        self.send_frame(&json!({ "jsonrpc": "2.0", "method": method, "params": params }));
    }

    fn send_frame(&mut self, frame: &Value) {
        let line = serde_json::to_string(frame).expect("请求序列化失败");
        let stdin = self.stdin.as_mut().expect("stdin 已关闭");
        stdin.write_all(line.as_bytes()).expect("写入请求失败");
        stdin.write_all(b"\n").expect("写入换行失败");
        stdin.flush().expect("刷新 stdin 失败");
    }

    /// 读一帧（LF 分隔；容忍 CRLF）。
    fn read_frame(&mut self) -> String {
        let mut line = String::new();
        let n = self
            .stdout
            .read_line(&mut line)
            .expect("读取插件 stdout 失败");
        assert!(n > 0, "插件 stdout 提前关闭（进程崩溃？）");
        line.trim_end_matches(['\r', '\n']).to_string()
    }

    /// 半关闭 stdin（EOF）→ 插件干净退出（退出码 0），模拟宿主回收。
    fn close(mut self) {
        drop(self.stdin.take());
        let status = self.child.wait().expect("等待插件退出失败");
        assert!(status.success(), "插件异常退出: {status:?}");
    }
}

impl Drop for PluginProcess {
    fn drop(&mut self) {
        // 断言失败（panic 展开）时也要回收常驻插件进程，避免孤儿进程。
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ---------------------------------------------------------------------------
// 响应断言
// ---------------------------------------------------------------------------

/// execute 响应的 `result`。
fn result(resp: &Value) -> &Value {
    resp.get("result").expect("响应必须携带 result")
}

/// 断言 execute 成功并返回 `output_data`。
fn output(resp: &Value) -> &Value {
    let result = result(resp);
    assert_eq!(
        result["success"].as_bool(),
        Some(true),
        "execute 必须成功, 实际: {result}"
    );
    result
        .get("output_data")
        .expect("成功响应必须携带 output_data")
}

/// 断言 execute 失败，且 `errors` 提到 `needle`（如 "connect"）。
fn failed_with(resp: &Value, needle: &str) {
    let result = result(resp);
    assert_eq!(
        result["success"].as_bool(),
        Some(false),
        "execute 必须失败, 实际: {result}"
    );
    let errors = result["errors"]
        .as_array()
        .expect("失败响应必须携带 errors");
    let joined = errors
        .iter()
        .filter_map(|e| e.get("message").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("; ");
    assert!(
        joined.contains(needle),
        "errors 必须提及 `{needle}`, 实际: {joined}"
    );
}

// ---------------------------------------------------------------------------
// 测试库生命周期（driver 仅用于 setup/teardown，插件交互一律走 stdio）
// ---------------------------------------------------------------------------

/// 短超时客户端：mongod 不可达时 setup/teardown 快速失败而非干等 30s。
async fn short_timeout_client(uri: &str) -> mongodb::error::Result<Client> {
    let mut options = ClientOptions::parse(uri).await?;
    options.connect_timeout = Some(Duration::from_millis(3000));
    options.server_selection_timeout = Some(Duration::from_millis(3000));
    Client::with_options(options)
}

/// 用 driver 直接种数据（只做 setup；插件交互一律走 stdio）。
async fn seed(uri: &str, db: &str, collection: &str, docs: &[Value]) {
    let client = short_timeout_client(uri).await.expect("种子客户端创建失败");
    let docs: Vec<Document> = docs
        .iter()
        .map(|doc| mongodb::bson::to_document(doc).expect("种子文档必须能转为 BSON"))
        .collect();
    client
        .database(db)
        .collection::<Document>(collection)
        .insert_many(docs)
        .await
        .expect("种子数据写入失败");
}

/// 测试库守卫：`Drop` 时执行 `db.drop()`——正常结束、`?` 提前返回、断言
/// 失败 panic 展开三种路径都保证清理，绝不留下脏数据。
struct DbGuard {
    uri: String,
    db: String,
}

impl DbGuard {
    /// 先清掉同名旧库（pid 复用时上次崩溃的残留），再进入用例。
    async fn new(uri: &str, db: &str) -> DbGuard {
        let guard = DbGuard {
            uri: uri.to_string(),
            db: db.to_string(),
        };
        guard.cleanup().await;
        guard
    }

    async fn cleanup(&self) {
        if let Ok(client) = short_timeout_client(&self.uri).await {
            let _ = client.database(&self.db).drop().await;
        }
    }
}

impl Drop for DbGuard {
    fn drop(&mut self) {
        // `Drop` 不能 await；当前 runtime 的 `Handle::block_on` 在异步上下文
        // 中会 panic。新起独立 runtime（嵌套 runtime 合法）驱动清理。
        let uri = self.uri.clone();
        let db = self.db.clone();
        let _ = std::thread::spawn(move || {
            let rt = match tokio::runtime::Runtime::new() {
                Ok(rt) => rt,
                Err(_) => return,
            };
            let _ = rt.block_on(async move {
                if let Ok(client) = short_timeout_client(&uri).await {
                    let _ = client.database(&db).drop().await;
                }
            });
        })
        .join();
    }
}

// ---------------------------------------------------------------------------
// 用例
// ---------------------------------------------------------------------------

/// 快速失败探针：mongod 未启动时，其它用例会各自超时报错，难以定位；这个
/// 用例先给出可读的启动指引。
#[tokio::test]
#[ignore = "requires MongoDB"]
async fn requires_mongod_is_running() {
    let uri = mongo_uri();
    let client = short_timeout_client(&uri).await.expect("URI 解析失败");
    let ping = client
        .database("admin")
        .run_command(doc! { "ping": 1 })
        .await;
    assert!(
        ping.is_ok(),
        "mongod 不可达（{uri}）。请先启动: docker run -d --name mpe-mongo-test -p 27017:27017 mongo:7"
    );
}

/// (a) connect → insert 3 docs（documents 走 JSON 文本形态）→ find 全量
/// 与 limit 限定，逐条核对输出形状。
#[tokio::test]
#[ignore = "requires MongoDB"]
async fn connect_insert_find_roundtrip() {
    let uri = mongo_uri();
    let db = test_db("connect_insert_find");
    let _guard = DbGuard::new(&uri, &db).await;
    let mut plugin = PluginProcess::spawn();

    let resp = plugin.request(
        "execute",
        connect_params("exec-a", "conn-1", &uri, &db),
    );
    let out = output(&resp);
    assert_eq!(out["connected"].as_bool(), Some(true));
    assert_eq!(out["database"].as_str(), Some(db.as_str()));

    let resp = plugin.request(
        "execute",
        json!({
            "execution_id": "exec-a",
            "config": json!({
                "type": "mongo:insert",
                "connection_uuid": "conn-1",
                "collection": "users",
                "documents": r#"[{"name":"a"},{"name":"b"},{"name":"c"}]"#,
            }),
        }),
    );
    assert_eq!(output(&resp)["inserted_count"].as_u64(), Some(3));

    let resp = plugin.request(
        "execute",
        json!({
            "execution_id": "exec-a",
            "config": json!({
                "type": "mongo:find",
                "connection_uuid": "conn-1",
                "collection": "users",
                "filter": "{}",
                "limit": 100,
            }),
        }),
    );
    let out = output(&resp);
    assert_eq!(out["count"].as_u64(), Some(3));
    assert_eq!(
        out["documents"]
            .as_array()
            .expect("documents 必须是数组")
            .len(),
        3
    );

    let resp = plugin.request(
        "execute",
        json!({
            "execution_id": "exec-a",
            "config": json!({
                "type": "mongo:find",
                "connection_uuid": "conn-1",
                "collection": "users",
                "filter": "{}",
                "limit": 2,
            }),
        }),
    );
    let out = output(&resp);
    assert_eq!(out["count"].as_u64(), Some(2));
    assert_eq!(
        out["documents"]
            .as_array()
            .expect("documents 必须是数组")
            .len(),
        2
    );

    plugin.close();
}

/// (b) update_one（默认）→ update_many → upsert 插入（driver 语义：
/// 插入时 matched/modified 均为 0），随后用 find 节点证明 upsert 文档可查。
#[tokio::test]
#[ignore = "requires MongoDB"]
async fn update_one_many_upsert() {
    let uri = mongo_uri();
    let db = test_db("update");
    let _guard = DbGuard::new(&uri, &db).await;
    seed(
        &uri,
        &db,
        "users",
        &[
            json!({ "group": "b", "name": "b1" }),
            json!({ "group": "b", "name": "b2" }),
        ],
    )
    .await;
    let mut plugin = PluginProcess::spawn();

    let resp = plugin.request(
        "execute",
        connect_params("exec-b", "conn-1", &uri, &db),
    );
    assert_eq!(output(&resp)["connected"].as_bool(), Some(true));

    // update_many: false（默认）只更新一条匹配文档。
    let resp = plugin.request(
        "execute",
        json!({
            "execution_id": "exec-b",
            "config": json!({
                "type": "mongo:update",
                "connection_uuid": "conn-1",
                "collection": "users",
                "filter": r#"{"group": "b"}"#,
                "update": r#"{"$set": {"tag": "one"}}"#,
            }),
        }),
    );
    let out = output(&resp);
    assert_eq!(out["matched_count"].as_u64(), Some(1));
    assert_eq!(out["modified_count"].as_u64(), Some(1));

    // update_many: true 命中两条。
    let resp = plugin.request(
        "execute",
        json!({
            "execution_id": "exec-b",
            "config": json!({
                "type": "mongo:update",
                "connection_uuid": "conn-1",
                "collection": "users",
                "filter": r#"{"group": "b"}"#,
                "update": r#"{"$set": {"tag": "many"}}"#,
                "update_many": true,
            }),
        }),
    );
    let out = output(&resp);
    assert_eq!(out["matched_count"].as_u64(), Some(2));
    assert_eq!(out["modified_count"].as_u64(), Some(2));

    // upsert 命中不存在的 filter → 插入新文档（matched/modified 均为 0）。
    let resp = plugin.request(
        "execute",
        json!({
            "execution_id": "exec-b",
            "config": json!({
                "type": "mongo:update",
                "connection_uuid": "conn-1",
                "collection": "users",
                "filter": r#"{"name": "upserted"}"#,
                "update": r#"{"$set": {"age": 99}}"#,
                "upsert": true,
            }),
        }),
    );
    let out = output(&resp);
    assert_eq!(out["matched_count"].as_u64(), Some(0));
    assert_eq!(out["modified_count"].as_u64(), Some(0));

    // 跟随 find 节点确认 upsert 插入的文档真实存在。
    let resp = plugin.request(
        "execute",
        json!({
            "execution_id": "exec-b",
            "config": json!({
                "type": "mongo:find",
                "connection_uuid": "conn-1",
                "collection": "users",
                "filter": r#"{"name": "upserted"}"#,
                "limit": 10,
            }),
        }),
    );
    let out = output(&resp);
    assert_eq!(out["count"].as_u64(), Some(1));
    assert_eq!(out["documents"][0]["age"].as_u64(), Some(99));

    plugin.close();
}

/// (c) delete_one（默认）删 1 条 → delete_many 删剩余 2 条 → find 确认清空。
#[tokio::test]
#[ignore = "requires MongoDB"]
async fn delete_one_then_many() {
    let uri = mongo_uri();
    let db = test_db("delete");
    let _guard = DbGuard::new(&uri, &db).await;
    seed(
        &uri,
        &db,
        "users",
        &[
            json!({ "name": "c1" }),
            json!({ "name": "c2" }),
            json!({ "name": "c3" }),
        ],
    )
    .await;
    let mut plugin = PluginProcess::spawn();

    let resp = plugin.request(
        "execute",
        connect_params("exec-c", "conn-1", &uri, &db),
    );
    assert_eq!(output(&resp)["connected"].as_bool(), Some(true));

    let resp = plugin.request(
        "execute",
        json!({
            "execution_id": "exec-c",
            "config": json!({
                "type": "mongo:delete",
                "connection_uuid": "conn-1",
                "collection": "users",
                "filter": "{}",
                "delete_many": false,
            }),
        }),
    );
    assert_eq!(output(&resp)["deleted_count"].as_u64(), Some(1));

    let resp = plugin.request(
        "execute",
        json!({
            "execution_id": "exec-c",
            "config": json!({
                "type": "mongo:delete",
                "connection_uuid": "conn-1",
                "collection": "users",
                "filter": "{}",
                "delete_many": true,
            }),
        }),
    );
    assert_eq!(output(&resp)["deleted_count"].as_u64(), Some(2));

    // 终态可见性：全部删除后 find 为空。
    let resp = plugin.request(
        "execute",
        json!({
            "execution_id": "exec-c",
            "config": json!({
                "type": "mongo:find",
                "connection_uuid": "conn-1",
                "collection": "users",
                "filter": "{}",
                "limit": 10,
            }),
        }),
    );
    assert_eq!(output(&resp)["count"].as_u64(), Some(0));

    plugin.close();
}

/// (d) aggregate：$match + limit 2 → 输出 ≤ 2 条且都命中条件；$count 阶段
/// → 输出非空且 total 正确。
#[tokio::test]
#[ignore = "requires MongoDB"]
async fn aggregate_bounded() {
    let uri = mongo_uri();
    let db = test_db("aggregate");
    let _guard = DbGuard::new(&uri, &db).await;
    seed(
        &uri,
        &db,
        "users",
        &[
            json!({ "n": 1 }),
            json!({ "n": 2 }),
            json!({ "n": 3 }),
            json!({ "n": 4 }),
            json!({ "n": 5 }),
        ],
    )
    .await;
    let mut plugin = PluginProcess::spawn();

    let resp = plugin.request(
        "execute",
        connect_params("exec-d", "conn-1", &uri, &db),
    );
    assert_eq!(output(&resp)["connected"].as_bool(), Some(true));

    let resp = plugin.request(
        "execute",
        json!({
            "execution_id": "exec-d",
            "config": json!({
                "type": "mongo:aggregate",
                "connection_uuid": "conn-1",
                "collection": "users",
                "pipeline": r#"[{"$match": {"n": {"$gte": 2}}}]"#,
                "limit": 2,
            }),
        }),
    );
    let docs = output(&resp)["documents"]
        .as_array()
        .expect("documents 必须是数组");
    assert_eq!(docs.len(), 2, "limit 必须约束聚合输出");
    for doc in docs {
        assert!(
            doc["n"].as_u64().expect("n 必须是数字") >= 2,
            "所有文档必须命中 $gte 2"
        );
    }

    let resp = plugin.request(
        "execute",
        json!({
            "execution_id": "exec-d",
            "config": json!({
                "type": "mongo:aggregate",
                "connection_uuid": "conn-1",
                "collection": "users",
                "pipeline": r#"[{"$count": "total"}]"#,
                "limit": 100,
            }),
        }),
    );
    let docs = output(&resp)["documents"]
        .as_array()
        .expect("documents 必须是数组");
    assert!(!docs.is_empty(), "$count 阶段必须产生文档");
    assert_eq!(docs[0]["total"].as_u64(), Some(5));

    plugin.close();
}

/// (e) 同一进程内两个 execution_id：exec A connect + insert，exec B 未
/// connect 直接 find → 失败（请先 connect），证明连接池按 execution_id
/// 隔离、无跨执行共享；A 的 close 不影响 B（B 本就没有连接）。
#[tokio::test]
#[ignore = "requires MongoDB"]
async fn execution_id_isolation() {
    let uri = mongo_uri();
    let db = test_db("isolation");
    let _guard = DbGuard::new(&uri, &db).await;
    let mut plugin = PluginProcess::spawn();

    // Exec A：connect + insert。
    let resp = plugin.request(
        "execute",
        connect_params("exec-a", "conn-a", &uri, &db),
    );
    assert_eq!(output(&resp)["connected"].as_bool(), Some(true));
    let resp = plugin.request(
        "execute",
        json!({
            "execution_id": "exec-a",
            "config": json!({
                "type": "mongo:insert",
                "connection_uuid": "conn-a",
                "collection": "users",
                "documents": r#"[{"name": "a"}]"#,
            }),
        }),
    );
    assert_eq!(output(&resp)["inserted_count"].as_u64(), Some(1));

    // Exec B：同一进程、不同 execution_id，未 connect 直接 find → 失败。
    let resp = plugin.request(
        "execute",
        json!({
            "execution_id": "exec-b",
            "config": json!({
                "type": "mongo:find",
                "connection_uuid": "conn-b",
                "collection": "users",
                "filter": "{}",
                "limit": 10,
            }),
        }),
    );
    failed_with(&resp, "connect");

    // A 的连接不受 B 失败影响。
    let resp = plugin.request(
        "execute",
        json!({
            "execution_id": "exec-a",
            "config": json!({
                "type": "mongo:find",
                "connection_uuid": "conn-a",
                "collection": "users",
                "filter": "{}",
                "limit": 10,
            }),
        }),
    );
    assert_eq!(output(&resp)["count"].as_u64(), Some(1));

    // A 的 close 只释放 A；B 依然未连接（find 依旧失败）。
    let resp = plugin.request(
        "execute",
        json!({ "execution_id": "exec-a", "config": json!({ "type": "mongo:close" }) }),
    );
    assert_eq!(result(&resp)["success"].as_bool(), Some(true));
    let resp = plugin.request(
        "execute",
        json!({
            "execution_id": "exec-b",
            "config": json!({
                "type": "mongo:find",
                "connection_uuid": "conn-b",
                "collection": "users",
                "filter": "{}",
                "limit": 10,
            }),
        }),
    );
    failed_with(&resp, "connect");

    plugin.close();
}

/// (f) connect → close（主动释放）→ find → 失败（请先 connect）；随后再次
/// connect 可恢复（释放不是永久失效）。
#[tokio::test]
#[ignore = "requires MongoDB"]
async fn close_then_find_fails() {
    let uri = mongo_uri();
    let db = test_db("close");
    let _guard = DbGuard::new(&uri, &db).await;
    let mut plugin = PluginProcess::spawn();

    let resp = plugin.request(
        "execute",
        connect_params("exec-f", "conn-1", &uri, &db),
    );
    assert_eq!(output(&resp)["connected"].as_bool(), Some(true));

    let resp = plugin.request(
        "execute",
        json!({ "execution_id": "exec-f", "config": json!({ "type": "mongo:close" }) }),
    );
    assert_eq!(result(&resp)["success"].as_bool(), Some(true));

    let resp = plugin.request(
        "execute",
        json!({
            "execution_id": "exec-f",
            "config": json!({
                "type": "mongo:find",
                "connection_uuid": "conn-1",
                "collection": "users",
                "filter": "{}",
                "limit": 10,
            }),
        }),
    );
    failed_with(&resp, "connect");

    // 再次 connect 必须干净恢复（close 不永久失效）。
    let resp = plugin.request(
        "execute",
        connect_params("exec-f", "conn-1", &uri, &db),
    );
    assert_eq!(output(&resp)["connected"].as_bool(), Some(true));

    plugin.close();
}

/// (g) connect（exec X）→ 宿主广播 flowEnded(execution_id: X) 通知 → 同
/// execution_id 的 find 失败（连接已被释放）；下次 execute 必须重新 connect。
#[tokio::test]
#[ignore = "requires MongoDB"]
async fn flow_ended_releases_connection() {
    let uri = mongo_uri();
    let db = test_db("flow_ended");
    let _guard = DbGuard::new(&uri, &db).await;
    let mut plugin = PluginProcess::spawn();

    let resp = plugin.request(
        "execute",
        connect_params("exec-g", "conn-1", &uri, &db),
    );
    assert_eq!(output(&resp)["connected"].as_bool(), Some(true));

    // 宿主 → 插件的 fire-and-forget flowEnded 通知（无 id、无响应）。
    plugin.notify("flowEnded", json!({ "execution_id": "exec-g" }));

    let resp = plugin.request(
        "execute",
        json!({
            "execution_id": "exec-g",
            "config": json!({
                "type": "mongo:find",
                "connection_uuid": "conn-1",
                "collection": "users",
                "filter": "{}",
                "limit": 10,
            }),
        }),
    );
    failed_with(&resp, "connect");

    // 下一次 execute 必须重新 connect 才能继续。
    let resp = plugin.request(
        "execute",
        connect_params("exec-g", "conn-1", &uri, &db),
    );
    assert_eq!(output(&resp)["connected"].as_bool(), Some(true));

    plugin.close();
}
