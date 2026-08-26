//! MongoDB sidecar plugin for MPE.
//!
//! Third-party plugin crate: depends only on `mpe-plugin-sdk` (which in turn
//! bundles the shared wire contract (`mpe-plugin-sdk::protocol`)). Never imports host
//! types (`flow-engine-core`, `flow-engine-plugin`, ...) — the plugin is a
//! separate process speaking JSON-RPC 2.0 over stdio.
//!
//! Node types: `mongo:connect` / `mongo:find` / `mongo:insert` /
//! `mongo:update` / `mongo:delete` / `mongo:aggregate` / `mongo:close`.
//! `execute` dispatches to `nodes/*.rs` by the `config["type"]`
//! discriminator. The six operation nodes declare `in`/`true`/`false`
//! tri-ports (success routes `true`, failure goes through the host `on_error`
//! strategy to `false`); `mongo:close` keeps the single `in`/`out` shape.
//!
//! Connection selection: the pool keys connections by the composite
//! `(execution_id, connection_uuid)` — the connect node registers under its
//! own instance id (`ctx.node_instance_id()`), operation nodes pick their
//! connection via the `connection_uuid` field the host resolves into their
//! config from the schema's `x-node-selector`.

use std::future::Future;

use mpe_plugin_sdk::prelude::*;

mod i18n;
mod json;
mod nodes;
mod pool;
use pool::MongoPool;

/// Process-level plugin singleton: node registry + per-execution pool.
#[derive(Default)]
pub struct MongoPlugin {
    pub pool: MongoPool,
}

/// Builds one MongoDB node description with host-facing metadata.
///
/// `default_config` is injected into a freshly added node; `properties` is the
/// JSON-Schema `properties` map (each value declares its own `type`) and
/// `required` the schema's required field list. Complex JSON fields (`filter`,
/// `project`, `documents`, `update`, `pipeline`) are declared as
/// `"type": "string"` JSON-text inputs with `"format": "json"`: the host
/// `SchemaConfigPanel` renders `string`/`integer`/`boolean`/enum properties
/// with proper inputs, while `object`/`array` types fall through to a bare
/// text input that drops the placeholder and description. `ports` declares
/// the node's input/output ports (see [`operation_ports`] /
/// [`close_ports`]).
fn mongo_node(
    type_id: &str,
    display_name: &str,
    icon: &str,
    color: &str,
    default_config: serde_json::Value,
    properties: serde_json::Value,
    required: &[&str],
    ports: Vec<PortDescription>,
    capabilities: Option<PluginCapabilities>,
) -> NodeDescription {
    let mut node = NodeDescription::new(type_id, display_name);
    node.category = Some("MongoDB".to_string());
    node.icon = Some(icon.to_string());
    node.color = Some(color.to_string());
    node.ports = ports;
    node.default_config = default_config;
    node.capabilities = capabilities.unwrap_or_default();
    node.config_schema = Some(serde_json::json!({
        "type": "object",
        "properties": properties,
        "required": required,
    }));
    node
}

/// 操作节点（connect/find/insert/update/delete/aggregate）的双输出端口：
/// `in`(输入) + `true`(成功) + `false`(失败)。失败不在此路由：节点返回
/// `ExecuteResult::fail` 后宿主按 `on_error` 策略（默认 RouteToFalse）自动
/// 走 `false` 端口。
fn operation_ports() -> Vec<PortDescription> {
    vec![
        PortDescription::new("in", i18n::t("输入", "Input"), PORT_KIND_IN),
        PortDescription::new("true", i18n::t("成功", "Success"), PORT_KIND_OUT),
        PortDescription::new("false", i18n::t("失败", "Failure"), PORT_KIND_OUT),
    ]
}

/// `mongo:close` 的单输出端口：`in`(输入) + `out`(输出)。
fn close_ports() -> Vec<PortDescription> {
    vec![
        PortDescription::new("in", i18n::t("输入", "Input"), PORT_KIND_IN),
        PortDescription::new("out", i18n::t("输出", "Output"), PORT_KIND_OUT),
    ]
}

/// 操作节点 config_schema 里的 `connection_uuid` 字段：宿主 `x-node-selector`
/// 渲染为对流程内 `mongo:connect` 节点的选择器，选中后把 connect 节点的
/// instance id 注入 config。
fn connection_uuid_property() -> serde_json::Value {
    serde_json::json!({
        "type": "string",
        "title": i18n::t("Mongo 连接", "Mongo Connection"),
        "x-node-selector": { "node_type": "mongo:connect" },
    })
}

/// 操作节点成功结果，显式选择 `true` 端口让宿主路由到下游成功分支。
///
/// 宿主把 execute 响应里的 `next_ports` 解析为连接路由
/// （flow-engine-plugin `wrapper.rs::resolve_next_node_ids`）：空列表意味着
/// "没有匹配的端口连接"，分支在此终止（todo-12 宿主实测）。因此所有成功
/// 路径都必须显式声明 `next_ports: ["true"]`，插件才能被宿主串联执行。
/// 失败不在此路由：返回 `ExecuteResult::fail` 后宿主按节点 `on_error`
/// 策略处理（默认 RouteToFalse → `false` 端口），插件无需自报 false。
pub(crate) fn ok_result(output: serde_json::Value) -> ExecuteResult {
    ExecuteResult {
        next_ports: vec!["true".to_string()],
        ..ExecuteResult::ok(output)
    }
}

/// `mongo:close` 成功结果：close 只有单输出端口，路由到 `out`。
pub(crate) fn close_ok_result(output: serde_json::Value) -> ExecuteResult {
    ExecuteResult {
        next_ports: vec!["out".to_string()],
        ..ExecuteResult::ok(output)
    }
}

impl Plugin for MongoPlugin {
    fn describe(&self) -> Vec<NodeDescription> {
        vec![
            // `mongo:connect` — 建立连接（连接池按 (execution_id, 节点实例) 复用）。
            mongo_node(
                "mongo:connect",
                "Mongo Connect",
                "database",
                "#10b981",
                serde_json::json!({
                    "uri": "mongodb://localhost:27017",
                    "database": "mydb",
                    "timeout_ms": 5000,
                }),
                serde_json::json!({
                    "uri": { "type": "string", "description": i18n::t("MongoDB 连接串，例如 mongodb://localhost:27017", "MongoDB connection string, e.g. mongodb://localhost:27017") },
                    "database": { "type": "string", "description": i18n::t("数据库名，例如 mydb", "Database name, e.g. mydb") },
                    "timeout_ms": { "type": "integer", "description": i18n::t("连接超时（毫秒）", "Connection timeout (ms)") },
                }),
                &["uri", "database", "timeout_ms"],
                operation_ports(),
                // mongo:connect 可经宿主单节点路径（execute_single_node /
                // mpe run node）做连接性验证 → 声明 single_node capability。
                Some(PluginCapabilities {
                    single_node: true,
                    ..Default::default()
                }),
            ),
            // `mongo:find` — 查询。
            mongo_node(
                "mongo:find",
                "Mongo Find",
                "search",
                "#10b981",
                serde_json::json!({
                    "collection": "users",
                    "filter": null,
                    "project": null,
                    "limit": 100,
                    "timeout_ms": 5000,
                }),
                serde_json::json!({
                    "connection_uuid": connection_uuid_property(),
                    "collection": { "type": "string", "description": i18n::t("集合名", "Collection name") },
                    "filter": { "type": "string", "format": "json", "description": i18n::t(r#"JSON 文本，例如 {"age": {"$gt": 18}}"#, r#"JSON text, e.g. {"age": {"$gt": 18}}"#) },
                    "project": { "type": "string", "format": "json", "description": i18n::t(r#"JSON 文本，例如 {"name": 1, "_id": 0}；null 表示不投影"#, r#"JSON text, e.g. {"name": 1, "_id": 0}; null means no projection"#) },
                    "limit": { "type": "integer", "description": i18n::t("返回条数上限", "Maximum documents to return") },
                    "timeout_ms": { "type": "integer", "description": i18n::t("操作超时（毫秒）", "Operation timeout (ms)") },
                }),
                &["collection"],
                operation_ports(),
                None,
            ),
            // `mongo:insert` — 插入文档。
            mongo_node(
                "mongo:insert",
                "Mongo Insert",
                "plus",
                "#10b981",
                serde_json::json!({
                    "collection": "users",
                    "documents": [],
                    "timeout_ms": 5000,
                }),
                serde_json::json!({
                    "connection_uuid": connection_uuid_property(),
                    "collection": { "type": "string", "description": i18n::t("集合名", "Collection name") },
                    "documents": { "type": "string", "format": "json", "description": i18n::t(r#"JSON 文本，例如 [{"name": "Alice", "age": 30}]"#, r#"JSON text, e.g. [{"name": "Alice", "age": 30}]"#) },
                    "timeout_ms": { "type": "integer", "description": i18n::t("操作超时（毫秒）", "Operation timeout (ms)") },
                }),
                &["collection"],
                operation_ports(),
                None,
            ),
            // `mongo:update` — 更新文档。
            mongo_node(
                "mongo:update",
                "Mongo Update",
                "refresh-cw",
                "#10b981",
                serde_json::json!({
                    "collection": "users",
                    "filter": null,
                    "update": null,
                    "upsert": false,
                    "update_many": false,
                    "timeout_ms": 5000,
                }),
                serde_json::json!({
                    "connection_uuid": connection_uuid_property(),
                    "collection": { "type": "string", "description": i18n::t("集合名", "Collection name") },
                    "filter": { "type": "string", "format": "json", "description": i18n::t(r#"JSON 文本，例如 {"name": "Alice"}"#, r#"JSON text, e.g. {"name": "Alice"}"#) },
                    "update": { "type": "string", "format": "json", "description": i18n::t(r#"JSON 文本，例如 {"$set": {"age": 31}}"#, r#"JSON text, e.g. {"$set": {"age": 31}}"#) },
                    "upsert": { "type": "boolean", "description": i18n::t("不存在匹配文档时是否插入新文档", "Insert a new document when no match is found") },
                    "update_many": { "type": "boolean", "description": i18n::t("是否更新所有匹配文档（updateMany）", "Update all matching documents (updateMany)") },
                    "timeout_ms": { "type": "integer", "description": i18n::t("操作超时（毫秒）", "Operation timeout (ms)") },
                }),
                &["collection"],
                operation_ports(),
                None,
            ),
            // `mongo:delete` — 删除文档。
            mongo_node(
                "mongo:delete",
                "Mongo Delete",
                "trash-2",
                "#10b981",
                serde_json::json!({
                    "collection": "users",
                    "filter": null,
                    "delete_many": false,
                    "timeout_ms": 5000,
                }),
                serde_json::json!({
                    "connection_uuid": connection_uuid_property(),
                    "collection": { "type": "string", "description": i18n::t("集合名", "Collection name") },
                    "filter": { "type": "string", "format": "json", "description": i18n::t(r#"JSON 文本，例如 {"name": "Alice"}"#, r#"JSON text, e.g. {"name": "Alice"}"#) },
                    "delete_many": { "type": "boolean", "description": i18n::t("是否删除所有匹配文档（deleteMany）", "Delete all matching documents (deleteMany)") },
                    "timeout_ms": { "type": "integer", "description": i18n::t("操作超时（毫秒）", "Operation timeout (ms)") },
                }),
                &["collection"],
                operation_ports(),
                None,
            ),
            // `mongo:aggregate` — 聚合管道。
            mongo_node(
                "mongo:aggregate",
                "Mongo Aggregate",
                "layers",
                "#10b981",
                serde_json::json!({
                    "collection": "users",
                    "pipeline": [],
                    "limit": 100,
                    "timeout_ms": 5000,
                }),
                serde_json::json!({
                    "connection_uuid": connection_uuid_property(),
                    "collection": { "type": "string", "description": i18n::t("集合名", "Collection name") },
                    "pipeline": { "type": "string", "format": "json", "description": i18n::t(r#"JSON 文本，例如 [{"$match": {"age": {"$gt": 18}}}]"#, r#"JSON text, e.g. [{"$match": {"age": {"$gt": 18}}}]"#) },
                    "limit": { "type": "integer", "description": i18n::t("返回条数上限", "Maximum documents to return") },
                    "timeout_ms": { "type": "integer", "description": i18n::t("操作超时（毫秒）", "Operation timeout (ms)") },
                }),
                &["collection"],
                operation_ports(),
                None,
            ),
            // `mongo:close` — 主动释放连接（可选 connection_uuid 选中单个，
            // 未选则释放当前执行的全部连接）。
            mongo_node(
                "mongo:close",
                "Mongo Close",
                "plug",
                "#10b981",
                serde_json::json!({}),
                serde_json::json!({
                    "connection_uuid": connection_uuid_property(),
                }),
                &[],
                close_ports(),
                None,
            ),
        ]
    }

    // The runtime spawns execute futures, so the future must be Send; an
    // `async fn` cannot express that bound on stable (see `Plugin`).
    #[allow(clippy::manual_async_fn)]
    fn execute(&self, ctx: &mut ExecuteContext) -> impl Future<Output = ExecuteResult> + Send {
        async move {
            let node_type = ctx
                .config()
                .get("type")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            match node_type {
                "mongo:connect" => nodes::connect::execute(ctx, &self.pool).await,
                "mongo:find" => nodes::find::execute(ctx, &self.pool).await,
                "mongo:insert" => nodes::insert::execute(ctx, &self.pool).await,
                "mongo:update" => nodes::update::execute(ctx, &self.pool).await,
                "mongo:delete" => nodes::delete::execute(ctx, &self.pool).await,
                "mongo:aggregate" => nodes::aggregate::execute(ctx, &self.pool).await,
                "mongo:close" => nodes::close::execute(ctx, &self.pool).await,
                other => ExecuteResult::fail(i18n::t("未知节点类型", "Unknown node type").to_string() + &format!(" `{other}`")),
            }
        }
    }

    // Flow-completion hook — release path 1 of the README §5 double-release
    // design: the host broadcasts flowEnded when a flow execution finishes
    // (success, failure, or cancellation), and the plugin releases the
    // per-execution connections here. The `mongo:close` node is release path 2
    // for flows that disconnect mid-run; both may fire for one execution, and
    // `release` is a no-op on a missing entry, so double-release is harmless.
    fn flow_ended(&self, execution_id: &str) {
        self.pool.release(execution_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Complex JSON-valued config fields, which MUST be declared as
    /// `"type": "string"` JSON-text inputs with `"format": "json"` (not
    /// `object`/`array` — the host `SchemaConfigPanel` renders those as bare
    /// inputs without placeholder).
    const COMPLEX_FIELDS: &[(&str, &[&str])] = &[
        ("mongo:find", &["filter", "project"]),
        ("mongo:insert", &["documents"]),
        ("mongo:update", &["filter", "update"]),
        ("mongo:delete", &["filter"]),
        ("mongo:aggregate", &["pipeline"]),
    ];

    /// Every node must be GUI-usable in the host config panel: it carries a
    /// category, a JSON-Schema `config_schema`, a schema-shaped
    /// `default_config` (no `type` discriminator key, no pool-owned
    /// `database` field outside `mongo:connect`), and the correct port
    /// declarations.
    #[test]
    fn describe_metadata_shapes() {
        let nodes = MongoPlugin::default().describe();
        assert_eq!(nodes.len(), 7, "all 7 mongo nodes must be described");

        let by_type: HashMap<&str, &NodeDescription> = nodes
            .iter()
            .map(|node| (node.type_id.as_str(), node))
            .collect();
        assert_eq!(by_type.len(), nodes.len(), "type_ids must be unique");

        for node in &nodes {
            assert_eq!(
                node.category.as_deref(),
                Some("MongoDB"),
                "{} must carry category `MongoDB`",
                node.type_id
            );
            assert!(
                node.icon.is_some(),
                "{} must declare an icon",
                node.type_id
            );
            assert_eq!(
                node.color.as_deref(),
                Some("#10b981"),
                "{} must declare the MongoDB green color",
                node.type_id
            );
            assert!(
                node.default_config.get("type").is_none(),
                "{} default_config must not contain a `type` key",
                node.type_id
            );
            if node.type_id != "mongo:connect" {
                assert!(
                    node.default_config.get("database").is_none(),
                    "{} must not carry `database` (pool-owned, connect-only)",
                    node.type_id
                );
            }

            // Port declarations: operation nodes carry in/true/false, close
            // keeps the single in/out shape.
            let ports = &node.ports;
            assert!(!ports.is_empty(), "{} must declare ports", node.type_id);
            if node.type_id == "mongo:close" {
                assert_eq!(
                    ports.len(),
                    2,
                    "{} must declare exactly 2 ports",
                    node.type_id
                );
                assert_eq!(ports[0].id, "in");
                assert_eq!(ports[0].kind, PORT_KIND_IN);
                assert_eq!(ports[1].id, "out");
                assert_eq!(ports[1].kind, PORT_KIND_OUT);
            } else {
                assert_eq!(
                    ports.len(),
                    3,
                    "{} must declare in/true/false ports",
                    node.type_id
                );
                assert_eq!(ports[0].id, "in");
                assert_eq!(ports[0].kind, PORT_KIND_IN);
                assert_eq!(ports[1].id, "true");
                assert_eq!(ports[1].kind, PORT_KIND_OUT);
                assert_eq!(ports[2].id, "false");
                assert_eq!(ports[2].kind, PORT_KIND_OUT);
            }

            // Serialize the whole description and index the wire JSON: the
            // host panel reads category + config_schema from this shape.
            let wire = serde_json::to_value(node).expect("NodeDescription must serialize");
            assert_eq!(
                wire["category"], "MongoDB",
                "{} category on the wire",
                node.type_id
            );
            assert!(
                wire["icon"].is_string(),
                "{} icon must be present on the wire",
                node.type_id
            );
            assert_eq!(
                wire["color"], "#10b981",
                "{} color on the wire",
                node.type_id
            );

            let schema = match wire["config_schema"].as_object() {
                Some(schema) => schema,
                None => panic!("{} config_schema must be present on the wire", node.type_id),
            };
            assert_eq!(
                schema["type"], "object",
                "{} schema must be an object schema",
                node.type_id
            );

            let properties = match schema["properties"].as_object() {
                Some(properties) => properties,
                None => panic!("{} schema must declare a `properties` map", node.type_id),
            };
            for (name, property) in properties {
                assert!(
                    property.get("type").is_some(),
                    "{} schema property `{name}` must declare a type",
                    node.type_id
                );
            }

            // Every node except `mongo:connect` picks its connection via the
            // `connection_uuid` selector over `mongo:connect` nodes.
            if node.type_id != "mongo:connect" {
                let selector = properties
                    .get("connection_uuid")
                    .unwrap_or_else(|| {
                        panic!("{} schema must declare `connection_uuid`", node.type_id)
                    });
                assert_eq!(
                    selector.get("type").and_then(|t| t.as_str()),
                    Some("string"),
                    "{} connection_uuid must be a string",
                    node.type_id
                );
                assert_eq!(
                    selector["x-node-selector"]["node_type"],
                    "mongo:connect",
                    "{} connection_uuid must select mongo:connect nodes",
                    node.type_id
                );
            }

            // Complex JSON fields must render as JSON-text strings with
            // `format: json`, not object/array (which the host panel renders
            // badly).
            if let Some((_, complex)) = COMPLEX_FIELDS
                .iter()
                .find(|(id, _)| *id == node.type_id.as_str())
            {
                for &field in *complex {
                    assert_eq!(
                        properties[field].get("type").and_then(|t| t.as_str()),
                        Some("string"),
                        "{} schema field `{field}` must be a JSON-text string, not object/array",
                        node.type_id
                    );
                    assert_eq!(
                        properties[field].get("format").and_then(|f| f.as_str()),
                        Some("json"),
                        "{} schema field `{field}` must declare format `json`",
                        node.type_id
                    );
                }
            }
        }

        // Representative default_config values (README §4 + plan decisions D9).
        assert_eq!(
            by_type["mongo:connect"].default_config,
            serde_json::json!({
                "uri": "mongodb://localhost:27017",
                "database": "mydb",
                "timeout_ms": 5000,
            })
        );
        assert_eq!(
            by_type["mongo:find"].default_config,
            serde_json::json!({
                "collection": "users",
                "filter": null,
                "project": null,
                "limit": 100,
                "timeout_ms": 5000,
            })
        );
        assert_eq!(
            by_type["mongo:update"].default_config,
            serde_json::json!({
                "collection": "users",
                "filter": null,
                "update": null,
                "upsert": false,
                "update_many": false,
                "timeout_ms": 5000,
            })
        );
    }

    /// Success results of the operation nodes must route to the `true` port,
    /// while `mongo:close` keeps routing to its single `out` port.
    #[test]
    fn ok_results_route_to_true() {
        assert_eq!(
            ok_result(serde_json::json!({})).next_ports,
            vec!["true".to_string()],
            "operation nodes must route success to the true port"
        );
        assert_eq!(
            close_ok_result(serde_json::json!({})).next_ports,
            vec!["out".to_string()],
            "close must route success to the out port"
        );
    }

    /// The host broadcasts flowEnded after every flow execution — the plugin
    /// must release the execution's pooled connections (README §5 release
    /// path 1), including EVERY connection when several connect nodes
    /// registered under one execution.
    #[tokio::test]
    async fn flow_ended_releases_pool_entries() {
        let plugin = MongoPlugin::default();
        let uri = "mongodb://localhost:27017";
        let client_a = mongodb::Client::with_uri_str(uri)
            .await
            .expect("lazy client must build");
        let client_b = mongodb::Client::with_uri_str(uri)
            .await
            .expect("lazy client must build");
        plugin.pool.connect("exec-1", "conn-a", uri, "db", client_a);
        plugin.pool.connect("exec-1", "conn-b", uri, "db", client_b);
        assert!(
            plugin.pool.client("exec-1", "conn-a").is_some(),
            "seeded entry must exist"
        );
        assert!(
            plugin.pool.client("exec-1", "conn-b").is_some(),
            "seeded entry must exist"
        );

        plugin.flow_ended("exec-1");
        assert!(
            plugin.pool.client("exec-1", "conn-a").is_none(),
            "flow_ended must release the pooled connection"
        );
        assert!(
            plugin.pool.client("exec-1", "conn-b").is_none(),
            "flow_ended must release every pooled connection"
        );
    }
}
