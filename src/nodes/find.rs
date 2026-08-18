//! `mongo:find` node — 按 filter / projection / limit 查询集合。
//!
//! 输入（host resolve 后的 config）：`collection`（必填）、`filter`
//! （JSON 文本/对象，默认空 `{}`）、`project`（JSON 文本/对象，null 不投影）、
//! `limit`（默认 100；显式 `<= 0` 也按 100 处理——MongoDB `limit(0)` 表示
//! "不限"，必须避开，否则违反"禁止无界取全表"约束）。
//!
//! 输出：`{ "count": N, "documents": [...] }`，`count` 是实际返回条数（≤ limit），
//! 不是总匹配数。结果经 [`MAX_OUTPUT_BYTES`] 字节数守卫后进 `output_data`，
//! 不塞 `report_data`（宿主 wire 上限 1 MiB，约束 #4）。

use futures::TryStreamExt;
use mongodb::bson::Document;
use mpe_plugin_sdk::prelude::*;

use crate::pool::MongoPool;

/// 单帧上限 2 MiB（约束 #7）下的安全余量：序列化后的 documents 超过
/// 1.5 MiB 即报错要求减小 limit。
const MAX_OUTPUT_BYTES: usize = 1_500_000;

/// limit 兜底值：缺失、非法（`<= 0`，含 MongoDB 的"不限"语义）都归一为它。
const DEFAULT_LIMIT: i64 = 100;

pub async fn execute(ctx: &mut ExecuteContext, pool: &MongoPool) -> ExecuteResult {
    let exec_id = ctx.execution_id().unwrap_or("default").to_string();
    let config = ctx.config().clone();
    match find_core(pool, &exec_id, &config).await {
        Ok(result) => result,
        Err(msg) => {
            ctx.log("error", &msg);
            ExecuteResult::fail(msg)
        }
    }
}

/// 可测核心（测试无法构造 `ExecuteContext`，故与 `execute` 分离）：
/// 连接检查发生在任何查询之前——未 connect 的池直接短路失败，单测无需 mongod。
async fn find_core(
    pool: &MongoPool,
    exec_id: &str,
    config: &serde_json::Value,
) -> Result<ExecuteResult, String> {
    let collection = config
        .get("collection")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| crate::i18n::t("缺少 collection 字段", "Missing collection field").to_string())?;
    let filter = parse_filter(config)?;
    let project = crate::json::parse_json_field(config, "project")?;
    let limit = parse_limit(config);

    let conn = pool
        .client(exec_id, &crate::nodes::resolve_connection_uuid(config)?)
        .ok_or_else(|| crate::i18n::t("请先 connect", "Please connect first").to_string())?;
    let coll = conn
        .client
        .database(&conn.database)
        .collection::<Document>(collection);

    let mut find_action = coll.find(filter).limit(limit);
    if let Some(project) = project {
        find_action = find_action.projection(project);
    }
    let mut cursor = find_action
        .await
        .map_err(|err| {
            let msg = crate::i18n::t("查询失败", "Query failed");
            format!("{msg}: {err}")
        })?;

    // `limit` 已由 FindOptions 下发给服务端；这里再显式兜底 break，
    // 双保险确保绝无无界取全表（约束 #5）。
    let mut docs: Vec<Document> = Vec::new();
    while let Some(doc) = cursor
        .try_next()
        .await
        .map_err(|err| {
            let msg = crate::i18n::t("读取结果失败", "Failed to read results");
            format!("{msg}: {err}")
        })?
    {
        docs.push(doc);
        if docs.len() >= limit as usize {
            break;
        }
    }

    let documents = crate::json::docs_to_json_array(&docs)?;
    let serialized = serde_json::to_string(&documents).map_err(|err| {
        let msg = crate::i18n::t("结果序列化失败", "Failed to serialize results");
        format!("{msg}: {err}")
    })?;
    if serialized.len() > MAX_OUTPUT_BYTES {
        return Err(crate::i18n::t("结果过大，请减小 limit", "Result too large; reduce the limit").to_string());
    }

    Ok(crate::ok_result(serde_json::json!({
        "count": docs.len(),
        "documents": documents,
    })))
}

/// limit 解析：缺失 → 100；显式 `<= 0` → 100（MongoDB `limit(0)` = 不限，必须归一）。
fn parse_limit(config: &serde_json::Value) -> i64 {
    config
        .get("limit")
        .and_then(serde_json::Value::as_i64)
        .map(|n| if n <= 0 { DEFAULT_LIMIT } else { n })
        .unwrap_or(DEFAULT_LIMIT)
}

/// filter 解析：缺失 / null → 空 `{}`（查全集合），其余语义交给
/// `crate::json::parse_json_field`（JSON 文本 / 对象双形态）。
fn parse_filter(config: &serde_json::Value) -> Result<Document, String> {
    Ok(crate::json::parse_json_field(config, "filter")?.unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::{doc, Bson};

    /// 未 connect 的池必须在任何查询前失败，且消息包含 "connect"。
    /// 不需要 mongod：空池在连接检查处短路。
    #[tokio::test]
    async fn find_without_connect_fails() {
        let pool = MongoPool::default();
        let err = find_core(
            &pool,
            "exec-1",
            &serde_json::json!({ "collection": "users" }),
        )
        .await
        .expect_err("must fail without a connection");
        assert!(
            err.contains("connect"),
            "message must mention connect, got: {err}"
        );
    }

    /// limit 归一：缺失 / 0 / 负数 → 100；正数原样保留。
    #[test]
    fn find_limit_parsing() {
        for config in [
            serde_json::json!({}),
            serde_json::json!({ "limit": 0 }),
            serde_json::json!({ "limit": -5 }),
        ] {
            assert_eq!(parse_limit(&config), DEFAULT_LIMIT, "config: {config}");
        }
        assert_eq!(parse_limit(&serde_json::json!({ "limit": 50 })), 50);
    }

    /// filter 的 JSON 文本形态与对象形态解析结果一致（与 json.rs 相同的
    /// 宽度约定：对象形态也从 JSON 文本解析，保证 Int64 宽度一致）。
    #[test]
    fn find_filter_string_parses() {
        let as_string = parse_filter(&serde_json::json!({ "filter": r#"{"age": {"$gt": 18}}"# }))
            .expect("string form must parse");
        let as_object = parse_filter(
            &serde_json::from_str::<serde_json::Value>(r#"{ "filter": {"age": {"$gt": 18}} }"#)
                .expect("config JSON must parse"),
        )
        .expect("object form must parse");
        assert_eq!(as_string, as_object);
        assert_eq!(as_string, doc! { "age": { "$gt": Bson::Int64(18) } });
        // 缺失 / null → 空 filter（查全集合）。
        assert_eq!(
            parse_filter(&serde_json::json!({ "collection": "users" })).expect("no error"),
            doc! {}
        );
        assert_eq!(
            parse_filter(&serde_json::json!({ "filter": null })).expect("no error"),
            doc! {}
        );
    }

    /// 缺少必填 `collection` → 可读错误，命名该字段。
    #[tokio::test]
    async fn find_missing_collection_errors() {
        let err = find_core(&MongoPool::default(), "exec-1", &serde_json::json!({}))
            .await
            .expect_err("missing collection must error");
        assert!(
            err.contains("collection"),
            "error must name the collection field, got: {err}"
        );
    }
}
