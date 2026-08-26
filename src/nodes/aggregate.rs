//! `mongo:aggregate` node — runs an aggregation pipeline with a bounded
//! result set.
//!
//! Config (`README §4.3` + plan decision D9): `collection` (required),
//! `pipeline` (required, non-empty JSON array — or JSON text of one),
//! `limit` (default 100; non-positive values treated as 100, same rule as
//! `mongo:find` — the README's original aggregate config has no limit, this
//! is the added guard). Output: `{ "documents": [...] }`.
//!
//! Bounded collection is mandatory (plan: aggregate 无界收集 forbidden — the
//! host wraps execute in a 30s timeout and caps wire frames at 2 MiB):
//! iteration stops after `limit` documents and the serialized output is
//! size-guarded at [`MAX_RESULT_BYTES`].
//!
//! mongodb 3.8 API note: `Collection::aggregate` returns an `Action` builder
//! (`Aggregate`, `IntoFuture`) — the bare `.await` yields
//! `Result<Cursor<Document>>`. Cursor iteration uses the driver's inherent
//! `advance()` / `deserialize_current()` pair, so NO `futures` dependency is
//! needed (the `TryStreamExt` shown in driver docs is for its dev-tests).

use mongodb::bson::Document;
use serde_json::{json, Value};

use crate::json::{docs_to_json_array, parse_json_array_field};
use crate::pool::MongoPool;
use mpe_plugin_sdk::prelude::*;

/// Output ceiling for serialized result documents (same guard as
/// `mongo:find`): beyond this the pipeline must be bounded via `limit`.
const MAX_RESULT_BYTES: usize = 1_500_000;

/// Adapts the `ExecuteContext` to the testable [`aggregate_core`].
pub async fn execute(ctx: &mut ExecuteContext, pool: &MongoPool) -> ExecuteResult {
    let exec_id = ctx.execution_id().unwrap_or("default");
    let config = ctx.config().clone();
    let timeout_ms = config
        .get("timeout_ms")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(5000);
    let timeout_dur = std::time::Duration::from_millis(timeout_ms.max(100));

    match tokio::time::timeout(timeout_dur, aggregate_core(pool, exec_id, &config)).await {
        Ok(Ok(result)) => result,
        Ok(Err(message)) => {
            ctx.log("error", message.clone());
            ExecuteResult::fail(message)
        }
        Err(_) => {
            let msg = format!("{}: {}ms", crate::i18n::t("Mongo 操作超时", "Mongo operation timed out"), timeout_ms);
            ctx.log("error", msg.clone());
            ExecuteResult::fail(msg)
        }
    }
}

/// Core aggregate logic: parse the config, take the execution's pooled
/// connection, run the pipeline, and collect AT MOST `limit` documents.
async fn aggregate_core(
    pool: &MongoPool,
    exec_id: &str,
    config: &Value,
) -> Result<ExecuteResult, String> {
    let collection = config
        .get("collection")
        .and_then(Value::as_str)
        .ok_or_else(|| crate::i18n::t("缺少 collection 字段", "Missing collection field").to_string())?;
    let pipeline = parse_pipeline(config)?;
    let limit = parse_limit(config);

    let conn = pool
        .client(exec_id, &crate::nodes::resolve_connection_uuid(config)?)
        .ok_or_else(|| crate::i18n::t("请先 connect", "Please connect first").to_string())?;
    let coll = conn
        .client
        .database(&conn.database)
        .collection::<Document>(collection);

    let mut cursor = coll
        .aggregate(pipeline)
        .await
        .map_err(|err| {
            let msg = crate::i18n::t("聚合失败", "Aggregation failed");
            format!("{msg}: {err}")
        })?;
    let mut docs: Vec<Document> = Vec::new();
    while cursor
        .advance()
        .await
        .map_err(|err| {
            let msg = crate::i18n::t("聚合读取失败", "Failed to read aggregation results");
            format!("{msg}: {err}")
        })?
    {
        let doc = cursor
            .deserialize_current()
            .map_err(|err| {
                let msg = crate::i18n::t("聚合结果反序列化失败", "Failed to deserialize aggregation result");
                format!("{msg}: {err}")
            })?;
        docs.push(doc);
        if docs.len() >= limit as usize {
            break;
        }
    }

    let documents = docs_to_json_array(&docs)?;
    let size = serde_json::to_vec(&documents)
        .map_err(|err| {
            let msg = crate::i18n::t("结果序列化失败", "Failed to serialize results");
            format!("{msg}: {err}")
        })?
        .len();
    if size > MAX_RESULT_BYTES {
        return Err(crate::i18n::t("结果过大，请减小 limit", "Result too large; reduce the limit").to_string());
    }
    Ok(crate::ok_result(json!({ "documents": documents })))
}

/// Aggregation pipeline: required and non-empty. `None` (missing / null) or
/// an empty array both fail with the same message.
fn parse_pipeline(config: &Value) -> Result<Vec<Document>, String> {
    match parse_json_array_field(config, "pipeline")? {
        Some(pipeline) if !pipeline.is_empty() => Ok(pipeline),
        _ => Err(crate::i18n::t("pipeline 不能为空", "pipeline must not be empty").to_string()),
    }
}

/// `limit` (D9 guard): default 100; missing, non-integer, or non-positive
/// values are treated as 100 (same rule as `mongo:find`).
fn parse_limit(config: &Value) -> i64 {
    let limit = config.get("limit").and_then(Value::as_i64).unwrap_or(100);
    if limit <= 0 {
        100
    } else {
        limit
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::{doc, Bson};

    fn valid_config() -> Value {
        json!({
            "collection": "users",
            "pipeline": r#"[{"$match": {"age": {"$gt": 18}}}]"#,
        })
    }

    /// A valid config on an empty pool must fail with the connect hint.
    #[tokio::test]
    async fn aggregate_without_connect_fails() {
        let pool = MongoPool::default();
        let err = aggregate_core(&pool, "exec-1", &valid_config())
            .await
            .expect_err("aggregate without connect must fail");
        assert!(err.contains("connect"), "got: {err}");
    }

    /// The pipeline is required AND non-empty: missing, empty array, and
    /// empty JSON-text array all fail before any DB call.
    #[tokio::test]
    async fn aggregate_empty_pipeline_fails() {
        let pool = MongoPool::default();
        for config in [
            json!({ "collection": "users" }),
            json!({ "collection": "users", "pipeline": [] }),
            json!({ "collection": "users", "pipeline": "[]" }),
        ] {
            let err = aggregate_core(&pool, "exec-1", &config)
                .await
                .expect_err("empty pipeline must fail");
            assert!(err.contains("pipeline"), "got: {err}");
        }
    }

    /// The pipeline JSON-text string form must parse to the same documents
    /// as the object form (both parsed from JSON text — the production wire
    /// shape; see the Int32/Int64 width note in json.rs).
    #[test]
    fn aggregate_pipeline_parses() {
        let as_string = parse_pipeline(&json!({
            "pipeline": r#"[{"$match": {"age": {"$gt": 18}}}]"#,
        }))
        .expect("string form must parse");
        let as_object = parse_pipeline(
            &serde_json::from_str::<Value>(r#"{ "pipeline": [{"$match": {"age": {"$gt": 18}}}] }"#)
                .expect("config JSON must parse"),
        )
        .expect("object form must parse");
        assert_eq!(as_string, as_object);
        assert_eq!(
            as_object,
            vec![doc! { "$match": { "age": { "$gt": Bson::Int64(18) } } }]
        );
    }

    /// `limit` normalization: 0 and negatives become 100, explicit positive
    /// values survive, missing defaults to 100.
    #[test]
    fn aggregate_parses_limit() {
        assert_eq!(parse_limit(&json!({ "limit": 0 })), 100);
        assert_eq!(parse_limit(&json!({ "limit": -5 })), 100);
        assert_eq!(parse_limit(&json!({ "limit": 50 })), 50);
        assert_eq!(parse_limit(&json!({})), 100);
        // Non-integer values fall back to the default too.
        assert_eq!(parse_limit(&json!({ "limit": "many" })), 100);
    }
}
