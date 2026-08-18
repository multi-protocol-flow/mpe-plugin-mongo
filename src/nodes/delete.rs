//! `mongo:delete` node — deletes documents matching a filter.
//!
//! Config (`README §4.3`): `collection` (required), `filter` (required, JSON
//! text or object), `delete_many` (default `false` — 是否删除所有匹配).
//! Output: `{ "deleted_count": N }`.
//!
//! mongodb 3.8 API note: `Collection::delete_one` / `delete_many` are no
//! longer `async fn` with an options argument — they return an `Action`
//! builder (`Delete`) that implements `IntoFuture`, so the bare `.await`
//! (no second argument) is the driver's current shape.

use mongodb::bson::Document;
use serde_json::{json, Value};

use crate::json::parse_json_field;
use crate::pool::MongoPool;
use mpe_plugin_sdk::prelude::*;

/// Adapts the `ExecuteContext` to the testable [`delete_core`].
pub async fn execute(ctx: &mut ExecuteContext, pool: &MongoPool) -> ExecuteResult {
    let exec_id = ctx.execution_id().unwrap_or("default");
    let config = ctx.config().clone();
    match delete_core(pool, exec_id, &config).await {
        Ok(result) => result,
        Err(message) => {
            ctx.log("error", message.clone());
            ExecuteResult::fail(message)
        }
    }
}

/// Core delete logic: parse the config, take the execution's pooled
/// connection, run delete_one/delete_many, and report the deleted count.
///
/// Connection lookup happens AFTER parsing so config errors surface before
/// any pool state is needed (unit tests run on an empty pool).
async fn delete_core(
    pool: &MongoPool,
    exec_id: &str,
    config: &Value,
) -> Result<ExecuteResult, String> {
    let collection = config
        .get("collection")
        .and_then(Value::as_str)
        .ok_or_else(|| crate::i18n::t("缺少 collection 字段", "Missing collection field").to_string())?;
    let filter = parse_json_field(config, "filter")?
        .ok_or_else(|| crate::i18n::t("缺少 filter 字段", "Missing filter field").to_string())?;
    let delete_many = parse_delete_many(config);

    let conn = pool
        .client(exec_id, &crate::nodes::resolve_connection_uuid(config)?)
        .ok_or_else(|| crate::i18n::t("请先 connect", "Please connect first").to_string())?;
    let coll = conn
        .client
        .database(&conn.database)
        .collection::<Document>(collection);

    let result = if delete_many {
        coll.delete_many(filter)
            .await
            .map_err(|err| {
                let msg = crate::i18n::t("删除失败", "Delete failed");
                format!("{msg}: {err}")
            })?
    } else {
        coll.delete_one(filter)
            .await
            .map_err(|err| {
                let msg = crate::i18n::t("删除失败", "Delete failed");
                format!("{msg}: {err}")
            })?
    };
    Ok(crate::ok_result(
        json!({ "deleted_count": result.deleted_count }),
    ))
}

/// `delete_many` flag: default `false` (README §4.3 semantics — 是否删除所有
/// 匹配文档).
fn parse_delete_many(config: &Value) -> bool {
    config
        .get("delete_many")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> Value {
        json!({
            "collection": "users",
            "filter": r#"{"name": "Alice"}"#,
        })
    }

    /// `delete_many` must honor the config flag with `false` as the default.
    #[test]
    fn delete_parses_many_flag() {
        assert!(parse_delete_many(&json!({ "delete_many": true })));
        assert!(!parse_delete_many(&json!({ "delete_many": false })));
        assert!(!parse_delete_many(&json!({})));
        // Non-bool values fall back to the default too.
        assert!(!parse_delete_many(&json!({ "delete_many": "yes" })));
    }

    /// A valid config on an empty pool must fail with the connect hint.
    #[tokio::test]
    async fn delete_without_connect_fails() {
        let pool = MongoPool::default();
        let err = delete_core(&pool, "exec-1", &valid_config())
            .await
            .expect_err("delete without connect must fail");
        assert!(err.contains("connect"), "got: {err}");
    }

    /// The filter is required: missing it must fail before any DB call.
    #[tokio::test]
    async fn delete_missing_filter_fails() {
        let pool = MongoPool::default();
        let err = delete_core(&pool, "exec-1", &json!({ "collection": "users" }))
            .await
            .expect_err("delete without filter must fail");
        assert!(err.contains("filter"), "got: {err}");
    }
}
