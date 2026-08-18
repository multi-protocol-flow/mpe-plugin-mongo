//! `mongo:update` node — updates documents matching a filter.
//!
//! Config: `{ collection, filter, update, upsert?, update_many? }`. `filter`
//! and `update` are required (plain JSON objects or JSON-text strings);
//! `upsert` and `update_many` default to `false` — the latter selects
//! `update_many` over `update_one`. The `update` document is passed through
//! as-is: update-operator semantics (e.g. `$set`) are the user's
//! responsibility, this node never treats update as replace. Output:
//! `{ "matched_count": N, "modified_count": N }`.
//!
//! mongodb 3.8 API note (empirically verified): the 2.x signature
//! `update_one(filter, update, options)` no longer exists. 3.8 returns an
//! `Update` **action** from `update_one(query, update)` / `update_many(...)`,
//! with per-option setters (`.upsert(bool)`); the legacy
//! `UpdateOptions::builder()` path is replaced by those setters.

use mpe_plugin_sdk::prelude::*;
use serde_json::json;

use crate::{json::parse_json_field, pool::MongoPool};

/// Thin adapter over the host context: resolves the execution id, clones the
/// config, and turns `update_core` failures into an error log + failed
/// result.
///
/// Signature is fixed by the dispatch in `lib.rs` (`nodes::update::execute`).
pub async fn execute(ctx: &mut ExecuteContext, pool: &MongoPool) -> ExecuteResult {
    let exec_id = ctx.execution_id().unwrap_or("default");
    let config = ctx.config().clone();
    match update_core(pool, exec_id, &config).await {
        Ok(result) => result,
        Err(msg) => {
            ctx.log("error", &msg);
            ExecuteResult::fail(msg)
        }
    }
}

/// Reads the `update_many` flag: `true` → `update_many`, anything else
/// (missing, `false`, non-bool) → `update_one`.
pub fn parse_update_many(config: &serde_json::Value) -> bool {
    config
        .get("update_many")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

/// Testable core (no `ExecuteContext` — the plugin crate cannot construct
/// one): parses the config, resolves the pooled connection, and runs the
/// update.
pub async fn update_core(
    pool: &MongoPool,
    exec_id: &str,
    config: &serde_json::Value,
) -> Result<ExecuteResult, String> {
    let collection = config
        .get("collection")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| crate::i18n::t("缺少 collection 字段", "Missing collection field").to_string())?;

    // Both `filter` and `update` are required; parsing happens BEFORE the
    // connection check so config errors surface even without a connection.
    let filter = parse_json_field(config, "filter")?
        .ok_or_else(|| crate::i18n::t("缺少 filter 字段", "Missing filter field").to_string())?;
    let update = parse_json_field(config, "update")?
        .ok_or_else(|| crate::i18n::t("缺少 update 字段", "Missing update field").to_string())?;

    let upsert = config
        .get("upsert")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let update_many = parse_update_many(config);

    let conn = pool
        .client(exec_id, &crate::nodes::resolve_connection_uuid(config)?)
        .ok_or_else(|| crate::i18n::t("请先 connect", "Please connect first").to_string())?;
    let coll = conn
        .client
        .database(&conn.database)
        .collection::<mongodb::bson::Document>(collection);

    let result = if update_many {
        coll.update_many(filter, update).upsert(upsert).await
    } else {
        coll.update_one(filter, update).upsert(upsert).await
    }
    .map_err(|err| {
        let msg = crate::i18n::t("更新文档失败", "Failed to update documents");
        format!("{msg}: {err}")
    })?;

    Ok(crate::ok_result(json!({
        "matched_count": result.matched_count,
        "modified_count": result.modified_count,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn update_without_connect_fails() {
        // Valid config on an empty pool: parsing passes, the connection check
        // fires, and no DB call is attempted.
        let pool = MongoPool::default();
        let err = update_core(
            &pool,
            "exec-1",
            &serde_json::json!({
                "collection": "users",
                "filter": { "name": "Alice" },
                "update": { "$set": { "age": 31 } },
            }),
        )
        .await
        .expect_err("missing connection must fail");
        assert!(
            err.contains("connect"),
            "error must mention connect, got: {err}"
        );
    }

    #[tokio::test]
    async fn update_missing_filter_fails() {
        let pool = MongoPool::default();
        let err = update_core(
            &pool,
            "exec-1",
            &serde_json::json!({
                "collection": "users",
                "update": { "$set": { "age": 31 } },
            }),
        )
        .await
        .expect_err("missing filter must fail");
        assert!(
            err.contains("filter"),
            "error must name the field, got: {err}"
        );
    }

    #[tokio::test]
    async fn update_missing_update_fails() {
        let pool = MongoPool::default();
        let err = update_core(
            &pool,
            "exec-1",
            &serde_json::json!({
                "collection": "users",
                "filter": { "name": "Alice" },
            }),
        )
        .await
        .expect_err("missing update must fail");
        assert!(
            err.contains("update"),
            "error must name the field, got: {err}"
        );
    }

    /// The `update_many` flag decides one-vs-many; absent and non-bool
    /// values must not panic and default to `false` (update_one).
    #[test]
    fn update_parses_many_flag() {
        assert!(parse_update_many(
            &serde_json::json!({ "update_many": true })
        ));
        assert!(!parse_update_many(
            &serde_json::json!({ "update_many": false })
        ));
        assert!(!parse_update_many(&serde_json::json!({})));
        assert!(!parse_update_many(
            &serde_json::json!({ "update_many": "yes" })
        ));
    }
}
