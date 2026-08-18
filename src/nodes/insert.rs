//! `mongo:insert` node — inserts documents into a collection.
//!
//! Config: `{ collection, documents }` where `documents` is a JSON array (or
//! JSON-text string containing one) of plain JSON objects; required and
//! non-empty. Output: `{ "inserted_count": N }`.
//!
//! mongodb 3.8 API note (empirically verified): the 2.x signature
//! `insert_many(docs, options)` no longer exists. 3.8 returns an
//! `InsertMany` **action** from `insert_many(docs)` that is awaited directly,
//! and `InsertManyResult` carries `inserted_ids` (a `HashMap<usize, Bson>`),
//! NOT an `inserted_count` field — the count is `inserted_ids.len()`.

use mpe_plugin_sdk::prelude::*;
use serde_json::json;

use crate::{json::parse_json_array_field, pool::MongoPool};

/// Thin adapter over the host context: resolves the execution id, clones the
/// config, and turns `insert_core` failures into an error log + failed result.
///
/// Signature is fixed by the dispatch in `lib.rs` (`nodes::insert::execute`).
pub async fn execute(ctx: &mut ExecuteContext, pool: &MongoPool) -> ExecuteResult {
    let exec_id = ctx.execution_id().unwrap_or("default");
    let config = ctx.config().clone();
    match insert_core(pool, exec_id, &config).await {
        Ok(result) => result,
        Err(msg) => {
            ctx.log("error", &msg);
            ExecuteResult::fail(msg)
        }
    }
}

/// Testable core (no `ExecuteContext` — the plugin crate cannot construct
/// one): parses the config, resolves the pooled connection, and runs the
/// insert.
pub async fn insert_core(
    pool: &MongoPool,
    exec_id: &str,
    config: &serde_json::Value,
) -> Result<ExecuteResult, String> {
    let collection = config
        .get("collection")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| crate::i18n::t("缺少 collection 字段", "Missing collection field").to_string())?;

    // `documents` is required AND non-empty; checked before the connection
    // so an empty pool cannot mask the config error (and no DB call is
    // attempted either way).
    let docs = match parse_json_array_field(config, "documents")? {
        Some(docs) if !docs.is_empty() => docs,
        _ => return Err(crate::i18n::t("documents 不能为空", "documents must not be empty").to_string()),
    };

    let conn = pool
        .client(exec_id, &crate::nodes::resolve_connection_uuid(config)?)
        .ok_or_else(|| crate::i18n::t("请先 connect", "Please connect first").to_string())?;
    let coll = conn
        .client
        .database(&conn.database)
        .collection::<mongodb::bson::Document>(collection);

    let result = coll
        .insert_many(docs)
        .await
        .map_err(|err| {
            let msg = crate::i18n::t("插入文档失败", "Failed to insert documents");
            format!("{msg}: {err}")
        })?;
    Ok(crate::ok_result(json!({
        "inserted_count": result.inserted_ids.len()
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn insert_empty_documents_fails() {
        // Empty pool: the empty-documents check must fire BEFORE any
        // connection lookup, proving no DB call is attempted.
        let pool = MongoPool::default();
        let err = insert_core(
            &pool,
            "exec-1",
            &serde_json::json!({ "collection": "users", "documents": [] }),
        )
        .await
        .expect_err("empty documents must fail");
        assert!(
            err.contains("empty"),
            "error must mention emptiness, got: {err}"
        );
    }

    #[tokio::test]
    async fn insert_missing_collection_fails() {
        let pool = MongoPool::default();
        let err = insert_core(
            &pool,
            "exec-1",
            &serde_json::json!({ "documents": [{ "name": "Alice" }] }),
        )
        .await
        .expect_err("missing collection must fail");
        assert!(
            err.contains("collection"),
            "error must name the field, got: {err}"
        );
    }

    #[tokio::test]
    async fn insert_without_connect_fails() {
        let pool = MongoPool::default();
        let err = insert_core(
            &pool,
            "exec-1",
            &serde_json::json!({
                "collection": "users",
                "documents": [{ "name": "Alice", "age": 30 }],
            }),
        )
        .await
        .expect_err("missing connection must fail");
        assert!(
            err.contains("connect"),
            "error must mention connect, got: {err}"
        );
    }
}
