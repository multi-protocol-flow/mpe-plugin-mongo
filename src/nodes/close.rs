//! `mongo:close` node — releases the execution's pooled connection(s).
//!
//! Config: `{}` — or an optional `connection_uuid` (the host node selector
//! over `mongo:connect` nodes) when the flow wants to tear down ONE of
//! several connections. With a selection, only that connection is released;
//! without one, every connection of the execution is released. Output: `{}`.
//!
//! Idempotent: releasing an execution that never connected (or was already
//! released) succeeds silently — [`MongoPool::release`] /
//! [`MongoPool::release_connection`] are no-ops on missing keys. This is
//! release path 2 of the README §5 double-release design (the `flow_ended`
//! hook is path 1); a flow may run both, in either order.

use crate::pool::MongoPool;
use mpe_plugin_sdk::prelude::*;

/// Thin `ExecuteContext` adapter: extracts `execution_id` (fallback `"default"`
/// for SDK tests that send none) and the resolved config, then delegates to
/// [`close_core`]. Logs the failure so the host surfaces it in the report.
pub async fn execute(ctx: &mut ExecuteContext, pool: &MongoPool) -> ExecuteResult {
    let exec_id = ctx.execution_id().unwrap_or("default");
    let config = ctx.config().clone();
    match close_core(pool, exec_id, &config).await {
        Ok(result) => result,
        Err(msg) => {
            ctx.log("error", &msg);
            ExecuteResult::fail(msg)
        }
    }
}

/// Core close logic, testable without an [`ExecuteContext`] (the SDK's
/// `ExecuteContext::from_params` is `pub(crate)`, so plugin tests cannot
/// construct one — see decisions.md).
///
/// Releases the execution's pooled connection(s). Idempotent: succeeds
/// whether or not the execution had a connection. With a config
/// `connection_uuid`, only that connection is released (multi-connect flows);
/// without one, every connection of the execution is released.
async fn close_core(
    pool: &MongoPool,
    exec_id: &str,
    config: &serde_json::Value,
) -> Result<ExecuteResult, String> {
    match crate::nodes::resolve_connection_uuid(config) {
        Ok(connection_uuid) => pool.release_connection(exec_id, &connection_uuid),
        Err(_) => pool.release(exec_id),
    }
    Ok(crate::close_ok_result(serde_json::json!({})))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A lazy client for seeding the pool in tests: `Client::with_uri_str` is
    /// async but lazy for plain `mongodb://` URIs — no sockets, no mongod.
    async fn make_client(uri: &str) -> mongodb::error::Result<mongodb::Client> {
        mongodb::Client::with_uri_str(uri).await
    }

    /// A connected execution is released: all pool entries are gone after
    /// close without a selection.
    #[tokio::test]
    async fn close_releases_pool_entry() {
        let pool = MongoPool::default();
        let uri = "mongodb://localhost:27017";
        pool.connect(
            "exec-1",
            "conn-a",
            uri,
            "db",
            make_client(uri).await.expect("lazy client must build"),
        );
        assert!(
            pool.client("exec-1", "conn-a").is_some(),
            "seeded entry must exist"
        );

        let result = close_core(&pool, "exec-1", &serde_json::json!({}))
            .await
            .expect("close must not error");
        assert!(result.success, "close must succeed");
        assert_eq!(
            result.output_data,
            Some(serde_json::json!({})),
            "close outputs an empty object"
        );
        assert!(
            pool.client("exec-1", "conn-a").is_none(),
            "close must release the pooled connection"
        );
    }

    /// With a `connection_uuid` selection, close releases ONLY that
    /// connection, leaving the others of the same execution alive.
    #[tokio::test]
    async fn close_releases_only_selected_connection() {
        let pool = MongoPool::default();
        let uri = "mongodb://localhost:27017";
        pool.connect(
            "exec-1",
            "conn-a",
            uri,
            "db",
            make_client(uri).await.expect("lazy client must build"),
        );
        pool.connect(
            "exec-1",
            "conn-b",
            uri,
            "db",
            make_client(uri).await.expect("lazy client must build"),
        );

        let result = close_core(&pool, "exec-1", &serde_json::json!({ "connection_uuid": "conn-a" }))
            .await
            .expect("close must not error");
        assert!(result.success, "close must succeed");
        assert!(
            pool.client("exec-1", "conn-a").is_none(),
            "selected connection must be released"
        );
        assert!(
            pool.client("exec-1", "conn-b").is_some(),
            "unselected connection must survive"
        );
    }

    /// Close on an execution that never connected (or was already released)
    /// is a silent success — release is a no-op on a missing key.
    #[tokio::test]
    async fn close_without_connect_is_idempotent() {
        let pool = MongoPool::default();
        for _ in 0..2 {
            let result = close_core(&pool, "exec-1", &serde_json::json!({}))
                .await
                .expect("close without a connection must not error");
            assert!(result.success, "idempotent release must succeed");
        }
    }

    /// Without a `connection_uuid`, close ignores the rest of the config:
    /// any other value is irrelevant and close still releases everything.
    #[tokio::test]
    async fn close_ignores_config() {
        let pool = MongoPool::default();
        let result = close_core(&pool, "exec-1", &serde_json::json!({ "anything": 1 }))
            .await
            .expect("config must be irrelevant");
        assert!(result.success, "close must succeed regardless of config");
    }
}
