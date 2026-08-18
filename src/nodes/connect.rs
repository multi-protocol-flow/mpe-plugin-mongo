//! `mongo:connect` node — establishes a MongoDB connection with ping
//! verification and idempotent pool reuse.
//!
//! Config: `{ "uri", "database", "timeout_ms" }` — all optional, with
//! defaults (see [`DEFAULT_URI`] / [`DEFAULT_DATABASE`] / [`DEFAULT_TIMEOUT_MS`]).
//! Output: `{ "connected": true, "database": "<name>" }`.
//!
//! Idempotency: a second connect for the same `execution_id` with the SAME
//! uri + database reuses the pooled connection — no client creation, no ping.
//! A DIFFERENT uri/database replaces the stale entry (surface via
//! `ctx.log("warn", …)`).

use std::time::Duration;

use crate::pool::MongoPool;
use mongodb::bson::doc;
use mongodb::options::ClientOptions;
use mongodb::Client;
use mpe_plugin_sdk::prelude::*;

/// Default connection string when `uri` is missing.
const DEFAULT_URI: &str = "mongodb://localhost:27017";
/// Default database when `database` is missing.
const DEFAULT_DATABASE: &str = "test";
/// Default connect + server-selection timeout in milliseconds.
const DEFAULT_TIMEOUT_MS: u64 = 5000;

/// Thin `ExecuteContext` adapter: extracts `execution_id` (fallback `"default"`
/// for SDK tests that send none), the node instance id as the connection
/// selector (`connection_uuid` — the host sends `node_instance_id` = the
/// connect node's own uuid, so operation nodes can pick THIS connection via
/// their `connection_uuid` config), and the resolved config, then delegates
/// to [`connect_core`]. Logs the failure so the host surfaces it in the
/// report.
pub async fn execute(ctx: &mut ExecuteContext, pool: &MongoPool) -> ExecuteResult {
    let exec_id = ctx.execution_id().unwrap_or("default").to_string();
    let connection_uuid = ctx.node_instance_id().unwrap_or("default").to_string();
    let config = ctx.config().clone();
    match connect_core(pool, &exec_id, &connection_uuid, &config).await {
        Ok((result, replaced)) => {
            if replaced {
                ctx.log("warn", crate::i18n::t("连接目标变化，已替换旧连接", "Connection target changed; old connection replaced"));
            }
            result
        }
        Err(msg) => {
            ctx.log("error", &msg);
            ExecuteResult::fail(msg)
        }
    }
}

/// Core connect logic, testable without an [`ExecuteContext`] (the SDK's
/// `ExecuteContext::from_params` is `pub(crate)`, so plugin tests cannot
/// construct one — see decisions.md).
///
/// Returns `(result, replaced)`; `replaced` is `true` only when an existing
/// connection with a DIFFERENT uri/database was swapped out.
async fn connect_core(
    pool: &MongoPool,
    exec_id: &str,
    connection_uuid: &str,
    config: &serde_json::Value,
) -> Result<(ExecuteResult, bool), String> {
    let uri = config
        .get("uri")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(DEFAULT_URI)
        .to_string();
    let database = config
        .get("database")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(DEFAULT_DATABASE)
        .to_string();
    let timeout_ms = config
        .get("timeout_ms")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(DEFAULT_TIMEOUT_MS);

    // Idempotent reuse: same execution + same connection + same uri/database
    // → return the cached connection WITHOUT creating a client or pinging
    // (the host may re-run a connect node mid-flow; the pool entry already
    // proved reachable).
    if let Some(existing) = pool.client(exec_id, connection_uuid) {
        if existing.uri == uri && existing.database == database {
            return Ok((
                crate::ok_result(serde_json::json!({ "connected": true, "database": database })),
                false,
            ));
        }
    }

    // Client creation lives HERE (async, on the tokio runtime), not inside the
    // pool: the SDK `ConnectionPool::get_or_insert` factory is a sync `FnOnce`
    // (decision D5). `ClientOptions::parse` is async in the 3.x driver (SRV
    // resolution), but for plain `mongodb://` URIs it is lazy — sockets open
    // on the first operation, so a parse error here never touches the network.
    let mut options = ClientOptions::parse(&uri)
        .await
        .map_err(|err| {
            let msg = crate::i18n::t("无法解析 Mongo URI", "Failed to parse Mongo URI");
            format!("{msg} `{uri}`: {err}")
        })?;
    options.connect_timeout = Some(Duration::from_millis(timeout_ms));
    options.server_selection_timeout = Some(Duration::from_millis(timeout_ms));

    let client = Client::with_options(options)
        .map_err(|err| {
            let msg = crate::i18n::t("创建 Mongo 客户端失败", "Failed to create Mongo client");
            format!("{msg} (`{uri}`): {err}")
        })?;

    // Ping verifies the server is actually reachable BEFORE registering the
    // connection — the driver is lazy, so without this a dead URI would only
    // fail on the FIRST operation node, far from the connect node.
    client
        .database("admin")
        .run_command(doc! { "ping": 1 })
        .await
        .map_err(|err| {
            let msg = crate::i18n::t("Mongo ping 失败（无法连接", "Mongo ping failed (cannot connect to");
            format!("{msg} `{uri}`): {err}")
        })?;

    // Concurrency (plan M8): under pipelined concurrent connects the SDK pool
    // may run more than one insert factory and drop one `Arc` — benign, the
    // dropped client is just an extra lazy handle with no open sockets; no
    // locking needed.
    let (_, replaced) = pool.connect(exec_id, connection_uuid, &uri, &database, client);
    Ok((
        crate::ok_result(serde_json::json!({ "connected": true, "database": database })),
        replaced,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A lazy client for seeding the pool in tests: `Client::with_uri_str` is
    /// async but lazy for plain `mongodb://` URIs — no sockets, no mongod.
    async fn make_client(uri: &str) -> mongodb::error::Result<Client> {
        Client::with_uri_str(uri).await
    }

    /// An unparseable URI must fail BEFORE any network access, with a readable
    /// error naming the cause (the message contains both "URI" and "parse").
    #[tokio::test]
    async fn connect_rejects_bad_uri() {
        let pool = MongoPool::default();
        let err = connect_core(&pool, "exec-1", "conn-1", &serde_json::json!({ "uri": "://bad" }))
            .await
            .expect_err("an unparseable URI must error");
        assert!(
            err.contains("URI") && err.contains("parse"),
            "error must be readable and name the URI, got: {err}"
        );
        assert!(
            pool.client("exec-1", "conn-1").is_none(),
            "a failed connect must not register a connection"
        );
    }

    /// Missing `uri` / `database` / `timeout_ms` fall back to the defaults —
    /// and the default target (already in the pool) takes the IDEMPOTENT REUSE
    /// path: no client creation, no ping. mongod is not running in the test
    /// environment, so a ping would fail with connection-refused → this test
    /// would error instead of returning Ok, proving the reuse path skips it.
    #[tokio::test]
    async fn defaults_reuse_existing_connection() {
        let pool = MongoPool::default();
        let client = make_client(DEFAULT_URI)
            .await
            .expect("lazy client must build");
        pool.connect("exec-1", "conn-1", DEFAULT_URI, DEFAULT_DATABASE, client);

        let (result, replaced) = connect_core(&pool, "exec-1", "conn-1", &serde_json::json!({}))
            .await
            .expect("defaults must not panic and must succeed via reuse");
        assert!(!replaced, "reuse must not report replacement");
        assert!(result.success, "reuse must succeed");
        assert_eq!(
            result.output_data,
            Some(serde_json::json!({ "connected": true, "database": DEFAULT_DATABASE })),
            "output must report the DEFAULT database"
        );
    }
}
