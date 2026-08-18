//! Per-execution MongoDB connection pool.
//!
//! Keys ready-to-use connections by the composite `(execution_id,
//! connection_uuid)` pair so that:
//!
//! - concurrent flow executions never share a [`mongodb::Client`] (each client
//!   owns its own driver-level connection pool; sharing one across executions
//!   would interleave unrelated queries and let one execution's `close` tear
//!   down another's connection), and
//! - multiple `mongo:connect` nodes can coexist inside ONE execution —
//!   operation nodes pick their connection via the `connection_uuid` the host
//!   resolves into their config (the connect node's own instance id).
//!
//! The SDK's [`ConnectionPool`] stores values under opaque `String` keys and
//! exposes no key enumeration, so this crate keeps a small side registry
//! (`execution_id → set of connection_uuids`) to support releasing every
//! connection of one execution by prefix (`MongoPool::release`).
//!
//! Design note (deviation from README §5, per plan decision D5): the SDK's
//! [`ConnectionPool::get_or_insert`] factory is a SYNC `FnOnce`, but
//! `mongodb::Client::with_uri_str` is async in the 3.x driver — so client
//! creation cannot happen inside this pool. It happens in the `mongo:connect`
//! node (which runs on the tokio runtime); the pool only receives a READY
//! client and stores it under the composite key.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

/// A ready-to-use MongoDB connection bound to one flow execution.
///
/// `uri` is stored alongside the client (the plan sketch said `{client,
/// database}`) so [`MongoPool::connect`] can tell when a later connect node
/// targets a different server and replace the entry instead of silently
/// reusing a stale connection — required by the "不同 uri 替换" acceptance
/// case.
pub struct MongoConnection {
    /// The connected driver client; the connect node created it and the
    /// operation nodes use it for queries.
    pub client: mongodb::Client,
    /// Connection string this client was created from (idempotency key).
    pub uri: String,
    /// Database name this connection is scoped to (idempotency key).
    pub database: String,
}

/// Execution + connection-keyed pool of [`MongoConnection`]s, shared across
/// executions in this plugin process (the plugin is resident:
/// `capabilities.streaming: true`, so the pool lives for the process
/// lifetime).
///
/// Concurrency: the underlying [`ConnectionPool`] may run an insert factory
/// more than once under contention; that is benign here because `connect`
/// only inserts after checking the existing entry. The side registry uses a
/// plain [`Mutex`] and is only ever touched with short critical sections.
#[derive(Default)]
pub struct MongoPool {
    inner: mpe_plugin_sdk::pool::ConnectionPool,
    /// `execution_id → set of registered connection_uuids`, so
    /// [`MongoPool::release`] can drop every connection of one execution even
    /// though the SDK pool exposes no key enumeration.
    connections: Mutex<HashMap<String, HashSet<String>>>,
}

impl MongoPool {
    /// Registers the ready `client` for `(execution_id, connection_uuid)`,
    /// creating a [`MongoConnection`] entry if none exists.
    ///
    /// Idempotent: a second connect with the same `uri` + `database` reuses
    /// the existing connection (no new client is created).
    ///
    /// Returns `(connection, replaced)` where `replaced` is `true` only when
    /// an entry with a DIFFERENT uri or database existed and was replaced —
    /// the caller (connect node) can surface a warning via `ctx.log`. A fresh
    /// insertion reports `false`.
    pub fn connect(
        &self,
        execution_id: &str,
        connection_uuid: &str,
        uri: &str,
        database: &str,
        client: mongodb::Client,
    ) -> (Arc<MongoConnection>, bool) {
        let replaced = match self.client(execution_id, connection_uuid) {
            Some(existing) if existing.uri == uri && existing.database == database => {
                // Same target: reuse, do not touch the stored client.
                return (existing, false);
            }
            Some(_) => {
                // Different target: drop the stale connection first so the
                // new one is the only entry under this key.
                self.inner.remove(&pool_key(execution_id, connection_uuid));
                true
            }
            None => false,
        };
        let created = self
            .inner
            .get_or_insert(pool_key(execution_id, connection_uuid), || MongoConnection {
                client,
                uri: uri.to_string(),
                database: database.to_string(),
            });
        self.connections
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entry(execution_id.to_string())
            .or_default()
            .insert(connection_uuid.to_string());
        (created, replaced)
    }

    /// Returns the connection for `(execution_id, connection_uuid)`, or
    /// `None` when that connection has not been established (or already
    /// released).
    pub fn client(&self, execution_id: &str, connection_uuid: &str) -> Option<Arc<MongoConnection>> {
        self.inner
            .get::<MongoConnection>(&pool_key(execution_id, connection_uuid))
    }

    /// Drops EVERY connection registered under `execution_id` (close node
    /// without a specific selection / `flow_ended` callback). A no-op when
    /// the execution has no entries.
    pub fn release(&self, execution_id: &str) {
        let uuids: Vec<String> = self
            .connections
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(execution_id)
            .map(|set| set.into_iter().collect())
            .unwrap_or_default();
        for uuid in uuids {
            self.inner.remove(&pool_key(execution_id, &uuid));
        }
    }

    /// Drops a single connection of `execution_id` (close node with a
    /// selected `connection_uuid`). A no-op when the entry is absent.
    pub fn release_connection(&self, execution_id: &str, connection_uuid: &str) {
        if self.inner.remove(&pool_key(execution_id, connection_uuid)) {
            if let Ok(mut guard) = self.connections.lock() {
                if let Some(set) = guard.get_mut(execution_id) {
                    set.remove(connection_uuid);
                }
            }
        }
    }
}

/// Composite pool key: `execution_id` and `connection_uuid` are opaque host
/// strings, `:` is a safe separator because neither contains it (the host
/// generates both as plain uuids/ids).
fn pool_key(execution_id: &str, connection_uuid: &str) -> String {
    format!("{execution_id}:{connection_uuid}")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Creates a client WITHOUT connecting: `Client::with_uri_str` is async
    /// in the 3.x driver but lazy — for a plain `mongodb://` URI it only
    /// parses the string and constructs the client; sockets are opened on the
    /// first operation. No mongod is required.
    async fn make_client(uri: &str) -> mongodb::error::Result<mongodb::Client> {
        mongodb::Client::with_uri_str(uri).await
    }

    #[tokio::test]
    async fn isolation_between_execution_ids() -> Result<(), Box<dyn std::error::Error>> {
        let pool = MongoPool::default();
        let uri = "mongodb://localhost:27017";

        let (a, replaced_a) = pool.connect("exec-1", "conn-a", uri, "db-a", make_client(uri).await?);
        let (b, replaced_b) = pool.connect("exec-2", "conn-b", uri, "db-b", make_client(uri).await?);

        assert!(!replaced_a);
        assert!(!replaced_b);
        assert!(
            !Arc::ptr_eq(&a, &b),
            "different execution_ids must not share a connection"
        );
        assert_eq!(a.database, "db-a");
        assert_eq!(b.database, "db-b");
        Ok(())
    }

    /// Composite-key isolation: two connection uuids under ONE execution id
    /// must not share a connection — this is what lets multiple
    /// `mongo:connect` nodes coexist in a single flow.
    #[tokio::test]
    async fn same_execution_different_uuids_do_not_share() -> Result<(), Box<dyn std::error::Error>> {
        let pool = MongoPool::default();
        let uri = "mongodb://localhost:27017";

        let (a, replaced_a) = pool.connect("exec-1", "conn-a", uri, "db-a", make_client(uri).await?);
        let (b, replaced_b) = pool.connect("exec-1", "conn-b", uri, "db-b", make_client(uri).await?);

        assert!(!replaced_a);
        assert!(!replaced_b);
        assert!(
            !Arc::ptr_eq(&a, &b),
            "different connection_uuids must not share a connection"
        );
        assert_eq!(a.database, "db-a");
        assert_eq!(b.database, "db-b");
        Ok(())
    }

    #[tokio::test]
    async fn reuse_same_key() -> Result<(), Box<dyn std::error::Error>> {
        let pool = MongoPool::default();
        let uri = "mongodb://localhost:27017";

        let (first, replaced_first) =
            pool.connect("exec-1", "conn-a", uri, "db", make_client(uri).await?);
        let (second, replaced_second) =
            pool.connect("exec-1", "conn-a", uri, "db", make_client(uri).await?);

        assert!(!replaced_first);
        assert!(
            Arc::ptr_eq(&first, &second),
            "same execution_id + same connection_uuid + same uri/database must reuse the connection"
        );
        assert!(!replaced_second, "reuse must not report replacement");
        Ok(())
    }

    /// `release` drops EVERY connection of the execution (prefix semantics),
    /// leaving other executions untouched. Releasing again is a no-op.
    #[tokio::test]
    async fn release_removes_all_of_execution() -> Result<(), Box<dyn std::error::Error>> {
        let pool = MongoPool::default();
        let uri = "mongodb://localhost:27017";

        pool.connect("exec-1", "conn-a", uri, "db", make_client(uri).await?);
        pool.connect("exec-1", "conn-b", uri, "db", make_client(uri).await?);
        pool.connect("exec-2", "conn-c", uri, "db", make_client(uri).await?);
        assert!(pool.client("exec-1", "conn-a").is_some());
        assert!(pool.client("exec-1", "conn-b").is_some());

        pool.release("exec-1");
        assert!(
            pool.client("exec-1", "conn-a").is_none(),
            "all of exec-1 must be released"
        );
        assert!(
            pool.client("exec-1", "conn-b").is_none(),
            "all of exec-1 must be released"
        );
        assert!(
            pool.client("exec-2", "conn-c").is_some(),
            "other executions must survive"
        );

        // Releasing again must be a no-op, not a panic.
        pool.release("exec-1");
        Ok(())
    }

    /// `release_connection` drops exactly one connection of the execution.
    #[tokio::test]
    async fn release_connection_removes_single() -> Result<(), Box<dyn std::error::Error>> {
        let pool = MongoPool::default();
        let uri = "mongodb://localhost:27017";

        pool.connect("exec-1", "conn-a", uri, "db", make_client(uri).await?);
        pool.connect("exec-1", "conn-b", uri, "db", make_client(uri).await?);

        pool.release_connection("exec-1", "conn-a");
        assert!(
            pool.client("exec-1", "conn-a").is_none(),
            "selected connection must be released"
        );
        assert!(
            pool.client("exec-1", "conn-b").is_some(),
            "unselected connection must survive"
        );
        Ok(())
    }

    #[tokio::test]
    async fn different_uri_replaces() -> Result<(), Box<dyn std::error::Error>> {
        let pool = MongoPool::default();
        let uri_a = "mongodb://a.example:27017";
        let uri_b = "mongodb://b.example:27017";

        let (first, replaced_first) =
            pool.connect("exec-1", "conn-a", uri_a, "db", make_client(uri_a).await?);
        assert!(!replaced_first);

        let (second, replaced_second) =
            pool.connect("exec-1", "conn-a", uri_b, "db", make_client(uri_b).await?);
        assert!(replaced_second, "different uri must report replacement");
        assert!(
            !Arc::ptr_eq(&first, &second),
            "a different target must yield a fresh connection"
        );
        assert_eq!(second.uri, uri_b);

        // The stale entry is fully gone: reconnecting the original uri
        // replaces the current one again.
        let (third, replaced_third) =
            pool.connect("exec-1", "conn-a", uri_a, "db", make_client(uri_a).await?);
        assert!(replaced_third);
        assert_eq!(third.uri, uri_a);
        Ok(())
    }
}
