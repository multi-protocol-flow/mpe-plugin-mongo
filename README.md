# MPE MongoDB Plugin

> **Positioning**: this plugin is one of MPE's **official protocol plugins** — it
> depends on none of the host repository's code, only the public
> `mpe-plugin-sdk` (sidecar process + JSON-RPC over stdio, git tag `v0.1.0`).
> The host scans its plugin directory at startup, runs the `describe` handshake,
> registers the node types, and calls this plugin process via the `execute` RPC.
>
> This repository is independent of the host (the host repository's `.gitignore`
> ignores `/plugins/` and the host never builds it). It is built and released by
> this repository's own CI (GitHub Actions → Release artifacts).

---

## 0. How the plugin works in one minute

```
Host (mpe / mpe-cli)                    Plugin process (this crate)
   │  scans plugins/ dir                       │
   │  ── describe ───────────────────────────► │  returns node descriptions (type, ports, config schema)
   │  ◄─────────── node list ─────────────────  │
   │  ── execute(config, execution_id) ──────► │  runs the MongoDB operation
   │  ◄─────────── result / variable updates ─  │
   │  ── flowEnded(execution_id) ────────────► │  releases the per-execution connection pool
```

- **Transport**: stdin/stdout, one JSON document per line (JSON-RPC 2.0, LF-framed)
- **Resident**: `capabilities.streaming: true` → the process stays alive, the
  connection pool is reused across executions
- **No shared memory**: the plugin is a separate process; the host can only pass
  values as JSON

---

## 1. Project structure

```
mpe-plugin-mongo/
├── Cargo.toml            # standalone package, no host workspace dependency
├── plugin.json           # manifest scanned by the host (launch description, residency mode)
├── .github/workflows/ci.yml  # 3-platform build + integration tests + Release packaging
├── src/
│   ├── main.rs           # mpe_plugin_main! entry point
│   ├── lib.rs            # MongoPlugin: Plugin trait impl + node dispatch
│   ├── pool.rs           # per-execution connection pool (execution_id → Client)
│   └── nodes/
│       ├── mod.rs        # per-node execute dispatch
│       ├── connect.rs    # mongo:connect
│       ├── find.rs       # mongo:find
│       ├── insert.rs     # mongo:insert
│       ├── update.rs     # mongo:update
│       ├── delete.rs     # mongo:delete
│       ├── aggregate.rs  # mongo:aggregate
│       └── close.rs      # mongo:close
└── tests/
    ├── roundtrip.rs      # offline stdio roundtrip tests (describe/execute via JSON-RPC, no MongoDB needed)
    └── integration.rs    # real-MongoDB integration tests (#[ignore], needs mongod)
```

---

## 2. Cargo.toml (standalone build)

```toml
[package]
name = "mpe-plugin-mongo"
version = "0.1.0"
edition = "2021"

[dependencies]
# Official plugins: public SDK only, never host types (flow-engine-core etc.)
mpe-plugin-sdk = { git = "https://github.com/multi-protocol-flow/mpe-plugin-sdk.git", tag = "v0.1.0" }
# Official MongoDB driver (async, tokio runtime — tokio-only by default
# since 3.0; the 2.x-era `tokio-runtime` feature no longer exists)
mongodb = { version = "3" }
# Cursor iteration (mongo:find): mongodb 3.8 does NOT re-export `futures`;
# its own docs (cursor.rs) use `futures::stream::TryStreamExt` for
# `cursor.try_next()` — driver companion crate, standard 0.3 line.
futures = "0.3"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tokio = { version = "1", features = ["full"] }

[profile.release]
opt-level = 3

[[bin]]
name = "mpe_mongo_plugin"
path = "src/main.rs"
```

> **SDK acquisition (two options)**:
> 1. **git dependency (tag-pinned)**: `mpe-plugin-sdk = { git = "https://github.com/multi-protocol-flow/mpe-plugin-sdk.git", tag = "v0.1.0" }` (default features, includes the runtime).
>    Verified to compile standalone — cargo pulls the git tag, no host directory needed.
> 2. **Local development**: `[patch."https://github.com/multi-protocol-flow/mpe-plugin-sdk.git"] mpe-plugin-sdk = { path = "../mpe-plugin-sdk" }` pointing at a local checkout.

---

## 3. plugin.json (host scan manifest)

```json
{
  "name": "mongo",
  "version": "0.1.0",
  "description": "MongoDB protocol plugin: connect/find/insert/update/delete/aggregate/close",
  "entry": {
    "command": "./mpe_mongo_plugin"
  },
  "env": {},
  "permissions": [],
  "capabilities": { "streaming": true }
}
```

Key fields:

| field | value | meaning |
|---|---|---|
| `name` | `mongo` | unique; keep consistent with the directory name |
| `entry.command` | `./mpe_mongo_plugin` | plugin binary path, marketplace-contract relative form |
| `capabilities.streaming` | `true` | **required**: process stays resident, the MongoDB connection pool survives the 60s idle reaper |
| `min_host_version` | optional | version gate; the host skips the plugin if unmet |

> **Why `streaming: true` is required**: with the default `false` the host
> spawns the process on demand and reaps it after 60s idle — when the process
> dies the whole pool goes with it and every execution must rebuild the TCP+TLS
> handshake. MongoDB is a long-lived-connection protocol; the plugin must stay
> resident.

---

## 4. Node design

All nodes have `in` (left) / `out` (right) ports. Failures follow the host's
`on_error` strategy.

### 4.1 `mongo:connect` — establish a connection

```json
{ "uri": "mongodb://localhost:27017", "database": "mydb", "timeout_ms": 5000 }
```

- Pools keyed by `execution_id` (`get_or_insert`), idempotent: re-connect within
  the same execution reuses the existing connection
- Output: `{ "connected": true, "database": "mydb" }`
- On failure: error → the host routes per `on_error`

### 4.2 `mongo:find` — query

```json
{ "collection": "users", "filter": { "age": { "$gt": 18 } }, "project": null, "limit": 100 }
```

- Fetches the `Client` from the current `execution_id`'s pool; errors with
  "connect first" if not connected
- Output: `{ "count": N, "documents": [ ... ] }`

### 4.3 `mongo:insert` / `mongo:update` / `mongo:delete` / `mongo:aggregate`

| node | config | output |
|---|---|---|
| `insert` | `{ collection, documents: [...] }` | `{ inserted_count }` |
| `update` | `{ collection, filter, update, upsert? }` | `{ matched_count, modified_count }` |
| `delete` | `{ collection, filter, delete_many? }` | `{ deleted_count }` |
| `aggregate` | `{ collection, pipeline: [...] }` | `{ documents: [...] }` |

### 4.5 `mongo:close` — explicit release

```json
{ }
```

- `pool.remove(execution_id)` → drops every connection of that execution
- For mid-flow disconnect (e.g. the later half of a long flow no longer needs the DB)

---

## 5. Connection pool design (core: per-execution isolation)

```rust
// pool.rs
pub struct MongoPool {
    inner: mpe_plugin_sdk::pool::ConnectionPool,  // key → Arc<dyn Any>
}

impl MongoPool {
    /// connect node: pool keyed by execution_id (idempotent reuse)
    pub fn connect(&self, execution_id: &str, uri: &str) -> Result<Client> {
        self.inner.get_or_insert(execution_id, || /* Client::with_uri_str */)
    }
    /// find etc.: fetch the current execution's connection, error if absent
    pub fn client(&self, execution_id: &str) -> Option<Arc<Client>> {
        self.inner.get::<Client>(execution_id)
    }
    /// close node / flow_ended callback: release
    pub fn release(&self, execution_id: &str) {
        self.inner.remove(execution_id);
    }
}
```

**Why key on `execution_id`**:
- One flow execution = one `execution_id`, shared by all nodes in the flow → a
  pool created by `connect` is reachable by `find`
- Different executions (re-runs / stress-test virtual users / dataset rows) each
  get a unique id → pools never leak across executions
- `execution_id` is generated by the host per execution; the plugin treats it as
  an **opaque key** and must never assume it equals the flow uuid

**Release paths (belt and suspenders)**:

```
1. Normal path: host broadcasts flowEnded(execution_id) when the flow finishes
   → plugin Plugin::flow_ended() callback → pool.release(execution_id)
2. Explicit mid-flow: mongo:close node → pool.release(execution_id)
```

---

## 6. Plugin main implementation (skeleton)

```rust
// src/lib.rs
use mpe_plugin_sdk::prelude::*;

#[derive(Default)]
pub struct MongoPlugin {
    pub pool: MongoPool,   // process-level singleton
}

impl Plugin for MongoPlugin {
    fn describe(&self) -> Vec<NodeDescription> {
        vec![
            NodeDescription::new("mongo:connect", "Mongo Connect"),
            NodeDescription::new("mongo:find", "Mongo Find"),
            NodeDescription::new("mongo:insert", "Mongo Insert"),
            NodeDescription::new("mongo:update", "Mongo Update"),
            NodeDescription::new("mongo:delete", "Mongo Delete"),
            NodeDescription::new("mongo:aggregate", "Mongo Aggregate"),
            NodeDescription::new("mongo:close", "Mongo Close"),
        ]
    }

    fn execute(&self, ctx: &mut ExecuteContext) -> impl Future<Output = ExecuteResult> + Send {
        // ctx.execution_id() → current execution id; ctx.config() → host-resolved config
        // dispatch to nodes/*.rs by the config's node type
        async move { /* ... */ }
    }

    fn flow_ended(&self, execution_id: &str) {
        self.pool.release(execution_id);   // release the pool automatically at flow end
    }
}

// src/main.rs
use mpe_plugin_sdk::prelude::*;
mpe_plugin_main!(MongoPlugin);
```

---

## 7. Development & testing

> **plugin.json `entry.command`**: a relative path in marketplace-contract form,
> `./mpe_mongo_plugin`. The current host resolves the command against its own CWD
> (it does not chdir into the plugin directory). When developing in this
> repository, if you want the host to launch the plugin directly, either change
> `command` to an absolute path on your machine, or start the host from the
> plugin directory.

```bash
# build the release binary (the host launches it via plugin.json entry.command)
# run from the repository root (mpe-plugin-mongo)
cargo build --release
# artifact: target/release/mpe_mongo_plugin (Linux has no .exe suffix)

# offline unit tests (pool isolation, config parsing) + stdio roundtrip tests
cargo test

# manual smoke test: simulate a host conversation (describe → execute)
echo '{"jsonrpc":"2.0","id":1,"method":"describe","params":{}}' | target/debug/mpe_mongo_plugin

# real-MongoDB integration tests (skipped by default; enable when mongod is up)
MPE_MONGO_URI=mongodb://127.0.0.1:27017 cargo test --test integration -- --include-ignored
```

### End-to-end (with a real host)

```bash
# give the plugin directory to the host
export MPE_PLUGIN_DIR=/path/to/plugins   # or copy into the host data dir plugins/
mpe run -f flow_with_mongo.json
```

Verified end-to-end (with a local mongod): `mpe-cli run` exits 0 and reports
`executed_count=5` (entry→connect→insert→find→end); the insert node reports
`inserted_count=2`, the find node `count=2`. Note that `mpe-cli validate` only
uses the built-in registry and reports `E_VAL_UNKNOWN_NODE_TYPE` for plugin node
types — expected behavior; verify plugin flows by the `run` report instead.

---

## 8. Constraints checklist

| # | constraint | why |
|---|---|---|
| 1 | Use only the public `mpe-plugin-sdk` API | the plugin is a subprocess; inheriting host-internal types is forbidden |
| 2 | `type_id` in `vendor:name` namespace | avoid colliding with the host's built-in node types |
| 3 | `variable_updates` keys must be **flat top-level names** (no dots) | host VariableStorage is flat; dotted keys are warned and dropped |
| 4 | `report_data` ≤ 1 MiB | host wire limit |
| 5 | Avoid stuffing large result sets into the output | convert to variables/pagination/aggregation to avoid timeouts and memory pressure |
| 6 | `execute` returns a `Send` future | the SDK spawns a task per request |
| 7 | Each frame ≤ 2 MiB (both directions) | framing protocol limit |
| 8 | `streaming: true` resident process | otherwise the connection pool dies with the process reaper |

---

## 9. Next steps

- [x] connect/find end-to-end (against a local mongod)
- [x] insert/update/delete/aggregate
- [ ] Config panel (P1 iframe / inline HTML) — not required (config is provided via `config_schema` from `describe`; custom panel not implemented)
- [ ] Once the SDK is published to crates.io, switch the dependency to crates.io and fully detach from the host repo
