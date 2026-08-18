//! Binary entry point: `mpe_plugin_main!` wires `MongoPlugin` into the SDK
//! event loop (tokio runtime + JSON-RPC over stdio). Exit code 0 = clean
//! shutdown (EOF on stdin after flushing the last response).

use mpe_plugin_mongo::MongoPlugin;
use mpe_plugin_sdk::prelude::*;

mpe_plugin_main!(MongoPlugin);
