//! MongoDB operation nodes, dispatched from `MongoPlugin::execute` by the
//! `config["type"]` discriminator.
pub mod aggregate;
pub mod close;
pub mod connect;
pub mod delete;
pub mod find;
pub mod insert;
pub mod update;

/// Resolves the `connection_uuid` an operation node targets, read from its
/// config's `connection_uuid` field (the host `x-node-selector` fills it with
/// the chosen `mongo:connect` node's instance id). Missing or empty → a
/// readable error telling the user to pick a connect node.
pub(crate) fn resolve_connection_uuid(config: &serde_json::Value) -> Result<String, String> {
    config
        .get("connection_uuid")
        .and_then(serde_json::Value::as_str)
        .filter(|uuid| !uuid.is_empty())
        .map(str::to_string)
        .ok_or_else(|| {
            crate::i18n::t(
                "请选择连接节点（缺少 connection_uuid，需先添加 mongo:connect 节点并选择）",
                "Please select a connection node (missing connection_uuid; add and select a mongo:connect node first)",
            )
            .to_string()
        })
}
