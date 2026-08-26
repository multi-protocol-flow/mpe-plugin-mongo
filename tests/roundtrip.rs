//! Offline stdio JSON-RPC roundtrip tests against the compiled
//! `mpe_plugin_mongo` binary — the exact transport shape the host uses:
//! JSON-RPC 2.0 requests on stdin, LF-framed responses on stdout.
//!
//! Fully offline: no mongod, no network. Every `execute` case short-circuits
//! before any database I/O — find-without-connect fails at the pool lookup,
//! unknown/missing type fails at the dispatch match, and close on an empty
//! pool is an idempotent no-op.
//!
//! Wire notes mirrored from `mpe-plugin-sdk/tests/roundtrip.rs`:
//! - The runtime spawns one task per `execute` and correlates responses by
//!   request `id`; responses may interleave with other traffic.
//! - Failing node executions emit an id-less `log` notification frame on
//!   stdout *before* their response. Readers must skip id-less frames and
//!   collect only frames carrying an `id`.
//! - Notifications (no `id`) never produce a response.
//! - A clean EOF on stdin (dropped write end) exits the loop with code 0.

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, ExitStatus, Stdio};

use serde_json::Value;

/// Path to the compiled plugin binary (set by cargo for `[[bin]]` targets).
const PLUGIN_BIN: &str = env!("CARGO_BIN_EXE_mpe_plugin_mongo");

/// The 7 node type_ids the plugin must describe.
const EXPECTED_TYPE_IDS: [&str; 7] = [
    "mongo:connect",
    "mongo:find",
    "mongo:insert",
    "mongo:update",
    "mongo:delete",
    "mongo:aggregate",
    "mongo:close",
];

/// A spawned plugin process with piped stdio.
struct PluginProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl PluginProcess {
    /// Spawns the plugin binary with piped stdio (stderr piped and ignored —
    /// malformed-frame diagnostics land there, never on stdout).
    fn spawn() -> Result<Self, Box<dyn std::error::Error>> {
        let mut child = Command::new(PLUGIN_BIN)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| "plugin stdin not available".to_string())?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "plugin stdout not available".to_string())?;
        Ok(Self {
            child,
            stdin,
            stdout: BufReader::new(stdout),
        })
    }

    /// Sends one request and reads frames until the single response for it
    /// arrives. Id-less notification frames (e.g. the `log` notifications
    /// emitted by failing node executions) are read and discarded.
    fn exchange(&mut self, request: &str) -> Result<Value, Box<dyn std::error::Error>> {
        self.stdin.write_all(request.as_bytes())?;
        self.stdin.write_all(b"\n")?;
        self.stdin.flush()?;
        loop {
            let frame = self.read_frame()?;
            let value: Value = serde_json::from_str(&frame)?;
            if value.get("id").is_some() {
                return Ok(value);
            }
        }
    }

    /// Writes one fire-and-forget frame (a notification — no response).
    fn send_notification(&mut self, frame: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.stdin.write_all(frame.as_bytes())?;
        self.stdin.write_all(b"\n")?;
        self.stdin.flush()?;
        Ok(())
    }

    /// Reads one LF-delimited frame (CRLF-tolerant).
    fn read_frame(&mut self) -> Result<String, Box<dyn std::error::Error>> {
        let mut line = String::new();
        let n = self.stdout.read_line(&mut line)?;
        if n == 0 {
            return Err("plugin stdout closed unexpectedly".into());
        }
        Ok(line.trim_end_matches(['\r', '\n']).to_string())
    }

    /// The `errors` array joined into one string for substring assertions.
    fn joined_errors(response: &Value) -> String {
        response["result"]["errors"]
            .as_array()
            .expect("result.errors must be an array")
            .iter()
            .filter_map(|e| e.get("message").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// Half-closes stdin (the child sees EOF) and waits for a clean exit.
    fn close_stdin_and_wait(mut self) -> Result<ExitStatus, Box<dyn std::error::Error>> {
        drop(self.stdin);
        self.child.wait().map_err(Into::into)
    }
}

/// (a) `describe` returns all 7 nodes with the expected type_ids.
#[test]
fn describe_returns_seven_nodes() -> Result<(), Box<dyn std::error::Error>> {
    let mut plugin = PluginProcess::spawn()?;
    let response =
        plugin.exchange(r#"{"jsonrpc":"2.0","id":1,"method":"describe","params":{}}"#)?;
    assert_eq!(response["id"], 1);
    let nodes = response["result"]
        .as_array()
        .ok_or_else(|| "describe result must be an array".to_string())?;
    assert_eq!(nodes.len(), 7, "all 7 mongo nodes must be described");
    let type_ids: Vec<&str> = nodes
        .iter()
        .filter_map(|node| node["type_id"].as_str())
        .collect();
    assert_eq!(
        type_ids.len(),
        7,
        "every described node must carry a type_id"
    );
    for expected in EXPECTED_TYPE_IDS {
        assert!(
            type_ids.contains(&expected),
            "missing node type `{expected}`"
        );
    }
    let status = plugin.close_stdin_and_wait()?;
    assert!(status.success(), "plugin exited with {status:?}");
    Ok(())
}

/// (b) `mongo:close` without an execution_id succeeds — the D12 `"default"`
/// fallback key must not panic, and close on the empty default key is an
/// idempotent success.
#[test]
fn execute_close_without_execution_id_succeeds() -> Result<(), Box<dyn std::error::Error>> {
    let mut plugin = PluginProcess::spawn()?;
    let response = plugin.exchange(
        r#"{"jsonrpc":"2.0","id":2,"method":"execute","params":{"config":{"type":"mongo:close"}}}"#,
    )?;
    assert_eq!(response["id"], 2);
    assert_eq!(
        response["result"]["success"], true,
        "close without execution_id must succeed"
    );
    let status = plugin.close_stdin_and_wait()?;
    assert!(status.success(), "plugin exited with {status:?}");
    Ok(())
}

/// (c) `mongo:find` without a prior connect fails, and the error mentions
/// "connect". The failed execution also emits a `log` notification frame,
/// which `exchange` skips.
#[test]
fn execute_find_without_connect_fails() -> Result<(), Box<dyn std::error::Error>> {
    let mut plugin = PluginProcess::spawn()?;
    let response = plugin.exchange(
        r#"{"jsonrpc":"2.0","id":3,"method":"execute","params":{"config":{"type":"mongo:find","collection":"users"}}}"#,
    )?;
    assert_eq!(response["id"], 3);
    assert_eq!(response["result"]["success"], false);
    let joined = PluginProcess::joined_errors(&response);
    assert!(
        joined.contains("connect"),
        "error must hint at connect, got: {joined}"
    );
    let status = plugin.close_stdin_and_wait()?;
    assert!(status.success(), "plugin exited with {status:?}");
    Ok(())
}

/// (d) An unknown node type fails dispatch — never a silent success.
#[test]
fn execute_unknown_type_fails() -> Result<(), Box<dyn std::error::Error>> {
    let mut plugin = PluginProcess::spawn()?;
    let response = plugin.exchange(
        r#"{"jsonrpc":"2.0","id":4,"method":"execute","params":{"config":{"type":"mongo:nope"}}}"#,
    )?;
    assert_eq!(response["id"], 4);
    assert_eq!(
        response["result"]["success"], false,
        "unknown type must fail, not silently succeed"
    );
    let joined = PluginProcess::joined_errors(&response);
    assert!(
        joined.contains("mongo:nope"),
        "error must name the offending type, got: {joined}"
    );
    let status = plugin.close_stdin_and_wait()?;
    assert!(status.success(), "plugin exited with {status:?}");
    Ok(())
}

/// (e) A config without a `type` key fails dispatch with the D3
/// "unknown node type" error. The actual message is
/// `Unknown node type \`\`` (empty backtick content — the `other` match arm
/// formats `format!("Unknown node type `{other}`")` with `other == ""`); it does
/// NOT contain the ASCII substring "type", so the assertion targets the
/// stable prefix instead.
#[test]
fn execute_missing_type_fails() -> Result<(), Box<dyn std::error::Error>> {
    let mut plugin = PluginProcess::spawn()?;
    let response = plugin.exchange(
        r#"{"jsonrpc":"2.0","id":5,"method":"execute","params":{"config":{"collection":"x"}}}"#,
    )?;
    assert_eq!(response["id"], 5);
    assert_eq!(response["result"]["success"], false);
    let joined = PluginProcess::joined_errors(&response);
    assert!(
        !joined.is_empty(),
        "missing type must produce an error message"
    );
    assert!(
        joined.contains("Unknown node type"),
        "error must be the unknown-node-type message, got: {joined}"
    );
    let status = plugin.close_stdin_and_wait()?;
    assert!(status.success(), "plugin exited with {status:?}");
    Ok(())
}

/// (f) A `flowEnded` notification (no id) is tolerated: no response frame,
/// no crash, no desync — the next normal request is still answered.
///
/// Name kept exactly as the plan specifies (camelCase mirrors the wire
/// method `flowEnded`); the lint is scoped to this one function.
#[allow(non_snake_case)]
#[test]
fn flowEnded_notification_tolerated() -> Result<(), Box<dyn std::error::Error>> {
    let mut plugin = PluginProcess::spawn()?;
    // Phase 1: describe (1 request → 1 response).
    let response =
        plugin.exchange(r#"{"jsonrpc":"2.0","id":1,"method":"describe","params":{}}"#)?;
    assert_eq!(response["id"], 1);
    assert_eq!(
        response["result"].as_array().map(Vec::len),
        Some(7),
        "describe must answer 7 nodes"
    );

    // Phase 2: fire-and-forget flowEnded notification — must be silently
    // handled (the plugin's flow_ended hook releases the pool entry) with no
    // response frame.
    plugin.send_notification(
        r#"{"jsonrpc":"2.0","method":"flowEnded","params":{"execution_id":"exec-1"}}"#,
    )?;

    // Phase 3: a normal execute after the notification must still succeed.
    let response = plugin.exchange(
        r#"{"jsonrpc":"2.0","id":6,"method":"execute","params":{"config":{"type":"mongo:close"}}}"#,
    )?;
    assert_eq!(response["id"], 6);
    assert_eq!(
        response["result"]["success"], true,
        "execute after flowEnded must still succeed"
    );
    let status = plugin.close_stdin_and_wait()?;
    assert!(status.success(), "plugin exited with {status:?}");
    Ok(())
}

/// (g) An unknown method produces a JSON-RPC `-32601` error response.
#[test]
fn unknown_method_returns_error() -> Result<(), Box<dyn std::error::Error>> {
    let mut plugin = PluginProcess::spawn()?;
    let response =
        plugin.exchange(r#"{"jsonrpc":"2.0","id":7,"method":"bogus_method","params":{}}"#)?;
    assert_eq!(response["id"], 7);
    assert_eq!(response["error"]["code"], -32601);
    assert_eq!(response["error"]["message"], "Method not found");
    let status = plugin.close_stdin_and_wait()?;
    assert!(status.success(), "plugin exited with {status:?}");
    Ok(())
}

/// (h) After a roundtrip, closing the child's stdin (EOF) makes the plugin
/// exit cleanly with code 0.
#[test]
fn eof_exits_cleanly() -> Result<(), Box<dyn std::error::Error>> {
    let mut plugin = PluginProcess::spawn()?;
    let response =
        plugin.exchange(r#"{"jsonrpc":"2.0","id":1,"method":"describe","params":{}}"#)?;
    assert_eq!(response["id"], 1);
    assert_eq!(
        response["result"].as_array().map(Vec::len),
        Some(7),
        "describe must answer 7 nodes"
    );
    let status = plugin.close_stdin_and_wait()?;
    assert!(
        status.success(),
        "clean EOF must exit 0, plugin exited with {status:?}"
    );
    Ok(())
}
