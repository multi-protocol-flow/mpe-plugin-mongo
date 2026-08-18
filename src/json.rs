//! JSON ↔ BSON conversion helpers shared by every MongoDB operation node.
//!
//! Host configs arrive as plain JSON, with complex fields (`filter`,
//! `project`, `documents`, `update`, `pipeline`) declared as JSON-**text**
//! strings (see `mongo_node` in lib.rs — the host config panel renders
//! `string` fields with proper inputs). These helpers parse such a field
//! into `bson::Document`s for the driver, and serialize result documents
//! back into plain JSON for the host.
//!
//! **Extended-JSON input** (v2): [`value_to_bson`] recognizes the canonical
//! MongoDB extended-JSON type wrappers — `{"$oid": "<24-hex>"}` → ObjectId,
//! `{"$date": "<ISO-8601>"}` / `{"$date": {"$numberLong": "<ms>"}}` →
//! DateTime, `{"$numberLong": "<i64>"}` / `{"$numberInt": ...}` /
//! `{"$numberDouble": ...}` — at any nesting depth, so filters can match
//! ObjectId `_id` fields. Unknown `$xxx` keys (query operators like `$gt`,
//! `$in`, `$match`, `$expr`) pass through as plain documents untouched.
//!
//! Driver OUTPUT serialization also uses extended-JSON shapes (e.g.
//! `{"$oid":"…"}`, `{"$date":{"$numberLong":"…"}}`); the exact shapes are
//! locked by `special_type_output_shape_locked_by_test`.
//!
//! Pure conversion: no `ExecuteContext`, no I/O — unit tests are plain
//! `#[test]`, no tokio runtime needed.

use mongodb::bson::oid::ObjectId;
use mongodb::bson::{to_bson, Bson, DateTime, Document};

/// Converts one plain/extended-JSON value into BSON, recursively.
///
/// Recognizes the canonical MongoDB extended-JSON **type wrappers** at any
/// nesting depth:
/// - `{"$oid": "<24-hex>"}` → `Bson::ObjectId` (match ObjectId `_id` fields)
/// - `{"$date": "<ISO-8601>"}` / `{"$date": {"$numberLong": "<ms>"}}` →
///   `Bson::DateTime`
/// - `{"$numberLong": "<i64>"}` / `{"$numberInt": "<i32>"}` /
///   `{"$numberDouble": "<f64>"}` → numeric BSON (canonical extJSON values
///   are JSON strings)
///
/// Only **single-key objects whose key is a KNOWN wrapper** are converted;
/// everything else — including unknown `$xxx` keys (query operators `$gt`,
/// `$in`, `$match`, `$expr`, `$regex`, …) and multi-key documents — is
/// converted field-by-field, so filters keep their operators untouched.
///
/// Plain JSON numbers keep i64/u64 width (no lossy f64 round-trip).
pub fn value_to_bson(value: &serde_json::Value) -> Result<Bson, String> {
    match value {
        serde_json::Value::Object(map) => {
            if map.len() == 1 {
                if let (Some(key), Some(val)) = (map.keys().next(), map.values().next()) {
                    if let Some(converted) = try_extended_wrapper(key, val)? {
                        return Ok(converted);
                    }
                }
            }
            let mut doc = Document::new();
            for (k, v) in map {
                doc.insert(k.clone(), value_to_bson(v)?);
            }
            Ok(Bson::Document(doc))
        }
        serde_json::Value::Array(items) => {
            let mut arr = Vec::with_capacity(items.len());
            for item in items {
                arr.push(value_to_bson(item)?);
            }
            Ok(Bson::Array(arr))
        }
        serde_json::Value::Null => Ok(Bson::Null),
        serde_json::Value::Bool(b) => Ok(Bson::Boolean(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Bson::Int64(i))
            } else if let Some(u) = n.as_u64() {
                // u64 beyond i64::MAX cannot be stored by the driver
                // (locked in `numeric_fidelity`); reject loudly.
                i64::try_from(u)
                    .map(Bson::Int64)
                    .map_err(|_| crate::i18n::t("u64 超出 BSON Int64 范围", "u64 exceeds BSON Int64 range").to_string())
            } else {
                Ok(Bson::Double(n.as_f64().unwrap_or(f64::NAN)))
            }
        }
        serde_json::Value::String(s) => Ok(Bson::String(s.clone())),
    }
}

/// Recognized extended-JSON type-wrapper keys.
const EXTENDED_WRAPPERS: &[&str] = &["$oid", "$date", "$numberLong", "$numberInt", "$numberDouble"];

/// Tries to convert a single-key object as an extended-JSON type wrapper.
///
/// Returns `Ok(Some(bson))` when the key is a known wrapper (conversion
/// failure surfaces as `Err`), `Ok(None)` when the key is NOT a wrapper (the
/// caller converts the object field-by-field — this keeps query operators
/// like `$gt` intact).
fn try_extended_wrapper(key: &str, value: &serde_json::Value) -> Result<Option<Bson>, String> {
    if !EXTENDED_WRAPPERS.contains(&key) {
        return Ok(None);
    }
    match key {
        "$oid" => {
            let hex = value
                .as_str()
                .ok_or_else(|| crate::i18n::t("字段 $oid 必须是 24 位 hex 字符串", "field $oid must be a 24-char hex string").to_string())?;
            let oid = ObjectId::parse_str(hex)
                .map_err(|err| {
                    let msg = crate::i18n::t("字段 $oid 不是合法 ObjectId", "field $oid is not a valid ObjectId");
                    format!("{msg}: {err}")
                })?;
            Ok(Some(Bson::ObjectId(oid)))
        }
        "$date" => {
            let date_time = match value {
                serde_json::Value::String(iso) => DateTime::parse_rfc3339_str(iso)
                    .map_err(|err| {
                        let msg = crate::i18n::t("字段 $date 不是合法 ISO-8601", "field $date is not valid ISO-8601");
                        format!("{msg}: {err}")
                    })?,
                serde_json::Value::Object(inner) => {
                    let millis = inner
                        .get("$numberLong")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| crate::i18n::t("字段 $date 对象形式必须是 {\"$numberLong\": \"<ms>\"}", "field $date object form must be {\"$numberLong\": \"<ms>\"}").to_string())?;
                    let millis: i64 = millis
                        .parse()
                        .map_err(|err| {
                            let msg = crate::i18n::t("字段 $date.$numberLong 不是合法整数", "field $date.$numberLong is not a valid integer");
                            format!("{msg}: {err}")
                        })?;
                    DateTime::from_millis(millis)
                }
                _ => return Err(crate::i18n::t("字段 $date 必须是 ISO-8601 字符串或 {\"$numberLong\": \"<ms>\"}", "field $date must be an ISO-8601 string or {\"$numberLong\": \"<ms>\"}").to_string()),
            };
            Ok(Some(Bson::DateTime(date_time)))
        }
        "$numberLong" => {
            let text = number_wrapper_text(value, "$numberLong")?;
            let n: i64 = text
                .parse()
                .map_err(|err| {
                    let msg = crate::i18n::t("字段 $numberLong 不是合法 Int64", "field $numberLong is not a valid Int64");
                    format!("{msg}: {err}")
                })?;
            Ok(Some(Bson::Int64(n)))
        }
        "$numberInt" => {
            let text = number_wrapper_text(value, "$numberInt")?;
            let n: i32 = text
                .parse()
                .map_err(|err| {
                    let msg = crate::i18n::t("字段 $numberInt 不是合法 Int32", "field $numberInt is not a valid Int32");
                    format!("{msg}: {err}")
                })?;
            Ok(Some(Bson::Int32(n)))
        }
        "$numberDouble" => {
            let text = number_wrapper_text(value, "$numberDouble")?;
            let f: f64 = text
                .parse()
                .map_err(|err| {
                    let msg = crate::i18n::t("字段 $numberDouble 不是合法 Double", "field $numberDouble is not a valid Double");
                    format!("{msg}: {err}")
                })?;
            Ok(Some(Bson::Double(f)))
        }
        _ => Ok(None),
    }
}

/// Canonical extended-JSON numeric wrappers carry their value as a JSON
/// string; plain numbers are tolerated too.
fn number_wrapper_text(value: &serde_json::Value, key: &str) -> Result<String, String> {
    match value {
        serde_json::Value::String(s) => Ok(s.clone()),
        serde_json::Value::Number(n) => Ok(n.to_string()),
        _ => Err(crate::i18n::t(
            "字段 {key} 必须是 JSON 字符串或数字",
            "field {key} must be a JSON string or number",
        )
        .replace("{key}", key)),
    }
}

/// Parses one JSON field of a node config into a BSON document (a MongoDB
/// filter / projection / update document).
///
/// Accepted field forms:
/// - JSON **string** → parsed as JSON text, then converted with
///   [`value_to_bson`]; unparseable text yields `field {key} is not valid JSON: {err}`
///   (Chinese equivalent under a `zh-CN` host language).
/// - JSON **object** → converted directly with [`value_to_bson`].
/// - **null**, **missing**, or an **empty/whitespace string** (the GUI
///   clears an input box to `""`) → `Ok(None)` (field optional).
///
/// Anything else (array, number, bool) is rejected: filters must be
/// objects, and returning `Err` beats silently converting a mistyped
/// config. (An array value parsed out of a JSON-text string is rejected
/// with the same message.)
pub fn parse_json_field(config: &serde_json::Value, key: &str) -> Result<Option<Document>, String> {
    let Some(value) = config.get(key) else {
        return Ok(None);
    };
    match value {
        serde_json::Value::Null => Ok(None),
        // 空字符串 = 字段未填（GUI 清空输入框保存为 ""，等价于 null/缺失）。
        serde_json::Value::String(text) if text.trim().is_empty() => Ok(None),
        serde_json::Value::String(text) => {
            let parsed: serde_json::Value = serde_json::from_str(text).map_err(|err| {
                crate::i18n::t(
                    "字段 {key} 不是合法 JSON: {err}",
                    "field {key} is not valid JSON: {err}",
                )
                .replace("{key}", key)
                .replace("{err}", &err.to_string())
            })?;
            match value_to_bson(&parsed)? {
                Bson::Document(doc) => Ok(Some(doc)),
                _ => Err(crate::i18n::t(
                    "字段 {key} 必须是 JSON 文本或对象",
                    "field {key} must be a JSON text or object",
                )
                .replace("{key}", key)),
            }
        }
        serde_json::Value::Object(_) => match value_to_bson(value)? {
            Bson::Document(doc) => Ok(Some(doc)),
            _ => Err(crate::i18n::t(
                "字段 {key} 必须是 JSON 文本或对象",
                "field {key} must be a JSON text or object",
            )
            .replace("{key}", key)),
        },
        // Filters/pipelines are objects; arrays/numbers/bools are mistakes.
        _ => Err(crate::i18n::t(
            "字段 {key} 必须是 JSON 文本或对象",
            "field {key} must be a JSON text or object",
        )
        .replace("{key}", key)),
    }
}

/// Parses one JSON field of a node config into a list of BSON documents (an
/// insert `documents` list or an aggregate `pipeline`).
///
/// Accepted field forms:
/// - JSON **string** → parsed as JSON text, must be a JSON **array**; each
///   element must be an object (`field {key} must be a JSON array` otherwise;
///   Chinese equivalent under a `zh-CN` host language).
/// - JSON **array** → each element converted directly.
/// - **null**, **missing**, or an **empty/whitespace string** → `Ok(None)`.
///
/// Every element that is not a JSON object is rejected with a readable
/// message (`field {key} array elements must be JSON objects`; Chinese
/// equivalent under a `zh-CN` host language).
pub fn parse_json_array_field(
    config: &serde_json::Value,
    key: &str,
) -> Result<Option<Vec<Document>>, String> {
    let Some(value) = config.get(key) else {
        return Ok(None);
    };
    let array: Vec<serde_json::Value> = match value {
        serde_json::Value::Null => return Ok(None),
        // 空字符串 = 字段未填（GUI 清空输入框保存为 ""，等价于 null/缺失）。
        serde_json::Value::String(text) if text.trim().is_empty() => return Ok(None),
        serde_json::Value::String(text) => {
            let parsed: serde_json::Value = serde_json::from_str(text).map_err(|err| {
                crate::i18n::t(
                    "字段 {key} 不是合法 JSON: {err}",
                    "field {key} is not valid JSON: {err}",
                )
                .replace("{key}", key)
                .replace("{err}", &err.to_string())
            })?;
            match parsed {
                serde_json::Value::Array(items) => items,
                _ => {
                    return Err(crate::i18n::t(
                        "字段 {key} 必须是 JSON 数组",
                        "field {key} must be a JSON array",
                    )
                    .replace("{key}", key))
                }
            }
        }
        serde_json::Value::Array(items) => items.clone(),
        _ => {
            return Err(crate::i18n::t(
                "字段 {key} 必须是 JSON 数组或文本",
                "field {key} must be a JSON array or text",
            )
            .replace("{key}", key))
        }
    };
    array
        .iter()
        .map(|item| {
            match value_to_bson(item).map_err(|err| {
                crate::i18n::t(
                    "字段 {key} 数组元素无法转换为 BSON: {err}",
                    "field {key} array element could not be converted to BSON: {err}",
                )
                .replace("{key}", key)
                .replace("{err}", &err.to_string())
            })? {
                Bson::Document(doc) => Ok(doc),
                _ => Err(crate::i18n::t(
                    "字段 {key} 的数组元素必须是 JSON 对象",
                    "field {key} array elements must be JSON objects",
                )
                .replace("{key}", key)),
            }
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

/// Serializes a BSON document into plain JSON for the host.
///
/// Non-finite doubles (NaN / ±Infinity) are rejected up front with the
/// readable message `result contains a non-finite floating-point value`
/// (Chinese equivalent under a `zh-CN` host language) — the driver stores them happily, so
/// they CAN arrive in query results, but JSON has no representation for
/// them. Note (observed on serde_json 1.0.151): `serde_json::to_value` does
/// NOT fail on NaN — it silently maps it to JSON `null`, which would
/// corrupt results; hence the explicit [`contains_non_finite`] scan (plan
/// decision D7).
///
/// NOTE: special BSON types serialize through the driver's serde impl as
/// extended-JSON (e.g. `{"$oid":"…"}`, `{"$date":{"$numberLong":"…"}}`)
/// while `Binary` serializes as a plain byte array — the exact shapes are
/// locked by `special_type_output_shape_locked_by_test`; treat that test as
/// the contract, not this doc comment.
pub fn doc_to_json(doc: &Document) -> Result<serde_json::Value, String> {
    let bson = to_bson(doc).map_err(|err| {
        let msg = crate::i18n::t("文档无法转换为 BSON 值", "document could not be converted to a BSON value");
        format!("{msg}: {err}")
    })?;
    if contains_non_finite(&bson) {
        return Err(crate::i18n::t("结果含非有限浮点值", "result contains a non-finite floating-point value").to_string());
    }
    serde_json::to_value(&bson).map_err(|err| {
        let msg = crate::i18n::t("文档无法转换为 JSON", "document could not be converted to JSON");
        format!("{msg}: {err}")
    })
}

/// Serializes a list of BSON documents into a JSON array, propagating the
/// first serialization error (e.g. a NaN in any document).
pub fn docs_to_json_array(docs: &[Document]) -> Result<serde_json::Value, String> {
    docs.iter()
        .map(doc_to_json)
        .collect::<Result<Vec<_>, _>>()
        .map(serde_json::Value::Array)
}

/// True when the BSON value contains a non-finite double anywhere (NaN,
/// ±Infinity), recursively through documents and arrays.
fn contains_non_finite(bson: &Bson) -> bool {
    match bson {
        Bson::Double(value) => !value.is_finite(),
        Bson::Document(doc) => doc.values().any(contains_non_finite),
        Bson::Array(items) => items.iter().any(contains_non_finite),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::doc;

    /// The same filter expressed as a JSON-text string and as a JSON object
    /// must parse to the same BSON document.
    ///
    /// Both configs are parsed from JSON text (the production path — the
    /// host delivers configs through the JSON-RPC wire): with the `json!`
    /// macro the object form would carry `Int32(18)` while text parsing
    /// yields `Int64(18)` (serde_json Number width nuance — locked in
    /// learnings; the driver treats both as numerically equal).
    #[test]
    fn string_and_object_forms_equivalent() {
        let as_string = parse_json_field(
            &serde_json::json!({ "filter": r#"{"age": {"$gt": 18}}"# }),
            "filter",
        )
        .expect("string form must parse");
        let as_object = parse_json_field(
            &serde_json::from_str::<serde_json::Value>(r#"{ "filter": {"age": {"$gt": 18}} }"#)
                .expect("config JSON must parse"),
            "filter",
        )
        .expect("object form must parse");
        assert_eq!(as_string, as_object);
        assert_eq!(as_string, Some(doc! { "age": { "$gt": Bson::Int64(18) } }));
    }

    /// Integers beyond f64's exact range and the u64/i64 boundary must
    /// survive parse → doc → doc_to_json without precision loss.
    #[test]
    fn numeric_fidelity() {
        // 2^53+1 is not representable as f64; i64::MAX is the largest i64.
        let ints = [
            9007199254740993i64,
            -9007199254740993i64,
            i64::MAX,
            i64::MIN,
        ];
        for &n in &ints {
            let expected = serde_json::json!({ "n": n });
            let doc = parse_json_field(&serde_json::json!({ "filter": expected }), "filter")
                .expect("i64 must parse")
                .expect("field present");
            assert_eq!(
                doc_to_json(&doc).expect("i64 roundtrip"),
                expected,
                "i64 {n} lost precision through BSON"
            );
        }
        // u64 values up to i64::MAX round-trip; the driver refuses larger
        // ones (bson 2.15 `serialize_u64` → UnsignedIntegerExceededRange) —
        // locked so a driver upgrade silently changing this is caught.
        let u64_boundary = 9223372036854775807u64;
        let expected = serde_json::json!({ "n": u64_boundary });
        let doc = parse_json_field(&serde_json::json!({ "filter": expected }), "filter")
            .expect("boundary u64 must parse")
            .expect("field present");
        assert_eq!(
            doc_to_json(&doc).expect("boundary u64 roundtrip"),
            expected,
            "u64 {u64_boundary} lost precision through BSON"
        );
        let beyond = parse_json_field(
            &serde_json::json!({ "filter": {"n": 18446744073709551615u64} }),
            "filter",
        );
        assert!(
            beyond.is_err(),
            "u64::MAX cannot fit BSON i64 and must be rejected by the driver"
        );
    }

    /// Locks the ACTUAL JSON shape the driver's serde impl produces for
    /// special BSON types — the probe run's real output is the contract:
    /// `{"oid":{"$oid":"<24-hex>"},"date":{"$date":{"$numberLong":"0"}},"bin":[1,2,3]}`
    /// (observed on mongodb 3.8.0 / bson 2.15.0 / serde_json 1.0.151).
    ///
    /// Surprises locked here (plan decision D7 — 以实测值锁定):
    /// - `ObjectId` → extended JSON `{"$oid": hex}` (NOT a bare hex string).
    /// - `DateTime` → extended JSON `{"$date":{"$numberLong":"0"}}` with the
    ///   millis as a JSON **string** (canonical extJSON, not a bare int).
    /// - `Binary` → a plain byte ARRAY `[1,2,3]` (NOT `{"$binary":…}`
    ///   extJSON — the driver's serde emits the raw bytes).
    /// If a dependency upgrade changes these, this test fails on purpose.
    #[test]
    fn special_type_output_shape_locked_by_test() {
        use mongodb::bson::{oid::ObjectId, spec::BinarySubtype, Binary, DateTime};

        let doc = doc! {
            "oid": ObjectId::new(),
            "date": DateTime::from_millis(0),
            "bin": Binary { subtype: BinarySubtype::Generic, bytes: vec![1, 2, 3] },
        };
        let json = doc_to_json(&doc).expect("special-type doc must serialize");
        let oid = doc.get_object_id("oid").expect("oid field");
        let expected = serde_json::json!({
            "oid": { "$oid": oid.to_hex() },
            "date": { "$date": { "$numberLong": "0" } },
            "bin": [1, 2, 3],
        });
        assert_eq!(json, expected, "driver special-type serde shape changed");
    }

    /// Unparseable JSON text must yield a readable Err, not a panic.
    #[test]
    fn invalid_json_string_errors() {
        let err = parse_json_field(&serde_json::json!({ "filter": "not json" }), "filter")
            .expect_err("invalid JSON text must error");
        assert!(
            err.contains("JSON") && err.contains("filter"),
            "error must be readable and name the field, got: {err}"
        );
        assert!(
            err.contains("not valid JSON"),
            "error must be the documented message, got: {err}"
        );
    }

    /// A NaN stored in a result document must produce a readable Err, never
    /// a panic and never silent corruption (plan decision D7).
    ///
    /// Observed on serde_json 1.0.151: `Value::from(NaN)` silently yields
    /// JSON `null` (to_value does NOT fail), so `doc_to_json` rejects
    /// non-finite doubles via [`contains_non_finite`] before serializing.
    #[test]
    fn nan_document_errors_not_panics() {
        let doc = doc! { "v": f64::NAN };
        let err = doc_to_json(&doc).expect_err("NaN must not serialize to JSON");
        assert!(
            err.contains("non-finite"),
            "error must name the non-finite cause, got: {err}"
        );
        // ±Infinity take the same path.
        assert!(doc_to_json(&doc! { "v": f64::INFINITY }).is_err());
        assert!(doc_to_json(&doc! { "v": f64::NEG_INFINITY }).is_err());
        // Non-finite values nested in documents/arrays are caught too.
        let nested = doc! { "nested": { "vals": [1.5, f64::NAN] } };
        assert!(doc_to_json(&nested).is_err());
    }

    /// The same pipeline/document list as JSON text and as a JSON array
    /// value must parse to the same Vec<Document> (both configs parsed from
    /// JSON text — the production wire shape; see the width nuance note on
    /// `string_and_object_forms_equivalent`).
    #[test]
    fn array_field_parses_string_and_array() {
        let as_string = parse_json_array_field(
            &serde_json::json!({ "pipeline": r#"[{"name": "a"}, {"$match": {"age": {"$gt": 18}}}]"# }),
            "pipeline",
        )
        .expect("string form must parse");
        let as_array = parse_json_array_field(
            &serde_json::from_str::<serde_json::Value>(
                r#"{ "pipeline": [{"name": "a"}, {"$match": {"age": {"$gt": 18}}}] }"#,
            )
            .expect("config JSON must parse"),
            "pipeline",
        )
        .expect("array form must parse");
        assert_eq!(as_string, as_array);
        assert_eq!(
            as_array,
            Some(vec![
                doc! { "name": "a" },
                doc! { "$match": { "age": { "$gt": Bson::Int64(18) } } },
            ])
        );
    }

    /// A numeric-safe document survives parse → doc → doc_to_json with the
    /// JSON unchanged.
    #[test]
    fn roundtrip() {
        let original = serde_json::json!({
            "s": "hello",
            "i": 42,
            "f": 3.5,
            "b": true,
            "nested": { "x": 1 },
            "arr": [1, 2, 3],
            "nil": null,
        });
        let doc = parse_json_field(&serde_json::json!({ "filter": original }), "filter")
            .expect("document must parse")
            .expect("field present");
        assert_eq!(doc_to_json(&doc).expect("roundtrip"), original);
    }

    /// null / missing / empty-string fields are optional: both parse helpers
    /// return Ok(None). Empty string is the GUI's "cleared input box" shape —
    /// the host config panel stores `""` when the user deletes the value.
    #[test]
    fn null_missing_and_empty_string_are_none() {
        for config in [
            serde_json::json!({}),
            serde_json::json!({ "filter": null }),
            serde_json::json!({ "pipeline": null }),
            serde_json::json!({ "filter": "", "pipeline": "" }),
            serde_json::json!({ "filter": "   ", "pipeline": " \t " }),
        ] {
            assert_eq!(parse_json_field(&config, "filter").expect("no error"), None);
            assert_eq!(
                parse_json_array_field(&config, "pipeline").expect("no error"),
                None
            );
        }
    }

    /// Non-object array elements are rejected with a readable message.
    #[test]
    fn array_element_must_be_object() {
        let err = parse_json_array_field(
            &serde_json::json!({ "documents": [{"name": "a"}, 42, "text"] }),
            "documents",
        )
        .expect_err("non-object element must error");
        assert!(
            err.contains("documents") && err.contains("object"),
            "error must name the field and the element problem, got: {err}"
        );
        // A string that parses to a non-array is rejected too.
        let err = parse_json_array_field(
            &serde_json::json!({ "pipeline": r#"{"$match": {}}"# }),
            "pipeline",
        )
        .expect_err("non-array JSON text must error");
        assert!(err.contains("JSON array"), "got: {err}");
    }

    // ========================================================================
    // Extended-JSON input (v2): $oid / $date / $numberLong / $numberInt /
    // $numberDouble 类型包装识别，以及查询操作符原样保留。
    // ========================================================================

    /// `{"$oid": "<24-hex>"}` 在 filter 顶层 → BSON ObjectId，驱动 roundtrip
    /// 输出 canonical extended-JSON `{"$oid": ...}`。这是修复"按 _id 查不到"
    /// 的核心路径：filter 写 `{"_id": {"$oid": "..."}}` 即可匹配 ObjectId。
    #[test]
    fn oid_wrapper_matches_object_id() {
        let hex = "6a797a7542ec4eacbd05bcb2";
        let doc = parse_json_field(
            &serde_json::json!({ "filter": format!(r#"{{"_id": {{"$oid": "{hex}"}}}}"#) }),
            "filter",
        )
        .expect("oid wrapper must parse")
        .expect("field present");
        let oid = doc.get_object_id("_id").expect("_id must be an ObjectId");
        assert_eq!(oid.to_hex(), hex);

        // 与驱动输出形状互逆：doc_to_json 的 extended-JSON 能再喂回 parse。
        let json = doc_to_json(&doc).expect("doc must serialize");
        assert_eq!(json["_id"]["$oid"], hex);
    }

    /// ObjectId 与普通字符串必须区分——这是最初 bug 的根源。filter 里
    /// `_id` 写纯字符串仍是 String，不会匹配 ObjectId 字段。
    #[test]
    fn plain_string_oid_is_not_object_id() {
        let doc = parse_json_field(
            &serde_json::json!({ "filter": r#"{"_id": "6a797a7542ec4eacbd05bcb2"}"# }),
            "filter",
        )
        .expect("plain string must parse")
        .expect("field present");
        assert!(
            doc.get("_id").is_some(),
            "string _id must remain a plain string"
        );
        assert_eq!(doc.get_str("_id").expect("must be string"), "6a797a7542ec4eacbd05bcb2");
    }

    /// 非法 ObjectId hex → 可读错误，指明 $oid 字段。
    #[test]
    fn invalid_oid_errors() {
        for bad in ["6a797a75", "zzzzzzzzzzzzzzzzzzzzzzzz", ""] {
            let err = parse_json_field(
                &serde_json::json!({ "filter": format!(r#"{{"_id": {{"$oid": "{bad}"}}}}"#) }),
                "filter",
            )
            .expect_err("invalid $oid must error");
            assert!(
                err.contains("$oid") && err.contains("ObjectId"),
                "error must name $oid and ObjectId, got: {err}"
            );
        }
    }

    /// `$date` 支持 ISO-8601 字符串与 canonical `{"$numberLong": "<ms>"}` 两种
    /// 形态，roundtrip 输出 canonical 形态。
    #[test]
    fn date_wrapper_accepts_iso_and_millis() {
        let iso = parse_json_field(
            &serde_json::json!({ "filter": r#"{"ts": {"$date": "2026-08-10T00:00:00Z"}}"# }),
            "filter",
        )
        .expect("ISO date must parse")
        .expect("field present");
        let ts = iso.get_datetime("ts").expect("must be DateTime");
        // ISO 与对应 millis 的 $numberLong 形态必须解析为同一时刻。
        let millis = ts.timestamp_millis();
        let millis_form = parse_json_field(
            &serde_json::json!({ "filter": format!(r#"{{"ts": {{"$date": {{"$numberLong": "{millis}"}}}}}}"#) }),
            "filter",
        )
        .expect("millis date must parse")
        .expect("field present");
        assert_eq!(iso, millis_form, "ISO-8601 and millis forms must be equivalent");
        // roundtrip 形状与驱动输出锁定一致。
        let json = doc_to_json(&iso).expect("doc must serialize");
        assert_eq!(json["ts"], serde_json::json!({ "$date": { "$numberLong": millis.to_string() } }));
    }

    /// 非法 `$date` → 可读错误。
    #[test]
    fn invalid_date_errors() {
        let err = parse_json_field(
            &serde_json::json!({ "filter": r#"{"ts": {"$date": "not-a-date"}}"# }),
            "filter",
        )
        .expect_err("invalid $date must error");
        assert!(err.contains("$date"), "error must name $date, got: {err}");

        let err = parse_json_field(
            &serde_json::json!({ "filter": r#"{"ts": {"$date": {"$numberLong": "abc"}}}"# }),
            "filter",
        )
        .expect_err("invalid $numberLong must error");
        assert!(err.contains("$numberLong"), "error must name $numberLong, got: {err}");
    }

    /// canonical `$numberLong`（值必须是字符串）→ BSON Int64，大整数保精度
    /// 不经过 f64 中间态。
    #[test]
    fn number_long_wrapper_preserves_precision() {
        let big = i64::MAX;
        let doc = parse_json_field(
            &serde_json::json!({ "filter": format!(r#"{{"n": {{"$numberLong": "{big}"}}}}"#) }),
            "filter",
        )
        .expect("numberLong must parse")
        .expect("field present");
        assert_eq!(doc.get_i64("n").expect("must be Int64"), big);

        // 超 i64 范围 → 可读错误。
        let err = parse_json_field(
            &serde_json::json!({ "filter": r#"{"n": {"$numberLong": "9223372036854775808"}}"# }),
            "filter",
        )
        .expect_err("overflowing numberLong must error");
        assert!(err.contains("$numberLong"), "error must name $numberLong, got: {err}");
    }

    /// `$numberInt` / `$numberDouble` 同样被识别。
    #[test]
    fn number_int_and_double_wrappers() {
        let doc = parse_json_field(
            &serde_json::json!({ "filter": r#"{"i": {"$numberInt": "42"}, "f": {"$numberDouble": "3.5"}}"# }),
            "filter",
        )
        .expect("number wrappers must parse")
        .expect("field present");
        assert_eq!(doc.get_i32("i").expect("Int32"), 42);
        assert_eq!(doc.get_f64("f").expect("Double"), 3.5);
    }

    /// 查询操作符（`$gt` / `$in` / `$match` / `$expr`）必须原样保留——filter
    /// 里它们是最常见的内容，绝不能因 extended-JSON 识别而误转换。
    #[test]
    fn query_operators_pass_through_untouched() {
        let doc = parse_json_field(
            &serde_json::json!({ "filter": r#"{"age": {"$gt": 18}, "name": {"$in": ["a", "b"]}}"# }),
            "filter",
        )
        .expect("operators must parse")
        .expect("field present");
        assert_eq!(
            doc.get_document("age").expect("age is a doc").get_i64("$gt").expect("$gt i64"),
            18
        );
        // $in 数组仍是数组，不是单键包装。
        let age = doc.get_document("age").expect("age is a doc");
        assert!(age.get("$gt").is_some(), "$gt key preserved");
        let name = doc.get_document("name").expect("name is a doc");
        assert!(name.get("$in").is_some(), "$in key preserved");
    }

    /// 嵌套深度：数组元素 / 多级对象内的 $oid 也转换（如 aggregate pipeline
    /// 的 $match 里按 _id 过滤）。
    #[test]
    fn oid_nested_in_arrays_and_deep_objects() {
        let pipeline = parse_json_array_field(
            &serde_json::json!({
                "pipeline": r#"[{"$match": {"_id": {"$oid": "6a797a7542ec4eacbd05bcb2"}}}]"#
            }),
            "pipeline",
        )
        .expect("pipeline must parse")
        .expect("field present");
        let stage = pipeline.first().expect("one stage");
        let matched = stage.get_document("$match").expect("match doc");
        assert!(
            matched.get_object_id("_id").is_ok(),
            "nested $oid must become ObjectId"
        );
    }
}
