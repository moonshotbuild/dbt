//! The change set a Postgres materialized view needs to match its model.
//!
//! Ports dbt-postgres's
//! `PostgresRelation.get_materialized_view_config_change_collection`
//! (https://github.com/dbt-labs/dbt-adapters/blob/802d40f0f0eb663c17e5be08d1e3fb2e55b9ee34/dbt-postgres/src/dbt/adapters/postgres/relation.py).
//! The only setting a Postgres materialized view can change in place is its
//! indexes, so the change set is an index diff: an index present on the view
//! but not in the model's `indexes` config is dropped, and one in the config
//! but not on the view is created. Indexes compare by their columns (as a
//! set), uniqueness and method, never by name, which is generated.

use std::collections::BTreeSet;

use dbt_adapter_core::AdapterType;
use dbt_agate::AgateTable;
use minijinja::{Error, ErrorKind, Value};
use serde_json::json;

use crate::adapter::index_config::IndexConfig;
use crate::value::none_value;

/// The method Postgres uses when an index config names none.
const DEFAULT_METHOD: &str = "btree";

/// What makes two indexes the same index.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct IndexKey {
    columns: BTreeSet<String>,
    unique: bool,
    method: String,
}

/// An index on the existing view, from `postgres__get_show_indexes_sql`.
#[derive(Debug, Clone)]
struct ExistingIndex {
    name: String,
    key: IndexKey,
}

/// An index the model's `indexes` config asks for, columns in config order.
#[derive(Debug, Clone)]
struct DesiredIndex {
    columns: Vec<String>,
    key: IndexKey,
}

/// Builds the change set for `postgres__get_alter_materialized_view_as_sql`
/// from the `postgres__describe_materialized_view` results (`{'indexes':
/// <table>}`) and the model node. Returns none when nothing changed, which
/// tells the materialization to refresh the view instead.
pub fn changeset(remote_state: &Value, model: &Value) -> Result<Value, Error> {
    let existing = existing_indexes(remote_state)?;
    let desired = desired_indexes(model)?;

    let mut changes = Vec::new();
    for index in &existing {
        if !desired.iter().any(|d| d.key == index.key) {
            changes.push(json!({
                "action": "drop",
                "context": context(&index.name, &sorted(&index.key.columns), &index.key),
            }));
        }
    }
    for index in &desired {
        if !existing.iter().any(|e| e.key == index.key) {
            changes.push(json!({
                "action": "create",
                "context": context("", &index.columns, &index.key),
            }));
        }
    }

    if changes.is_empty() {
        return Ok(none_value());
    }
    Ok(Value::from_serialize(json!({
        // Index changes apply in place, so none needs a full refresh.
        "requires_full_refresh": false,
        "indexes": changes,
    })))
}

/// The `context` of a change: the index, plus `as_node_config`, the
/// `indexes` entry `postgres__get_create_index_sql` takes.
fn context(name: &str, columns: &[String], key: &IndexKey) -> serde_json::Value {
    json!({
        "name": name,
        "column_names": columns,
        "unique": key.unique,
        "method": key.method,
        "as_node_config": {
            "columns": columns,
            "unique": key.unique,
            "type": key.method,
        },
    })
}

fn sorted(columns: &BTreeSet<String>) -> Vec<String> {
    columns.iter().cloned().collect()
}

fn invalid(message: String) -> Error {
    Error::new(ErrorKind::InvalidOperation, message)
}

fn existing_indexes(remote_state: &Value) -> Result<Vec<ExistingIndex>, Error> {
    let table = remote_state
        .get_item(&Value::from("indexes"))
        .ok()
        .and_then(|v| v.downcast_object::<AgateTable>())
        .ok_or_else(|| {
            invalid("expected the describe results to hold an `indexes` table".to_string())
        })?;
    let mut indexes = Vec::new();
    for row in table.rows() {
        let text = |column: &str| -> Result<String, Error> {
            row.get_attr(column)
                .ok()
                .and_then(|v| v.as_str().map(str::to_string))
                .ok_or_else(|| invalid(format!("index row has no `{column}` text")))
        };
        let unique = row.get_attr("unique").map(|v| v.is_true()).unwrap_or(false);
        indexes.push(ExistingIndex {
            name: text("name")?,
            key: IndexKey {
                columns: text("column_names")?
                    .split(',')
                    .filter(|c| !c.is_empty())
                    .map(str::to_string)
                    .collect(),
                unique,
                method: text("method")?.to_lowercase(),
            },
        });
    }
    Ok(indexes)
}

fn desired_indexes(model: &Value) -> Result<Vec<DesiredIndex>, Error> {
    let indexes = model
        .get_attr("config")
        .and_then(|config| config.get_attr("indexes"))
        .unwrap_or_default();
    if indexes.is_none() || indexes.is_undefined() {
        return Ok(Vec::new());
    }
    let mut desired = Vec::new();
    for raw in indexes.try_iter()? {
        let Some(config) = IndexConfig::parse(AdapterType::Postgres, &raw)? else {
            continue;
        };
        desired.push(DesiredIndex {
            columns: config.columns().to_vec(),
            key: IndexKey {
                columns: config.columns().iter().cloned().collect(),
                unique: config.unique(),
                method: config.index_type().unwrap_or(DEFAULT_METHOD).to_lowercase(),
            },
        });
    }
    Ok(desired)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{BooleanArray, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    /// Describe results as `postgres__describe_materialized_view` returns
    /// them: (name, method, unique, column_names) per index.
    fn remote(rows: &[(&str, &str, bool, &str)]) -> Value {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("method", DataType::Utf8, false),
            Field::new("unique", DataType::Boolean, false),
            Field::new("column_names", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
                Arc::new(BooleanArray::from_iter(rows.iter().map(|r| Some(r.2)))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.3))),
            ],
        )
        .unwrap();
        let table = AgateTable::from_record_batch(Arc::new(batch));
        Value::from(std::collections::BTreeMap::from([(
            "indexes".to_string(),
            Value::from_object(table),
        )]))
    }

    fn model(indexes: serde_json::Value) -> Value {
        Value::from_serialize(json!({ "config": { "indexes": indexes } }))
    }

    fn changes(result: &Value) -> Vec<(String, serde_json::Value)> {
        let json: serde_json::Value = serde_json::to_value(result).unwrap();
        json["indexes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| {
                (
                    c["action"].as_str().unwrap().to_string(),
                    c["context"].clone(),
                )
            })
            .collect()
    }

    #[test]
    fn matching_indexes_mean_no_changes() {
        let result = changeset(
            &remote(&[("ix1", "btree", true, "id"), ("ix2", "hash", false, "b,a")]),
            &model(json!([
                {"columns": ["id"], "unique": true},
                {"columns": ["a", "b"], "type": "HASH"},
            ])),
        )
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn no_indexes_on_either_side_mean_no_changes() {
        assert!(
            changeset(&remote(&[]), &model(json!(null)))
                .unwrap()
                .is_none()
        );
        assert!(
            changeset(&remote(&[]), &Value::from_serialize(json!({"config": {}})))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn stale_indexes_drop_and_new_ones_create() {
        let result = changeset(
            &remote(&[
                ("ix_old", "btree", false, "id"),
                ("ix_keep", "btree", false, "b"),
            ]),
            &model(json!([
                {"columns": ["b"]},
                {"columns": ["id"], "unique": true},
            ])),
        )
        .unwrap();
        assert_eq!(
            result.get_attr("requires_full_refresh").unwrap(),
            Value::from(false)
        );
        let changes = changes(&result);
        assert_eq!(changes.len(), 2, "{changes:?}");
        assert_eq!(changes[0].0, "drop");
        assert_eq!(changes[0].1["name"], "ix_old");
        assert_eq!(changes[1].0, "create");
        assert_eq!(
            changes[1].1["as_node_config"],
            json!({"columns": ["id"], "unique": true, "type": "btree"})
        );
    }
}
