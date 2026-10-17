//! The index config `adapter.parse_index` returns to the Postgres and DuckDB
//! `get_create_index_sql` macros.
//!
//! Ports dbt-postgres's `PostgresIndexConfig` and dbt-duckdb's
//! `DuckDBIndexConfig`: the macros read `columns`, `unique` and `type`, and call
//! `render(relation)` for the index name.

use std::rc::Rc;
use std::sync::Arc;

use dbt_adapter_core::AdapterType;
use minijinja::listener::RenderingEventListener;
use minijinja::value::{Enumerator, Object, ValueKind};
use minijinja::{Error, ErrorKind, State, Value};

use crate::cast_util::downcast_value_to_dyn_base_relation;
use crate::value::none_value;

/// One parsed entry of a model's `indexes` config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexConfig {
    columns: Vec<String>,
    unique: bool,
    index_type: Option<String>,
    /// Whether `type` is part of the name hash. dbt-postgres hashes it,
    /// dbt-duckdb (which has no index types) does not.
    hash_type: bool,
}

impl IndexConfig {
    /// Parses one raw `indexes` entry. `None` (and undefined) parse to `None`,
    /// as in dbt-postgres.
    pub fn parse(adapter_type: AdapterType, raw_index: &Value) -> Result<Option<Self>, Error> {
        if raw_index.is_none() || raw_index.is_undefined() {
            return Ok(None);
        }
        if raw_index.kind() != ValueKind::Map {
            return Err(Error::new(
                ErrorKind::InvalidOperation,
                format!(
                    "Invalid index config:\n  Got: {raw_index}\n  Expected a dictionary with at minimum a \"columns\" key"
                ),
            ));
        }
        let invalid = |detail: String| {
            Error::new(
                ErrorKind::InvalidOperation,
                format!("Could not parse index config: {detail}"),
            )
        };
        let field = |name: &str| -> Option<Value> {
            raw_index
                .get_item(&Value::from(name))
                .ok()
                .filter(|v| !v.is_undefined() && !v.is_none())
        };

        let columns = match field("columns") {
            Some(value) if value.kind() == ValueKind::Seq => value
                .try_iter()?
                .map(|column| {
                    column
                        .as_str()
                        .map(str::to_string)
                        .ok_or_else(|| invalid(format!("{column} in `columns` is not a string")))
                })
                .collect::<Result<Vec<_>, _>>()?,
            Some(value) => return Err(invalid(format!("`columns` must be a list, got {value}"))),
            None => return Err(invalid("'columns' is a required property".to_string())),
        };
        let unique = match field("unique") {
            Some(value) if value.kind() == ValueKind::Bool => value.is_true(),
            Some(value) => return Err(invalid(format!("`unique` must be a boolean, got {value}"))),
            None => false,
        };
        let hash_type = adapter_type != AdapterType::DuckDB;
        let index_type = match field("type") {
            Some(value) if hash_type => match value.as_str() {
                Some(index_type) => Some(index_type.to_string()),
                None => return Err(invalid(format!("`type` must be a string, got {value}"))),
            },
            _ => None,
        };

        Ok(Some(Self {
            columns,
            unique,
            index_type,
            hash_type,
        }))
    }

    /// The indexed columns, in config order.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Whether the index is unique.
    pub fn unique(&self) -> bool {
        self.unique
    }

    /// The index method (`type`), if the config names one.
    pub fn index_type(&self) -> Option<&str> {
        self.index_type.as_deref()
    }

    /// The index name: an md5 of the columns, the rendered relation, the flags
    /// and the current time. The timestamp keeps the name from colliding with
    /// the index of the relation being replaced
    /// (https://github.com/dbt-labs/dbt-core/issues/1945#issuecomment-576714925).
    fn name(&self, relation: &str, now: &str) -> String {
        let mut inputs = self.columns.clone();
        inputs.push(relation.to_string());
        inputs.push(python_bool(self.unique).to_string());
        if self.hash_type {
            inputs.push(self.index_type.as_deref().unwrap_or("None").to_string());
        }
        inputs.push(now.to_string());
        format!("{:x}", md5::compute(inputs.join("_")))
    }
}

fn python_bool(value: bool) -> &'static str {
    if value { "True" } else { "False" }
}

impl Object for IndexConfig {
    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str()? {
            "columns" => Some(Value::from(self.columns.clone())),
            "unique" => Some(Value::from(self.unique)),
            "type" => Some(
                self.index_type
                    .as_ref()
                    .map_or_else(none_value, |t| Value::from(t.as_str())),
            ),
            _ => None,
        }
    }

    fn enumerate(self: &Arc<Self>) -> Enumerator {
        Enumerator::Str(&["columns", "unique", "type"])
    }

    fn call_method(
        self: &Arc<Self>,
        _state: &State<'_, '_>,
        method: &str,
        args: &[Value],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, Error> {
        match method {
            "render" => {
                let [relation] = args else {
                    return Err(Error::new(
                        ErrorKind::MissingArgument,
                        "render() takes exactly one argument, the relation",
                    ));
                };
                let relation = downcast_value_to_dyn_base_relation(relation)?;
                let now = chrono::Utc::now()
                    .naive_utc()
                    .format("%Y-%m-%dT%H:%M:%S%.6f")
                    .to_string();
                Ok(Value::from(self.name(&relation.render_self_as_str(), &now)))
            }
            _ => Err(Error::new(
                ErrorKind::UnknownMethod,
                format!("Unknown method on IndexConfig: '{method}'"),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(adapter_type: AdapterType, yaml: &str) -> Result<Option<IndexConfig>, Error> {
        let value: serde_json::Value = dbt_yaml::from_str(yaml).expect("yaml");
        IndexConfig::parse(adapter_type, &Value::from_serialize(&value))
    }

    #[test]
    fn parses_every_field() {
        let config = parse(
            AdapterType::Postgres,
            "{columns: [id, created_at], unique: true, type: btree}",
        )
        .unwrap()
        .unwrap();
        assert_eq!(config.columns, ["id", "created_at"]);
        assert!(config.unique);
        assert_eq!(config.index_type.as_deref(), Some("btree"));
    }

    #[test]
    fn unique_and_type_default() {
        let config = parse(AdapterType::Postgres, "{columns: [id]}")
            .unwrap()
            .unwrap();
        assert!(!config.unique);
        assert_eq!(config.index_type, None);
    }

    #[test]
    fn none_parses_to_none() {
        assert_eq!(
            IndexConfig::parse(AdapterType::Postgres, &none_value()).unwrap(),
            None
        );
    }

    #[test]
    fn rejects_malformed_entries() {
        let not_a_dict = parse(AdapterType::Postgres, "[id]").unwrap_err();
        assert!(
            not_a_dict.to_string().contains("Expected a dictionary"),
            "{not_a_dict}"
        );
        for (yaml, expected) in [
            ("{unique: true}", "'columns' is a required property"),
            ("{columns: id}", "`columns` must be a list"),
            ("{columns: [1]}", "is not a string"),
            (
                "{columns: [id], unique: 'yes'}",
                "`unique` must be a boolean",
            ),
            ("{columns: [id], type: 3}", "`type` must be a string"),
        ] {
            let error = parse(AdapterType::Postgres, yaml).unwrap_err();
            assert!(error.to_string().contains(expected), "{yaml}: {error}");
        }
    }

    #[test]
    fn name_matches_dbt_postgres() {
        // md5("id_created_at_\"s\".\"t\"_True_btree_2026-10-17T00:00:00.000000"),
        // the string dbt-postgres's PostgresIndexConfig.render hashes.
        let config = parse(
            AdapterType::Postgres,
            "{columns: [id, created_at], unique: true, type: btree}",
        )
        .unwrap()
        .unwrap();
        let expected = format!(
            "{:x}",
            md5::compute("id_created_at_\"s\".\"t\"_True_btree_2026-10-17T00:00:00.000000")
        );
        assert_eq!(
            config.name("\"s\".\"t\"", "2026-10-17T00:00:00.000000"),
            expected
        );

        let untyped = parse(AdapterType::Postgres, "{columns: [id]}")
            .unwrap()
            .unwrap();
        let expected = format!("{:x}", md5::compute("id_t_False_None_now"));
        assert_eq!(untyped.name("t", "now"), expected);
    }

    #[test]
    fn duckdb_ignores_type() {
        let config = parse(AdapterType::DuckDB, "{columns: [id], type: btree}")
            .unwrap()
            .unwrap();
        assert_eq!(config.index_type, None);
        let expected = format!("{:x}", md5::compute("id_t_False_now"));
        assert_eq!(config.name("t", "now"), expected);
    }
}
